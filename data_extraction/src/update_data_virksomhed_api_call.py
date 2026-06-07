"""
incremental update of CVR virksomhed data.

it refreshes the founding-year partitions already on disk: for each year it fetches
only the companies whose record changed since that year was last updated and upserts
them into the year files (old rows dropped, fresh rows appended), so only the files
that actually changed get rewritten. new years are never created here, only by
get_historical_data.py.

it adds a safety lookback buffer of 1 day so lags in the client or the server do not create gaps in the data.

_state.json holds a per-founding-year "last update" timestamp (watermark). each year
resumes from its own watermark, so updating one year never advances another and can
never leave a gap.

Examples:
    # by default every founding year on disk is refreshed.
    python update_data.py

    # --start-year and --end-year to refresh only those year set.
    python update_data.py --start-year 1980 --end-year 2005

    # --year-unknown to refresh only the companies with no founding year date.
    python update_data.py --year-unknown

    # --since to override the start date for the selected years, use with care
    python update_data.py --since 2026-06-01

    # --buffer-days to change the default time lookback buffer.
    python update_data.py --buffer-days 2
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

# utils/ lives at the project root, two levels up from this file (src/).
UTILS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "utils",
)
sys.path.insert(1, UTILS_FOLDER)

import utils_virksomhed as utils


def run(folder, since, buffer_days, start_year, end_year, year_unknown):
    year_ts = utils.read_year_timestamps(folder)
    if not year_ts:
        raise SystemExit(
            "No previous run found. Run get_historical_data.py first, "
            "or pass --since YYYY-MM-DD."
        )

    # The existing founding-year in scope for this run. Update only refreshes years already downloaded. New years come from get_historical().
    selected = utils.select_years(sorted(year_ts), start_year, end_year, year_unknown)
    if not selected:
        raise SystemExit("No matching founding years on disk for the requested scope.")

    # Fetch from the oldest watermark among the selected years minus the lookback buffer.
    if since:
        since_dt = datetime.fromisoformat(since)
    else:
        since_dt = min(datetime.fromisoformat(year_ts[y]) for y in selected)
    since_dt -= timedelta(days=buffer_days)
    since_iso = since_dt.date().isoformat()
    print(f"Updating {len(selected)} founding year(s) changed since {since_iso} "
          f"(buffer {buffer_days}d)")

    # Record the start time now and persist it only after a successful upsert
    run_started = datetime.now(timezone.utc).isoformat()

    # Only scan selected founding-year set
    numeric = [int(y) for y in selected if y != "unknown"]
    query = utils.query_updated_since(
        since_iso,
        start_year=min(numeric) if numeric else None,
        end_year=max(numeric) if numeric else None,
        include_unknown="unknown" in selected,
    )
    all_hits = utils.fetch_query(query)
    print(f"{len(all_hits)} changed companies")

    # Group by founding year, keeping only the selected, already-existing downloads
    selected_set = set(selected)
    hits_by_year = {}
    for hit in all_hits:
        year = utils.founding_year(hit)
        if year in selected_set:
            hits_by_year.setdefault(year, []).append(hit)

    skipped = len(all_hits) - sum(len(v) for v in hits_by_year.values())
    if skipped:
        print(f"Skipping {skipped} changed companies outside the selected partitions")

    for year in sorted(hits_by_year):
        hits = hits_by_year[year]
        print(f"Upserting year {year}: {len(hits)} companies")
        utils.upsert_year(folder, year, hits)

    # print for every year, not only changed
    for year in selected:
        year_ts[year] = run_started
    utils.write_year_timestamps(folder, year_ts)
    print(f"Update complete. {len(selected)} year(s) now current as of {run_started}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Incrementally update CVR data using sidstOpdateret."
    )
    parser.add_argument("--folder", default=utils.RAW_VIRKSOMHED_FOLDER_PATH)
    parser.add_argument("--since",
                        help="Override start date (YYYY-MM-DD or ISO). "
                             "Default: last run from _state.json.")
    parser.add_argument("--buffer-days", type=int, default=1,
                        help="Lookback overlap to absorb replication lag (default 1).")
    parser.add_argument("--start-year", type=int, default=None,
                        help="Only update companies founded in/after this year.")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Only update companies founded in/before this year (inclusive).")
    parser.add_argument("--year-unknown", action="store_true",
                        help="Only update companies with no founding date. "
                             "Cannot be combined with a year range.")
    args = parser.parse_args()

    if not args.folder:
        raise SystemExit("No data folder. Set RAW_VIRKSOMHED_FOLDER_PATH in .env or pass --folder.")

    if args.year_unknown and (args.start_year is not None or args.end_year is not None):
        parser.error("--year-unknown cannot be combined with --start-year/--end-year.")

    run(args.folder, args.since, args.buffer_days,
        args.start_year, args.end_year, args.year_unknown)
