"""
incremental update of CVR virksomhed data, writing to an S3 object storage. this is the "S3 version" of update_data_virksomhed_api_call.py.

i only tested it with Garage (https://garagehq.deuxfleurs.fr/).

it refreshes the founding-year partitions already in the bucket: for each year it fetches only the
companies whose record changed since that year was last updated and upserts them into
the per-year table objects (old rows dropped, fresh rows appended), so only the
objects that actually changed get rewritten. new years are not created here.

<prefix>/_state.json holds a per-founding-year "last update" timestamp. each year resumes from its own watermark, so updating one year never advances another and can never leave a gap.

a safety lookback buffer of 1 day re-scans a little overlap so replication lag never creates a gap in the data.

config (endpoint, bucket, prefix, credentials) comes from .env -> see .env.example.

Args:

- by default every founding year on disk is refreshed.
- --start-year and --end-year to refresh only those year set.
- --year-unknown to refresh only the companies with no founding year date.
- --since to override the start date for the selected years.
- --buffer-days to change the default time lookback buffer.

Examples:
    python update_data_virksomhed_s3_api_call.py
    python update_data_virksomhed_s3_api_call.py --since 2026-06-01
    python update_data_virksomhed_s3_api_call.py --buffer-days 2
    python update_data_virksomhed_s3_api_call.py --bucket cvr-data --prefix virksomhed
    python update_data_virksomhed_s3_api_call.py --start-year 1980 --end-year 1980
    python update_data_virksomhed_s3_api_call.py --year-unknown
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
import utils_virksomhed_s3 as s3


def run(client, bucket, prefix, since, buffer_days, start_year, end_year, year_unknown):
    year_ts = s3.read_year_timestamps(client, bucket, prefix)
    if not year_ts:
        raise SystemExit(
            "No previous run found in the bucket. Upload the historical data and "
            "its _state.json first, or pass --since YYYY-MM-DD."
        )

    # The existing founding-year partitions in scope for this run. Update only
    # refreshes years already in the bucket; new years come from get_historical.
    selected = utils.select_years(sorted(year_ts), start_year, end_year, year_unknown)
    if not selected:
        raise SystemExit("No matching founding years in the bucket for the requested scope.")

    # Fetch from the oldest watermark among the selected years (a year that lagged
    # behind is still caught), minus the lookback buffer. Overlap is harmless since
    # the upsert is idempotent. --since overrides the watermark for every year.
    if since:
        since_dt = datetime.fromisoformat(since)
    else:
        since_dt = min(datetime.fromisoformat(year_ts[y]) for y in selected)
    since_dt -= timedelta(days=buffer_days)
    since_iso = since_dt.date().isoformat()
    print(f"Updating {len(selected)} founding year(s) changed since {since_iso} "
          f"(buffer {buffer_days}d)")

    # Record the start time now and persist it only after a successful upsert.
    run_started = datetime.now(timezone.utc).isoformat()

    # Bound the scan server-side to the selected founding-year span.
    numeric = [int(y) for y in selected if y != "unknown"]
    query = utils.query_updated_since(
        since_iso,
        start_year=min(numeric) if numeric else None,
        end_year=max(numeric) if numeric else None,
        include_unknown="unknown" in selected,
    )
    all_hits = utils.fetch_query(query)
    print(f"{len(all_hits)} changed companies")

    # Group by founding year, keeping only the selected, already-existing partitions.
    # Anything else (e.g. a new year inside the span we never backfilled) is left for
    # get_historical, not silently created here.
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
        s3.upsert_year(client, bucket, prefix, year, hits)

    # Advance the watermark for every selected year, including those with no
    # changes: we scanned their full window, so they are current as of run_started.
    for year in selected:
        year_ts[year] = run_started
    s3.write_year_timestamps(client, bucket, prefix, year_ts)
    print(f"Update complete. {len(selected)} year(s) now current as of {run_started}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Incrementally update CVR data in S3 (Garage) using sidstOpdateret."
    )
    parser.add_argument("--bucket", default=s3.S3_BUCKET,
                        help="S3 bucket. Default: S3_BUCKET from .env.")
    parser.add_argument("--prefix", default=s3.S3_VIRKSOMHED_PREFIX,
                        help="Key prefix for the virksomhed data. "
                             "Default: S3_VIRKSOMHED_PREFIX from .env.")
    parser.add_argument("--since",
                        help="Override start date (YYYY-MM-DD or ISO). "
                             "Default: last run from <prefix>/_state.json.")
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

    if not args.bucket:
        raise SystemExit("No bucket. Set S3_BUCKET in .env or pass --bucket.")

    if args.year_unknown and (args.start_year is not None or args.end_year is not None):
        parser.error("--year-unknown cannot be combined with --start-year/--end-year.")

    client = s3.make_client()
    run(client, args.bucket, args.prefix, args.since, args.buffer_days,
        args.start_year, args.end_year, args.year_unknown)
