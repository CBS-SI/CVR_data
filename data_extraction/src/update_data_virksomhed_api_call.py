"""
incremental update of CVR virksomhed data.

fetches only companies whose record changed since the last run and upserts them into the year files: old rows are dropped and new rows are appended.

therefore only yearly parquet files that actually changed get rewritten.

last run date comes from _state.json, written by get_historical_data.py and by
each successful update.

it adds a safety lookback buffer of 1 day so lags in the client or the server do not create gaps in the data.

it can also be run with a custom start date using the --since option or change the buffer size using the --buffer-days option.

Examples:
    python update_data.py
    python update_data.py --since 2026-06-01
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


def run(folder, since, buffer_days):
    last_run = since or utils.read_last_run(folder)
    if last_run is None:
        raise SystemExit(
            "No previous run found. Run get_historical_data.py first, "
            "or pass --since YYYY-MM-DD."
        )

    # Apply the lookback buffer so we re-scan a little overlap and never miss
    # an edit that landed late. Date granularity is enough for sidstOpdateret.
    since_dt = datetime.fromisoformat(last_run) - timedelta(days=buffer_days)
    since_iso = since_dt.date().isoformat()
    print(f"Fetching companies updated since {since_iso} (buffer {buffer_days}d)")

    # Record the start time now and persist it only after a successful upsert.
    run_started = datetime.now(timezone.utc).isoformat()

    query = utils.query_updated_since(since_iso)
    all_hits = utils.fetch_query(query)
    print(f"{len(all_hits)} changed companies")

    # this normally never happen, thats why the print message.
    if not all_hits:
        utils.write_last_run(folder, run_started)
        print("This is weird...Nothing to update.")
        return

    # Go one by one year upserting the files
    hits_by_year = {}
    for hit in all_hits:
        year = utils.founding_year(hit)
        hits_by_year.setdefault(year, []).append(hit)

    for year in sorted(hits_by_year):
        hits = hits_by_year[year]
        print(f"Upserting year {year}: {len(hits)} companies")
        utils.upsert_year(folder, year, hits)

    utils.write_last_run(folder, run_started)
    print(f"Update complete. Next run will start from {run_started}")


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
    args = parser.parse_args()

    if not args.folder:
        raise SystemExit("No data folder. Set RAW_VIRKSOMHED_FOLDER_PATH in .env or pass --folder.")

    run(args.folder, args.since, args.buffer_days)
