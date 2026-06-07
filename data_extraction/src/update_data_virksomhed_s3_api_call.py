"""
incremental update of CVR virksomhed data, writing to S3 (Garage) object storage.

this is the S3 counterpart of update_data_virksomhed_api_call.py. it fetches only
the companies whose record changed since the last run and upserts them into the
per-year table objects in the bucket: old rows are dropped and the fresh rows
appended, so only the year files that actually changed get rewritten.

last run date comes from <prefix>/_state.json in the bucket, written by each
successful update. a safety lookback buffer of 1 day re-scans a little overlap so
replication lag never creates a gap in the data.

config (endpoint, bucket, prefix, credentials) comes from .env; see .env.example.

Examples:
    python update_data_virksomhed_s3_api_call.py
    python update_data_virksomhed_s3_api_call.py --since 2026-06-01
    python update_data_virksomhed_s3_api_call.py --buffer-days 2
    python update_data_virksomhed_s3_api_call.py --bucket cvr-data --prefix virksomhed
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


def run(client, bucket, prefix, since, buffer_days):
    last_run = since or s3.read_last_run(client, bucket, prefix)
    if last_run is None:
        raise SystemExit(
            "No previous run found in the bucket. Upload the historical data and "
            "its _state.json first, or pass --since YYYY-MM-DD."
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

    # this normally never happens, thats why the print message.
    if not all_hits:
        s3.write_last_run(client, bucket, prefix, run_started)
        print("This is weird...Nothing to update.")
        return

    # Group the changed companies by founding year, then upsert one year at a time.
    hits_by_year = {}
    for hit in all_hits:
        year = utils.founding_year(hit)
        hits_by_year.setdefault(year, []).append(hit)

    for year in sorted(hits_by_year):
        hits = hits_by_year[year]
        print(f"Upserting year {year}: {len(hits)} companies")
        s3.upsert_year(client, bucket, prefix, year, hits)

    s3.write_last_run(client, bucket, prefix, run_started)
    print(f"Update complete. Next run will start from {run_started}")


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
    args = parser.parse_args()

    if not args.bucket:
        raise SystemExit("No bucket. Set S3_BUCKET in .env or pass --bucket.")

    client = s3.make_client()
    run(client, args.bucket, args.prefix, args.since, args.buffer_days)
