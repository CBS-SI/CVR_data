"""
full backfill of CVR "virksomhed" data.

iterates founding years, from 1800. it writes the 20 tables for each year as <table>_<year>.parquet, then adds an "unknown" year for companies that have no founding date.

it runs one year at a time to avoid memory spikes, and the run is yearly resumable: years already on disk are skipped unless --overwrite is given.

Examples:
    python get_historical_data.py
    python get_historical_data.py --start-year 1990 --end-year 2025
    python get_historical_data.py --overwrite
"""

import os
import sys
import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

UTILS_FOLDER = os.getenv("UTILS_FOLDER")
sys.path.insert(1, UTILS_FOLDER)
import utils_virksomhed as utils

def run(start_year, end_year, folder, overwrite):
    # Capture the start time up front: a later incremental update should pick up
    # anything that changed *during* this (possibly long) backfill.
    run_started = datetime.now(timezone.utc).isoformat()

    # Every founding year, plus an "unknown" batch for companies with no founding date.
    years = [str(year) for year in range(start_year, end_year + 1)] + ["unknown"]

    # The 20 files written per year. A year counts as done only if all of them
    # exist, so a single deleted table file triggers a re-fetch of that year.
    tables = ["main"] + utils.TABLES


    for year in years:
        files_present = all(
            os.path.exists(f"{folder}/{table}_{year}.parquet") for table in tables
        )
        if files_present and not overwrite:
            print(f"{year}: all files in the output folder, skipping... ")
            continue

        print(f"Fetching founding year: {year}...")
        if year == "unknown":
            query = utils.query_no_founding_year()
        else:
            query = utils.query_by_founding_year(int(year))
        all_hits = utils.fetch_query(query)

        print(f"Saving {len(all_hits)} records in parquet file for {year}...")


        # One parquet file per table. Build and write each in a single step so
        tables = ["main"] + utils.TABLES
        for table in tables:
            if table == "main":
                utils.build_main(all_hits).to_parquet(f"{folder}/main_{year}.parquet", index=False)
            else:
                utils.build_table(all_hits, table).to_parquet(f"{folder}/{table}_{year}.parquet", index=False)

    utils.write_last_run(folder, run_started)
    print(f"Backfill complete. Update timestamp set to {run_started}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill all CVR virksomhed data, one parquet per table and founding year."
    )
    parser.add_argument("--start-year", type=int, default=1800)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--folder", default=utils.VIRKSOMHED_FOLDER_PATH)
    parser.add_argument("--overwrite", action="store_true",
                        help="Re-download years that are already in the output folder.")
    args = parser.parse_args()

    if not args.folder:
        raise SystemExit("No output folder. Set VIRKSOMHED_FOLDER_PATH in .env or pass --folder.")

    run(args.start_year, args.end_year, args.folder, args.overwrite)
