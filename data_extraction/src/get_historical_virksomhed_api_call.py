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
from funcy import print_durations
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# utils/ lives at the project root, two levels up from this file (src/).
UTILS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "utils",
)
sys.path.insert(1, UTILS_FOLDER)
import utils_virksomhed as utils


# Queries used only by this backfill (size 3000 is the max allowed by the API).
def query_by_founding_year(year, size=3000):
    """Companies founded within a single calendar year."""
    return {
        "size": size,
        "sort": ["_doc"],
        "track_total_hits": True,
        "query": {
            "range": {
                "Vrvirksomhed.virksomhedMetadata.stiftelsesDato": {
                    "gte": f"{year}-01-01",
                    "lte": f"{year}-12-31",
                }
            }
        },
    }


def query_no_founding_year(size=3000):
    """Companies that have no stiftelsesDato (skipped by the year ranges)."""
    return {
        "size": size,
        "sort": ["_doc"],
        "track_total_hits": True,
        "query": {
            "bool": {
                "must_not": {
                    "exists": {"field": "Vrvirksomhed.virksomhedMetadata.stiftelsesDato"}
                }
            }
        },
    }


@print_durations()
def run(years, folder, overwrite):
    # Capture the start time up front: a later incremental update should pick up
    # anything that changed *during* this (possibly long) backfill.
    timestamp = datetime.now(timezone.utc).isoformat()

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
            query = query_no_founding_year()
        else:
            query = query_by_founding_year(int(year))
        all_hits = utils.fetch_query(query)

        print(f"-> Saving {len(all_hits)} records in parquet files for {year}...")


        # One parquet file per table. Build and write each in a single step so
        for table in tables:
            if table == "main":
                utils.build_main(all_hits).to_parquet(f"{folder}/main_{year}.parquet", index=False)
            else:
                utils.build_table(all_hits, table).to_parquet(f"{folder}/{table}_{year}.parquet", index=False)

    utils.write_last_run(folder, timestamp)
    state_path = os.path.relpath(os.path.join(folder, "_state.json"))
    human_time = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"-> Download complete. Parquet files saved in {folder}.")
    print(f"-> Timestamp set to {human_time} and stored as `_state.json` within the same folder.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill all CVR virksomhed data, one parquet per table and founding year."
    )
    parser.add_argument("--start-year", type=int, default=None,
                        help="First founding year to fetch (default 1800).")
    parser.add_argument("--end-year", type=int, default=None,
                        help="Last founding year to fetch, inclusive (default: current year).")
    parser.add_argument("--folder", default=utils.RAW_VIRKSOMHED_FOLDER_PATH)
    parser.add_argument("--overwrite", action="store_true",
                        help="Re-download years that are already in the output folder.")
    parser.add_argument("--year-unknown", action="store_true",
                        help="Fetch ONLY the 'unknown' partition (companies with no founding "
                             "date) and nothing else. Cannot be combined with a year range.")
    args = parser.parse_args()

    if not args.folder:
        raise SystemExit("No output folder. Set RAW_VIRKSOMHED_FOLDER_PATH in .env or pass --folder.")

    if args.year_unknown and (args.start_year is not None or args.end_year is not None):
        parser.error("--year-unknown cannot be combined with --start-year/--end-year.")

    # Decide which partitions to fetch. The "unknown" partition (companies with no
    # founding date) is separate from the founding years: a full default backfill
    # includes it, a narrowed year range does not, and --year-unknown fetches only it.
    full_run = args.start_year is None and args.end_year is None and not args.year_unknown
    start_year = args.start_year if args.start_year is not None else 1800
    end_year = args.end_year if args.end_year is not None else datetime.now().year

    if args.year_unknown:
        years = ["unknown"]
    else:
        years = [str(year) for year in range(start_year, end_year + 1)]
        if full_run:
            years.append("unknown")

    run(years, args.folder, args.overwrite)
