"""
Shared helpers for downloading and shaping CVR "virksomhed" (company) data.

Both get_historical_data.py and update_data.py import from here so the
fetch + build + save logic lives in one place.

The data is split into 20 logical tables, partitioned on disk by founding
year:  main_<year>.parquet, navne_<year>.parquet, ...  Records with no
founding date go in the "unknown" partition.
"""

import os
import glob
import json
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Env files
VIRK_USERNAME = os.getenv("VIRK_USERNAME")
VIRK_PASSWORD = os.getenv("VIRK_PASSWORD")
RAW_VIRKSOMHED_FOLDER_PATH = os.getenv("RAW_VIRKSOMHED_FOLDER_PATH")

# API endpoints
# Note: the API uses HTTP (not HTTPS)
VIRKSOMHED_API_ENDPOINT = "http://distribution.virk.dk/cvr-permanent/virksomhed/_search"
VIRKSOMHED_API_ENDPOINT_SCROLL = "http://distribution.virk.dk/_search/scroll"

# tables to use with build_table() to create dataframes. "main" is built separately
TABLES = [
    "navne", "binavne",
    "beliggenhedsadresse", "postadresse",
    "hovedbranche", "bibranche1", "bibranche2", "bibranche3",
    "aarsbeskaeftigelse", "kvartalsbeskaeftigelse", "maanedsbeskaeftigelse",
    "virksomhedsstatus",
    "telefonNummer", "telefaxNummer", "elektroniskPost", "hjemmeside",
    "virksomhedsform", "regNummer", "livsforloeb",
]


# Queries
# Note: size 3000 is the max allowed by the API.
def query_updated_since(since_iso, start_year=None, end_year=None,
                        include_unknown=False, size=3000):
    """Companies whose record changed on/after since_iso (incremental).

       It uses start_year/end_year set or "unknown" founding year (given by stiftelsesDato).
    With neither, every founding year matches get queried.
    """
    field = "Vrvirksomhed.virksomhedMetadata.stiftelsesDato"
    must = [{"range": {"Vrvirksomhed.sidstOpdateret": {"gte": since_iso}}}]

    should = []
    if start_year is not None or end_year is not None:
        bounds = {}
        if start_year is not None:
            bounds["gte"] = f"{start_year}-01-01"
        if end_year is not None:
            bounds["lte"] = f"{end_year}-12-31"
        should.append({"range": {field: bounds}})
    if include_unknown:
        should.append({"bool": {"must_not": {"exists": {"field": field}}}})

    bool_query = {"must": must}
    if should:                                # restrict to the founding-year scope
        bool_query["should"] = should
        bool_query["minimum_should_match"] = 1

    return {
        "size": size,
        "sort": ["_doc"],
        "track_total_hits": True,
        "query": {"bool": bool_query},
    }


# Fetch data
def fetch_query(query,
                scroll_keepalive="30m",
                timeout_connect=30,
                timeout_read=300):
    """Run a query and scroll through every page. Returns the list of hits."""
    auth = (VIRK_USERNAME, VIRK_PASSWORD)
    url = f"{VIRKSOMHED_API_ENDPOINT}?scroll={scroll_keepalive}"

    response = requests.post(url,
                        json=query,
                        auth=auth,
                        timeout=(timeout_connect, timeout_read))
    response.raise_for_status()
    payload = response.json()

    scroll_id = payload["_scroll_id"]
    total = payload["hits"]["total"]
    if isinstance(total, dict):          # ES 7+ returns {"value": N, "relation": "eq"}
        total = total.get("value")
    all_hits = payload["hits"]["hits"]
    print(f"Total hits to fetch: {total}")

    while True:
        scroll_response = requests.post(
            VIRKSOMHED_API_ENDPOINT_SCROLL,
            json={"scroll": scroll_keepalive, "scroll_id": scroll_id},
            auth=auth,
            timeout=(timeout_connect, timeout_read),
        )
        scroll_response.raise_for_status()
        payload = scroll_response.json()

        hits = payload["hits"]["hits"]
        if not hits:
            break
        all_hits.extend(hits)
        scroll_id = payload["_scroll_id"]
        print(f"Fetched {len(all_hits)}/{total}")

    return all_hits


# Create dataframes
def build_main(all_hits):
    """Main table: Non-list fields, one row per company."""
    rows = []
    for hit in all_hits:
        company = hit["_source"]["Vrvirksomhed"]
        row = {}
        for key in company:
            value = company[key]
            if not isinstance(value, list):   # skip the list fields
                row[key] = value
        rows.append(row)
    return pd.DataFrame(rows)


def build_table(all_hits, field):
    """One list field exploded to a row per item, with the company id attached."""
    rows = []
    for hit in all_hits:
        company = hit["_source"]["Vrvirksomhed"]
        cvr_nummer = company.get("cvrNummer")
        enheds_nummer = company.get("enhedsNummer")
        items = company.get(field)
        if not items:                         # company has none of this field
            continue
        for item in items:
            row = {"cvrNummer": cvr_nummer, "enhedsNummer": enheds_nummer}
            for key in item:
                value = item[key]
                if isinstance(value, dict):   # e.g. periode -> periode_gyldigFra, periode_gyldigTil
                    for subkey in value:
                        row[key + "_" + subkey] = value[subkey]
                else:
                    row[key] = value
            rows.append(row)
    return pd.DataFrame(rows)


def build_panel(all_hits):
    """Build all 20 tables. Returns {table_name: DataFrame}."""
    panel = {"main": build_main(all_hits)}
    for field in TABLES:
        panel[field] = build_table(all_hits, field)
    return panel


# Get Founding year
# Note: some companies does not have a stiftelsesDato, in which case we return 'unknown'
def founding_year(hit):
    """Return the founding year for a company"""
    company = hit["_source"]["Vrvirksomhed"]
    metadata = company.get("virksomhedMetadata") or {}
    stiftelses_dato = metadata.get("stiftelsesDato")
    if stiftelses_dato:
        return stiftelses_dato[:4]            # the year, example: "2024"
    return "unknown"


def select_years(years, start_year=None, end_year=None, year_unknown=False):
    """Filter founding-year keys down to a requested scope.

    `years` is an iterable of "YYYY" strings and/or the "unknown" partition.
    Mirrors the get_historical interface:
      - year_unknown=True        -> only the "unknown" partition.
      - start_year/end_year set  -> only those founding years (inclusive),
                                    excluding "unknown".
      - nothing set              -> every year, including "unknown".
    """
    if year_unknown:
        return [y for y in years if y == "unknown"]
    if start_year is None and end_year is None:
        return list(years)
    lo = start_year if start_year is not None else float("-inf")
    hi = end_year if end_year is not None else float("inf")
    return [y for y in years if y != "unknown" and lo <= int(y) <= hi]


# Note: minimal upserts avoids gaps.
def upsert_year(folder, year, hits_for_year):
    """
    Merge changed companies into an existing founding year.
    """
    os.makedirs(folder, exist_ok=True)
    panel = build_panel(hits_for_year)

    changed_cvrs = set()
    for hit in hits_for_year:
        changed_cvrs.add(hit["_source"]["Vrvirksomhed"].get("cvrNummer"))

    for name, new_df in panel.items():
        path = os.path.join(folder, f"{name}_{year}.parquet")
        if os.path.exists(path):
            existing = pd.read_parquet(path)
            if "cvrNummer" in existing.columns:
                existing = existing[~existing["cvrNummer"].isin(changed_cvrs)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        combined.to_parquet(path, index=False)


# Founding-year partitions and their last successful update timestamp.
def existing_years(folder):
    """Founding-year partitions present on disk, taken from the main_<year> files."""
    years = []
    for path in glob.glob(os.path.join(folder, "main_*.parquet")):
        base = os.path.basename(path)
        years.append(base[len("main_"):-len(".parquet")])
    return years


def read_year_timestamps(folder):
    """Return {founding_year: last_update_iso} from _state.json.

    A legacy {"last_run_utc": ...} file (single global watermark) is migrated by
    seeding every on-disk year partition with that timestamp. Returns {} when
    there is no state yet.
    """
    path = os.path.join(folder, "_state.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        state = json.load(f)
    years = state.get("years")
    if years:
        return dict(years)
    legacy = state.get("last_run_utc")
    if legacy is None:
        return {}
    return {year: legacy for year in existing_years(folder)}


def write_year_timestamps(folder, year_ts):
    """Persist per-year update timestamps, plus a derived last_run_utc (the max)."""
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "_state.json")
    state = {"years": year_ts}
    if year_ts:
        state["last_run_utc"] = max(year_ts.values())   # for a quick global glance
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
