"""
Shared helpers for downloading and shaping CVR "virksomhed" (company) data.

Both get_historical_data.py and update_data.py import from here so the
fetch + build + save logic lives in one place.

The data is split into 20 logical tables, partitioned on disk by founding
year:  main_<year>.parquet, navne_<year>.parquet, ...  Records with no
founding date go in the "unknown" partition.
"""

import os
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
def query_updated_since(since_iso, size=3000):
    """Companies whose record changed on/after the given date (incremental)."""
    return {
        "size": size,
        "sort": ["_doc"],
        "track_total_hits": True,
        "query": {
            "range": {
                "Vrvirksomhed.sidstOpdateret": {"gte": since_iso}
            }
        },
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


# Last successful run timestamp
def read_last_run(folder):
    """Return the stored last-run UTC timestamp, or None if never run."""
    path = os.path.join(folder, "_state.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f).get("last_run_utc")
    return None


def write_last_run(folder, when_iso):
    """Persist the timestamp the next incremental update should start from."""
    os.makedirs(folder, exist_ok=True)
    path = path = os.path.join(folder, "_state.json")
    with open(path, "w") as f:
        json.dump({"last_run_utc": when_iso}, f)
