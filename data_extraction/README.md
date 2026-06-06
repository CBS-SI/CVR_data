# Danish Business Authority: Data Extraction

To run these scripts, you will need:

1. A valid CVR API key. To get it, email the CVR office at [this website](https://erhvervsstyrelsen.dk/kom-godt-igang-med-elasticSearch)
2. Install the required dependencies at the environment file `environment.yml`. You can use `conda` (`conda env create -f environment.yml -n cvr`), `uv`, `pipx` or whatever you prefer.
3. Set environment variables at `.env`. See `.env.example` for an example.

## 1. Historical Company Data (`virksomhed`)

- Script: `src/get_historical_virksomhed_api_call.py`
- Endpoint 1: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Endpoint 2: `http://distribution.virk.dk/_search/scroll`
- Data files: `<table>_<year>.parquet` (e.g. `main_2024.parquet`, `navne_2024.parquet`)

The script downloads the **complete dataset** of all Danish companies with their full history. Each company record contains temporal fields (`navne`, addresses, `virksomhedsstatus`, etc.) with `gyldigFra`/`gyldigTil` validity periods that can be used to reconstruct point-in-time snapshots for any date.

It works **one founding year at a time** to avoid memory spikes, and writes the 20 tables (see below) for each year. Companies with no founding date (`stiftelsesDato`) are written to an `unknown` partition (e.g. `main_unknown.parquet`).

The run is **resumable**: a year is considered done only when all 20 of its table files exist on disk, so years already downloaded are skipped unless `--overwrite` is passed. On success it writes the run timestamp to `_state.json`, which the incremental update script (section 2) uses as its starting point.

Script usage:

```py
# Backfill everything (founding years 1800 .. current year, plus the "unknown" partition)
python get_historical_virksomhed_api_call.py

# Backfill a specific founding-year range (inclusive on both ends)
python get_historical_virksomhed_api_call.py --start-year 1990 --end-year 2025

# Re-download years that are already present on disk
python get_historical_virksomhed_api_call.py --overwrite

# Fetch ONLY companies with no founding date (the "unknown" partition)
python get_historical_virksomhed_api_call.py --year-unknown

# Write to a specific output folder (defaults to RAW_VIRKSOMHED_FOLDER_PATH in .env)
python get_historical_virksomhed_api_call.py --folder /path/to/output
```

Options:

- `--start-year`: First founding year to fetch (default `1800`).
- `--end-year`: Last founding year to fetch, inclusive (default: current year).
- `--folder`: Output folder. Defaults to `RAW_VIRKSOMHED_FOLDER_PATH` from `.env`.
- `--overwrite`: Re-download years already present on disk. Without it, completed years are skipped.
- `--year-unknown`: Fetch **only** the `unknown` partition (companies with no founding date) and nothing else. Cannot be combined with `--start-year`/`--end-year`. (The `unknown` partition is included automatically on a full default backfill.)

### 1.1 Folder Data Structure (`virksomhed`)

Each founding year produces 20 `.parquet` files, named `<table>_<year>.parquet` (the `unknown` partition uses `<table>_unknown.parquet`). Replace `<year>` below with the founding year, e.g. `main_2024.parquet`.

1. **main\_<year>.parquet** - Latest company records of the non-list fields. Think of it as a view of multiple tables with the main fields (holds the `cvrNummer`, `enhedsNummer` and `reklamebeskyttelse` flag).
2. **navne\_<year>.parquet** - Company names
3. **binavne\_<year>.parquet** - Secondary names
4. **beliggenhedsadresse\_<year>.parquet** - Business addresses
5. **postadresse\_<year>.parquet** - Postal addresses
6. **hovedbranche\_<year>.parquet** - Main industry
7. **bibranche1\_<year>.parquet** - Secondary industry 1
8. **bibranche2\_<year>.parquet** - Secondary industry 2
9. **bibranche3\_<year>.parquet** - Secondary industry 3
10. **aarsbeskaeftigelse\_<year>.parquet** - Annual employment data
11. **kvartalsbeskaeftigelse\_<year>.parquet** - Quarterly employment data
12. **maanedsbeskaeftigelse\_<year>.parquet** - Monthly employment data
13. **virksomhedsstatus\_<year>.parquet** - Company status
14. **telefonNummer\_<year>.parquet** - Phone numbers
15. **telefaxNummer\_<year>.parquet** - Fax numbers
16. **elektroniskPost\_<year>.parquet** - Email addresses
17. **hjemmeside\_<year>.parquet** - Websites
18. **virksomhedsform\_<year>.parquet** - Company legal form
19. **regNummer\_<year>.parquet** - Registration numbers
20. **livsforloeb\_<year>.parquet** - Company lifecycle

## 1.2 Fields Data Structure (`virksomhed`)

Every dataset (e.g. `virksomhed_2025_addresses.parquet`) has multiple fields, also in Danish. I relied in [this field identification unofficial documentation](https://brokk-sindre.github.io/cvr-documentation/api-reference/field-reference/) for the translation of the Danish column names to English.

Fields available at [virksomhed_data.md](virksomhed_data.md)

## 2. Update Company Data (`virksomhed`)

- Script: `src/update_data_virksomhed_api_call.py`
- Endpoint 1: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Endpoint 2: `http://distribution.virk.dk/_search/scroll`
- Data files: the same `<table>_<year>.parquet` files produced by the historical backfill

This script performs an **incremental update** rather than a full download. It fetches only the companies whose record changed since the last successful run (using the `sidstOpdateret` field) and **upserts** them into the matching founding-year files: existing rows for those companies are dropped and the fresh rows appended, so only the year files that actually changed are rewritten.

The starting point ("last run") is read from `_state.json`, written by the historical backfill and refreshed after every successful update. A small **lookback buffer** (default 1 day) re-scans a little overlap so a late-landing edit is never missed. Run the historical backfill (section 1) at least once before using this script.

Script usage:

```py
# Incremental update from the last run recorded in _state.json
python update_data_virksomhed_api_call.py

# Override the start date instead of using _state.json
python update_data_virksomhed_api_call.py --since 2026-06-01

# Widen the lookback overlap to absorb more replication lag
python update_data_virksomhed_api_call.py --buffer-days 2

# Write to a specific output folder (defaults to RAW_VIRKSOMHED_FOLDER_PATH in .env)
python update_data_virksomhed_api_call.py --folder /path/to/output
```

Options:

- `--since`: Override the start date (`YYYY-MM-DD` or ISO). Default: last run from `_state.json`.
- `--buffer-days`: Lookback overlap in days to absorb replication lag (default `1`).
- `--folder`: Output folder. Defaults to `RAW_VIRKSOMHED_FOLDER_PATH` from `.env`.

### 2.1 Update Company Data on S3 (`virksomhed`)

- Script: `src/update_data_virksomhed_s3_api_call.py`
- Storage layer: `utils/utils_virksomhed_s3.py`
- Objects: `<prefix>/<table>_<year>.parquet` and `<prefix>/_state.json` in the bucket

Same incremental-upsert logic as section 2, but it reads and writes the parquet files and `_state.json` directly to an S3-compatible bucket instead of local path (e.g. I use [Garage](https://garagehq.deuxfleurs.fr/)).

You will need to set up the S3 storage config in `.env` (see `.env.example`).

I recommend uploading the historical data and `_state.json` to the bucket first, using `aws s3 cp` or a similar tool.

Script usage:

```py
# Incremental update from the last run recorded in <prefix>/_state.json
python update_data_virksomhed_s3_api_call.py

# Override the start date
python update_data_virksomhed_s3_api_call.py --since 2026-06-01

# Widen the lookback overlap
python update_data_virksomhed_s3_api_call.py --buffer-days 2

# Target a specific bucket/prefix (defaults come from .env)
python update_data_virksomhed_s3_api_call.py --bucket cvr-data --prefix virksomhed
```

Options:

- `--bucket`: S3 bucket. Defaults to `S3_BUCKET` from `.env`.
- `--prefix`: Key prefix for the data. Defaults to `VIRKSOMHED_S3_PREFIX` from `.env`.
- `--since`: Override the start date (`YYYY-MM-DD` or ISO). Default: last run from `<prefix>/_state.json`.
- `--buffer-days`: Lookback overlap in days to absorb replication lag (default `1`).

## 3. Financial Statements (`offentliggoerelser`)

- Script: `src/financial_statements_api_call.py`
- Endpoint: `http://distribution.virk.dk/offentliggoerelser/_search`
- Data files: `financial_statements_*.parquet`

Contains the companies financial statements submitted for a given year.

It is mandatory to submit one per year but they can do more than 1 per year for different reasons (e.g. board meetings). After September 2025, this endpoint does not contain that much data besides the `.xml` URL link to the financials. This link used to be useful to extract the individual company financials, but now the endpoints are dead.

It writes a single file: `financial_statements.parquet`, or `financial_statements_<year>.parquet` when `--year` is given.

Script usage:

```py
# All available statements
python financial_statements_api_call.py

# Filter by a single year (writes financial_statements_2020.parquet)
python financial_statements_api_call.py --year 2020

# Output formatting: parquet (default) or json
python financial_statements_api_call.py --year 2020 --format "json"
```

Options:

- `--year`: Filter data by a single year. Omit to fetch everything.
- `--format`: `parquet` (default) or `json`.

## 4. Expanded Financial Statements (shut down)

- Script: `src/expand_financial_statements_api_call.py`
- Endpoint: None
- Data files: `expanded_financial_statements_*.parquet`

It expands the `.xml` file information in the financial statements into a readable table-wide format. Revenue, Total Assets, and other financials for a given year.

Contains all the companies that have submitted a financial statement that year, with all the posible fields the API gave ("tags").

_Erhvervsstyrelsen_ (The Danish Business Authority) has shut down the `.xml` endpoints in September 25th - therefore the script no longer works. The field in the `.xml` files exists but you cannot ping it. See: [https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere](https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere).

It reads the `financial_statements.parquet` produced in section 3 and writes `expanded_financial_statements_<year>.parquet`.

Script usage (kept for documentation — the endpoints are dead):

```py
# Single year
python expand_financial_statements_api_call.py --years 2020

# Year range
python expand_financial_statements_api_call.py --years 2018 2020

# With custom batch size
python expand_financial_statements_api_call.py --years 2020 --batch-size 500
```

Options:

- `--years`: One year (e.g. `2020`) or a two-value inclusive range (e.g. `2018 2020`). Required.
- `--batch-size`: Number of URLs to process per batch (default `1000`).
