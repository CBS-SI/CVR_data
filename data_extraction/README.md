# Danish Business Authority: Data Extraction

To run this script, you will need:

- A valid CVR API key. To get it, email the CVR office at [this website](https://erhvervsstyrelsen.dk/kom-godt-igang-med-elasticSearch)
- Install the required dependencies at the environment file `environment.yml`. You can use `conda` (`conda env create -f environment.yml -n cvr`), `uv`, `pipx` or whatever you prefer.
- Set environment variables at `.env`. See `.env.example` for an example.

## 1. Historical Company Data (`virksomhed`)

- Script: `src/get_historical_virksomhed_api_call.py`
- Endpoint 1: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Endpoint 2: `http://distribution.virk.dk/_search/scroll`
- Data files: `virksomhed_all_*.parquet`

The script downloads the **complete dataset** of all Danish companies with their full history. Each company record contains temporal fields (`navne`, `addresses`, `status`, etc.) with `gyldigFra`/`gyldigTil` validity periods that can be used to reconstruct point-in-time snapshots for any date.

Script usage:

```py
# Download all data at once may timeout or hit API limits as we use concurrent requests
python virksomhed_api_call.py

# Download in batches by founding year (recommended for reliability)
# Range of years are inclusive (e.g. 1800-1990 includes from Jan 1st 1800 to Dec 31st 1990)
python virksomhed_api_call.py --founding-years 1800 1990
python virksomhed_api_call.py --founding-years 1991 2000
python virksomhed_api_call.py --founding-years 2001 2010
python virksomhed_api_call.py --founding-years 2011 2020
python virksomhed_api_call.py --founding-years 2021 2025

# Output formatting: parquet (default) or json
python virksomhed_api_call.py --format "json"

# Output mode: panel (default, recommended, multiple files) or wide (single file, nested fields)
python virksomhed_api_call.py --mode "panel"
```

Options:

- `--founding-years START END`: Filter by founding date range (stiftelsesDato). I recommend using 5-10 year range for batching large downloads.
- `--format`: `parquet` (default) or `json`.
- `--mode`: `panel` (default) outputs 22 files with all unnested json files while `wide` is only one file with all historical records as JSON nested strings within fields/columns. **Panel mode is almost always better if you do not plan to unnest the data yourself.**

### 1.1 Folder Data Structure (`virksomhed`)

Calling the API via the python script in panel mode (e.g. `virksomhed_api_call.py --year 2018 --mode "panel"`) generates 22 `.parquet` files.

1. **virksomhed_YYYY_main.parquet** - Latest company records of selected fields. Thing it as a view of multiple tables with the main fields.
2. **virksomhed_YYYY_navne.parquet** - Company names
3. **virksomhed_YYYY_binavne.parquet** - Secondary names
4. **virksomhed_YYYY_beliggenhedsadresse.parquet** - Business addresses
5. **virksomhed_YYYY_postadresse.parquet** - Postal addresses
6. **virksomhed_YYYY_hovedbranche.parquet** - Main industry
7. **virksomhed_YYYY_bibranche1.parquet** - Secondary industry 1
8. **virksomhed_YYYY_bibranche2.parquet** - Secondary industry 2
9. **virksomhed_YYYY_bibranche3.parquet** - Secondary industry 3
10. **virksomhed_YYYY_maanedsbeskaeftigelse.parquet** - Monthly employment data
11. **virksomhed_YYYY_kvartalsbeskaeftigelse.parquet** - Quarterly employment data
12. **virksomhed_YYYY_aarsbeskaeftigelse.parquet** - Annual employment data
13. **virksomhed_YYYY_virksomhedsstatus.parquet** - Company status
14. **virksomhed_YYYY_virksomhedsform.parquet** - Company legal form
15. **virksomhed_YYYY_livsforloeb.parquet** - Company lifecycle
16. **virksomhed_YYYY_deltagerRelation.parquet** - Participant relations
17. **virksomhed_YYYY_attributter.parquet** - Company attributes
18. **virksomhed_YYYY_regNummer.parquet** - Registration numbers
19. **virksomhed_YYYY_telefonNummer.parquet** - Phone numbers
20. **virksomhed_YYYY_telefaxNummer.parquet** - Fax numbers
21. **virksomhed_YYYY_elektroniskPost.parquet** - Email addresses
22. **virksomhed_YYYY_hjemmeside.parquet** - Websites

## 1.2 Fields Data Structure (`virksomhed`)

Every dataset (e.g. `virksomhed_2025_addresses.parquet`) has multiple fields, also in Danish. I relied in [this field identification unofficial documentation](https://brokk-sindre.github.io/cvr-documentation/api-reference/field-reference/) for the translation of the Danish column names to English.

Fields available at [virksomhed_data.md](virksomhed_data.md)

## 2. Update Company Data (`virksomhed`)

- Script: `src/update_data_virksomhed_api_call.py`
- Endpoint 1: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Endpoint 2: `http://distribution.virk.dk/_search/scroll`
- Data files: `virksomhed_all_*.parquet`

The script downloads the **complete dataset** of all Danish companies with their full history. Each company record contains temporal fields (`navne`, `addresses`, `status`, etc.) with `gyldigFra`/`gyldigTil` validity periods that can be used to reconstruct point-in-time snapshots for any date.

Script usage:

```py
# Download all data at once may timeout or hit API limits as we use concurrent requests
python virksomhed_api_call.py

# Download in batches by founding year (recommended for reliability)
# Range of years are inclusive (e.g. 1800-1990 includes from Jan 1st 1800 to Dec 31st 1990)
python virksomhed_api_call.py --founding-years 1800 1990
python virksomhed_api_call.py --founding-years 1991 2000
python virksomhed_api_call.py --founding-years 2001 2010
python virksomhed_api_call.py --founding-years 2011 2020
python virksomhed_api_call.py --founding-years 2021 2025

# Output formatting: parquet (default) or json
python virksomhed_api_call.py --format "json"

# Output mode: panel (default, recommended, multiple files) or wide (single file, nested fields)
python virksomhed_api_call.py --mode "panel"
```

## 3. Financial Statements (`offentliggoerelser`)

- Script: `src/financial_statements_api_call.py`
- Endpoint: `http://distribution.virk.dk/offentliggoerelser/_search`
- Data files: `financial_statements_*.parquet`

Contains the companies financial statements submitted for a given year.

It is mandatory to submit one per year but they can do more than 1 per year for different reasons (e.g. board meetings). After September 2025, this endpoint does not contain that much data besides the `.xml` URL link to the financials. This link used to be useful to extract the individual company financials, but now the endpoints are dead.

Script usage:

```py
# Single year
python financial_statements_api_call.py --years 2020

# Year range
python financial_statements_api_call.py --years 2018 2020

# Output formatting: parquet (default) or json
python financial_statements_api_call.py --years 2020 --format "json"
```

## 3. Expanded Financial Statements (shut down)

- Script: `src/expand_financial_statements_api_call.py`
- Endpoint: None
- Data files: `expanded_financial_statements_*.parquet`

It expands the `.xml` file information in the financial statements into a readable table-wide format. Revenue, Total Assets, and other financials for a given year.

Contains all the companies that have submitted a financial statement that year, with all the posible fields the API gave ("tags").

_Erhvervsstyrelsen_ (The Danish Business Authority) has shut down the `.xml` endpoints in September 25th - therefore the script no longer works. The field in the `.xml` files exists but you cannot ping it. See: [https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere](https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere).

Script usage:

```py
# Single year
python individual_statements_api_call.py --years 2020

# Year range
python individual_statements_api_call.py --years 2018 2020

# With custom batch size
python individual_statements_api_call.py --years 2020 --batch-size 500
```
