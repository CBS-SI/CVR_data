# Danish Business Authority: Data Extraction

Please notice that the data validation of Danish companies' digital annual accounts is not very thorough, so use Denmark Statistics (DST) when possible.

Companies have to legally submit their info in XBRL format (eXtensible Business Reporting Language) and it is extractred using the scripts in this repository - but there is not a workforce in charge of making sure it's correct (see [https://www.version2.dk/artikel/fejl-i-regnskabsdata-tvinger-virksomheder-og-myndigheder-til-manuel-kontrol](https://www.version2.dk/artikel/fejl-i-regnskabsdata-tvinger-virksomheder-og-myndigheder-til-manuel-kontrol))


## 1. Company Data (`virksomhed`)

- Script: `src/virksomhed_data_api_call.py`
- Endpoint: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Data files: `virksomhed_*_*.parquet`

Please notice that the `virksomhed_*_*.parquet` files contain the complete history of the companies as of that year.

Example. If you query for companies that were updated in the year 2013 (in the API/script `Vrvirksomhed.sidstOpdateret` == 2024), each company's record will contain its complete history until that year.

Script usage:

```
# Single year
python virksomhed_api_call.py --year 2018

# Output formatting: parquet (default) or json
python virksomhed_api_call.py --year 2018 --format "parquet"

# Output mode: panel (multiple files) or wide (single flat file)
python virksomhed_api_call.py --year 2018 --format "json" --mode "panel"
```

- Output formatting options (`--format`): `json` or `parquet`.
- Output mode options (`--format`): `panel` will output 22 files will all unnested json files while `wide` is only file with all historical records as JSON nested strings within fields/columns. Panel mode is almost always better is you do not plan to unnest yourself the data.

#### Company Data Structure (`virksomhed`)

Doing the API call via the python script in panel mode 22 `.parquet` files will be generated (e.g. `virksomhed_api_call.py --year 2018 --mode "panel"`)

1. **virksomhed_YYYY_main.parquet** - Main company info (non-temporal fields only)
2. **virksomhed_YYYY_navne.parquet** - Company names with validity periods
3. **virksomhed_YYYY_binavne.parquet** - Secondary names with validity periods
4. **virksomhed_YYYY_addresses.parquet** - Addresses with validity periods
5. **virksomhed_YYYY_hovedbranche.parquet** - Main industry branch with validity periods
6. **virksomhed_YYYY_bibranche1.parquet** - Secondary company branches
7. **virksomhed_YYYY_bibranche2.parquet** - Secondary company branches
8. **virksomhed_YYYY_bibranche3.parquet** - Secondary company branches
9. **virksomhed_YYYY_maanedsbeskaeftigelse.parquet** - Monthly employment data
10. **virksomhed_YYYY_kvartalsbeskaeftigelse.parquet** - Quarterly employment data
11. **virksomhed_YYYY_aarsbeskaeftigelse.parquet** - Annual employment data

## 

## 2. Financial Statements (`offentliggoerelser`)

- Script: `src/financial_statements_api_call.py`
- Endpoint: `http://distribution.virk.dk/offentliggoerelser/_search`
- Data files: `financial_statements_*.parquet`

Contains the companies submitted a financial statement for a given year.

It is mandatory but they can do more than 1 per year for different reasons (e.g. board meetings). It does not contain that much data besides the `.xml` URL link to the financials. This link used to link to the financials, but now it is a dead link.

Script usage:

```
# Single year
python financial_statements_api_call.py --years 2020

# Year range
python financial_statements_api_call.py --years 2018 2020

# Output formatting: parquet (default) or json
python financial_statements_api_call.py --years 2020 --format "json"
```

## 3. Expanded Financial Statements

- Script: `src/individual_statements_api_call.py`
- Endpoint: None/Shut down
- Data files: `companies_all_tags_*.parquet.parquet`

It expands the `.xml` information in the financial statements into a readable table format. Revenue, Total Assets, etc for a given year.

Contains all the companies that have submitted a financial statement that year, with all the posible fields the API gave ("tags").

Erhvervsstyrelsen has shut down the `.xml` endpoints in September 25th - therefore the script no longer works. See: [https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere](https://erhvervsstyrelsen.dk/vejledning-adgang-til-oplysninger-om-reelle-ejere).

Script usage:

```
# Single year
python individual_statements_api_call.py --years 2020

# Year range
python individual_statements_api_call.py --years 2018 2020

# With custom batch size
python individual_statements_api_call.py --years 2020 --batch-size 500
```

## Limitations

### Missing data: Production Units

CVR P-number (production unit) are available via direct API queries to the `http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search` endpoint.

### Missing data: Participant Units

Individuals and entities participating in companies are available via direct API queries to the `http://distribution.virk.dk/offentliggoerelser/_search` endpoint.
