# Danish Business Authority: Data Extraction

## 1. Company Data (`virksomhed`)

- Script: `src/virksomhed_data_api_call.py`
- Endpoint: `http://distribution.virk.dk/cvr-permanent/virksomhed/_search`
- Data files: `virksomhed_*_*.parquet`

Please notice that the `virksomhed_*_*.parquet` files contain the complete history of the companies as of that year.

Example. If you query for companies in the year 2024 (in the API/script `Vrvirksomhed.sidstOpdateret` == 2024), each company's record will contain its complete history until that year.

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

#### 1.1 Folder Data Structure (`virksomhed`)

Doing the API call via the python script in panel mode 22 `.parquet` files will be generated (e.g. `virksomhed_api_call.py --year 2018 --mode "panel"`)

1. **virksomhed_YYYY_main.parquet** - Main company records
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

Every dataset (e.g. `virksomhed_2025_addresses.parquet`) has  multiple fields, also in Danish. I relied in [this field identification documentation](https://brokk-sindre.github.io/cvr-documentation/api-reference/field-reference/).

### Employment-related Fields (`aarsbeskaeftigelse`, `kvartalsbeskaeftigelse`, `maanedsbeskaeftigelse`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| aar | year | Year of the employment data |
| kvartal | quarter | Quarter of the year (Q1-Q4) |
| maaned | month | Month of the data |
| antalInklusivEjere | count_including_owners | Number of employees including owners |
| antalAarsvaerk | full_time_equivalents | Number of full-time equivalent employees (FTE/annual work units) |
| antalAnsatte | employee_count | Number of employees |
| intervalKodeAntalInklusivEjere | interval_code_including_owners | Interval code for number of employees including owners (categorical range) |
| intervalKodeAntalAarsvaerk | interval_code_fte | Interval code for full-time equivalents (categorical range) |
| intervalKodeAntalAnsatte | interval_code_employees | Interval code for number of employees (categorical range) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Business address Fields (`beliggenhedsadresse`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| landekode | country_code | Country code (ISO alpha-2) |
| fritekst | free_text | Unstructured address text |
| vejkode | road_code | Road code identifier |
| vejnavn | street_name | Street name |
| husnummerFra | house_number_from | Starting house number in range |
| husnummerTil | house_number_to | Ending house number in range |
| bogstavFra | letter_from | Starting house letter in range |
| bogstavTil | letter_to | Ending house letter in range |
| etage | floor | Floor designation |
| sidedoer | side_door | Side door or apartment identifier |
| conavn | care_of_name | Care-of name (c/o) |
| postboks | po_box | Post office box number |
| postnummer | postal_code | Postal code |
| postdistrikt | postal_district | Postal district name |
| bynavn | city_name | City, town, or locality name |
| adresseId | address_id | Unique address identifier |
| sidstValideret | last_validated | Timestamp when the address was last validated |
| kommuneKode | municipality_code | Municipality code |
| kommuneNavn | municipality_name | Municipality name |
| gyldigFra | valid_from | Date when the address became valid |
| gyldigTil | valid_to | Date when the address stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Postal address Fields (`postadresse`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| landekode | country_code | Country code (ISO alpha-2) |
| fritekst | free_text | Unstructured address text |
| vejkode | road_code | Road code identifier |
| vejnavn | street_name | Street name |
| husnummerFra | house_number_from | Starting house number in range |
| husnummerTil | house_number_to | Ending house number in range |
| bogstavFra | letter_from | Starting house letter in range |
| bogstavTil | letter_to | Ending house letter in range |
| etage | floor | Floor designation |
| sidedoer | side_door | Side door or apartment identifier |
| conavn | care_of_name | Care-of name (c/o) |
| postboks | po_box | Post office box number |
| postnummer | postal_code | Postal code |
| postdistrikt | postal_district | Postal district name |
| bynavn | city_name | City, town, or locality name |
| adresseId | address_id | Unique address identifier |
| sidstValideret | last_validated | Timestamp when the address was last validated |
| kommuneKode | municipality_code | Municipality code |
| kommuneNavn | municipality_name | Municipality name |
| gyldigFra | valid_from | Date when the address became valid |
| gyldigTil | valid_to | Date when the address stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Industry Fields (`hovedbranche`, `bibranche1`, `bibranche2`, `bibranche3`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| branchekode | industry_code | Industry / NACE code |
| branchetekst | industry_text | Text description of the industry code |
| gyldigFra | valid_from | Date when the industry classification became valid |
| gyldigTil | valid_to | Date when the industry classification stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Name Fields (`navne`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| navn | name | Official registered name of the business unit |
| gyldigFra | valid_from | Date when the name became valid |
| gyldigTil | valid_to | Date when the name stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Secondary name Fields (`binavne`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| navn | name | Secondary/alternative registered name of the business unit |
| gyldigFra | valid_from | Date when the secondary name became valid |
| gyldigTil | valid_to | Date when the secondary name stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Company status Fields (`virksomhedsstatus`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| status | status | Status code or text for the company |
| gyldigFra | valid_from | Date when the status became valid |
| gyldigTil | valid_to | Date when the status stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Contact information Fields (`telefonNummer`, `telefaxNummer`, `elektroniskPost`, `hjemmeside`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| kontaktoplysning | contact_info | The contact information value (phone number, fax, email, or website URL) |
| hemmelig | secret | Whether the contact information is marked as secret/private |
| gyldigFra | valid_from | Date when the contact info became valid |
| gyldigTil | valid_to | Date when the contact info stopped being valid (if applicable) |

### Company form Fields (`virksomhedsform`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| virksomhedsformkode | legal_form_code | Code for the legal form of the company |
| kortBeskrivelse | short_description | Short description of the legal form |
| langBeskrivelse | long_description | Long description of the legal form |
| ansvarligDataleverandoer | responsible_data_provider | The responsible data provider for this information |
| gyldigFra | valid_from | Date when the legal form became valid |
| gyldigTil | valid_to | Date when the legal form stopped being valid (if applicable) |

### Registration number Fields (`regNummer`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| regnummer | registration_number | Registration number from another registry |
| gyldigFra | valid_from | Date when the registration became valid |
| gyldigTil | valid_to | Date when the registration stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Lifecycle Fields (`livsforloeb`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| gyldigFra | valid_from | Start date of this lifecycle period (e.g., founding date) |
| gyldigTil | valid_to | End date of this lifecycle period (e.g., dissolution date) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Participant relations Fields (`deltagerRelation`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| deltagerEnhedsNummer | participant_unit_number | Unit number of the participating entity |
| deltagerCvrNummer | participant_cvr_number | CVR number of the participating entity (if applicable) |
| deltagerNavn | participant_name | Name of the participant |
| deltagerType | participant_type | Type of participant (e.g., person, company) |
| rolle | role | Role of the participant in the company |
| kontorsteder | office_locations | Office locations associated with the participant |
| organisationer | organizations | Organizations associated with the participant |
| gyldigFra | valid_from | Date when the relation became valid |
| gyldigTil | valid_to | Date when the relation stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Attributes Fields (`attributter`)

| Danish Column | English Column | Definition |
|---------------|----------------|------------|
| cvrNummer | cvr_number | Central Business Register (CVR) number - unique identifier for Danish businesses |
| enhedsNummer | unit_number | Unit number - unique identifier for the specific business unit |
| sekvensnr | sequence_number | Sequence number for ordering attributes |
| type | type | Type of the attribute |
| vaerditype | value_type | Data type of the attribute value |
| vaerdi | value | The attribute value |
| gyldigFra | valid_from | Date when the attribute became valid |
| gyldigTil | valid_to | Date when the attribute stopped being valid (if applicable) |
| sidstOpdateret | last_updated | Timestamp of last update to the record |

### Main Fields (`main`)

These columns are a flattened version of the `Vrvirksomhed` structure from the CVR API. Below are the main groups of fields:

| Column | Definition |
|--------|------------|
| _score | Search relevance score from the source index |
| _id, _type, _index | Technical fields from the search index |
| Vrvirksomhed_cvrNummer | CVR number for the company |
| Vrvirksomhed_enhedsNummer | Unit number for the company entity |
| Vrvirksomhed_enhedstype | Type of unit (e.g. company, production unit) |
| Vrvirksomhed_virksomhedMetadata_nyesteNavn_* | Latest registered company name and its validity period |
| Vrvirksomhed_virksomhedMetadata_nyesteVirksomhedsform_* | Latest legal form of the company and descriptions |
| Vrvirksomhed_virksomhedMetadata_nyesteBeliggenhedsadresse_* | Latest registered business address, including municipality, road, house number etc. |
| Vrvirksomhed_virksomhedMetadata_nyesteHovedbranche_* | Latest main industry classification for the company |
| Vrvirksomhed_virksomhedMetadata_nyesteBibranche{1,2,3}_* | Latest secondary industry classifications |
| Vrvirksomhed_virksomhedMetadata_nyesteAarsbeskaeftigelse_* | Latest annual employment figures and intervals |
| Vrvirksomhed_virksomhedMetadata_nyesteKvartalsbeskaeftigelse_* | Latest quarterly employment figures and intervals |
| Vrvirksomhed_virksomhedMetadata_nyesteMaanedsbeskaeftigelse_* | Latest monthly employment figures and intervals |
| Vrvirksomhed_virksomhedMetadata_nyesteStatus_* | Latest status code/text for the company, including credit information |
| Vrvirksomhed_virksomhedMetadata_sammensatStatus | Composite status for the company |
| Vrvirksomhed_virksomhedMetadata_stiftelsesDato | Date when the company was founded |
| Vrvirksomhed_virksomhedMetadata_virkningsDato | Date when the current state took effect |
| Vrvirksomhed_reklamebeskyttet | Whether the company is protected from marketing (advertising protection) |
| Vrvirksomhed_dataAdgang | Data access indicator from the CVR system |
| Vrvirksomhed_sidstIndlaest | Timestamp when the record was last loaded into the system |
| Vrvirksomhed_sidstOpdateret | Timestamp when the record was last updated in the system |
| Vrvirksomhed_fejlRegistreret, Vrvirksomhed_fejlVedIndlaesning, Vrvirksomhed_fejlBeskrivelse | Error flags and descriptions related to data loading |
| Vrvirksomhed_samtId | Internal identifier from the data source |
| Vrvirksomhed_virkningsAktoer | Actor responsible for the change in state |
| Vrvirksomhed_naermesteFremtidigeDato | Nearest future date for a scheduled change to the record |

## 2. Financial Statements (`offentliggoerelser`)

- Script: `src/financial_statements_api_call.py`
- Endpoint: `http://distribution.virk.dk/offentliggoerelser/_search`
- Data files: `financial_statements_*.parquet`

Contains the companies financial statements submitted for a given year.

It is mandatory to submit one per year but they can do more than 1 per year for different reasons (e.g. board meetings). After September 2025, this endpoint does not contain that much data besides the `.xml` URL link to the financials. This link used to be useful to extract the individual company financials, but now the endpoints are dead.

Script usage:

```
# Single year
python financial_statements_api_call.py --years 2020

# Year range
python financial_statements_api_call.py --years 2018 2020

# Output formatting: parquet (default) or json
python financial_statements_api_call.py --years 2020 --format "json"
```

## 3. Expanded Financial Statements (Shut down)

- Script: `src/individual_statements_api_call.py`
- Endpoint: None
- Data files: `companies_all_tags_*.parquet.parquet`

It expands the `.xml` information in the financial statements into a readable table format. Revenue, Total Assets, and other financials for a given year.

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
