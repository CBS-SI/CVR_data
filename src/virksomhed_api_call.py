import os
import json
import base64
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from funcy import print_durations

load_dotenv()

# Environment Variables
VIRK_USERNAME = os.getenv("VIRK_USERNAME")
VIRK_PASSWORD = os.getenv("VIRK_PASSWORD")
COMPANY_DATA_FOLDER_PATH = os.getenv("COMPANY_DATA_FOLDER_PATH")
OUTPUT_FILENAME = "virksomhed"

# API ENDPOINT
COMPANY_DATA_API_ENDPOINT = "http://distribution.virk.dk/cvr-permanent/virksomhed/_search"


def create_main_dataframe(json_data):
    """
    Create main company dataframe with non-temporal fields only.
    This excludes list-based temporal fields.
    """
    print("Creating main company dataframe with simple fields...")

    # Extract the _source field from each record
    sources = [record.get("_source", {}) for record in json_data]

    # Flatten the nested structure
    df_flat = pd.json_normalize(sources, sep='_')

    # Select only non-list columns (simple fields)
    simple_cols = []
    for col in df_flat.columns:
        # Check if the column contains lists
        if not df_flat[col].apply(lambda x: isinstance(x, list)).any():
            simple_cols.append(col)

    df_main = df_flat[simple_cols].copy()

    # Add metadata from top-level
    metadata_df = pd.json_normalize(json_data, sep='_')
    metadata_cols = [col for col in metadata_df.columns if col.startswith('_') and not col.startswith('_source')]

    if metadata_cols:
        for col in metadata_cols:
            df_main.insert(0, col, metadata_df[col].values)

    return df_main


def explode_temporal_field(json_data, field_name, value_cols):
    """
    Explode a temporal field from nested lists into panel format.

    Args:
        json_data: List of records from API
        field_name: Name of the field in Vrvirksomhed to explode (e.g., 'navne', 'binavne')
        value_cols: List of column names to extract from each record (e.g., ['navn'])

    Returns:
        DataFrame with one row per temporal record
    """
    print(f"Exploding temporal field: {field_name}...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        temporal_data = vrvirksomhed.get(field_name, [])

        if isinstance(temporal_data, list) and len(temporal_data) > 0:
            for item in temporal_data:
                record_data = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer
                }

                # Extract value columns
                for col in value_cols:
                    record_data[col] = item.get(col)

                # Extract temporal info
                periode = item.get('periode', {})
                record_data['gyldigFra'] = periode.get('gyldigFra')
                record_data['gyldigTil'] = periode.get('gyldigTil')
                record_data['sidstOpdateret'] = item.get('sidstOpdateret')

                records.append(record_data)

    return pd.DataFrame(records)


def explode_addresses(json_data):
    """
    Explode the beliggenhedsadresse (address) field which has a more complex structure.
    """
    print("Exploding addresses...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        addresses = vrvirksomhed.get('beliggenhedsadresse', [])

        if isinstance(addresses, list) and len(addresses) > 0:
            for addr in addresses:
                # Extract kommune information if it exists
                kommune = addr.get('kommune', {})
                if isinstance(kommune, dict):
                    kommune_kode = kommune.get('kommuneKode')
                    kommune_navn = kommune.get('kommuneNavn')
                    kommune_periode_fra = kommune.get('periode', {}).get('gyldigFra')
                    kommune_periode_til = kommune.get('periode', {}).get('gyldigTil')
                else:
                    kommune_kode = None
                    kommune_navn = None
                    kommune_periode_fra = None
                    kommune_periode_til = None

                addr_record = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer,
                    'landekode': addr.get('landekode'),
                    'fritekst': addr.get('fritekst'),
                    'vejkode': addr.get('vejkode'),
                    'vejnavn': addr.get('vejnavn'),
                    'husnummerFra': addr.get('husnummerFra'),
                    'husnummerTil': addr.get('husnummerTil'),
                    'bogstavFra': addr.get('bogstavFra'),
                    'bogstavTil': addr.get('bogstavTil'),
                    'etage': addr.get('etage'),
                    'sidedoer': addr.get('sidedoer'),
                    'conavn': addr.get('conavn'),
                    'postboks': addr.get('postboks'),
                    'postnummer': addr.get('postnummer'),
                    'postdistrikt': addr.get('postdistrikt'),
                    'bynavn': addr.get('bynavn'),
                    'adresseId': addr.get('adresseId'),
                    'sidstValideret': addr.get('sidstValideret'),
                    'kommuneKode': kommune_kode,
                    'kommuneNavn': kommune_navn,
                    'gyldigFra': addr.get('periode', {}).get('gyldigFra'),
                    'gyldigTil': addr.get('periode', {}).get('gyldigTil'),
                    'sidstOpdateret': addr.get('sidstOpdateret')
                }
                records.append(addr_record)

    return pd.DataFrame(records)


def explode_branches(json_data, branch_field):
    """
    Explode branch (branche) fields: hovedbranche, bibranche1, bibranche2, bibranche3.
    """
    print(f"Exploding {branch_field}...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        branches = vrvirksomhed.get(branch_field, [])

        if isinstance(branches, list) and len(branches) > 0:
            for branch in branches:
                branch_record = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer,
                    'branchekode': branch.get('branchekode'),
                    'branchetekst': branch.get('branchetekst'),
                    'gyldigFra': branch.get('periode', {}).get('gyldigFra'),
                    'gyldigTil': branch.get('periode', {}).get('gyldigTil'),
                    'sidstOpdateret': branch.get('sidstOpdateret')
                }
                records.append(branch_record)

    return pd.DataFrame(records)


def explode_employment(json_data, employment_field):
    """
    Explode employment (beskaeftigelse) fields: aarsbeskaeftigelse, kvartalsbeskaeftigelse, maanedsbeskaeftigelse.
    """
    print(f"Exploding {employment_field}...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        employment_data = vrvirksomhed.get(employment_field, [])

        if isinstance(employment_data, list) and len(employment_data) > 0:
            for emp in employment_data:
                emp_record = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer,
                    'aar': emp.get('aar'),
                    'kvartal': emp.get('kvartal'),
                    'maaned': emp.get('maaned'),
                    'antalInklusivEjere': emp.get('antalInklusivEjere'),
                    'antalAarsvaerk': emp.get('antalAarsvaerk'),
                    'antalAnsatte': emp.get('antalAnsatte'),
                    'intervalKodeAntalInklusivEjere': emp.get('intervalKodeAntalInklusivEjere'),
                    'intervalKodeAntalAarsvaerk': emp.get('intervalKodeAntalAarsvaerk'),
                    'intervalKodeAntalAnsatte': emp.get('intervalKodeAntalAnsatte'),
                    'sidstOpdateret': emp.get('sidstOpdateret')
                }
                records.append(emp_record)

    return pd.DataFrame(records)


def flatten_permanent_data_wide(json_data):
    """
    Flatten the nested JSON structure into a single wide dataframe.
    WARNING: This will lose temporal/historical information as it only keeps the flattened structure.
    Use panel format instead if you need temporal data.
    """
    print("Flattening nested JSON structure to wide format...")
    print("WARNING: Temporal/historical information in list fields will be converted to string representation")

    # Extract the _source field from each record
    sources = [record.get("_source", {}) for record in json_data]

    # Use pandas json_normalize to flatten the nested structure
    df_flattened = pd.json_normalize(sources, sep='_')

    # Convert list columns to string representation
    for col in df_flattened.columns:
        if df_flattened[col].apply(lambda x: isinstance(x, list)).any():
            df_flattened[col] = df_flattened[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

    # Also include the top-level metadata fields
    metadata_df = pd.json_normalize(json_data, sep='_')

    # Select only the metadata columns we want to keep
    metadata_cols = [col for col in metadata_df.columns if col.startswith('_') and not col.startswith('_source')]
    if metadata_cols:
        metadata_df = metadata_df[metadata_cols]

        # Combine metadata with flattened source data
        df_combined = pd.concat([metadata_df.reset_index(drop=True),
                                df_flattened.reset_index(drop=True)], axis=1)
    else:
        df_combined = df_flattened

    return df_combined


@print_durations()
def main(virk_username=VIRK_USERNAME,
         virk_password=VIRK_PASSWORD,
         company_data_api_endpoint=COMPANY_DATA_API_ENDPOINT,
         company_data_folder_path=COMPANY_DATA_FOLDER_PATH,
         output_filename=OUTPUT_FILENAME,
         size=3000,
         year=None,
         save_format="parquet",
         output_mode="panel"):
    """
    Download CVR permanent data from Virk API.

    Args:
        output_mode: "panel" for multiple dataframes (recommended), "wide" for single wide dataframe
    """

    url = f"{company_data_api_endpoint}?scroll=1m"
    credentials = f"{virk_username}:{virk_password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

    # Build query based on whether year is specified
    if year is not None:
        # Filter by last update date within the specified year
        query = {
            "size": size,
            "query": {
                "range": {
                    "Vrvirksomhed.sidstOpdateret": {
                        "gte": f"{year}-01-01",
                        "lte": f"{year}-12-31"
                    }
                }
            }
        }
        # Update filename to include year
        output_filename = f"{output_filename}_{year}"
        print(f"Filtering data for year: {year}")
    else:
        # Original query to get all data
        query = {
            "size": size,
            "query": {
                "match_all": {}
            }
        }
        print("Retrieving all CVR permanent data...")

    response = requests.post(url, json=query, headers=headers)

    if response.status_code != 200:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return None
    else:
        print("Starting data retrieval...")

    response_data = response.json()

    # Scroll to get all data
    if '_scroll_id' not in response_data:
        print("No scroll ID found in the response.")
        return None

    scroll_id = response_data['_scroll_id']
    hits = response_data['hits']['hits']
    all_results = hits

    while len(hits) > 0:
        scroll_url = "http://distribution.virk.dk/_search/scroll"
        scroll_query = {
            "scroll": "1m",
            "scroll_id": scroll_id
        }
        scroll_response = requests.post(scroll_url, json=scroll_query, headers=headers)

        if scroll_response.status_code != 200:
            print(f"Scroll request failed with status code: {scroll_response.status_code}")
            print(scroll_response.text)
            break

        scroll_data = scroll_response.json()

        if '_scroll_id' not in scroll_data:
            print("No scroll ID found in the scroll response.")
            break

        scroll_id = scroll_data['_scroll_id']
        hits = scroll_data['hits']['hits']
        all_results.extend(hits)

        # Print progress
        print(f"Retrieved {len(all_results)} records so far...")

    print(f"API call completed. Total records retrieved: {len(all_results)}")

    # Process data based on output mode
    if output_mode == "panel":
        print("\nCreating multiple panel dataframes...")

        # Create main dataframe
        df_main = create_main_dataframe(all_results)
        print(f"Main dataframe shape: {df_main.shape}")

        # Create panel dataframes for temporal fields
        df_navne = explode_temporal_field(all_results, 'navne', ['navn'])
        df_binavne = explode_temporal_field(all_results, 'binavne', ['navn'])
        df_addresses = explode_addresses(all_results)
        df_hovedbranche = explode_branches(all_results, 'hovedbranche')
        df_bibranche1 = explode_branches(all_results, 'bibranche1')
        df_bibranche2 = explode_branches(all_results, 'bibranche2')
        df_bibranche3 = explode_branches(all_results, 'bibranche3')
        df_aarsbeskaeftigelse = explode_employment(all_results, 'aarsbeskaeftigelse')
        df_kvartalsbeskaeftigelse = explode_employment(all_results, 'kvartalsbeskaeftigelse')
        df_maanedsbeskaeftigelse = explode_employment(all_results, 'maanedsbeskaeftigelse')

        # Save all dataframes
        if save_format.lower() == "parquet":
            base_path = os.path.join(company_data_folder_path, output_filename)

            df_main.to_parquet(f"{base_path}_main.parquet", index=False)
            df_navne.to_parquet(f"{base_path}_navne.parquet", index=False)
            df_binavne.to_parquet(f"{base_path}_binavne.parquet", index=False)
            df_addresses.to_parquet(f"{base_path}_addresses.parquet", index=False)
            df_hovedbranche.to_parquet(f"{base_path}_hovedbranche.parquet", index=False)
            df_bibranche1.to_parquet(f"{base_path}_bibranche1.parquet", index=False)
            df_bibranche2.to_parquet(f"{base_path}_bibranche2.parquet", index=False)
            df_bibranche3.to_parquet(f"{base_path}_bibranche3.parquet", index=False)
            df_aarsbeskaeftigelse.to_parquet(f"{base_path}_aarsbeskaeftigelse.parquet", index=False)
            df_kvartalsbeskaeftigelse.to_parquet(f"{base_path}_kvartalsbeskaeftigelse.parquet", index=False)
            df_maanedsbeskaeftigelse.to_parquet(f"{base_path}_maanedsbeskaeftigelse.parquet", index=False)

            print(f"\nSaved {11} parquet files to {company_data_folder_path}")
        else:
            # Save as JSON
            file_path = os.path.join(company_data_folder_path, f"{output_filename}_raw.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=4)
            print(f"Saved raw JSON to {file_path}")

        print("\nPanel dataframes created:")
        print(f"  - Main: {df_main.shape}")
        print(f"  - Names (navne): {df_navne.shape}")
        print(f"  - Secondary names (binavne): {df_binavne.shape}")
        print(f"  - Addresses: {df_addresses.shape}")
        print(f"  - Main branch: {df_hovedbranche.shape}")
        print(f"  - Secondary branches (1-3): {df_bibranche1.shape}, {df_bibranche2.shape}, {df_bibranche3.shape}")
        print(f"  - Employment (year/quarter/month): {df_aarsbeskaeftigelse.shape}, {df_kvartalsbeskaeftigelse.shape}, {df_maanedsbeskaeftigelse.shape}")

        return {
            'main': df_main,
            'navne': df_navne,
            'binavne': df_binavne,
            'addresses': df_addresses,
            'hovedbranche': df_hovedbranche,
            'bibranche1': df_bibranche1,
            'bibranche2': df_bibranche2,
            'bibranche3': df_bibranche3,
            'aarsbeskaeftigelse': df_aarsbeskaeftigelse,
            'kvartalsbeskaeftigelse': df_kvartalsbeskaeftigelse,
            'maanedsbeskaeftigelse': df_maanedsbeskaeftigelse
        }

    else:  # wide format
        df_flattened = flatten_permanent_data_wide(all_results)
        print(f"Flattened DataFrame shape: {df_flattened.shape}")
        print(f"Columns: {len(df_flattened.columns)}")

        # Save as parquet or json based on save_format parameter
        if save_format.lower() == "parquet":
            file_path = os.path.join(company_data_folder_path, f"{output_filename}_wide.parquet")
            print(f"Saving data as parquet to {file_path}...")
            df_flattened.to_parquet(file_path, index=False)
        else:
            # Fallback to JSON
            file_path = os.path.join(company_data_folder_path, f"{output_filename}_raw.json")
            print(f"Saving data as JSON to {file_path}...")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=4)

        print("Data saved successfully!")
        return df_flattened


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download CVR permanent data from Virk API')
    parser.add_argument('--year', type=int, help='Filter data by specific year')
    parser.add_argument('--format', choices=['parquet', 'json'], default='parquet',
                        help='Output format (default: parquet)')
    parser.add_argument('--mode', choices=['panel', 'wide'], default='panel',
                        help='Output mode: panel (multiple files with temporal data) or wide (single flat file). Default: panel')
    args = parser.parse_args()

    main(year=args.year, save_format=args.format, output_mode=args.mode)
