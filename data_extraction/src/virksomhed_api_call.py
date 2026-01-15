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
# Note: API uses HTTP (not HTTPS) as per documentation
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


def explode_addresses(json_data, address_field='beliggenhedsadresse'):
    """
    Explode address fields (beliggenhedsadresse or postadresse) which have a more complex structure.
    """
    print(f"Exploding {address_field}...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        addresses = vrvirksomhed.get(address_field, [])

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


def explode_virksomhedsform(json_data):
    """
    Explode the virksomhedsform (company legal form) field.
    """
    print("Exploding virksomhedsform...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        forms = vrvirksomhed.get('virksomhedsform', [])

        if isinstance(forms, list) and len(forms) > 0:
            for form in forms:
                form_record = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer,
                    'kortBeskrivelse': form.get('kortBeskrivelse'),
                    'langBeskrivelse': form.get('langBeskrivelse'),
                    'ansvarligDataleverandoer': form.get('ansvarligDataleverandoer'),
                    'gyldigFra': form.get('periode', {}).get('gyldigFra'),
                    'gyldigTil': form.get('periode', {}).get('gyldigTil'),
                    'sidstOpdateret': form.get('sidstOpdateret')
                }
                records.append(form_record)

    return pd.DataFrame(records)


def explode_livsforloeb(json_data):
    """
    Explode the livsforloeb (lifecycle) field which tracks company start/end dates.
    """
    print("Exploding livsforloeb...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        lifecycle = vrvirksomhed.get('livsforloeb', [])

        if isinstance(lifecycle, list) and len(lifecycle) > 0:
            for period in lifecycle:
                period_record = {
                    'cvrNummer': cvr_nummer,
                    'enhedsNummer': enheds_nummer,
                    'gyldigFra': period.get('periode', {}).get('gyldigFra'),
                    'gyldigTil': period.get('periode', {}).get('gyldigTil'),
                    'sidstOpdateret': period.get('sidstOpdateret')
                }
                records.append(period_record)

    return pd.DataFrame(records)


def explode_deltager_relation(json_data):
    """
    Explode the deltagerRelation (participant relations) field.
    This is a complex nested structure containing relations to owners, board members, etc.
    """
    print("Exploding deltagerRelation...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        relations = vrvirksomhed.get('deltagerRelation', [])

        if isinstance(relations, list) and len(relations) > 0:
            for rel in relations:
                # Get participant info (can be None)
                deltager = rel.get('deltager') or {}

                # Extract organization info from nested structures
                organisationer = rel.get('organisationer', [])
                for org in organisationer:
                    # Get organization attributes
                    org_hovedtype = org.get('hovedtype')

                    # Get organization names
                    org_navne = org.get('organisationsNavn', [])
                    org_navn = org_navne[0].get('navn') if org_navne else None

                    # Get member data (rolle/function within organization)
                    medlemsdata = org.get('medlemsData', [])
                    for medlem in medlemsdata:
                        attributter = medlem.get('attributter', [])

                        # Extract each attribute as a separate record
                        for attr in attributter:
                            rel_record = {
                                'cvrNummer': cvr_nummer,
                                'enhedsNummer': enheds_nummer,
                                'deltagerEnhedsNummer': deltager.get('enhedsNummer'),
                                'deltagerEnhedstype': deltager.get('enhedstype'),
                                'deltagerForretningsnoegle': deltager.get('forretningsnoegle'),
                                'organisationHovedtype': org_hovedtype,
                                'organisationNavn': org_navn,
                                'attributType': attr.get('type'),
                                'attributVapitype': attr.get('vapitype'),
                                'attributSekvensnr': attr.get('sekvensnr'),
                                'attributVaerdi': attr.get('vaerdier', [{}])[0].get('vaerdi') if attr.get('vaerdier') else None,
                                'gyldigFra': attr.get('periode', {}).get('gyldigFra'),
                                'gyldigTil': attr.get('periode', {}).get('gyldigTil'),
                                'sidstOpdateret': attr.get('sidstOpdateret')
                            }
                            records.append(rel_record)

                    # If no medlemsData, still record the organization relation
                    if not medlemsdata:
                        rel_record = {
                            'cvrNummer': cvr_nummer,
                            'enhedsNummer': enheds_nummer,
                            'deltagerEnhedsNummer': deltager.get('enhedsNummer'),
                            'deltagerEnhedstype': deltager.get('enhedstype'),
                            'deltagerForretningsnoegle': deltager.get('forretningsnoegle'),
                            'organisationHovedtype': org_hovedtype,
                            'organisationNavn': org_navn,
                            'attributType': None,
                            'attributVapitype': None,
                            'attributSekvensnr': None,
                            'attributVaerdi': None,
                            'gyldigFra': org.get('periode', {}).get('gyldigFra') if org.get('periode') else None,
                            'gyldigTil': org.get('periode', {}).get('gyldigTil') if org.get('periode') else None,
                            'sidstOpdateret': org.get('sidstOpdateret')
                        }
                        records.append(rel_record)

    return pd.DataFrame(records)


def explode_attributter(json_data):
    """
    Explode the attributter (company attributes) field.
    Contains capital, purpose, accounting period, etc.
    """
    print("Exploding attributter...")

    records = []

    for record in json_data:
        source = record.get("_source", {})
        vrvirksomhed = source.get("Vrvirksomhed", {})

        cvr_nummer = vrvirksomhed.get('cvrNummer')
        enheds_nummer = vrvirksomhed.get('enhedsNummer')
        attributter = vrvirksomhed.get('attributter', [])

        if isinstance(attributter, list) and len(attributter) > 0:
            for attr in attributter:
                # Get values array
                vaerdier = attr.get('vaerdier', [])

                for vaerdi in vaerdier:
                    attr_record = {
                        'cvrNummer': cvr_nummer,
                        'enhedsNummer': enheds_nummer,
                        'type': attr.get('type'),
                        'vapitype': attr.get('vapitype'),
                        'sekvensnr': attr.get('sekvensnr'),
                        'vaerdi': vaerdi.get('vaerdi'),
                        'gyldigFra': attr.get('periode', {}).get('gyldigFra'),
                        'gyldigTil': attr.get('periode', {}).get('gyldigTil'),
                        'sidstOpdateret': attr.get('sidstOpdateret')
                    }
                    records.append(attr_record)

                # If no vaerdier, still record the attribute
                if not vaerdier:
                    attr_record = {
                        'cvrNummer': cvr_nummer,
                        'enhedsNummer': enheds_nummer,
                        'type': attr.get('type'),
                        'vapitype': attr.get('vapitype'),
                        'sekvensnr': attr.get('sekvensnr'),
                        'vaerdi': None,
                        'gyldigFra': attr.get('periode', {}).get('gyldigFra'),
                        'gyldigTil': attr.get('periode', {}).get('gyldigTil'),
                        'sidstOpdateret': attr.get('sidstOpdateret')
                    }
                    records.append(attr_record)

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

    # Use a scroll with a reasonable keep-alive; can be tuned if needed
    scroll_keepalive = "5m"
    url = f"{company_data_api_endpoint}?scroll={scroll_keepalive}"
    credentials = f"{virk_username}:{virk_password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }
    
    # Timeout settings: connect timeout and read timeout (in seconds)
    # For large scrolls, we need generous timeouts
    timeout_connect = 30  # Connection timeout
    timeout_read = 300    # Read timeout (5 minutes for large responses)

    # Build query based on whether year is specified
    if year is not None:
        # Filter by last update date within the specified year
        query = {
            "size": size,
            # Recommended when using scroll to get a consistent view and avoid duplicates/skips
            "sort": ["_doc"],
            "track_total_hits": True,
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
            # Recommended when using scroll to get a consistent view and avoid duplicates/skips
            "sort": ["_doc"],
            "track_total_hits": True,
            "query": {
                "match_all": {}
            }
        }
        print("Retrieving all CVR permanent data...")

    # Initial request with timeout handling
    try:
        response = requests.post(url, json=query, headers=headers, timeout=(timeout_connect, timeout_read))
    except requests.exceptions.Timeout:
        print("Request timed out. The server may be slow or unresponsive.")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        return None

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

    # Log total hits if provided (can be "value" or dict depending on ES version)
    total_hits = response_data.get("hits", {}).get("total")
    if isinstance(total_hits, dict):
        total_hits_value = total_hits.get("value")
    else:
        total_hits_value = total_hits
    if total_hits_value is not None:
        print(f"Total hits reported by server: {total_hits_value}")

    try:
        while len(hits) > 0:
            scroll_url = "http://distribution.virk.dk/_search/scroll"
            scroll_query = {
                "scroll": scroll_keepalive,
                "scroll_id": scroll_id
            }
            
            # Scroll request with timeout and retry logic
            max_retries = 3
            retry_count = 0
            scroll_response = None
            
            while retry_count < max_retries:
                try:
                    scroll_response = requests.post(
                        scroll_url, 
                        json=scroll_query, 
                        headers=headers,
                        timeout=(timeout_connect, timeout_read)
                    )
                    break  # Success, exit retry loop
                except requests.exceptions.Timeout:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Scroll request timed out. Retrying ({retry_count}/{max_retries})...")
                    else:
                        print("Scroll request timed out after multiple retries. Stopping scroll.")
                        break
                except requests.exceptions.ConnectionError as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Connection error during scroll. Retrying ({retry_count}/{max_retries})...")
                    else:
                        print(f"Connection error after multiple retries: {e}. Stopping scroll.")
                        break
            
            if scroll_response is None or scroll_response.status_code != 200:
                if scroll_response:
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
    finally:
        # Best-effort scroll cleanup to release server-side resources
        if scroll_id is not None:
            try:
                cleanup_url = "http://distribution.virk.dk/_search/scroll"
                cleanup_body = {"scroll_id": [scroll_id]}
                cleanup_response = requests.delete(
                    cleanup_url, 
                    json=cleanup_body, 
                    headers=headers,
                    timeout=(timeout_connect, 10)  # Shorter timeout for cleanup
                )
                if cleanup_response.status_code != 200:
                    print(f"Scroll cleanup failed with status code: {cleanup_response.status_code}")
            except Exception as e:
                print(f"Exception during scroll cleanup: {e}")

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
        df_beliggenhedsadresse = explode_addresses(all_results, 'beliggenhedsadresse')
        df_postadresse = explode_addresses(all_results, 'postadresse')
        df_hovedbranche = explode_branches(all_results, 'hovedbranche')
        df_bibranche1 = explode_branches(all_results, 'bibranche1')
        df_bibranche2 = explode_branches(all_results, 'bibranche2')
        df_bibranche3 = explode_branches(all_results, 'bibranche3')
        df_aarsbeskaeftigelse = explode_employment(all_results, 'aarsbeskaeftigelse')
        df_kvartalsbeskaeftigelse = explode_employment(all_results, 'kvartalsbeskaeftigelse')
        df_maanedsbeskaeftigelse = explode_employment(all_results, 'maanedsbeskaeftigelse')
        df_virksomhedsstatus = explode_temporal_field(all_results, 'virksomhedsstatus', ['status'])

        # Contact information
        df_telefonNummer = explode_temporal_field(all_results, 'telefonNummer', ['kontaktoplysning'])
        df_telefaxNummer = explode_temporal_field(all_results, 'telefaxNummer', ['kontaktoplysning'])
        df_elektroniskPost = explode_temporal_field(all_results, 'elektroniskPost', ['kontaktoplysning'])
        df_hjemmeside = explode_temporal_field(all_results, 'hjemmeside', ['kontaktoplysning'])

        # Company form and registration
        df_virksomhedsform = explode_virksomhedsform(all_results)
        df_regNummer = explode_temporal_field(all_results, 'regNummer', ['regnummer'])
        df_livsforloeb = explode_livsforloeb(all_results)

        # Complex nested fields
        df_deltagerRelation = explode_deltager_relation(all_results)
        df_attributter = explode_attributter(all_results)

        # Save all dataframes
        if save_format.lower() == "parquet":
            base_path = os.path.join(company_data_folder_path, output_filename)

            df_main.to_parquet(f"{base_path}_main.parquet", index=False)
            df_navne.to_parquet(f"{base_path}_navne.parquet", index=False)
            df_binavne.to_parquet(f"{base_path}_binavne.parquet", index=False)
            df_beliggenhedsadresse.to_parquet(f"{base_path}_beliggenhedsadresse.parquet", index=False)
            df_postadresse.to_parquet(f"{base_path}_postadresse.parquet", index=False)
            df_hovedbranche.to_parquet(f"{base_path}_hovedbranche.parquet", index=False)
            df_bibranche1.to_parquet(f"{base_path}_bibranche1.parquet", index=False)
            df_bibranche2.to_parquet(f"{base_path}_bibranche2.parquet", index=False)
            df_bibranche3.to_parquet(f"{base_path}_bibranche3.parquet", index=False)
            df_aarsbeskaeftigelse.to_parquet(f"{base_path}_aarsbeskaeftigelse.parquet", index=False)
            df_kvartalsbeskaeftigelse.to_parquet(f"{base_path}_kvartalsbeskaeftigelse.parquet", index=False)
            df_maanedsbeskaeftigelse.to_parquet(f"{base_path}_maanedsbeskaeftigelse.parquet", index=False)
            df_virksomhedsstatus.to_parquet(f"{base_path}_virksomhedsstatus.parquet", index=False)
            df_telefonNummer.to_parquet(f"{base_path}_telefonNummer.parquet", index=False)
            df_telefaxNummer.to_parquet(f"{base_path}_telefaxNummer.parquet", index=False)
            df_elektroniskPost.to_parquet(f"{base_path}_elektroniskPost.parquet", index=False)
            df_hjemmeside.to_parquet(f"{base_path}_hjemmeside.parquet", index=False)
            df_virksomhedsform.to_parquet(f"{base_path}_virksomhedsform.parquet", index=False)
            df_regNummer.to_parquet(f"{base_path}_regNummer.parquet", index=False)
            df_livsforloeb.to_parquet(f"{base_path}_livsforloeb.parquet", index=False)
            df_deltagerRelation.to_parquet(f"{base_path}_deltagerRelation.parquet", index=False)
            df_attributter.to_parquet(f"{base_path}_attributter.parquet", index=False)

            print(f"\nSaved {22} parquet files to {company_data_folder_path}")
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
        print(f"  - Business address (beliggenhedsadresse): {df_beliggenhedsadresse.shape}")
        print(f"  - Postal address (postadresse): {df_postadresse.shape}")
        print(f"  - Main branch: {df_hovedbranche.shape}")
        print(f"  - Secondary branches (1-3): {df_bibranche1.shape}, {df_bibranche2.shape}, {df_bibranche3.shape}")
        print(f"  - Employment (year/quarter/month): {df_aarsbeskaeftigelse.shape}, {df_kvartalsbeskaeftigelse.shape}, {df_maanedsbeskaeftigelse.shape}")
        print(f"  - Company status (virksomhedsstatus): {df_virksomhedsstatus.shape}")
        print(f"  - Phone (telefonNummer): {df_telefonNummer.shape}")
        print(f"  - Fax (telefaxNummer): {df_telefaxNummer.shape}")
        print(f"  - Email (elektroniskPost): {df_elektroniskPost.shape}")
        print(f"  - Website (hjemmeside): {df_hjemmeside.shape}")
        print(f"  - Company form (virksomhedsform): {df_virksomhedsform.shape}")
        print(f"  - Registration number (regNummer): {df_regNummer.shape}")
        print(f"  - Lifecycle (livsforloeb): {df_livsforloeb.shape}")
        print(f"  - Participant relations (deltagerRelation): {df_deltagerRelation.shape}")
        print(f"  - Attributes (attributter): {df_attributter.shape}")

        return {
            'main': df_main,
            'navne': df_navne,
            'binavne': df_binavne,
            'beliggenhedsadresse': df_beliggenhedsadresse,
            'postadresse': df_postadresse,
            'hovedbranche': df_hovedbranche,
            'bibranche1': df_bibranche1,
            'bibranche2': df_bibranche2,
            'bibranche3': df_bibranche3,
            'aarsbeskaeftigelse': df_aarsbeskaeftigelse,
            'kvartalsbeskaeftigelse': df_kvartalsbeskaeftigelse,
            'maanedsbeskaeftigelse': df_maanedsbeskaeftigelse,
            'virksomhedsstatus': df_virksomhedsstatus,
            'telefonNummer': df_telefonNummer,
            'telefaxNummer': df_telefaxNummer,
            'elektroniskPost': df_elektroniskPost,
            'hjemmeside': df_hjemmeside,
            'virksomhedsform': df_virksomhedsform,
            'regNummer': df_regNummer,
            'livsforloeb': df_livsforloeb,
            'deltagerRelation': df_deltagerRelation,
            'attributter': df_attributter
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
