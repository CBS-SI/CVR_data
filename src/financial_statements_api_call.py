"""
This script creates a financial_statements.parquet file
with the financial statements of Danish companies. It downloads
the financial data, flattens nested structures, and saves as parquet
"""

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
FS_FOLDER_PATH = os.getenv("FS_FOLDER_PATH") # "FS=financial_statements"
OUTPUT_FILENAME = "financial_statements"

# API ENDPOINT
FINANCIAL_STATMENTS_API_ENDPOINT = "http://distribution.virk.dk/offentliggoerelser/_search"


def flatten_financial_data(json_data):
    """
    Flatten the nested JSON structure from the financial statements API
    """
    print("Flattening nested JSON structure...")

    # Extract the _source field from each record
    sources = [record.get("_source", {}) for record in json_data]

    # Handle the nested "dokumenter" field specially
    processed_sources = []
    for source in sources:
        # Make a copy to avoid modifying original
        processed_source = source.copy()

        # Extract dokumenter information and flatten it
        dokumenter = source.get("dokumenter", [])

        # Remove original dokumenter field to avoid conflicts
        if "dokumenter" in processed_source:
            del processed_source["dokumenter"]

        # Create document URL fields for each document type and MIME type
        for doc in dokumenter:
            doc_type = doc.get("dokumentType", "UNKNOWN")
            mime_type = doc.get("dokumentMimeType", "")
            doc_url = doc.get("dokumentUrl", "")

            # Map MIME types to readable formats
            format_map = {
                "application/xhtml+xml": "html",
                "application/pdf": "pdf",
                "application/xml": "xml",
                "image/tiff": "tiff"
            }
            format_name = format_map.get(mime_type, "unknown")

            # Create column name like "AARSRAPPORT_pdf" or "REGNSKAB_html"
            if doc_type and format_name != "unknown":
                col_name = f"{doc_type}_{format_name}"
                processed_source[col_name] = doc_url

        processed_sources.append(processed_source)

    # Use pandas json_normalize to flatten the remaining nested structure
    df_flattened = pd.json_normalize(processed_sources, sep='_')

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


@print_durations() # 25 min to 1 hour 500 Mb/s high speed internet
def main(virk_username=VIRK_USERNAME,
         virk_password=VIRK_PASSWORD,
         financial_statments_api_endpoint=FINANCIAL_STATMENTS_API_ENDPOINT,
         fs_folder_path=FS_FOLDER_PATH,
         output_filename=OUTPUT_FILENAME,
         size=3000,
         year=None,
         save_format="parquet"):

    url = f"{financial_statments_api_endpoint}?scroll=1m"
    credentials = f"{virk_username}:{virk_password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json"
    }

    # Build query based on whether year is specified
    if year is not None:
        # Filter by accounting period dates within the specified year
        query = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {
                            "range": {
                                "regnskab.regnskabsperiode.startDato": {
                                    "gte": f"{year}-01-01",
                                    "lte": f"{year}-12-31"
                                }
                            }
                        },
                        {
                            "range": {
                                "regnskab.regnskabsperiode.slutDato": {
                                    "gte": f"{year}-01-01",
                                    "lte": f"{year}-12-31"
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
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
        print("Retrieving all financial data...")

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

    print(f"API call completed. Total records retrieved: {len(all_results)}")

    # Flatten the nested JSON structure
    df_flattened = flatten_financial_data(all_results)

    print(f"Flattened DataFrame shape: {df_flattened.shape}")
    print(f"Columns: {len(df_flattened.columns)}")

    # Save as parquet or json based on save_format parameter
    if save_format.lower() == "parquet":
        file_path = os.path.join(fs_folder_path, f"{output_filename}.parquet")
        print(f"Saving data as parquet to {file_path}...")
        df_flattened.to_parquet(file_path, index=False)
    else:
        # Fallback to JSON for backward compatibility
        file_path = os.path.join(fs_folder_path, f"{output_filename}.json")
        print(f"Saving data as JSON to {file_path}...")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=4)

    print("Data saved successfully!")
    return df_flattened


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download financial statements from Virk API')
    parser.add_argument('--year', type=int, help='Filter data by specific year')
    parser.add_argument('--format', choices=['parquet', 'json'], default='parquet',
                        help='Output format (default: parquet)')
    args = parser.parse_args()

    main(year=args.year, save_format=args.format)
