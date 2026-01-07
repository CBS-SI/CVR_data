"""
(!) THE ORIGINAL API HAS BEEN SHUT DOWN. THIS SCRIPT IS FOR REFERENCE. (!)

This script:
1. Fetches XML URLs for a given year.
2. Downloads and parses XBRL XML files in batches.
3. Directly transforms the data into wide format (pivot table).
4. Outputs companies_all_tags_{year}.parquet files

"""

import os
import httpx
import asyncio
import argparse
import pandas as pd
from dotenv import load_dotenv
from funcy import print_durations
import xml.etree.ElementTree as ET
from tqdm.asyncio import tqdm_asyncio

load_dotenv()

# --- Environment Variables ---
FS_FOLDER_PATH = os.getenv("FS_FOLDER_PATH") # "FS=financial_statements"
INPUT_FILENAME = "financial_statements"

EFS_FOLDER_PATH = os.getenv("EFS_FOLDER_PATH") # "EFS=expanded_financial_statements"
OUTPUT_FILENAME = "companies_all_tags"

# --- Argument Parsing ---
def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Download and process company financial data into wide format for specified year range'
    )
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        metavar=('YEAR', 'END_YEAR'),
        required=True,
        help='Provide one year (e.g., 2005) or a range (e.g., 2005 2007)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of URLs to process per batch (default: 1000)'
    )
    args = parser.parse_args()

    if len(args.years) == 1:
        args.start_year = args.end_year = args.years[0]
    elif len(args.years) == 2:
        args.start_year, args.end_year = args.years
        if args.start_year > args.end_year:
            parser.error("Start year must not be greater than end year.")
    else:
        parser.error("Please provide a year(s)")

    args.years = list(range(args.start_year, args.end_year + 1))

    return args


def get_xml_dataframe(fs_folder_path=FS_FOLDER_PATH, input_filename=INPUT_FILENAME):
    """Load the financial statements metadata file."""
    input_path = os.path.join(fs_folder_path, f"{input_filename}.parquet")
    df = pd.read_parquet(input_path)
    return df


def get_xml_urls_by_year(data, year):
    """Extract XML URLs for a specific year."""
    df = data.copy()
    df["regnskabsperiode_slutDato"] = pd.to_datetime(df["regnskabsperiode_slutDato"])
    cond_1 = df['regnskabsperiode_slutDato'].dt.year == year
    cond_2 = ~df['AARSRAPPORT_xml'].isna()
    xml_urls = df[cond_1 & cond_2]["AARSRAPPORT_xml"].unique().tolist()
    return xml_urls


async def fetch_xml(client, url):
    """Fetch a single XML file from URL."""
    try:
        response = await client.get(url, timeout=60.0)
        response.raise_for_status()
        return url, response.content
    except httpx.RequestError as exc:
        print(f"Error occurred while requesting {exc.request.url!r}: {exc}")
        return url, None
    except Exception as e:
        print(f"Unexpected error for {url}: {e}")
        return url, None


def parse_xml_content(xml_content):
    """Parse XBRL XML content and return a DataFrame."""
    if xml_content is None:
        return pd.DataFrame()

    try:
        root = ET.fromstring(xml_content)

        ns = {
            "xbrli": "http://www.xbrl.org/2003/instance",
            "gsd": "http://xbrl.dcca.dk/gsd",
            "sob": "http://xbrl.dcca.dk/sob",
            "cmn": "http://xbrl.dcca.dk/cmn",
            "mrv": "http://xbrl.dcca.dk/mrv",
            "fsa": "http://xbrl.dcca.dk/fsa"
        }

        # Extract context information
        contexts = {}
        for context in root.findall("xbrli:context", ns):
            context_id = context.get("id")
            period = context.find("xbrli:period", ns)
            entity = context.find("xbrli:entity", ns)

            identifier_elem = entity.find("xbrli:identifier", ns)
            identifier = identifier_elem.text if identifier_elem is not None else ""

            start_date = period.findtext("xbrli:startDate", default="", namespaces=ns)
            end_date = period.findtext("xbrli:endDate", default="", namespaces=ns)
            instant = period.findtext("xbrli:instant", default="", namespaces=ns)

            contexts[context_id] = {
                "identifier": identifier,
                "start_date": start_date,
                "end_date": end_date,
                "instant": instant,
            }

        # Extract all financial data elements
        records = []
        for elem in root.iter():
            if 'contextRef' in elem.attrib:
                tag = elem.tag.split("}")[-1]
                value = elem.text
                context_ref = elem.attrib['contextRef']
                unit_ref = elem.attrib.get('unitRef', '')
                decimals = elem.attrib.get('decimals', '')
                records.append({
                    "tag": tag,
                    "value": value,
                    "contextRef": context_ref,
                    "unitRef": unit_ref,
                    "decimals": decimals,
                    **contexts.get(context_ref, {})
                })
        return pd.DataFrame(records)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error during XML parsing: {e}")
        return pd.DataFrame()


async def process_urls_async(urls_batch):
    """Fetch and parse a batch of XML URLs asynchronously."""
    all_dfs = []
    async with httpx.AsyncClient() as client:
        tasks = [fetch_xml(client, url) for url in urls_batch]
        for url, xml_content in await tqdm_asyncio.gather(*tasks, desc="Fetching and Parsing XMLs"):
            if xml_content:
                df_company = parse_xml_content(xml_content)
                if not df_company.empty:
                    all_dfs.append(df_company)
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def transform_to_wide_format(df, year):
    """Transform long format DataFrame to wide format (pivot table)."""
    if df.empty:
        return pd.DataFrame()

    print("Formatting data to wide format...")

    # Clean and prepare data
    df["tag"] = df["tag"].astype(str).str.strip()
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df["instant"] = pd.to_datetime(df["instant"], errors="coerce")

    # Apply filters to match the specified year
    cond = (df["end_date"].dt.year == year) | (df["instant"].dt.year == year)
    df = df[cond]

    if df.empty:
        print(f"Warning: No data found for year {year} after filtering.")
        return pd.DataFrame()

    # Create pivot table
    pivot = df.pivot_table(
        index="identifier",
        columns="tag",
        values="value",
        aggfunc="first"
    ).reset_index()

    pivot.columns.name = None

    # Add year indicator
    pivot["Year"] = year

    return pivot


@print_durations()
def download_and_process_year(year: int,
                               fs_folder_path=FS_FOLDER_PATH,
                               input_filename=INPUT_FILENAME,
                               efs_folder_path=EFS_FOLDER_PATH,
                               output_filename=OUTPUT_FILENAME,
                               batch_size: int = 1000):
    """Download XML data for a year and directly produce wide format output."""

    print(f"\n{'='*60}")
    print(f"Processing year: {year}")
    print(f"{'='*60}\n")

    print("Reading local virk.dk financial statement metadata file...")
    xml_df = get_xml_dataframe(fs_folder_path, input_filename)
    xml_urls = get_xml_urls_by_year(xml_df, year)

    total_urls = len(xml_urls)
    print(f"Total number of XML URLs to process: {total_urls}")

    if total_urls == 0:
        print(f"No XML URLs found for year {year}.")
        return

    # Process in batches and accumulate data
    num_batches = (total_urls + batch_size - 1) // batch_size
    all_batch_dfs = []

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_urls)
        current_batch_urls = xml_urls[start_index:end_index]

        print(f"\nProcessing Batch {i+1}/{num_batches} (URLs {start_index} to {end_index-1})...")

        df_batch = asyncio.run(process_urls_async(current_batch_urls))

        if not df_batch.empty:
            print(f"Batch {i+1} completed. Total rows in this batch: {len(df_batch)}")
            all_batch_dfs.append(df_batch)
        else:
            print(f"No data extracted for Batch {i+1}.")

    # Combine all batches
    if not all_batch_dfs:
        print(f"No data extracted for year {year} across all batches.")
        return

    print(f"\nCombining {len(all_batch_dfs)} batches...")
    df_combined = pd.concat(all_batch_dfs, ignore_index=True)
    print(f"Total rows combined: {len(df_combined)}")

    # Transform to wide format
    print("\nTransforming to wide format...")
    df_wide = transform_to_wide_format(df_combined, year)

    if df_wide.empty:
        print(f"No data in wide format for year {year}.")
        return

    # Save final output
    output_path = os.path.join(efs_folder_path, f"{output_filename}_{year}.parquet")
    print(f"\nSaving final wide format data to: {output_path}")
    df_wide.to_parquet(output_path, index=False)
    print(f"✓ Successfully saved! Total companies: {len(df_wide)}, Total columns: {len(df_wide.columns)}")


# --- Main ---
def main():
    args = parse_arguments()
    years = args.years
    batch_size = args.batch_size

    print(f"Years to process: {years}")
    print(f"Batch size: {batch_size}")

    for year in years:
        try:
            download_and_process_year(year=year, batch_size=batch_size)
        except Exception as e:
            print(f"Error processing year {year}: {e}")
            continue

    print(f"\n{'='*60}")
    print("All years processed!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
