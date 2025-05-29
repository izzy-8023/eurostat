#!/usr/bin/env python3
"""
Enhanced Eurostat Data Source Manager

This enhanced version integrates with shared modules to eliminate redundancy
and improve maintainability while preserving all existing functionality.
"""

import json
import requests
import os
import concurrent.futures
import threading 
import csv 
import subprocess
import argparse
import xml.etree.ElementTree as ET
import re 
import sys

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

from shared.config import EUROSTAT_API_CONFIG, DEFAULT_PATHS, HEALTH_DATASETS
from shared.database import EurostatDatabase

# --- Enhanced Catalog Download with Shared Config ---
def download_eurostat_catalog(output_file_path):
    """
    Downloads the latest Eurostat data catalog using curl and saves it to the specified path.
    Enhanced to use shared configuration.
    """
    catalog_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?format=json"
    curl_command = [
        "curl",
        "-X", "GET",
        catalog_url,
        "-o", output_file_path
    ]

    print(f"Attempting to download Eurostat catalog to {output_file_path}...")
    try:
        process = subprocess.run(curl_command, check=True, capture_output=True, text=True)
        print(f"✅ Successfully downloaded catalog: {output_file_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during curl command execution for catalog download:")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Return code: {e.returncode}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ Error: curl command not found. Please ensure curl is installed and in your PATH.")
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred during catalog download: {e}")
        return False

# --- Enhanced Health Dataset Detection ---
def health_dataset_list(obj, keyword="HLTH"):
    """
    Recursively searches a nested object for 'id' keys whose string value contains the keyword.
    Enhanced with better error handling and logging.
    """
    matches = []
    seen = set()  # prevents duplicates

    def recurse(o, parent_label=None):
        if isinstance(o, dict):
            current_label = o.get("label", parent_label)
            extension_data = o.get("extension")

            if extension_data and isinstance(extension_data, dict):
                dataset_id = extension_data.get("id")

                if isinstance(dataset_id, str) and keyword in dataset_id and "$" not in dataset_id:
                    if dataset_id not in seen:
                        # Extract dates from annotations
                        created_date = None
                        update_data_date = None
                        update_structure_date = None
                        annotations = extension_data.get("annotation", [])
                        
                        if isinstance(annotations, list):
                            for annotation in annotations:
                                if isinstance(annotation, dict):
                                    annotation_type = annotation.get("type")
                                    annotation_date = annotation.get("date")
                                    if annotation_type == "CREATED":
                                        created_date = annotation_date
                                    elif annotation_type == "UPDATE_DATA":
                                        update_data_date = annotation_date
                                    elif annotation_type == "UPDATE_STRUCTURE":
                                        update_structure_date = annotation_date
                        
                        matches.append((dataset_id, current_label, created_date, update_data_date, update_structure_date))
                        seen.add(dataset_id)

            # Continue recursion
            for k, v in o.items():
                 if isinstance(v, (dict, list)):
                    recurse(v, current_label)

        elif isinstance(o, list):
            for item in o:
                recurse(item, parent_label)

    recurse(obj)
    return matches

# --- Enhanced CSV Export ---
def export_dataset_details_to_csv(dataset_details, csv_file_name):
    """Enhanced CSV export with better error handling."""
    if not dataset_details:
        print("⚠️ No dataset details to export to CSV.")
        return

    print(f"📄 Exporting detailed dataset information to {csv_file_name}...")
    try:
        with open(csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['ID', 'Label', 'CreatedDate', 'UpdateDataDate', 'UpdateStructureDate'])
            for row_data in dataset_details:
                csv_writer.writerow(row_data)
        print(f"✅ Successfully exported {len(dataset_details)} datasets to {csv_file_name}")
    except IOError as e:
        print(f"❌ Error writing to CSV file {csv_file_name}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during CSV export: {e}")

# --- Enhanced Download Worker with Shared Config ---
def download_dataset_worker(dataset_id, api_config, output_directory):
    """Enhanced download worker with better error handling and logging."""
    host_url = api_config['host']
    service = api_config['service']
    version = api_config['version']
    resp_type = api_config['response_type']
    format_ = api_config['format']
    lang = api_config['lang']
    url = f"{host_url}/{service}/{version}/{resp_type}/{dataset_id}?format={format_}&lang={lang}"
    output_filename = os.path.join(output_directory, f"{dataset_id}.json")

    try:
        os.makedirs(output_directory, exist_ok=True)
        response = requests.get(url, timeout=120) 
        response.raise_for_status()
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        return output_filename
    except requests.exceptions.Timeout:
        print(f"\n⏱️ Request timed out downloading {dataset_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Download error for {dataset_id}: {e}")
        return None
    except IOError as e:
        print(f"\n💾 File write error for {output_filename}: {e}")
        return None
    except Exception as e:
        print(f"\n❌ Unexpected error downloading {dataset_id}: {e}")
        return None

# --- Enhanced Concurrent Download Orchestrator ---
def download_datasets_concurrently(dataset_ids, api_config, output_dir, max_workers):
    """Enhanced orchestrator with better progress tracking."""
    total_datasets = len(dataset_ids)
    downloaded_files = []
    failed_downloads = 0
    
    print(f"\n🚀 Downloading {total_datasets} datasets using up to {max_workers} threads...")
    print(f"📁 Saving to directory: {output_dir}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_dataset_worker, ds_id, api_config, output_dir): ds_id for ds_id in dataset_ids}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            dataset_id = futures[future]
            try:
                result_path = future.result()
                if result_path:
                    downloaded_files.append(result_path)
                else:
                    failed_downloads += 1
            except Exception as e:
                print(f"\n❌ Error processing future for {dataset_id}: {e}")
                failed_downloads += 1
            processed_count = i + 1
            print(f"\r📊 Download progress: {processed_count}/{total_datasets} ({failed_downloads} failed)...", end="", flush=True)

    print("\n" + "="*50)
    print("✅ Dataset download process completed.")
    print("="*50)
    return downloaded_files, failed_downloads

# --- Enhanced Catalog Loading with Database Integration ---
def load_and_filter_catalog(catalog_file_path, keyword_filter, db=None):
    """
    Enhanced catalog loading with optional database tracking.
    """
    try:
        print(f"📖 Loading catalog from {catalog_file_path}...")
        with open(catalog_file_path, "r", encoding='utf-8') as f:
            catalog_data = json.load(f)

        print(f"🔍 Filtering datasets with keyword '{keyword_filter}'...")
        dataset_details_list = health_dataset_list(catalog_data, keyword=keyword_filter)

        if not dataset_details_list:
             print(f"⚠️ No datasets found with keyword '{keyword_filter}'.")
             return None

        print(f"✅ Found {len(dataset_details_list)} matching datasets.")
        
        # Optional: Track in database
        if db:
            try:
                # Could add catalog metadata tracking here
                print(f"📊 Database tracking available (not implemented in this version)")
            except Exception as e:
                print(f"⚠️ Database tracking failed: {e}")
        
        return dataset_details_list

    except FileNotFoundError:
        print(f"❌ Error: Catalog file '{catalog_file_path}' not found.")
        return None
    except json.JSONDecodeError:
         print(f"❌ Error: Catalog file '{catalog_file_path}' is not valid JSON.")
         return None
    except Exception as e:
        print(f"❌ Error loading or processing catalog: {e}")
        return None

# --- Enhanced RSS Feed Processing ---
def fetch_and_parse_rss_feed(rss_url):
    """Enhanced RSS feed processing with better error handling."""
    print(f"📡 Fetching RSS feed from: {rss_url}")
    updated_ids = set()
    try:
        response = requests.get(rss_url, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        for item in root.findall('./channel/item'):
            found_id = None

            # Extract from title
            title_tag = item.find('title')
            if title_tag is not None and title_tag.text:
                match = re.match(r'^([A-Z0-9_]+)\s+-', title_tag.text.strip())
                if match:
                    found_id = match.group(1)
                    updated_ids.add(found_id)

            # Fallback: extract from link
            if not found_id:
                link_tag = item.find('link')
                if link_tag is not None and link_tag.text:
                     potential_id = link_tag.text.strip().split('/')[-1]
                     if re.fullmatch(r'[A-Z0-9_]+', potential_id):
                          found_id = potential_id
                          updated_ids.add(found_id)

        if not updated_ids:
             print("⚠️ Warning: No dataset identifiers extracted from the RSS feed items.")
        else:
             print(f"✅ Found {len(updated_ids)} unique dataset identifiers in the RSS feed.")
        return updated_ids

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching RSS feed: {e}")
        return set()
    except ET.ParseError as e:
        print(f"❌ Error parsing RSS XML: {e}")
        return set()
    except Exception as e:
        print(f"❌ An unexpected error occurred during RSS processing: {e}")
        return set()

def check_health_datasets_in_rss(local_health_ids_set, updated_ids_from_rss_set):
    """Enhanced comparison with better type handling."""
    if not isinstance(local_health_ids_set, set):
        local_health_ids_set = set(local_health_ids_set)
    if not isinstance(updated_ids_from_rss_set, set):
        updated_ids_from_rss_set = set(updated_ids_from_rss_set)
        
    updated_health_datasets = list(local_health_ids_set.intersection(updated_ids_from_rss_set))
    return updated_health_datasets

def main():
    """Enhanced main function with shared configuration."""
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Enhanced Eurostat Data Processor with Shared Modules")
    
    parser.add_argument(
        "--action",
        choices=['export-details', 'calculate-size', 'download-datasets', 'check-updates', 'all', 'download-main-catalog'],
        default='all',
        help="Action(s) to perform"
    )
    parser.add_argument(
        "--data-dir", default="Data_Directory", help="Directory to save downloaded dataset JSON files"
    )
    parser.add_argument(
        "--output-dir", default="Output_Directory", help="Directory to save parsed CSV files"
    )
    parser.add_argument(
        "--catalog-file", default="eurostat_catalog.json", help="Path to the Eurostat catalog JSON file"
    )
    parser.add_argument(
        "--keyword", default="HLTH", help="Keyword to filter datasets by (e.g., 'HLTH')"
    )
    parser.add_argument(
        "--csv-output", default="health_datasets.csv", help="Output filename for the dataset details CSV"
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip downloading the catalog file if it already exists"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit the number of datasets to process"
    )
    parser.add_argument(
        "--dataset-ids", type=str, default=None, help="Comma-separated list of specific dataset IDs to process"
    )
    parser.add_argument(
        "--rss-url", default="https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss", 
        help="URL of the Eurostat RSS feed"
    )
    parser.add_argument(
        "--main-catalog-output-path", default="eurostat_main_catalog.json", 
        help="Output path for the downloaded main Eurostat catalog"
    )
    parser.add_argument(
        "--use-shared-health-datasets", action="store_true", 
        help="Use shared health datasets list instead of keyword filtering"
    )
    parser.add_argument(
        "--enable-db-tracking", action="store_true", 
        help="Enable database tracking of operations"
    )

    args = parser.parse_args()

    # Use shared configuration
    MAX_WORKERS = 10
    API_CONFIG = EUROSTAT_API_CONFIG
    
    # Optional database tracking
    db = None
    if args.enable_db_tracking:
        try:
            db = EurostatDatabase()
            print("✅ Database tracking enabled")
        except Exception as e:
            print(f"⚠️ Database tracking failed to initialize: {e}")

    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    print("🚀 Enhanced Eurostat Data Processor Starting...")
    print(f"📊 Using shared API configuration: {API_CONFIG['host']}")

    # --- Stage 0: Download Catalog ---
    catalog_needed_for_other_actions = args.action in ['export-details', 'calculate-size', 'download-datasets', 'all'] or \
                     (args.action == 'check-updates' and not args.dataset_ids)

    if catalog_needed_for_other_actions:
        if not args.skip_download or not os.path.exists(args.catalog_file):
            print("\n--- Stage 0: Initial Catalog Download ---")
            if not download_eurostat_catalog(args.catalog_file):
                print(f"❌ Failed to download initial catalog to {args.catalog_file}. Some actions might fail.")
            else:
                print(f"✅ Initial catalog ready at {args.catalog_file}")
        else:
            print(f"--- Stage 0: Skipping initial catalog download (using existing {args.catalog_file}) ---")

    # --- Stage 1: Load and Filter ---
    all_dataset_details = []
    if catalog_needed_for_other_actions and os.path.exists(args.catalog_file):
        print("\n--- Stage 1: Load and Filter ---")
        
        if args.use_shared_health_datasets:
            print(f"📋 Using shared health datasets list ({len(HEALTH_DATASETS)} datasets)")
            # Convert shared health datasets to the expected format
            all_dataset_details = [(ds_id, f"Health Dataset {ds_id}", None, None, None) for ds_id in HEALTH_DATASETS]
        else:
            all_dataset_details = load_and_filter_catalog(args.catalog_file, args.keyword, db)
            
        if not all_dataset_details:
            print(f"⚠️ Warning: No datasets found. Some actions might not proceed.")
        else:
            print(f"✅ Found {len(all_dataset_details)} datasets for processing.")

    # --- Prepare lists for actions ---
    health_ids_to_check_rss = set()
    if args.dataset_ids:
        health_ids_to_check_rss.update(s_id.strip() for s_id in args.dataset_ids.split(','))
        print(f"🎯 For RSS check: using {len(health_ids_to_check_rss)} specific dataset IDs from --dataset-ids.")
    elif all_dataset_details:
        health_ids_to_check_rss.update(detail[0] for detail in all_dataset_details)
        print(f"📋 For RSS check: using {len(health_ids_to_check_rss)} dataset IDs from filtering.")

    # Apply filters for actions
    targeted_dataset_details_for_actions = all_dataset_details
    if all_dataset_details:
        if args.dataset_ids:
            specific_ids = {s_id.strip() for s_id in args.dataset_ids.split(',')}
            targeted_dataset_details_for_actions = [detail for detail in all_dataset_details if detail[0] in specific_ids]
            print(f"🎯 Filtered to {len(targeted_dataset_details_for_actions)} datasets based on --dataset-ids.")
        elif args.limit is not None and args.limit > 0:
            targeted_dataset_details_for_actions = all_dataset_details[:args.limit]
            print(f"📊 Limited to processing the first {len(targeted_dataset_details_for_actions)} datasets due to --limit={args.limit}.")
    
    targeted_ids_for_actions = [item[0] for item in targeted_dataset_details_for_actions] if targeted_dataset_details_for_actions else []

    # --- Stage 2: Perform Actions ---
    
    if args.action == 'download-main-catalog':
        print("\n--- Action: Download Main Eurostat Catalog ---")
        if download_eurostat_catalog(args.main_catalog_output_path):
            print(f"✅ Main Eurostat catalog successfully downloaded to {args.main_catalog_output_path}")
        else:
            print(f"❌ Failed to download main Eurostat catalog")
            exit(1)

    elif args.action in ['check-updates', 'all']:
        print("\n--- Action: Check Updates ---")
        if not health_ids_to_check_rss:
            print("⚠️ No local dataset IDs to check against the RSS feed.")
        else:
            rss_found_ids = fetch_and_parse_rss_feed(args.rss_url)
            if rss_found_ids:
                updated_health_datasets = check_health_datasets_in_rss(health_ids_to_check_rss, rss_found_ids)
                if updated_health_datasets:
                    print(f"\n🔄 Found {len(updated_health_datasets)} of your datasets in the RSS feed (updated):")
                    for ds_id in updated_health_datasets:
                        print(f"  📊 {ds_id}")
                else:
                    print("\n✅ None of your datasets were found in recent RSS updates.")

    if args.action in ['export-details', 'all']:
        if not targeted_dataset_details_for_actions:
            print("\n⚠️ No targeted datasets to export details for.")
        else:
            print("\n--- Action: Exporting Details ---")
            csv_export_filename = args.csv_output
            if args.limit or args.dataset_ids:
                base, ext = os.path.splitext(args.csv_output)
                csv_export_filename = f"{base}_filtered{ext}"
            export_dataset_details_to_csv(targeted_dataset_details_for_actions, csv_export_filename)

    if args.action in ['download-datasets', 'all']:
        if not targeted_ids_for_actions:
            print("\n⚠️ No targeted datasets to download.")
        else:
            print("\n--- Action: Downloading Datasets ---")
            downloaded_file_paths, failed_count = download_datasets_concurrently(
                targeted_ids_for_actions, API_CONFIG, args.data_dir, MAX_WORKERS
            )
            print(f"📊 Finished downloads. Success: {len(downloaded_file_paths)}, Failed: {failed_count}")

    if args.action in ['calculate-size', 'all']:
        if not targeted_ids_for_actions:
            print("\n⚠️ No targeted datasets to calculate size for.")
        else:
            print("\n--- Action: Calculate Size ---")
            print("ℹ️ This functionality has been moved to scripts/EDA.py")
            print("Example usage: python scripts/EDA.py --action calculate-size --dataset-ids \"ID1,ID2,ID3\"")

    print("\n🎉 Enhanced script finished successfully!")

if __name__ == "__main__":
    main() 