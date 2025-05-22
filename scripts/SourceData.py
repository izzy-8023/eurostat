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


# --- Downloading Eurostat Catalog ---
def download_eurostat_catalog(output_file_path):
    """
    Downloads the latest Eurostat data catalog using curl and saves it to the specified path.
    Returns True on success, False on failure.
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
        print(f"Successfully downloaded catalog: {output_file_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error during curl command execution for catalog download:")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Return code: {e.returncode}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: curl command not found. Please ensure curl is installed and in your PATH.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during catalog download: {e}")
        return False

# Get ID values containing HLTH in catalog.json file.
def health_dataset_list(obj, keyword="HLTH"):
    """Recursively searches a nested object for 'id' keys whose string value contains the keyword."""
    matches = []
    seen = set()  # prevents duplicates

    def recurse(o, parent_label=None):
        if isinstance(o, dict):
            # Check if this dictionary itself might be the 'item' containing 'label' and 'extension'
            current_label = o.get("label", parent_label) # Inherit label if needed
            extension_data = o.get("extension")

            if extension_data and isinstance(extension_data, dict):
                # Found an 'extension' dict, look for ID and annotations within it
                dataset_id = extension_data.get("id")

                if isinstance(dataset_id, str) and keyword in dataset_id and "$" not in dataset_id:
                    if dataset_id not in seen:
                        # Extract dates from the annotations list within extension_data
                        created_date = None
                        update_data_date = None
                        update_structure_date = None
                        annotations = extension_data.get("annotation", []) # Default to empty list
                        
                        if isinstance(annotations, list): # Ensure annotations is a list
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
                                        
                        # Append the found details
                        matches.append((dataset_id, current_label, created_date, update_data_date, update_structure_date))
                        seen.add(dataset_id)

            # Regardless of finding an ID here, continue recursion for other keys/nested structures
            for k, v in o.items():
                 if isinstance(v, (dict, list)):
                    # Pass down the 'current_label' found at this level (or inherited)
                    recurse(v, current_label)

        elif isinstance(o, list):
            for item in o:
                # Pass the parent_label down into list items
                recurse(item, parent_label)

    recurse(obj)
    return matches

# --- CSV Export ---
def export_dataset_details_to_csv(dataset_details, csv_file_name):
    """Exports a list of dataset details to a CSV file."""
    if not dataset_details:
        print("No dataset details to export to CSV.")
        return

    print(f"Exporting detailed dataset information to {csv_file_name}...")
    try:
        with open(csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header
            csv_writer.writerow(['ID', 'Label', 'CreatedDate', 'UpdateDataDate', 'UpdateStructureDate'])
            # Write the data rows
            for row_data in dataset_details:
                csv_writer.writerow(row_data)
        print(f"Successfully exported data to {csv_file_name}")
    except IOError as e:
        print(f"Error writing to CSV file {csv_file_name}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV export: {e}")


# --- Worker Function (for SAVING datasets) ---
def download_dataset_worker(dataset_id, api_config, output_directory):
    """Downloads and saves a single dataset JSON file."""
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
        return output_filename # Return path on success
    except requests.exceptions.Timeout:
        print(f"\nRequest timed out downloading {dataset_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"\nDownload error for {dataset_id}: {e}")
        return None
    except IOError as e:
        print(f"\nFile write error for {output_filename}: {e}")
        return None
    except Exception as e:
        print(f"\nUnexpected error downloading {dataset_id}: {e}")
        return None

# --- Orchestrator for Downloading ---
def download_datasets_concurrently(dataset_ids, api_config, output_dir, max_workers):
    """Orchestrates concurrent download and saving of datasets."""
    total_datasets = len(dataset_ids)
    downloaded_files = []
    failed_downloads = 0
    print(f"\nDownloading {total_datasets} datasets using up to {max_workers} threads...")
    print(f"Saving to directory: {output_dir}")

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
                print(f"\nError processing future for {dataset_id}: {e}")
                failed_downloads += 1
            processed_count = i + 1
            print(f"\rDownload progress: {processed_count}/{total_datasets} ({failed_downloads} failed)...", end="", flush=True)

    print("\n" + "="*30)
    print("Dataset download process completed.")
    print("="*30)
    return downloaded_files, failed_downloads

def load_and_filter_catalog(catalog_file_path, keyword_filter):
    """
    Loads the catalog JSON, filters datasets by keyword,
    and returns the list of detailed dataset information tuples.
    """
    try:
        print(f"Loading catalog from {catalog_file_path}...")
        with open(catalog_file_path, "r", encoding='utf-8') as f:
            catalog_data = json.load(f)

        print(f"Filtering datasets with keyword '{keyword_filter}'...")
        # dataset_details_list will be a list of tuples: (id, label, created_date, update_data_date, update_structure_date)
        dataset_details_list = health_dataset_list(catalog_data, keyword=keyword_filter)

        if not dataset_details_list:
             print(f"No datasets found with keyword '{keyword_filter}'.")
             return None # Indicate no datasets found

        print(f"Found {len(dataset_details_list)} matching datasets.")
        return dataset_details_list

    except FileNotFoundError:
        print(f"Error: Catalog file '{catalog_file_path}' not found.")
        return None
    except json.JSONDecodeError:
         print(f"Error: Catalog file '{catalog_file_path}' is not valid JSON.")
         return None
    except Exception as e:
        print(f"Error loading or processing catalog: {e}")
        return None




# --- RSS Feed Processing ---
def fetch_and_parse_rss_feed(rss_url):
    """
    Fetches the Eurostat RSS update feed and extracts updated dataset identifiers
    based on the observed structure in <title> and <link> tags.
    Returns a set of unique dataset identifiers found in the feed.
    """
    print(f"Fetching RSS feed from: {rss_url}")
    updated_ids = set()
    try:
        response = requests.get(rss_url, timeout=30)
        response.raise_for_status()

        # Parse the XML content
        root = ET.fromstring(response.content)

        # Iterate through each 'item' in the 'channel'
        for item in root.findall('./channel/item'):
            found_id = None

            # Attempt 1: Extract from <title> 
            title_tag = item.find('title')
            if title_tag is not None and title_tag.text:
                match = re.match(r'^([A-Z0-9_]+)\s+-', title_tag.text.strip())
                if match:
                    found_id = match.group(1)
                    updated_ids.add(found_id)
                    # print(f"Found ID in title: {found_id}") # Debug

            # Attempt 2: Extract from <link> 
            # Use this as a fallback or confirmation if title parsing fails
            if not found_id: # Only try link if title didn't yield ID
                link_tag = item.find('link')
                if link_tag is not None and link_tag.text:
                     # Extract the part after the last '/'
                     potential_id = link_tag.text.strip().split('/')[-1]
                     # Basic sanity check for Eurostat ID format (uppercase, numbers, underscore)
                     if re.fullmatch(r'[A-Z0-9_]+', potential_id):
                          found_id = potential_id
                          updated_ids.add(found_id)
                          # print(f"Found ID in link: {found_id}") # Debug


        if not updated_ids:
             print("Warning: No dataset identifiers extracted from the RSS feed items.")
        else:
             print(f"Found {len(updated_ids)} unique dataset identifiers in the RSS feed.")
        return updated_ids

    except requests.exceptions.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        return set() # Return empty set on error
    except ET.ParseError as e:
        print(f"Error parsing RSS XML: {e}")
        return set() # Return empty set on error
    except Exception as e:
        print(f"An unexpected error occurred during RSS processing: {e}")
        return set()

def check_health_datasets_in_rss(local_health_ids_set, updated_ids_from_rss_set):
    """
    Compares a set of local health dataset IDs with a set of IDs from an RSS feed.
    Returns a list of local health IDs that are present in the RSS feed.
    """
    # Ensure inputs are sets for efficient intersection
    if not isinstance(local_health_ids_set, set):
        local_health_ids_set = set(local_health_ids_set)
    if not isinstance(updated_ids_from_rss_set, set):
        updated_ids_from_rss_set = set(updated_ids_from_rss_set)
        
    # Find the common IDs (intersection)
    updated_health_datasets = list(local_health_ids_set.intersection(updated_ids_from_rss_set))
    return updated_health_datasets

if __name__ == "__main__":

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Eurostat Data Processor")
    
    parser.add_argument(
        "--action",
        choices=['export-details', 'calculate-size', 'download-datasets', 'check-updates', 'all', 'download-main-catalog'],
        default='all',
        help="Action(s) to perform. 'check-updates' looks for health datasets in RSS. 'all' performs export, download, size calc, AND check-updates. 'download-main-catalog' downloads the full Eurostat catalog."
    )
    parser.add_argument(
        "--data-dir", default="Data_Directory", help="Directory to save downloaded dataset JSON files."
    )
    parser.add_argument(
        "--output-dir", default="Output_Directory", help="Directory to save parsed CSV files."
    )
    parser.add_argument(
        "--catalog-file", default="eurostat_catalog.json", help="Path to the Eurostat catalog JSON file."
    )
    parser.add_argument(
        "--keyword", default="HLTH", help="Keyword to filter datasets by (e.g., 'HLTH')."
    )
    parser.add_argument(
        "--csv-output", default="health_datasets.csv", help="Output filename for the dataset details CSV."
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip downloading the catalog file if it already exists."
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit the number of datasets to process."
    )
    parser.add_argument(
        "--dataset-ids", type=str, default=None, help="Comma-separated list of specific dataset IDs to process."
    )
    parser.add_argument(
        "--rss-url", default="https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss", help="URL of the Eurostat RSS feed."
    )
    parser.add_argument(
        "--main-catalog-output-path", default="eurostat_main_catalog.json", help="Output path for the downloaded main Eurostat catalog (used with --action download-main-catalog)."
    )

    # TEMP_DOWNLOAD_DIR = "temp_eurostat_downloads" # MOVED TO EDA.py (or similar for its main block)
    MAX_WORKERS = 10
    API_CONFIG = {
        "host": "https://ec.europa.eu/eurostat/api/dissemination",
        "service": "statistics", "version": "1.0", "response_type": "data",
        "format": "JSON", "lang": "EN"
    }
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    # --- Stage 0: Download Catalog ---
    # This logic needs to run if the catalog is required by ANY selected action
    # and not skipped.
    catalog_needed_for_other_actions = args.action in ['export-details', 'calculate-size', 'download-datasets', 'all'] or \
                     (args.action == 'check-updates' and not args.dataset_ids) # If check-updates and no specific IDs, need catalog

    if catalog_needed_for_other_actions:
        if not args.skip_download or not os.path.exists(args.catalog_file):
            print("--- Stage 0: Initial Catalog Download (for filtering/listing actions) ---")
            if not download_eurostat_catalog(args.catalog_file): # This uses the --catalog-file for general purpose
                print(f"Failed to download initial catalog to {args.catalog_file}. Some actions might fail.")
                # Decide if exit is needed here or let actions fail individually
            else:
                print(f"Initial catalog ready at {args.catalog_file}")
        else:
            print(f"--- Stage 0: Skipping initial catalog download (using existing {args.catalog_file}) ---")

    # --- Stage 1: Load and Filter (if catalog was needed and exists) ---
    all_dataset_details = []
    if catalog_needed_for_other_actions and os.path.exists(args.catalog_file):
        print("\n--- Stage 1: Load and Filter (for filtering/listing actions) ---")
        all_dataset_details = load_and_filter_catalog(args.catalog_file, args.keyword)
        if not all_dataset_details:
            print(f"Warning: No datasets found matching keyword '{args.keyword}'. Some actions might not proceed.")
        else:
            print(f"Initially found {len(all_dataset_details)} datasets matching keyword '{args.keyword}'.")
    elif catalog_needed_for_other_actions and not os.path.exists(args.catalog_file):
        print(f"Error: Catalog file {args.catalog_file} not found and was required. Exiting.")
        exit()


    # --- Prepare lists for actions ---
    # For 'check-updates':
    health_ids_to_check_rss = set()
    if args.dataset_ids: # If specific IDs are given for any action, prioritize them for RSS check too
        health_ids_to_check_rss.update(s_id.strip() for s_id in args.dataset_ids.split(','))
        print(f"For RSS check: using {len(health_ids_to_check_rss)} specific dataset IDs from --dataset-ids.")
    elif all_dataset_details: # Else, use all from keyword filter for RSS check
        health_ids_to_check_rss.update(detail[0] for detail in all_dataset_details)
        print(f"For RSS check: using {len(health_ids_to_check_rss)} dataset IDs from '{args.keyword}' filter.")

    # For other actions ('export-details', 'download-datasets', 'calculate-size'):
    # Apply --dataset-ids or --limit filters to all_dataset_details
    targeted_dataset_details_for_actions = all_dataset_details
    if all_dataset_details: # Only filter if we have details
        if args.dataset_ids:
            specific_ids = {s_id.strip() for s_id in args.dataset_ids.split(',')}
            targeted_dataset_details_for_actions = [detail for detail in all_dataset_details if detail[0] in specific_ids]
            print(f"For other actions: filtered to {len(targeted_dataset_details_for_actions)} datasets based on --dataset-ids list.")
            if not targeted_dataset_details_for_actions:
                 print(f"Warning: None of the specified --dataset-ids were found in the initial '{args.keyword}' filtered list for other actions.")
        elif args.limit is not None and args.limit > 0:
            targeted_dataset_details_for_actions = all_dataset_details[:args.limit]
            print(f"For other actions: limited to processing the first {len(targeted_dataset_details_for_actions)} datasets due to --limit={args.limit}.")
    
    # Extract IDs from the *action-targeted* list
    targeted_ids_for_actions = [item[0] for item in targeted_dataset_details_for_actions] if targeted_dataset_details_for_actions else []


    # --- Stage 2: Perform Actions Based on Argument ---

    # Action: Download Main Catalog (New Action)
    if args.action == 'download-main-catalog':
        print("\n--- Action: Download Main Eurostat Catalog ---")
        if not args.main_catalog_output_path:
            print("Error: --main-catalog-output-path must be specified for action 'download-main-catalog'.")
            exit(1)
        print(f"Attempting to download main catalog to: {args.main_catalog_output_path}")
        if download_eurostat_catalog(args.main_catalog_output_path):
            print(f"Main Eurostat catalog successfully downloaded to {args.main_catalog_output_path}")
        else:
            print(f"Failed to download main Eurostat catalog to {args.main_catalog_output_path}")
            exit(1) # Exit if this specific action fails

    # Action: Check Updates (checks for health datasets in RSS)
    elif args.action in ['check-updates', 'all']:
        print("\n--- Action: Check Updates ---")
        if not health_ids_to_check_rss: # Use the set prepared above
            print("No local dataset IDs to check against the RSS feed. Provide --dataset-ids or ensure catalog filtering works.")
        else:
            rss_found_ids = fetch_and_parse_rss_feed(args.rss_url)
            if rss_found_ids is not None: # Check if fetch_and_parse_rss_feed succeeded (didn't return None on error)
                updated_health_datasets = check_health_datasets_in_rss(health_ids_to_check_rss, rss_found_ids)
                if updated_health_datasets:
                    print(f"\nFound {len(updated_health_datasets)} of your specified datasets in the RSS feed (updated):")
                    for ds_id in updated_health_datasets:
                        print(f"  - {ds_id}")
                else:
                    print("\nNone of your specified datasets were found in the RSS feed's recent updates.")
            else:
                print("Could not retrieve or parse update information from RSS feed.")

    # Action: Export Details to CSV
    elif args.action in ['export-details', 'all']:
        if not targeted_dataset_details_for_actions:
            print("\nNo targeted datasets to export details for (check --keyword, --dataset-ids, or --limit).")
        else:
            print("\n--- Action: Exporting Details of Targeted Datasets ---")
            csv_export_filename = args.csv_output
            if args.limit or args.dataset_ids: # If list was specifically filtered for actions
                base, ext = os.path.splitext(args.csv_output)
                csv_export_filename = f"{base}_filtered{ext}" # Changed suffix for clarity
            export_dataset_details_to_csv(targeted_dataset_details_for_actions, csv_export_filename)

    # Action: Download Datasets
    elif args.action in ['download-datasets', 'all']:
        if not targeted_ids_for_actions:
            print("\nNo targeted datasets to download (check --keyword, --dataset-ids, or --limit).")
        else:
            print("\n--- Action: Downloading Targeted Datasets ---")
            downloaded_file_paths, failed_count = download_datasets_concurrently(
                targeted_ids_for_actions, API_CONFIG, args.data_dir, MAX_WORKERS
            )
            print(f"Finished downloads. Success: {len(downloaded_file_paths)}, Failed: {failed_count}")

    # Action: Calculate Size
    elif args.action in ['calculate-size', 'all']:
        if not targeted_ids_for_actions:
            print("\nNo targeted datasets to calculate size for (check --keyword, --dataset-ids, or --limit).")
        else:
            print("\n--- Action: Calculating Size for Targeted Datasets ---")
            print("This functionality has been moved to scripts/EDA.py")
            print("Example usage: python scripts/EDA.py --action calculate-size --dataset-ids \"ID1,ID2,ID3\"")
            # os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True) # MOVED
            # processed, failed, total_bytes = calculate_total_size_concurrently(
            #     targeted_ids_for_actions, API_CONFIG, TEMP_DOWNLOAD_DIR, MAX_WORKERS
            # ) # MOVED
            # cleanup_temp_directory(TEMP_DOWNLOAD_DIR) # MOVED
            # print_summary(len(targeted_ids_for_actions), processed, failed, total_bytes) # MOVED

    print("\nScript finished.")               