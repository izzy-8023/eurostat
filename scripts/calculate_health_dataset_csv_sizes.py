import json
import requests
import os
import concurrent.futures
import argparse
import time
import threading

# --- Determine script's directory ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Constants ---
# MODIFIED: Construct path relative to script directory
DEFAULT_CATALOG_FILE = os.path.join(SCRIPT_DIR, "..", "eurostat_full_catalog.json") 
HEALTH_KEYWORD = "HLTH"
# Base URL pattern for CSV data files
CSV_URL_PATTERN = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{dataset_id}/1.0?compress=false&format=csvdata&formatVersion=2.0&lang=en&labels=name"
MAX_WORKERS = 2  # MODIFIED: Reduced default concurrent workers
REQUEST_TIMEOUT = 60 # Increased timeout for potential GET requests
DOWNLOAD_CHUNK_SIZE = 8192 # Bytes for streaming download
REQUEST_DELAY = 0.5  # MODIFIED: Added delay between requests

COMMON_HEADERS = { # MODIFIED: Added common headers
    "User-Agent": "Python-Eurostat-Size-Calculator/1.0 (https://github.com/user/project)", # Replace with actual project/contact if desired
    "Accept": "text/csv,application/csv,*/*;q=0.8"
}

# --- Shared counter for debug printing ---
successful_size_prints = 0
print_lock = threading.Lock() # To safely increment the counter from threads

# --- Functions to extract health dataset IDs (adapted from SourceData.py) ---

def health_dataset_list_from_catalog_data(obj, keyword="HLTH"):
    """Recursively searches a nested object for 'id' keys whose string value contains the keyword."""
    matches = []
    seen_ids = set()

    def recurse(o):
        if isinstance(o, dict):
            extension_data = o.get("extension")
            if extension_data and isinstance(extension_data, dict):
                dataset_id = extension_data.get("id")
                if isinstance(dataset_id, str) and keyword in dataset_id and "$" not in dataset_id:
                    if dataset_id not in seen_ids:
                        matches.append(dataset_id) # Just need the ID for this script
                        seen_ids.add(dataset_id)
            
            for k, v in o.items():
                if isinstance(v, (dict, list)):
                    recurse(v)
        elif isinstance(o, list):
            for item in o:
                recurse(item)
    recurse(obj)
    return matches

def load_and_get_health_dataset_ids(catalog_file_path, keyword_filter):
    """
    Loads the catalog JSON and filters for dataset IDs matching the keyword.
    Returns a list of dataset IDs.
    """
    try:
        print(f"Loading catalog from {catalog_file_path}...")
        print(f"Attempting to open absolute path: {os.path.abspath(catalog_file_path)}")
        with open(catalog_file_path, "r", encoding='utf-8') as f:
            catalog_data = json.load(f)
        
        print(f"Filtering datasets with keyword '{keyword_filter}'...")
        dataset_ids = health_dataset_list_from_catalog_data(catalog_data, keyword=keyword_filter)

        if not dataset_ids:
             print(f"No datasets found with keyword '{keyword_filter}'.")
             return []
        
        print(f"Found {len(dataset_ids)} matching dataset IDs.")
        return dataset_ids

    except FileNotFoundError:
        print(f"Error: Catalog file '{catalog_file_path}' not found.")
        return []
    except json.JSONDecodeError:
         print(f"Error: Catalog file '{catalog_file_path}' is not valid JSON.")
         return []
    except Exception as e:
        print(f"Error loading or processing catalog: {e}")
        return []

# --- Functions to get CSV content length ---

def get_csv_content_length(dataset_id, index, total_count):
    """
    Tries HEAD first. If Content-Length is 0 or missing, falls back to streaming GET.
    Returns (dataset_id, size_in_bytes, error_message).
    size_in_bytes is None on error.
    """
    global successful_size_prints # Refer to the global counter
    url = CSV_URL_PATTERN.format(dataset_id=dataset_id)
    
    if (index + 1) % 5 == 0 or (index + 1) == total_count or index == 0: # Print more frequently
        print(f"\rFetching size ({index + 1}/{total_count}): {dataset_id[:20]:<20} ...", end="", flush=True)

    size_via_head = None
    head_error = None
    result_size = None
    error_message_final = None

    try:
        # Stage 1: Try HEAD request
        try:
            response_head = requests.head(url, timeout=REQUEST_TIMEOUT / 2, allow_redirects=True, headers=COMMON_HEADERS) # MODIFIED
            response_head.raise_for_status()
            content_length_str = response_head.headers.get('content-length')

            if content_length_str and int(content_length_str) > 0:
                size_via_head = int(content_length_str)
                with print_lock:
                    if successful_size_prints < 5:
                        print(f"\n[Debug] Dataset: {dataset_id}, Method: HEAD, Content-Length: '{size_via_head}'")
                        successful_size_prints += 1
                result_size = size_via_head
            elif content_length_str and int(content_length_str) == 0:
                head_error = "HEAD Content-Length is 0"
                # Debug for 0-length HEAD moved to after GET attempt if it happens
            else: 
                head_error = "HEAD Content-Length missing"

        except requests.exceptions.Timeout:
            head_error = "HEAD Timeout"
        except requests.exceptions.HTTPError as e:
            head_error = f"HEAD HTTP Error: {e.response.status_code}"
        except requests.exceptions.RequestException as e:
            head_error = f"HEAD Request Error: {str(e)}"
        except ValueError as e:
            content_length_str_val = content_length_str if 'content_length_str' in locals() else '[unknown]'
            head_error = f"HEAD Invalid Content-Length: '{content_length_str_val}' ({e})"
        except Exception as e: 
            head_error = f"HEAD Unexpected error: {str(e)}"

        # Stage 2: Fallback to streaming GET if HEAD didn't yield a positive size
        if result_size is None:
            if head_error: # Print initial head error before trying GET
                 with print_lock:
                    # Limit printing these initial head errors if they are very frequent
                    if successful_size_prints < 10 and (head_error == "HEAD Content-Length is 0" or head_error == "HEAD Content-Length missing") :
                        print(f"\n[Debug] Dataset: {dataset_id}, Info: {head_error}. Will try GET.")
                        # Don't increment successful_size_prints here as it's not a final success print
            try:
                with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=COMMON_HEADERS) as response_get: # MODIFIED
                    response_get.raise_for_status()
                    size_via_get = 0
                    for chunk in response_get.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        size_via_get += len(chunk)
                    
                    result_size = size_via_get
                    with print_lock:
                        if successful_size_prints < 5 : 
                            print(f"\n[Debug] Dataset: {dataset_id}, Method: GET (fallback), Size: {size_via_get} bytes (Original HEAD info: {head_error or '-'})")
                            successful_size_prints +=1
            
            except requests.exceptions.Timeout:
                error_message_final = f"GET Timeout (after {head_error or 'HEAD attempt'})"
            except requests.exceptions.HTTPError as e:
                error_message_final = f"GET HTTP Error: {e.response.status_code} (after {head_error or 'HEAD attempt'})"
            except requests.exceptions.RequestException as e:
                error_message_final = f"GET Request Error: {str(e)} (after {head_error or 'HEAD attempt'})"
            except Exception as e:
                error_message_final = f"GET Unexpected error: {str(e)} (after {head_error or 'HEAD attempt'})"
        
        return dataset_id, result_size, error_message_final

    finally: # MODIFIED: Ensure delay happens
        time.sleep(REQUEST_DELAY)

def calculate_total_csv_size_concurrently(dataset_ids):
    """Orchestrates concurrent fetching of CSV content lengths and returns aggregate results."""
    total_datasets = len(dataset_ids)
    total_size_bytes = 0
    datasets_processed_successfully = 0
    datasets_failed_or_unknown_size = 0
    error_details = {}

    # Adjust MAX_WORKERS if it was changed by CLI args before this point
    # This ensures the printout reflects the actual number of workers used.
    global MAX_WORKERS 
    actual_max_workers = MAX_WORKERS # Already set by CLI or default

    print(f"\nCalculating total CSV size for {total_datasets} datasets using up to {actual_max_workers} threads...")
    print(f"(Delay between requests: {REQUEST_DELAY}s)")
    
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_max_workers) as executor:
        future_to_id = {
            executor.submit(get_csv_content_length, ds_id, idx, total_datasets): ds_id 
            for idx, ds_id in enumerate(dataset_ids)
        }

        for future in concurrent.futures.as_completed(future_to_id):
            ds_id_completed = future_to_id[future]
            try:
                _, size_bytes, error_msg = future.result()
                if error_msg:
                    datasets_failed_or_unknown_size += 1
                    error_details[ds_id_completed] = error_msg
                elif size_bytes is not None: # Ensure size_bytes is not None (could be 0 if file is empty)
                    total_size_bytes += size_bytes
                    datasets_processed_successfully += 1
                else: 
                    datasets_failed_or_unknown_size += 1
                    error_details[ds_id_completed] = "Unknown error (size and error both None from get_csv_content_length)"

            except Exception as exc:
                datasets_failed_or_unknown_size += 1
                error_details[ds_id_completed] = f"Exception in processing future: {exc}"

    end_time = time.time()
    print(f"\n\nAll threads completed in {end_time - start_time:.2f} seconds.")
    return datasets_processed_successfully, datasets_failed_or_unknown_size, total_size_bytes, error_details

def print_summary(total_targeted, processed_count, failed_count, total_bytes, errors):
    """Prints the final size calculation summary."""
    print("\n--- Total CSV Size Calculation Summary ---")
    print(f"Dataset IDs targeted:              {total_targeted}")
    print(f"Successfully sized (CSV):          {processed_count}")
    print(f"Failed or size unavailable (CSV):  {failed_count}")

    if total_bytes > 0:
        total_size_mb = total_bytes / (1024 * 1024)
        total_size_gb = total_bytes / (1024 * 1024 * 1024)
        print(f"\nTotal calculated size (from Content-Length/GET): {total_bytes:,} bytes")
        print(f"Total calculated size (from Content-Length/GET): {total_size_mb:.2f} MB")
        print(f"Total calculated size (from Content-Length/GET): {total_size_gb:.2f} GB")
    else:
        print("\nTotal calculated size (from Content-Length/GET): 0 bytes")

    if failed_count > 0:
        print("\nNote: Total size might be incomplete due to errors or missing Content-Length for some datasets.")
        # print("\nError Details:") # Keep this commented unless verbose errors are needed
        # for ds_id, err in errors.items():
        #     print(f"  - {ds_id}: {err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate total estimated size of Eurostat health datasets (CSV).")
    parser.add_argument(
        "--catalog-file", 
        default=DEFAULT_CATALOG_FILE,
        help=f"Path to the Eurostat catalog JSON file (default: {DEFAULT_CATALOG_FILE})."
    )
    parser.add_argument(
        "--keyword", 
        default=HEALTH_KEYWORD,
        help=f"Keyword to filter dataset IDs (default: {HEALTH_KEYWORD})."
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Maximum number of concurrent threads (default: {MAX_WORKERS})."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Delay in seconds between individual requests (default: {REQUEST_DELAY})."
    )
    args = parser.parse_args()

    MAX_WORKERS = args.max_workers
    REQUEST_DELAY = args.delay

    print(f"Starting health dataset CSV size calculation...")
    print(f"Using catalog: {args.catalog_file}, Keyword: {args.keyword}, Max Workers: {MAX_WORKERS}, Delay: {REQUEST_DELAY}s")

    health_dataset_ids = load_and_get_health_dataset_ids(args.catalog_file, args.keyword)

    if health_dataset_ids:
        processed, failed, total_bytes, error_list = calculate_total_csv_size_concurrently(health_dataset_ids)
        print_summary(len(health_dataset_ids), processed, failed, total_bytes, error_list)
    else:
        print("No health dataset IDs found or loaded. Exiting.")

    print("\nScript finished.") 