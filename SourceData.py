import ijson
import json
import requests
import os
import time
import concurrent.futures
import threading # Needed for Lock

# --- Function Definitions ---

# curl -X GET "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?format=json" -o eurostat_catalog.json\
# inspect the structure of the json file
def inspect_json_structure(file_path, max_events=50):
    """Prints the first few events from parsing a JSON file using ijson."""
    try:
        with open(file_path, "rb") as f:
            parser = ijson.parse(f)
            event_count = 0
            for prefix, event, value in parser:
                print(f"Prefix: {prefix}, Event: {event}, Value: {value}")
                event_count += 1
                if event_count >= max_events:
                    break
    except FileNotFoundError:
        print(f"Error: Catalog file '{file_path}' not found for inspection.")
    except Exception as e:
        print(f"Error during inspection: {e}")

# 获取包含 HLTH 的 id 值 (Get ID values containing HLTH)
def health_dataset_list(obj, keyword="HLTH"):
    """Recursively searches a nested object for 'id' keys whose string value contains the keyword."""
    matches = []
    seen = set()  # 用于去重，防止重复添加 (Used for deduplication, prevents duplicates)

    def recurse(o, parent_label=None):
        if isinstance(o, dict):
            # 提前获取当前层级的 label (Get label for current level beforehand)
            label = o.get("label", parent_label) # Use current label or inherit from parent

            for k, v in o.items():
                # 如果当前键是 'id' 且值包含关键词 (If current key is 'id' and value contains keyword)
                # Also excludes IDs containing '$' which might be internal references
                if k == "id" and isinstance(v, str) and keyword in v and "$" not in v:
                    if v not in seen:
                        matches.append((v, label))  # 保存 id 和它的 label (Save ID and its label)
                        seen.add(v)
                # 递归进入更深层级，同时传入当前的 label (Recurse into deeper levels, passing current label)
                elif isinstance(v, (dict, list)):
                    recurse(v, label)

        elif isinstance(o, list):
            for item in o:
                # Pass the parent_label down into list items
                recurse(item, parent_label)

    recurse(obj)
    return matches

# --- Shared variables and Lock for processing ---
total_size_bytes = 0
datasets_processed_size = 0
datasets_failed = 0
# Lock to protect access to the shared variables above
shared_data_lock = threading.Lock()

# --- Worker Function for Threading ---
def process_dataset(id_value, index, total_count, api_config, temp_dir):
    """Downloads, measures size, and deletes a single dataset."""
    global total_size_bytes, datasets_processed_size, datasets_failed # Declare use of global variables

    # Unpack API config for clarity
    host_url = api_config['host']
    service = api_config['service']
    version = api_config['version']
    resp_type = api_config['response_type']
    format_ = api_config['format'] # Avoid conflict with built-in format
    lang = api_config['lang']

    url = f"{host_url}/{service}/{version}/{resp_type}/{id_value}?format={format_}&lang={lang}"
    # Use a unique temp filename including thread ID to prevent potential race conditions
    temp_filename = os.path.join(temp_dir, f"{id_value}_temp_{threading.get_ident()}.json") 
    
    # Progress indicator (print less frequently in threads)
    if (index + 1) % 20 == 0 or (index + 1) == total_count:
         print(f"\rProcessing ({index+1}/{total_count}): {id_value} ...", end="", flush=True)
    
    file_size = None
    success = False
    
    try:
        # Download (consider adding session object for potential keep-alive)
        response = requests.get(url, timeout=60) 
        response.raise_for_status() # Check for HTTP errors (4xx, 5xx)

        # Save content temporarily
        with open(temp_filename, 'wb') as f:
             f.write(response.content) 

        # Get size & ensure deletion in finally block
        try:
            file_size = os.path.getsize(temp_filename)
            success = True
        except OSError as e:
             # Log error but continue to deletion
             print(f"\nError getting size for {temp_filename} (ID: {id_value}): {e}") 
        finally:
            # Attempt deletion regardless of size success/failure
            try:
                os.remove(temp_filename)
            except OSError as e:
                 # Log deletion error
                 print(f"\nError deleting temporary file {temp_filename} (ID: {id_value}): {e}")

    except requests.exceptions.Timeout:
         print(f"\nRequest timed out for {id_value}")
    except requests.exceptions.RequestException as e: # Catches connection errors, HTTP errors, etc.
        print(f"\nDownload error for {id_value}: {e}")
    except Exception as e: # Catch any other unexpected error during processing
        print(f"\nUnexpected error processing {id_value}: {e}")
        # Attempt cleanup if file exists from partial download/write
        if os.path.exists(temp_filename):
             try: os.remove(temp_filename)
             except OSError: pass # Ignore error during cleanup after another error

    # --- Update shared counters safely using the lock ---
    with shared_data_lock:
        if success and file_size is not None:
            total_size_bytes += file_size
            datasets_processed_size += 1
        else:
            # Count as failed if download failed OR size couldn't be determined
            datasets_failed += 1
    # ---


# --- Main execution block ---
if __name__ == "__main__":
    
    CATALOG_FILE = "eurostat_catalog.json"
    KEYWORD_FILTER = "HLTH"
    TEMP_DOWNLOAD_DIR = "temp_eurostat_downloads"
    MAX_WORKERS = 10 # Max concurrent downloads

    # API Configuration (centralized)
    API_CONFIG = {
        "host": "https://ec.europa.eu/eurostat/api/dissemination",
        "service": "statistics",
        "version": "1.0",
        "response_type": "data",
        "format": "JSON",
        "lang": "EN"
    }

    # --- 1. Load Catalog and Get Dataset IDs ---
    try:
        print(f"Loading catalog from {CATALOG_FILE}...")
        with open(CATALOG_FILE, "r", encoding='utf-8') as f: # Specify encoding
            catalog_data = json.load(f)
        # Optional: Inspect structure if needed
        # inspect_json_structure(CATALOG_FILE) 
        
        print(f"Filtering datasets with keyword '{KEYWORD_FILTER}'...")
        results = health_dataset_list(catalog_data, keyword=KEYWORD_FILTER)
        dataset_ids_to_check = [id_val for id_val, _ in results]
        total_datasets = len(dataset_ids_to_check)
        if total_datasets == 0:
             print(f"No datasets found with keyword '{KEYWORD_FILTER}'. Exiting.")
             exit()
        print(f"Found {total_datasets} datasets to process.")
        
    except FileNotFoundError:
        print(f"Error: Catalog file '{CATALOG_FILE}' not found.")
        exit()
    except json.JSONDecodeError:
         print(f"Error: Catalog file '{CATALOG_FILE}' is not valid JSON.")
         exit()
    except Exception as e:
        print(f"Error loading or processing catalog: {e}")
        exit()
    # ---

    # --- 2. Prepare Temporary Directory ---
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

    # --- 3. Process Datasets Concurrently ---
    print(f"\nCalculating total size using up to {MAX_WORKERS} threads...")

    # Use ThreadPoolExecutor for concurrent downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create argument lists for map
        indices = range(total_datasets)
        total_counts = [total_datasets] * total_datasets
        api_configs = [API_CONFIG] * total_datasets
        temp_dirs = [TEMP_DOWNLOAD_DIR] * total_datasets
        
        # Use map to submit all tasks
        # list() forces execution and waits for completion
        list(executor.map(process_dataset, dataset_ids_to_check, indices, total_counts, api_configs, temp_dirs))

    print("\n" + "="*30) 
    print("All processing threads completed.")
    print("="*30)

    # --- 4. Clean up Temporary Directory ---
    try:
        if not os.listdir(TEMP_DOWNLOAD_DIR):
            os.rmdir(TEMP_DOWNLOAD_DIR)
            print(f"Removed empty temporary directory: {TEMP_DOWNLOAD_DIR}")
        else:
             print(f"Warning: Temporary directory {TEMP_DOWNLOAD_DIR} not empty, skipping removal.")
             print("(This might happen if file deletion failed for some downloads)")
    except OSError as e:
        print(f"Warning: Could not check or remove temporary directory {TEMP_DOWNLOAD_DIR}: {e}")
    # ---

    # --- 5. Print Summary ---
    print("\n--- Total Size Calculation Summary ---")
    print(f"Datasets targeted:                 {total_datasets}")
    print(f"Successfully processed and sized:  {datasets_processed_size}")
    print(f"Failed or size unavailable:        {datasets_failed}")

    total_size_mb = total_size_bytes / (1024 * 1024)
    total_size_gb = total_size_bytes / (1024 * 1024 * 1024)

    print(f"\nTotal calculated size from successful downloads: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")
    if datasets_failed > 0:
         print("Note: Total size might be incomplete due to download/size errors for failed datasets.")