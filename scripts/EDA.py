import ijson
import json
import os
import threading
import concurrent.futures
import requests
import argparse
# inspect the structure of the json file
# curl -X GET "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?format=json" -o eurostat_catalog.json\

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

# inspect_json_structure("eurostat_catalog.json")

# recursive function to explore the json file
def explore_json(d, indent=0, max_depth=3):
    if indent >= max_depth:
        print('  ' * indent + '... (max depth reached)')
        return

    if isinstance(d, dict):
        for k, v in d.items():
            print('  ' * indent + f"- {k}")
            explore_json(v, indent + 1, max_depth)
    elif isinstance(d, list):
        print('  ' * indent + f"[list of {type(d[0]).__name__}]" if d else "[empty list]")
        if d:
            explore_json(d[0], indent + 1, max_depth)




with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)

# explore_json(data, max_depth=8)  # limit the max depth



# Shared lock for process_dataset_worker, if needed globally here
shared_data_lock = threading.Lock()

def process_dataset_worker(id_value, index, total_count, api_config, temp_dir):
    """
    Downloads, measures size, deletes a single dataset for EDA purposes.
    Returns (file_size, success_flag). file_size is None on failure.
    """
    host_url = api_config['host']
    service = api_config['service']
    version = api_config['version']
    resp_type = api_config['response_type']
    format_ = api_config['format']
    lang = api_config['lang']
    url = f"{host_url}/{service}/{version}/{resp_type}/{id_value}?format={format_}&lang={lang}"
    temp_filename = os.path.join(temp_dir, f"{id_value}_temp_{threading.get_ident()}.json")

    if (index + 1) % 20 == 0 or (index + 1) == total_count:
        print(f"\rProcessing ({index + 1}/{total_count}): {id_value} ...", end="", flush=True)

    file_size = None
    success = False
    download_ok = False

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        with open(temp_filename, 'wb') as f:
            f.write(response.content)
        download_ok = True
        try:
            file_size = os.path.getsize(temp_filename)
            success = True
        except OSError as e:
            print(f"\nError getting size for {temp_filename} (ID: {id_value}): {e}")
        finally:
            try:
                os.remove(temp_filename)
            except OSError as e:
                print(f"\nError deleting temporary file {temp_filename} (ID: {id_value}): {e}")
    except requests.exceptions.Timeout:
        print(f"\nRequest timed out for {id_value}")
    except requests.exceptions.RequestException as e:
        print(f"\nDownload error for {id_value}: {e}")
    except Exception as e:
        print(f"\nUnexpected error processing {id_value}: {e}")
        if not download_ok and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
            except OSError:
                pass
    return (file_size, success)

def calculate_total_size_concurrently(dataset_ids, api_config, temp_dir, max_workers):
    """Orchestrates concurrent download/sizing and returns aggregate results for EDA."""
    total_datasets = len(dataset_ids)
    local_total_size_bytes = 0
    local_datasets_processed_size = 0
    local_datasets_failed = 0

    print(f"\nCalculating total size for EDA using up to {max_workers} threads...")
    os.makedirs(temp_dir, exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        indices = range(total_datasets)
        total_counts = [total_datasets] * total_datasets
        api_configs = [api_config] * total_datasets
        temp_dirs = [temp_dir] * total_datasets

        results_iterator = executor.map(process_dataset_worker, dataset_ids, indices, total_counts, api_configs, temp_dirs)

        for file_size, success_flag in results_iterator:
            if success_flag and file_size is not None:
                local_total_size_bytes += file_size
                local_datasets_processed_size += 1
            else:
                local_datasets_failed += 1

    print("\n" + "=" * 30)
    print("All EDA processing threads completed.")
    print("=" * 30)
    return local_datasets_processed_size, local_datasets_failed, local_total_size_bytes

def cleanup_temp_directory(temp_dir_path):
    """Attempts to remove the temporary directory if it's empty (for EDA)."""
    try:
        if not os.listdir(temp_dir_path):
            os.rmdir(temp_dir_path)
            print(f"Removed empty temporary directory for EDA: {temp_dir_path}")
        else:
            print(f"Warning: EDA temporary directory {temp_dir_path} not empty, skipping removal.")
    except FileNotFoundError:
        print(f"EDA temporary directory {temp_dir_path} not found for cleanup.")
    except OSError as e:
        print(f"Warning: Could not check or remove EDA temporary directory {temp_dir_path}: {e}")

def print_summary(total_targeted, processed_count, failed_count, total_bytes):
    """Prints the final size calculation summary for EDA."""
    print("\n--- EDA: Total Size Calculation Summary ---")
    print(f"Datasets targeted:                 {total_targeted}")
    print(f"Successfully processed and sized:  {processed_count}")
    print(f"Failed or size unavailable:        {failed_count}")

    if total_bytes > 0:
        total_size_mb = total_bytes / (1024 * 1024)
        total_size_gb = total_bytes / (1024 * 1024 * 1024)
        print(f"\nTotal calculated size from successful downloads: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")
    else:
        print("\nTotal calculated size from successful downloads: 0 MB (0 GB)")

    if failed_count > 0:
        print("Note: Total size might be incomplete due to download/size errors for failed datasets.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eurostat EDA Script - Includes Dataset Size Calculation")
    parser.add_argument(
        "--action",
        choices=['inspect-structure', 'explore-json', 'calculate-size'],
        help="EDA action to perform."
    )
    parser.add_argument(
        "--file-path", default="eurostat_catalog.json",
        help="Path to the JSON file for inspection or exploration."
    )
    parser.add_argument(
        "--dataset-ids", type=str, default=None,
        help="Comma-separated list of specific dataset IDs to calculate size for (e.g., 'ID1,ID2,ID3'). Required for 'calculate-size' action."
    )
    args = parser.parse_args()

    if args.action == 'inspect-structure':
        inspect_json_structure(args.file_path)
    elif args.action == 'explore-json':
        try:
            with open(args.file_path, "r") as f_explore:
                explore_data = json.load(f_explore)
            explore_json(explore_data, max_depth=8)
        except FileNotFoundError:
            print(f"Error: File '{args.file_path}' not found for exploration.")
        except json.JSONDecodeError:
            print(f"Error: File '{args.file_path}' is not valid JSON.")
    elif args.action == 'calculate-size':
        if not args.dataset_ids:
            print("Error: --dataset-ids are required for 'calculate-size' action.")
            exit(1)
        
        dataset_ids_list = [ds_id.strip() for ds_id in args.dataset_ids.split(',')]
        
        # Configuration for size calculation
        EDA_TEMP_DOWNLOAD_DIR = "temp_eurostat_downloads_eda"
        EDA_MAX_WORKERS = 5 # Adjust as needed for EDA, maybe less than pipeline
        EDA_API_CONFIG = {
            "host": "https://ec.europa.eu/eurostat/api/dissemination",
            "service": "statistics", "version": "1.0", "response_type": "data",
            "format": "JSON", "lang": "EN"
        }

        if not dataset_ids_list:
            print("No dataset IDs provided for size calculation.")
        else:
            processed, failed, total_bytes = calculate_total_size_concurrently(
                dataset_ids_list, EDA_API_CONFIG, EDA_TEMP_DOWNLOAD_DIR, EDA_MAX_WORKERS
            )
            print_summary(len(dataset_ids_list), processed, failed, total_bytes)
            cleanup_temp_directory(EDA_TEMP_DOWNLOAD_DIR)
    else:
        print("No action specified or action not recognized. Use --action [inspect-structure|explore-json|calculate-size]")
