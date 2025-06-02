import json
import logging # Optional: for logging within the util
from datetime import datetime, timezone
import os # For local testing path joining

logger = logging.getLogger(__name__) # Optional

def get_metadata_from_dataset_json(json_file_path: str) -> dict | None:
    """
    Parses a downloaded Eurostat dataset JSON file (for a single dataset)
    to extract its title (label) and last update date.

    Args:
        json_file_path: Path to the downloaded .json file for a single dataset.

    Returns:
        A dictionary with {"title": "...", "update_data_date": "YYYY-MM-DDTHH:MM:SSZ"} 
        or None if extraction fails or essential fields are missing.
        The update_data_date should be an ISO 8601 string.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract title (label)
        # Common locations for title/label in Eurostat JSON:
        title = data.get('label')
        if not title and 'concept' in data and isinstance(data['concept'], list) and len(data['concept']) > 0:
            # Sometimes the main label is within a concept array
            # This is speculative, adjust based on actual single dataset JSON structure
            if isinstance(data['concept'][0], dict):
                 title = data['concept'][0].get('label')


        # Extract last update date
        # Common locations for update date in Eurostat JSON for a single dataset:
        # 1. Root level 'updated' key
        # 2. 'extension.annotation' with type 'UPDATE_DATA' (less common for single file, more for catalog)
        # 3. 'version' or 'publicationDate' might also be candidates if 'updated' is missing.
        
        update_date_str = data.get('updated') # Often the most direct for single dataset files

        if not update_date_str and data.get('extension') and data['extension'].get('annotation'):
            for annotation in data['extension']['annotation']:
                if isinstance(annotation, dict) and annotation.get('type') == 'UPDATE_DATA':
                    update_date_str = annotation.get('date')
                    break
        
        # Add more fallbacks if necessary based on your inspection of a sample single-dataset JSON file.
        # e.g. check data.get('version'), data.get('publicationDate') etc.

        if not title or not update_date_str:
            logger.warning(f"Could not extract essential metadata (title or update_date) from {json_file_path}. Title found: {title}, UpdateDate found: {update_date_str}")
            return None

        # Basic validation/normalization of the date string (optional but good)
        # For now, we assume it's already a valid ISO string.
        # If it's just a date like "2023-10-26", you might want to append a default time.
        # Example: if len(update_date_str) == 10: update_date_str += "T00:00:00Z"

        return {"title": title, "update_data_date": update_date_str}

    except FileNotFoundError:
        logger.error(f"File not found for metadata extraction: {json_file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for {json_file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing {json_file_path}: {e}")
        return None

def get_dataset_metadata_from_main_catalog(dataset_id_to_find: str, main_catalog_json_path: str) -> dict | None:
    """
    Searches the main Eurostat catalog JSON file for a specific dataset_id
    and extracts its title (label) and last update date (UpdateDataDate).

    Args:
        dataset_id_to_find: The dataset ID to search for.
        main_catalog_json_path: Path to the main Eurostat catalog JSON file.

    Returns:
        A dictionary {"title": "...", "update_data_date": "YYYY-MM-DDTHH:MM:SSZ"}
        or None if not found or essential fields are missing.
    """
    try:
        with open(main_catalog_json_path, 'r', encoding='utf-8') as f:
            catalog_data = json.load(f) # This can be large, consider memory if very constrained

        # The structure of the main catalog is typically a list of datasets or a nested structure.
        # This is based on the structure parsed by SourceData.py::health_dataset_list
        # We need to find the item where extension.id == dataset_id_to_find

        # This is a simplified search, might need to be more robust depending on exact catalog structure
        # Assuming 'link'.'item' structure based on previous SourceData.py logic
        
        items = []
        if 'link' in catalog_data and 'item' in catalog_data['link']:
            items = catalog_data['link']['item']
        elif isinstance(catalog_data, list): # Sometimes the root is a list of datasets
             items = catalog_data
        elif 'dataset' in catalog_data and isinstance(catalog_data['dataset'], list): # Another common pattern
            items = catalog_data['dataset']


        for item in items:
            if isinstance(item, dict):
                extension = item.get("extension")
                if extension and isinstance(extension, dict):
                    current_dataset_id = extension.get("id")
                    if current_dataset_id == dataset_id_to_find:
                        title = item.get("label", current_dataset_id) # Fallback to ID if no label
                        update_data_date_str = None
                        
                        annotations = extension.get("annotation", [])
                        if isinstance(annotations, list):
                            for annotation in annotations:
                                if isinstance(annotation, dict):
                                    annotation_type = annotation.get("type")
                                    annotation_date = annotation.get("date")
                                    if annotation_type == "UPDATE_DATA":
                                        update_data_date_str = annotation_date
                                        break # Found UpdateDataDate
                        
                        if not update_data_date_str:
                            # Fallback: check a root-level 'updated' or 'lastUpdate' key within the item if it exists
                            update_data_date_str = item.get('updated', item.get('lastUpdate'))


                        if update_data_date_str:
                            return {"title": title, "update_data_date": update_data_date_str}
                        else:
                            logger.warning(f"Found dataset {dataset_id_to_find} in main catalog but no 'UpdateDataDate' annotation or common date key.")
                            return {"title": title, "update_data_date": None} # Return title even if date is missing

        logger.info(f"Dataset ID {dataset_id_to_find} not found in main catalog {main_catalog_json_path}")
        return None

    except FileNotFoundError:
        logger.error(f"Main catalog file not found: {main_catalog_json_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for main catalog {main_catalog_json_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing main catalog {main_catalog_json_path}: {e}")
        return None

if __name__ == '__main__':
    # For local testing of this utility
    # 1. Download a sample single dataset JSON file (e.g., using SourceData.py)
    #    python scripts/SourceData.py --action download-datasets --dataset-ids HLTH_EHIS_HAHLPD.json --data-dir ./temp_test_json
    # 2. Place its path here:
    #    Ensure the test file path is relative to where you run the script from, or use an absolute path.
    #    If eurostat (project root) is CWD, and script is scripts/shared/catalog_utils.py
    #    then path could be 'temp_test_json/HLTH_EHIS_HAHLPD.json' if temp_test_json is in project root
    
    # Assuming you run this from the project root (e.g. /Users/izzy/Desktop/eurostat)
    # and temp_test_json is directly under it.
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_script_dir, '..', '..')) # Adjust if shared is deeper
    sample_json_path = os.path.join(project_root, 'temp_test_json', 'HLTH_EHIS_HAHLPD.json') # ADJUST THIS PATH if layout differs

    logging.basicConfig(level=logging.INFO)
    
    if os.path.exists(sample_json_path):
        metadata = get_metadata_from_dataset_json(sample_json_path)
        if metadata:
            logger.info(f"Successfully extracted metadata: {metadata}")
        else:
            logger.error(f"Failed to extract metadata from sample: {sample_json_path}")
    else:
        logger.error(f"Sample JSON file for testing not found at: {sample_json_path}. Please download/place one first.")

    # Example test code to be added to if __name__ == '__main__' in catalog_utils.py:
    main_catalog_path_for_test = os.path.join(project_root, 'eurostat_catalog.json') 
    if os.path.exists(main_catalog_path_for_test):
        metadata_from_main = get_dataset_metadata_from_main_catalog('hlth_cd_ainfo', main_catalog_path_for_test)
        if metadata_from_main:
            logger.info(f"Metadata for 'hlth_cd_ainfo' from main catalog: {metadata_from_main}")
        else:
            logger.warning("Could not find/parse 'hlth_cd_ainfo' in main catalog for test.")
    else:
        logger.warning(f"Main catalog {main_catalog_path_for_test} not found for testing get_dataset_metadata_from_main_catalog.") 