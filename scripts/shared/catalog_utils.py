import json
import logging 
from datetime import datetime, timezone
import os 
import ijson

logger = logging.getLogger(__name__) 

def get_metadata_from_dataset_json(file_path: str):
    """
    Extracts 'label' and update date from a Eurostat JSON file.
    Checks for top-level 'label', 'updated', or 'lastUpdate' keys.
    """
    title = None
    update_date = None

    # --- Pass 1: Check for top-level keys ---
    try:
        with open(file_path, 'rb') as f:
            # Use ijson.kvitems on the root to get key-value pairs.
            top_level_items = ijson.kvitems(f, '')
            for key, value in top_level_items:
                if key == 'label':
                    title = value
                if key == 'updated' or key == 'lastUpdate':
                    update_date = value
                if (title and update_date) or key in ['dimension', 'value']:
                    break
    except Exception as e:
        
        logger.warning(f"Could not perform first-pass metadata scan on {file_path}: {e}")

    # Check for nested update date if not found in Pass 1
    if not update_date:
        try:
            with open(file_path, 'rb') as f:
                # ijson.items iterates over an array at a given prefix.
                annotations = ijson.items(f, 'extension.annotation.item')
                for annotation in annotations:
                    if isinstance(annotation, dict) and annotation.get('type') == 'UPDATE_DATA':
                        date_val = annotation.get('date')
                        if date_val:
                            update_date = date_val
                            break 
        except Exception as e:
            logger.warning(f"Could not perform second-pass (annotation) metadata scan on {file_path}: {e}")

    #  Final Result
    if title or update_date:
        if not title:
            logger.warning(f"Found update_date but not title for {file_path}")
        if not update_date:
            logger.warning(f"Found title but not update_date for {file_path}")
        return {"title": title, "update_data_date": update_date}
    else:
        logger.warning(f"Could not find title or update date in {file_path} after all parsing attempts.")
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
        
        items = []
        if 'link' in catalog_data and 'item' in catalog_data['link']:
            items = catalog_data['link']['item']
        elif isinstance(catalog_data, list): 
             items = catalog_data
        elif 'dataset' in catalog_data and isinstance(catalog_data['dataset'], list): 
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
                                        break 
                        
                        if not update_data_date_str:
                            
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

# if __name__ == '__main__':
#     current_script_dir = os.path.dirname(os.path.abspath(__file__))
#     project_root = os.path.abspath(os.path.join(current_script_dir, '..', '..')) # Adjust if shared is deeper
#     sample_json_path = os.path.join(project_root, 'temp_test_json', 'HLTH_EHIS_HAHLPD.json') # for testing

#     logging.basicConfig(level=logging.INFO)
    
#     if os.path.exists(sample_json_path):
#         metadata = get_metadata_from_dataset_json(sample_json_path)
#         if metadata:
#             logger.info(f"Successfully extracted metadata: {metadata}")
#         else:
#             logger.error(f"Failed to extract metadata from sample: {sample_json_path}")
#     else:
#         logger.error(f"Sample JSON file for testing not found at: {sample_json_path}. Please download/place one first.")

#     # Example test code to be added to if __name__ == '__main__' in catalog_utils.py:
#     # Test code for get_dataset_metadata_from_main_catalog function
#     # main_catalog_path_for_test = os.path.join(project_root, 'eurostat_catalog.json') 
#     # if os.path.exists(main_catalog_path_for_test):
#     #     metadata_from_main = get_dataset_metadata_from_main_catalog('hlth_cd_ainfo', main_catalog_path_for_test)
#     #     if metadata_from_main:
#         #     logger.info(f"Metadata for 'hlth_cd_ainfo' from main catalog: {metadata_from_main}")
#         # else:
#         #     logger.warning("Could not find/parse 'hlth_cd_ainfo' in main catalog for test.")
#     # else:
#         # logger.warning(f"Main catalog {main_catalog_path_for_test} not found for testing get_dataset_metadata_from_main_catalog.") 