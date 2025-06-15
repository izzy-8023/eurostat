#!/usr/bin/env python3
"""
DAG to monitor Eurostat health-related RSS feeds, identify new or updated datasets,
and trigger the enhanced_eurostat_processor_dag for them.
"""

from datetime import datetime, timedelta
import logging
import json # For TriggerDagRunOperator conf
import sys
import os # For path joining if needed

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

# Assuming shared scripts are in /opt/airflow/scripts/
# Adjust if your Airflow environment has a different scripts location or PYTHONPATH setup
SCRIPTS_DIR = "/opt/airflow/scripts"
SHARED_SCRIPTS_DIR = os.path.join(SCRIPTS_DIR, "shared")
MAIN_CATALOG_FILE_PATH = os.path.join(SCRIPTS_DIR, "eurostat_catalog.json") # Path to the main catalog

# Add shared scripts to sys.path for imports if not discoverable by default
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)
if SHARED_SCRIPTS_DIR not in sys.path:
    sys.path.append(SHARED_SCRIPTS_DIR)

# Now import from shared utilities
from shared.db_utils import get_dataset_last_processed_info_db
from shared.catalog_utils import get_dataset_metadata_from_main_catalog
# For RSS parsing, we might need a function from SourceData.py or replicate its core logic
# Let's assume a helper for now, or inline the logic.
# from SourceData import fetch_and_parse_rss_feed # Or adapt it

logger = logging.getLogger(__name__)

# --- TaskFlow API Functions ---

@task
def fetch_rss_dataset_ids_task(rss_url: str = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss") -> list[str]:
    """Fetches dataset IDs from the specified Eurostat RSS feed."""
    import requests
    import xml.etree.ElementTree as ET
    import re

    logger.info(f"Fetching RSS feed from: {rss_url}")
    updated_ids = set()
    try:
        response = requests.get(rss_url, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        for item in root.findall('./channel/item'):
            found_id = None
            title_tag = item.find('title')
            if title_tag is not None and title_tag.text:
                match = re.match(r'^([A-Z0-9_]+)\s+-', title_tag.text.strip())
                if match:
                    found_id = match.group(1)
            
            if not found_id:
                link_tag = item.find('link')
                if link_tag is not None and link_tag.text:
                    potential_id = link_tag.text.strip().split('/')[-1]
                    if re.fullmatch(r'[A-Z0-9_]+', potential_id):
                        found_id = potential_id
            
            if found_id:
                # Filter out non-dataset codes if possible, e.g. if they contain lowercase or special chars not typical for IDs
                # For now, assuming most IDs from RSS are valid dataset codes.
                # Add more robust filtering if RSS feed contains other types of IDs.
                if found_id.lower().startswith('hlth') and found_id.isupper() and '_' in found_id:
                     updated_ids.add(found_id)
                else:
                    logger.debug(f"Skipping potential non-dataset ID or non-health ID from RSS: {found_id}")

        if not updated_ids:
            logger.info("No dataset identifiers extracted or matched typical pattern from the RSS feed items.")
        else:
            logger.info(f"Found {len(updated_ids)} potential dataset identifiers in the RSS feed: {list(updated_ids)}")
        return list(updated_ids)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching RSS feed: {e}")
        return [] # Return empty list on error to prevent downstream failures
    except ET.ParseError as e:
        logger.error(f"Error parsing RSS XML: {e}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred during RSS processing: {e}")
        return []

@task
def get_current_metadata_for_rss_ids_task(rss_dataset_ids: list[str]) -> dict[str, dict]:
    """
    For each dataset ID from RSS, fetches its current metadata (title, update_data_date)
    from the main Eurostat catalog file.
    Assumes MAIN_CATALOG_FILE_PATH is accessible and up-to-date.
    """
    if not rss_dataset_ids:
        logger.info("No dataset IDs from RSS to process for metadata lookup.")
        return {}

    logger.info(f"Fetching current metadata for {len(rss_dataset_ids)} dataset(s) from main catalog: {MAIN_CATALOG_FILE_PATH}")
    datasets_current_metadata = {}

    # Check if main catalog file exists
    if not os.path.exists(MAIN_CATALOG_FILE_PATH):
        logger.error(f"Main catalog file not found at {MAIN_CATALOG_FILE_PATH}. Cannot fetch metadata.")

        return {}

    for dataset_id in rss_dataset_ids:
        metadata = get_dataset_metadata_from_main_catalog(dataset_id, MAIN_CATALOG_FILE_PATH)
        if metadata:
            # Ensure 'update_data_date' is present, even if None from catalog util
            datasets_current_metadata[dataset_id] = {
                "title": metadata.get("title"),
                "update_data_date": metadata.get("update_data_date") 
            }
            logger.info(f"Found metadata for {dataset_id} in main catalog: {datasets_current_metadata[dataset_id]}")
        else:
            # If not found in main catalog, it might be an old ID or non-dataset entity from RSS.
            # We probably don't want to process it if we can't verify its current state.
            logger.warning(f"Dataset ID {dataset_id} from RSS feed not found or metadata incomplete in main catalog {MAIN_CATALOG_FILE_PATH}.")
            # Not adding to datasets_current_metadata means it won't be considered for processing.
            
    logger.info(f"Retrieved current metadata for {len(datasets_current_metadata)} dataset(s).")
    return datasets_current_metadata

@task
def determine_datasets_to_process_task(current_datasets_metadata: dict[str, dict]) -> dict[str, list | dict]:
    """
    Determines which datasets need to be processed (new or updated).
    Compares current metadata against the processed_dataset_log table.

    Returns: 
        A dictionary like: 
        {
            "ids_to_process": ["id1", "id2", ...],
            "processing_remarks": {"id1": "New dataset", "id2": "Updated version"}
        }
    """
    if not current_datasets_metadata:
        logger.info("No current metadata available. No datasets to process.")
        return {"ids_to_process": [], "processing_remarks": {}}

    logger.info(f"Determining which of the {len(current_datasets_metadata)} dataset(s) need processing...")
    ids_to_process = []
    processing_remarks = {}

    for dataset_id, current_meta in current_datasets_metadata.items():
        current_source_update_date_str = current_meta.get('update_data_date')
        
        # Skip if current metadata from catalog doesn't have an update date
        if not current_source_update_date_str:
            logger.warning(f"Dataset {dataset_id} has no current source update date from catalog. Skipping.")
            continue

        # Convert current source update date string to datetime object for comparison
        # ISO 8601 format like "YYYY-MM-DDTHH:MM:SSZ" or "YYYY-MM-DDTHH:MM:SS+HH:MM"
        try:
            # psycopg2 will return timezone-aware datetime if the DB column is TIMESTAMPTZ
            # For strings from JSON, ensure they can be parsed to comparable datetime objects.
            # Python's datetime.fromisoformat handles many ISO 8601 variants.
            current_source_update_dt = datetime.fromisoformat(current_source_update_date_str.replace('Z', '+00:00'))
        except ValueError:
            logger.error(f"Could not parse current_source_update_date '{current_source_update_date_str}' for {dataset_id}. Skipping.")
            continue

        last_processed_info = get_dataset_last_processed_info_db(dataset_id)

        if not last_processed_info:
            logger.info(f"Dataset {dataset_id} is new (not found in processed_dataset_log). Adding for processing.")
            ids_to_process.append(dataset_id)
            processing_remarks[dataset_id] = "New dataset from RSS feed."
        else:
            stored_source_update_date_str = last_processed_info.get('source_data_updated_at')
            if not stored_source_update_date_str:
                logger.info(f"Dataset {dataset_id} was processed before but without a source_data_updated_at. Adding for reprocessing to capture it.")
                ids_to_process.append(dataset_id)
                processing_remarks[dataset_id] = "Reprocessing to capture source update date."
            else:
                try:
                    stored_source_update_dt = datetime.fromisoformat(str(stored_source_update_date_str).replace('Z', '+00:00'))
                     # Ensure stored_source_update_dt is timezone-aware if current_source_update_dt is.
                    # If one is naive and other is aware, comparison will fail or be incorrect.
                    # fromisoformat should handle this if strings are proper ISO with timezone.
                    # If DB returns timezone-aware, and catalog string is timezone-aware, all good.
                except ValueError:
                    logger.error(f"Could not parse stored_source_update_date '{stored_source_update_date_str}' for {dataset_id}. Reprocessing.")
                    ids_to_process.append(dataset_id)
                    processing_remarks[dataset_id] = "Reprocessing due to unparsable stored source update date."
                    continue
                
                if current_source_update_dt > stored_source_update_dt:
                    logger.info(f"Dataset {dataset_id} has been updated at source (Current: {current_source_update_dt}, Stored: {stored_source_update_dt}). Adding for reprocessing.")
                    ids_to_process.append(dataset_id)
                    processing_remarks[dataset_id] = f"Source updated (New: {current_source_update_date_str}, Old: {stored_source_update_date_str})"
                else:
                    logger.info(f"Dataset {dataset_id} is already up-to-date (Current: {current_source_update_dt}, Stored: {stored_source_update_dt}). Skipping.")
    
    if ids_to_process:
        logger.info(f"Identified {len(ids_to_process)} dataset(s) to process/reprocess: {ids_to_process}")
    else:
        logger.info("No new or updated datasets identified for processing.")
        
    return {"ids_to_process": ids_to_process, "processing_remarks": processing_remarks}

@task.branch
def branch_on_datasets_to_process(trigger_info: dict):
    """
    Determines if there are datasets to process and branches accordingly.
    Returns the task_id to proceed to if datasets exist, otherwise None or a skip task_id.
    """
    if trigger_info and trigger_info.get("ids_to_process"):
        logger.info(f"Datasets found to process: {len(trigger_info['ids_to_process'])}. Proceeding to trigger.")
        return "trigger_enhanced_processor_dag_task" # Task ID of the TriggerDagRunOperator
    else:
        logger.info("No datasets to process. Skipping trigger.")
        return "end_rss_monitoring_task" # Assuming a dummy end task for the skip path

# --- DAG Definition ---
with DAG(
    dag_id="eurostat_health_rss_monitor_dag",
    schedule="0 */12 * * *",  # Runs twice a day (at midnight and noon UTC)
    start_date=(datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0), # Ensure this is a fixed past date, e.g., midnight yesterday
    catchup=False,
    tags=["eurostat", "rss", "monitor"],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    doc_md=__doc__,
) as dag:
    
    rss_ids = fetch_rss_dataset_ids_task()
    current_metadata = get_current_metadata_for_rss_ids_task(rss_dataset_ids=rss_ids)
    datasets_to_trigger_info = determine_datasets_to_process_task(current_datasets_metadata=current_metadata)

    # Branching task
    branch_choice = branch_on_datasets_to_process(trigger_info=datasets_to_trigger_info)

    # Define a dummy end task for the skip path
    end_task = EmptyOperator(task_id="end_rss_monitoring_task", dag=dag)

    # Task to trigger the enhanced_eurostat_processor_dag
    trigger_processor_dag = TriggerDagRunOperator(
        task_id="trigger_enhanced_processor_dag_task",
        trigger_dag_id="enhanced_eurostat_processor_dag", # The DAG ID to trigger
        # conf needs to be a dictionary or a JSON string.
        # We pass the output of 'determine_datasets_to_process_task' directly.
        # This output is a dict: {"ids_to_process": [...], "processing_remarks": {...}}
        # Airflow's TriggerDagRunOperator can directly use XComs if the upstream task returns a dict.
        # However, to be explicit and ensure correct formatting, we can use a Jinja template for the conf.
        # The XCom value from `datasets_to_trigger_info` will be a dictionary.
        # We want to pass its 'ids_to_process' and 'processing_remarks' components.
        conf={
            "dataset_ids": "{{ ti.xcom_pull(task_ids='determine_datasets_to_process_task')['ids_to_process'] }}",
            "processing_remarks": "{{ ti.xcom_pull(task_ids='determine_datasets_to_process_task')['processing_remarks'] }}",
            "triggered_by_dag_id": "{{ dag.dag_id }}",
            "triggered_by_run_id": "{{ run_id }}"
        },
        wait_for_completion=False, # Usually False for this kind of trigger
        reset_dag_run=True,      # Ensures a new DAG run is created even if one with same logical date exists (good for event-driven)
        poke_interval=30,        # How often to check if the triggered DAG run is created if wait_for_completion=True (not used here)
    )

    # Task dependencies
    rss_ids >> current_metadata >> datasets_to_trigger_info >> branch_choice
    branch_choice >> [trigger_processor_dag, end_task] # Connect branch to both possible outcomes
