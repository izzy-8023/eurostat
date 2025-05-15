from __future__ import annotations

import pendulum
import json
import os
import sys

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable

# --- Constants ---
SCRIPTS_PATH = "/opt/airflow/scripts"
STAGING_DIR = "/opt/airflow/staging"  # For intermediate files
MAIN_CATALOG_FILENAME = "eurostat_full_catalog.json"
MAIN_CATALOG_STAGING_PATH = os.path.join(STAGING_DIR, MAIN_CATALOG_FILENAME)
ALL_HLTH_DETAILS_VAR_KEY = "all_hlth_dataset_details"

# --- TaskFlow API Function ---
@task
def process_downloaded_catalog_and_store_hlth_details():
    """
    Loads the downloaded main Eurostat catalog, extracts details of 'HLTH' datasets
    using health_dataset_list from SourceData.py, and stores it in an Airflow Variable.
    """
    print(f"Attempting to load catalog from: {MAIN_CATALOG_STAGING_PATH}")
    try:
        with open(MAIN_CATALOG_STAGING_PATH, 'r', encoding='utf-8') as f:
            catalog_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Catalog file not found at {MAIN_CATALOG_STAGING_PATH}. Ensure download task succeeded.")
        raise
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON from {MAIN_CATALOG_STAGING_PATH}. Error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred loading the catalog: {e}")
        raise

    print("Catalog loaded. Attempting to import health_dataset_list from scripts.SourceData")
    try:
        abs_scripts_path = os.path.abspath(SCRIPTS_PATH)
        if abs_scripts_path not in sys.path:
            sys.path.insert(0, abs_scripts_path)
            print(f"Inserted {abs_scripts_path} at the start of sys.path for import.")
        else:
            print(f"{abs_scripts_path} was already in sys.path.")
            
        from SourceData import health_dataset_list
        print("Successfully imported health_dataset_list.")
    except ImportError as e:
        print(f"ERROR: Failed to import 'health_dataset_list' from SourceData.py.")
        print(f"Attempted to load from directory: {SCRIPTS_PATH} (resolved to: {os.path.abspath(SCRIPTS_PATH)})")
        print(f"Specific Python ImportError: {e}")
        print(f"Current sys.path: {sys.path}")
        print("Check these: ")
        print(f"  1. Does the file {os.path.join(SCRIPTS_PATH, 'SourceData.py')} exist in the Airflow worker container at that exact path?")
        print(f"  2. Is the file readable by the Airflow user?")
        print(f"  3. Does SourceData.py itself have any internal import errors (e.g., missing libraries like 'requests') that would prevent it from being loaded?")
        raise

    print("Extracting HLTH dataset details...")
    hlth_dataset_details_list = health_dataset_list(catalog_data, keyword="HLTH")

    if hlth_dataset_details_list:
        print(f"Found {len(hlth_dataset_details_list)} 'HLTH' dataset details.")
        Variable.set(ALL_HLTH_DETAILS_VAR_KEY, json.dumps(hlth_dataset_details_list))
        print(f"Stored HLTH dataset details in Airflow Variable: {ALL_HLTH_DETAILS_VAR_KEY}")
    else:
        print("No 'HLTH' dataset details found in the catalog.")
        Variable.set(ALL_HLTH_DETAILS_VAR_KEY, json.dumps([]))

with DAG(
    dag_id="eurostat_weekly_catalog_update_dag",
    schedule="0 2 * * 0",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["eurostat", "catalog_update"],
    doc_md="""
    ### Eurostat Weekly Catalog Update DAG
    Downloads the full Eurostat data catalog weekly, extracts details for all 'HLTH' datasets,
    and stores this information in an Airflow Variable (`all_hlth_dataset_details`).
    Uses TaskFlow API for Python tasks.
    """
) as dag:
    
    create_staging_dir_task = BashOperator(
        task_id="create_staging_directory",
        bash_command=f"mkdir -p {STAGING_DIR}",
    )

    download_full_catalog_task = BashOperator(
        task_id="download_full_eurostat_catalog",
        bash_command=(f"python {SCRIPTS_PATH}/SourceData.py "
                      f"--action download-main-catalog "
                      f"--main-catalog-output-path {MAIN_CATALOG_STAGING_PATH}"),
        doc_md=f"Downloads the full Eurostat catalog to {MAIN_CATALOG_STAGING_PATH}"
    )

    process_catalog_task = process_downloaded_catalog_and_store_hlth_details()

    create_staging_dir_task >> download_full_catalog_task >> process_catalog_task 