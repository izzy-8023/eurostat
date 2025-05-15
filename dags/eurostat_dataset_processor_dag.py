from __future__ import annotations

import pendulum
import os
import json
import subprocess

from airflow.decorators import task
from airflow.models.dag import DAG, DagRun
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator

# --- Constants ---
SCRIPTS_PATH = "/opt/airflow/scripts"
TEMP_DOWNLOAD_BASE_DIR_PATTERN = "/opt/airflow/temp_processor_downloads/{{ dag_run.run_id }}"
PROCESSED_IDS_VAR_KEY = "processed_hlth_rss_ids"

# --- TaskFlow API Functions & Helpers ---

@task
def get_dataset_ids_to_process(params: dict, dag_run: DagRun | None = None) -> list[str]:
    """
    Retrieves dataset_ids from dag_run.conf if available (triggered run),
    otherwise from params (manual run). Returns the list of dataset_ids.
    """
    dataset_ids = []

    if dag_run and dag_run.conf and 'dataset_ids' in dag_run.conf:
        dataset_ids = dag_run.conf['dataset_ids']
        print(f"Received dataset_ids from dag_run.conf: {dataset_ids}")
    elif params and 'dataset_ids' in params:
        dataset_ids = params['dataset_ids']
        print(f"Received dataset_ids from dag params: {dataset_ids}")

    if not isinstance(dataset_ids, list):
        if isinstance(dataset_ids, str):
            try:
                parsed_ids = json.loads(dataset_ids)
                if isinstance(parsed_ids, list):
                    dataset_ids = parsed_ids
                else:
                    print(f"Warning: dataset_ids was a string but not a JSON list: {dataset_ids}. Using as single item list if non-empty.")
                    dataset_ids = [dataset_ids] if dataset_ids else []
            except json.JSONDecodeError:
                print(f"Warning: dataset_ids string '{dataset_ids}' is not a valid JSON list. Treating as single item or empty.")
                dataset_ids = [dataset_ids] if dataset_ids else []
        else:
            print(f"Warning: dataset_ids is not a list: {type(dataset_ids)}. Attempting to clear.")
            dataset_ids = []
    
    dataset_ids = [str(item) for item in dataset_ids if item]
    
    if not dataset_ids:
        print("No dataset IDs found to process.")
    
    return dataset_ids

def process_dataset(dataset_id: str, dag_run_id: str):
    """Process a single dataset (download and load to DB)."""
    import subprocess
    import os
    
    print(f"Starting processing for dataset_id: {dataset_id}")
    temp_dir = TEMP_DOWNLOAD_BASE_DIR_PATTERN.replace("{{ dag_run.run_id }}", dag_run_id)
    dataset_dir = f"{temp_dir}/{dataset_id}"
    os.makedirs(dataset_dir, exist_ok=True)
    
    download_cmd_list = [
        "python", f"{SCRIPTS_PATH}/SourceData.py",
        "--action", "download-datasets", "--dataset-ids", dataset_id, "--data-dir", dataset_dir
    ]
    print(f"Executing download command: {' '.join(download_cmd_list)}")
    try:
        completed_process_download = subprocess.run(download_cmd_list, check=True, capture_output=True, text=True)
        print(f"Download stdout: {completed_process_download.stdout}")
        if completed_process_download.stderr:
            print(f"Download stderr: {completed_process_download.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Download failed for dataset {dataset_id}. CMD: '{' '.join(e.cmd)}', RC: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise
    
    load_cmd_list = [
        "python", f"{SCRIPTS_PATH}/json_to_postgres_loader.py",
        "--input-file", f"{dataset_dir}/{dataset_id}.json",
        "--table-name", dataset_id, "--dataset-id", dataset_id
    ]
    env = {
        "POSTGRES_HOST": "db", "POSTGRES_PORT": "5432", "POSTGRES_DB": "eurostat_data",
        "POSTGRES_USER": "eurostat_user", "POSTGRES_PASSWORD": "mysecretpassword", # TODO: Use Airflow Connections
        **os.environ
    }
    print(f"Executing load command: {' '.join(load_cmd_list)}")
    try:
        completed_process_load = subprocess.run(load_cmd_list, check=True, env=env, capture_output=True, text=True)
        print(f"Load stdout: {completed_process_load.stdout}")
        if completed_process_load.stderr:
            print(f"Load stderr: {completed_process_load.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Load to DB failed for dataset {dataset_id}. CMD: '{' '.join(e.cmd)}', RC: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise
    
    print(f"Successfully processed dataset_id: {dataset_id}")
    return dataset_id

@task
def process_datasets(dataset_ids_to_process: list[str], dag_run: DagRun | None = None) -> list[str]:
    """Process all datasets retrieved from get_dataset_ids_to_process."""
    if not dag_run or not dag_run.run_id:
        print("Error: dag_run or dag_run.run_id is not available. Cannot create temp directory.")
        print("CRITICAL: dag_run.run_id not available. Dataset processing will likely fail due to path issues.")
        if not dag_run:
            print("Critical: dag_run object not available in process_datasets. Cannot determine run_id.")
            return []
        dag_run_actual_id = dag_run.run_id
    else:
        dag_run_actual_id = dag_run.run_id

    if not dataset_ids_to_process:
        print("No dataset IDs to process.")
        return []

    successfully_processed_ids = []
    for dataset_id in dataset_ids_to_process:
        try:
            print(f"Attempting to process dataset: {dataset_id}")
            process_dataset(dataset_id, dag_run_id=dag_run_actual_id)
            successfully_processed_ids.append(dataset_id)
            print(f"Successfully processed and added to list: {dataset_id}")
        except Exception as e:
            print(f"Failed to process dataset {dataset_id}: {e}")

    if not successfully_processed_ids:
        print("No datasets were successfully processed in this run.")
    else:
        print(f"Successfully processed datasets in this run: {successfully_processed_ids}")
    
    return successfully_processed_ids

@task
def update_processed_ids_variable(successfully_processed_ids: list[str]):
    """
    Updates the Airflow Variable 'processed_hlth_rss_ids' with the dataset IDs
    processed in this DAG run.
    """
    if not successfully_processed_ids:
        print("No dataset IDs were processed in this run. Variable will not be updated.")
        return

    print(f"Attempting to update '{PROCESSED_IDS_VAR_KEY}' with: {successfully_processed_ids}")

    try:
        current_processed_ids_json = Variable.get(PROCESSED_IDS_VAR_KEY, default_var="[]")
        current_processed_ids_set = set(json.loads(current_processed_ids_json))
        print(f"Current '{PROCESSED_IDS_VAR_KEY}' contains {len(current_processed_ids_set)} IDs.")
    except json.JSONDecodeError:
        print(f"Warning: Variable '{PROCESSED_IDS_VAR_KEY}' had invalid JSON. Initializing with current run's IDs.")
        current_processed_ids_set = set()
    except Exception as e:
        print(f"Error reading '{PROCESSED_IDS_VAR_KEY}': {e}. Initializing with current run's IDs.")
        current_processed_ids_set = set()

    updated_ids_set = current_processed_ids_set.union(set(successfully_processed_ids))
    
    try:
        Variable.set(PROCESSED_IDS_VAR_KEY, json.dumps(list(updated_ids_set)))
        print(f"Successfully updated '{PROCESSED_IDS_VAR_KEY}'. Now contains {len(updated_ids_set)} IDs.")
    except Exception as e:
        print(f"CRITICAL: Failed to set Airflow Variable '{PROCESSED_IDS_VAR_KEY}'. Error: {e}")

# --- DAG Definition ---
with DAG(
    dag_id="eurostat_dataset_processor_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["eurostat", "processor"],
    params={
        "dataset_ids": Param(
            default=[], 
            type="array", 
            description="List of Eurostat dataset IDs to download and process."
        )
    },
    doc_md="""
    ### Eurostat Dataset Processor DAG
    This DAG downloads specified Eurostat datasets (JSON format), 
    and loads them into PostgreSQL using `json_to_postgres_loader.py`.
    It is designed to be triggered, expecting `dataset_ids` in the configuration.
    It can also be run manually with the `dataset_ids` parameter.
    Temporary download location: `/opt/airflow/temp_processor_downloads/{{ dag_run.run_id }}/<dataset_id>/`
    Uses TaskFlow API for Python tasks.
    """
) as dag:

    initial_dataset_ids_list = get_dataset_ids_to_process()

    make_dag_run_temp_dir = BashOperator(
        task_id="create_dag_run_temp_directory",
        bash_command=f"mkdir -p {TEMP_DOWNLOAD_BASE_DIR_PATTERN}",
    )

    successfully_processed_ids_list = process_datasets(
        dataset_ids_to_process=initial_dataset_ids_list
    )

    update_task = update_processed_ids_variable(
        successfully_processed_ids=successfully_processed_ids_list
    )

    cleanup_dag_run_temp_dir = BashOperator(
        task_id="cleanup_dag_run_temp_directory",
        bash_command=f"rm -rf {TEMP_DOWNLOAD_BASE_DIR_PATTERN}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    initial_dataset_ids_list >> make_dag_run_temp_dir
    make_dag_run_temp_dir >> successfully_processed_ids_list
    successfully_processed_ids_list >> update_task
    update_task >> cleanup_dag_run_temp_dir 