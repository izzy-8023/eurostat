from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --- Define functions to be called by PythonOperator ---
# You'll need to make sure your scripts are importable or callable.
# For simplicity here, we'll assume scripts can be called via bash.
# If your scripts (SourceData.py, jsonParser.py, load_to_postgres.py)
# are structured with main functions, BashOperator is straightforward.

# Example: Define where your scripts are inside the Airflow worker container
SCRIPTS_PATH = "/opt/airflow/scripts"

with DAG(
    dag_id="eurostat_data_pipeline",
    schedule=None, # Or a cron schedule e.g., "0 0 * * *"
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    tags=["eurostat", "data_pipeline"],
    doc_md="""
    ## Eurostat Data Pipeline
    This DAG downloads, processes, and loads Eurostat data.
    - Fetches dataset catalog
    - Downloads selected/updated datasets
    - Parses JSON to Parquet
    - Loads Parquet to PostgreSQL
    """
) as dag:
    # Task 1: Download the Eurostat Catalog
    # This assumes SourceData.py can take an action like 'download-catalog'
    task_download_catalog = BashOperator(
        task_id="download_eurostat_catalog",
        bash_command=f"python {SCRIPTS_PATH}/SourceData.py --action download-catalog",
        doc_md="Downloads the main Eurostat data catalog."
    )

    # Task 2: Identify and Export Health Datasets Details
    # This assumes SourceData.py can identify health datasets and export details
    task_identify_health_datasets = BashOperator(
        task_id="identify_health_datasets",
        bash_command=f"python {SCRIPTS_PATH}/SourceData.py --action export-details --keyword HLTH --output-csv-path /opt/airflow/dags/health_datasets_details.csv",
        # Note: writing to /opt/airflow/dags so it's accessible if needed,
        # but ideally intermediate data goes to a more structured location or XComs.
        doc_md="Filters catalog for 'HLTH' datasets and exports their details to a CSV."
    )

    # Task 3: Download a specific health dataset (example: HLTH_CD_ANR)
    # For a real pipeline, you'd read the output of task_identify_health_datasets
    # or have a more dynamic way to select datasets.
    DATASET_ID_TO_DOWNLOAD = "HLTH_CD_ANR" # Example
    TEMP_DOWNLOAD_DIR_AIRFLOW = "/opt/airflow/temp_downloads" # Make sure this exists or script creates it
    
    task_download_specific_dataset = BashOperator(
        task_id=f"download_{DATASET_ID_TO_DOWNLOAD}",
        bash_command=(
            f"mkdir -p {TEMP_DOWNLOAD_DIR_AIRFLOW} && "
            f"python {SCRIPTS_PATH}/SourceData.py "
            f"--action download "
            f"--dataset-ids {DATASET_ID_TO_DOWNLOAD} "
            f"--temp-download-dir {TEMP_DOWNLOAD_DIR_AIRFLOW}"
        ),
        doc_md=f"Downloads dataset {DATASET_ID_TO_DOWNLOAD}."
    )

    # Task 4: Parse the downloaded dataset JSON to Parquet
    # This assumes the downloaded file will be in a known location based on SourceData.py's behavior
    # and jsonParser.py can take input/output paths.
    # Adjust paths as per your scripts' logic.
    INPUT_JSON_PATH_AIRFLOW = f"{TEMP_DOWNLOAD_DIR_AIRFLOW}/{DATASET_ID_TO_DOWNLOAD}.json"
    OUTPUT_PARQUET_DIR_AIRFLOW = "/opt/airflow/output_parquet" # Should match volume mount if persisting outside container
    OUTPUT_PARQUET_PATH_AIRFLOW = f"{OUTPUT_PARQUET_DIR_AIRFLOW}/{DATASET_ID_TO_DOWNLOAD}.parquet"

    task_parse_to_parquet = BashOperator(
        task_id=f"parse_{DATASET_ID_TO_DOWNLOAD}_to_parquet",
        bash_command=(
            f"mkdir -p {OUTPUT_PARQUET_DIR_AIRFLOW} && "
            f"python {SCRIPTS_PATH}/jsonParser.py "
            f"--input-file {INPUT_JSON_PATH_AIRFLOW} "
            f"--output-file {OUTPUT_PARQUET_PATH_AIRFLOW}"
        ),
        doc_md=f"Parses {INPUT_JSON_PATH_AIRFLOW} to {OUTPUT_PARQUET_PATH_AIRFLOW}."
    )

    # Task 5: Load the Parquet file into PostgreSQL
    # This assumes load_to_postgres.py can take the Parquet file path as an argument.
    # It also needs environment variables for DB connection (POSTGRES_HOST=db etc.)
    # These should be available if Airflow workers are on the same network as 'db'.
    task_load_to_postgres = BashOperator(
        task_id=f"load_{DATASET_ID_TO_DOWNLOAD}_to_postgres",
        bash_command=(
            f"python {SCRIPTS_PATH}/load_to_postgres.py "
            f"--parquet-file {OUTPUT_PARQUET_PATH_AIRFLOW}"
        ),
        env={ # Pass DB connection details to this task
            "POSTGRES_HOST": "db", # Service name of your project's PostgreSQL
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "eurostat_data",
            "POSTGRES_USER": "eurostat_user",
            "POSTGRES_PASSWORD": "mysecretpassword" # Be mindful of secrets management in production
        },
        doc_md=f"Loads {OUTPUT_PARQUET_PATH_AIRFLOW} into PostgreSQL (service 'db')."
    )

    # Define task dependencies
    task_download_catalog >> task_identify_health_datasets
    task_identify_health_datasets >> task_download_specific_dataset # Simplified: normally you'd iterate
    task_download_specific_dataset >> task_parse_to_parquet
    task_parse_to_parquet >> task_load_to_postgres
