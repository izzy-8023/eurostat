from __future__ import annotations

import pendulum
import requests
import xml.etree.ElementTree as ET
import re
import json # For Airflow Variable parsing

from airflow.decorators import task # MODIFIED: Import task decorator
from airflow.models.dag import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable # For accessing Airflow Variables

# --- Constants ---
RSS_URL = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss"
TARGET_DAG_ID = "eurostat_dataset_processor_dag" # The DAG we will trigger
HLTH_KEYWORD = "HLTH"
PROCESSED_IDS_VAR_KEY = "processed_hlth_rss_ids" # Airflow Variable key

# --- TaskFlow API Functions ---

@task # MODIFIED: Added @task decorator
def fetch_and_identify_new_hlth_datasets(): # MODIFIED: Removed **kwargs
    """
    Fetches RSS, identifies new HLTH dataset IDs by comparing with a stored list
    of processed IDs (from an Airflow Variable), and returns new IDs.
    """
    print(f"Fetching RSS feed from: {RSS_URL}")
    current_hlth_ids_in_rss = set()
    try:
        response = requests.get(RSS_URL, timeout=30)
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
            if found_id and HLTH_KEYWORD in found_id:
                current_hlth_ids_in_rss.add(found_id)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        # MODIFIED: return empty list on error
        # No explicit XCom push needed, just return
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=[])
        raise # Fail task if RSS fetch is critical - returning [] will be done if not raised
    except ET.ParseError as e:
        print(f"Error parsing RSS XML: {e}")
        # MODIFIED: return empty list on error
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=[])
        raise # Fail task
    except Exception as e:
        print(f"An unexpected error during RSS processing: {e}")
        # MODIFIED: return empty list on error
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=[])
        raise # Fail task

    if not current_hlth_ids_in_rss:
        print("No 'HLTH' dataset IDs found in the current RSS feed.")
        # MODIFIED: return empty list
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=[])
        return [] # MODIFIED

    print(f"Found {len(current_hlth_ids_in_rss)} 'HLTH' dataset IDs in RSS: {current_hlth_ids_in_rss}")

    try:
        processed_ids_json = Variable.get(PROCESSED_IDS_VAR_KEY, default_var="[]")
        processed_ids_set = set(json.loads(processed_ids_json))
        print(f"Successfully loaded {len(processed_ids_set)} processed IDs from Variable '{PROCESSED_IDS_VAR_KEY}'.")
    except json.JSONDecodeError:
        print(f"Warning: Airflow Variable '{PROCESSED_IDS_VAR_KEY}' is not valid JSON. Assuming no processed IDs.")
        processed_ids_set = set()
    except Exception as e:
        print(f"Warning: Could not load Airflow Variable '{PROCESSED_IDS_VAR_KEY}'. Assuming no processed IDs. Error: {e}")
        processed_ids_set = set()

    new_hlth_ids = list(current_hlth_ids_in_rss - processed_ids_set)

    if new_hlth_ids:
        print(f"Identified {len(new_hlth_ids)} new HLTH dataset IDs to process: {new_hlth_ids}")
        # MODIFIED: No explicit XCom push, just return
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=new_hlth_ids)
    else:
        print("No new HLTH dataset IDs to process (all found IDs were already processed or none found).")
        # MODIFIED: No explicit XCom push, just return
        # kwargs['ti'].xcom_push(key='new_hlth_dataset_ids', value=[])
    
    return new_hlth_ids # MODIFIED: return the list


@task.short_circuit # MODIFIED: Added @task.short_circuit decorator
def check_if_datasets_found_to_process(dataset_ids_to_process: list): # MODIFIED: Signature changed
    """ShortCircuit callable: returns True if dataset_ids_to_process is not empty."""
    # MODIFIED: Removed ti and xcom_pull
    # ti = kwargs['ti']
    # dataset_ids_to_process = ti.xcom_pull(task_ids='fetch_and_identify_new_hlth_datasets_task', key='new_hlth_dataset_ids')
    if dataset_ids_to_process and len(dataset_ids_to_process) > 0:
        print(f"Proceeding: {len(dataset_ids_to_process)} new dataset(s) found.")
        return True
    print("Skipping trigger: No new datasets found to process.")
    return False

with DAG(
    dag_id="eurostat_health_rss_monitor_dag",
    schedule="0 7,19 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["eurostat", "rss_monitor"],
    doc_md="""
    ### Eurostat Health RSS Monitor DAG
    Checks Eurostat RSS for NEW 'HLTH' updates and triggers `eurostat_dataset_processor_dag`.
    It compares found HLTH ds IDs with a list stored in Airflow Variable `processed_hlth_rss_ids`.
    Uses TaskFlow API.
    """
) as dag:
    # MODIFIED: Task instantiation using decorated functions
    # The task_id will default to the function name: 'fetch_and_identify_new_hlth_datasets'
    # and 'check_if_datasets_found_to_process'
    new_dataset_ids_list = fetch_and_identify_new_hlth_datasets() 
    
    should_trigger_dag = check_if_datasets_found_to_process(dataset_ids_to_process=new_dataset_ids_list)

    trigger_processor_dag_task = TriggerDagRunOperator(
        task_id="trigger_dataset_processor",
        trigger_dag_id=TARGET_DAG_ID,
        # MODIFIED: Pass the direct output from the upstream task
        conf={"dataset_ids": new_dataset_ids_list},
        wait_for_completion=False,
    )

    new_dataset_ids_list >> should_trigger_dag >> trigger_processor_dag_task 