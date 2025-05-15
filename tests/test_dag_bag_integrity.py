import pytest
from airflow.models import DagBag

DAGS_FOLDER = 'dags/' # Relative to where pytest is run (project root)
EXPECTED_DAG_IDS = [
    "eurostat_health_rss_monitor_dag",
    "eurostat_weekly_catalog_update_dag",
    "eurostat_dataset_processor_dag",
    "eurostat_data_pipeline" # Your original DAG, if still present
]

@pytest.fixture(scope="session") # DagBag parsing can be slow, so do it once per session
def dag_bag():
    print(f"Loading DAGs from: {DAGS_FOLDER}")
    db = DagBag(dag_folder=DAGS_FOLDER, include_examples=False, read_dags_from_db=False)
    if db.import_errors:
        print("DAG Import Errors:")
        for dag_file, error_msg in db.import_errors.items():
            print(f"  File: {dag_file}\n    Error: {error_msg}")
    return db

def test_dag_bag_has_no_import_errors(dag_bag):
    """Assert that there are no DAG parsing errors."""
    assert not dag_bag.import_errors, f"DAG Import Errors found: {dag_bag.import_errors}"

@pytest.mark.parametrize("dag_id", EXPECTED_DAG_IDS)
def test_dag_is_loaded_in_dag_bag(dag_bag, dag_id):
    """Assert that a specific DAG ID is loaded in the DagBag."""
    assert dag_id in dag_bag.dags, f"DAG '{dag_id}' not found in DagBag. Loaded DAGs: {list(dag_bag.dags.keys())}"
    assert dag_bag.dags[dag_id] is not None

# You can add more tests here to check for specific tasks within each DAG if needed.
# Example:
# def test_processor_dag_has_get_ids_task(dag_bag):
#     dag_id = "eurostat_dataset_processor_dag"
#     assert dag_id in dag_bag.dags, f"DAG '{dag_id}' not found."
#     dag = dag_bag.dags[dag_id]
#     assert dag.has_task("get_dataset_ids_to_process"), "Task 'get_dataset_ids_to_process' not found." 