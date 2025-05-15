import pytest
from unittest.mock import MagicMock
import json

# Adjust the import path based on your project structure and PYTHONPATH
# This assumes 'dags' is a directory accessible from where pytest is run,
# or that PYTHONPATH is set up for pytest to find modules in 'dags'.
# If eurostat_dataset_processor_dag.py can be imported as a module:
from dags.eurostat_dataset_processor_dag import get_dataset_ids_from_conf_or_params

@pytest.fixture
def mock_task_instance(mocker): # mocker is a fixture from pytest-mock
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    return ti

@pytest.fixture
def mock_dag_run():
    dag_run = MagicMock()
    dag_run.conf = {}
    return dag_run

# Test cases

def test_get_ids_from_dag_run_conf(mock_task_instance, mock_dag_run):
    """Test IDs are correctly pulled from dag_run.conf."""
    expected_ids = ["ID1", "ID2"]
    mock_dag_run.conf = {'dataset_ids': expected_ids}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': {}
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == expected_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=expected_ids)

def test_get_ids_from_params(mock_task_instance, mock_dag_run):
    """Test IDs are correctly pulled from params when not in dag_run.conf."""
    expected_ids = ["ID3", "ID4"]
    # dag_run.conf is empty or doesn't have dataset_ids
    mock_dag_run.conf = {}
    params = {'dataset_ids': expected_ids}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': params
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == expected_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=expected_ids)

def test_get_ids_conf_takes_precedence(mock_task_instance, mock_dag_run):
    """Test dag_run.conf takes precedence over params."""
    conf_ids = ["CONF_ID1"]
    param_ids = ["PARAM_ID1"]
    mock_dag_run.conf = {'dataset_ids': conf_ids}
    params = {'dataset_ids': param_ids}

    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': params
    }

    result = get_dataset_ids_from_conf_or_params(**kwargs)

    assert result == conf_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=conf_ids)

def test_get_ids_empty_when_not_found(mock_task_instance, mock_dag_run):
    """Test returns empty list if IDs are not in conf or params."""
    mock_dag_run.conf = {}
    params = {}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': params
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == []
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=[])

def test_get_ids_from_json_string_in_conf(mock_task_instance, mock_dag_run):
    """Test IDs are correctly parsed from a JSON string list in dag_run.conf."""
    expected_ids = ["JSON_ID1", "JSON_ID2"]
    mock_dag_run.conf = {'dataset_ids': json.dumps(expected_ids)}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': {}
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == expected_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=expected_ids)

def test_get_ids_from_non_json_string_in_conf(mock_task_instance, mock_dag_run):
    """Test a non-JSON string is treated as a single item list."""
    input_id_string = "NON_JSON_ID"
    expected_ids = [input_id_string]
    mock_dag_run.conf = {'dataset_ids': input_id_string}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': {}
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == expected_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=expected_ids)

def test_get_ids_with_empty_string_in_conf(mock_task_instance, mock_dag_run):
    """Test an empty string results in an empty list."""
    mock_dag_run.conf = {'dataset_ids': ""}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': {}
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == []
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=[])

def test_get_ids_filters_empty_items_and_stringifies(mock_task_instance, mock_dag_run):
    """Test that input list items are stringified and empty/None items are filtered."""
    input_ids = ["ID1", None, "ID2", "", 123, "ID3"]
    expected_ids = ["ID1", "ID2", "123", "ID3"]
    mock_dag_run.conf = {'dataset_ids': input_ids}
    
    kwargs = {
        'ti': mock_task_instance,
        'dag_run': mock_dag_run,
        'params': {}
    }
    
    result = get_dataset_ids_from_conf_or_params(**kwargs)
    
    assert result == expected_ids
    mock_task_instance.xcom_push.assert_called_once_with(key='final_dataset_ids', value=expected_ids)

# To run these tests, navigate to your project root in the terminal and run: pytest 