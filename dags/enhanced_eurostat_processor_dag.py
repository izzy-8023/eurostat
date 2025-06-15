#!/usr/bin/env python3
"""
 Eurostat Dataset Processor DAG

This DAG leverages the scripts with shared modules, improved error handling,
and centralized configuration. It provides better observability and performance.

Features:
- SourceData.py with shared health datasets
- json_to_postgres_loader.py with streaming
- consolidated_model_generator.py for unified model generation
- topic_mart_generator.py for automated mart creation
- Centralized configuration via shared modules
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import logging
from pathlib import Path
import os
import sys
import subprocess
import shutil

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
from airflow.exceptions import AirflowException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _get_variable_with_default(key: str, default_value):
    """Helper function to get Airflow variable with default value"""
    try:
        return Variable.get(key)
    except KeyError:
        return default_value

# DAG Configuration
default_args = {
    'owner': 'eurostat-enhanced-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

dag = DAG(
    'enhanced_eurostat_processor',
    default_args=default_args,
    description='Enhanced Eurostat processor with shared modules and improved performance',
    schedule=None,  # Manual trigger or can be scheduled
    catchup=False,
    tags=['eurostat', 'enhanced', 'production', 'shared-modules'],
    doc_md=__doc__,
    is_paused_upon_creation=False,
)

@task(dag=dag)
def validate_environment(**context) -> Dict[str, Any]:
    """
    Validate that all scripts and shared modules are available
    
    Returns:
        Environment validation results
    """
    sys.path.append('/opt/airflow/scripts')
    
    validation_results = {
        'shared_modules': False,
        'enhanced_scripts': False,
        'database_config': False,
        'health_datasets': False,
        'errors': []
    }
    
    try:
        # Test shared modules
        from shared.database import EurostatDatabase
        from shared.config import HEALTH_DATASETS, EUROSTAT_COLUMN_PATTERNS, get_db_config
        from shared.patterns import detect_column_patterns
        
        validation_results['shared_modules'] = True
        validation_results['health_datasets'] = len(HEALTH_DATASETS)
        logger.info(f" Shared modules loaded: {len(HEALTH_DATASETS)} health datasets available")
        
        # Test database configuration
        db_config = get_db_config()
        db = EurostatDatabase()
        validation_results['database_config'] = True
        logger.info(" Database configuration validated")
        
        # Verify scripts exist
        script_paths = [
            '/opt/airflow/scripts/SourceData.py',
            '/opt/airflow/scripts/json_to_postgres_loader.py',
            # '/opt/airflow/scripts/consolidated_model_generator.py', # Deprecated
            '/opt/airflow/scripts/topic_mart_generator.py'
        ]
        
        missing_scripts = []
        for script_path in script_paths:
            if not Path(script_path).exists():
                missing_scripts.append(script_path)
        
        if missing_scripts:
            validation_results['errors'].append(f"Missing scripts: {missing_scripts}")
        else:
            validation_results['enhanced_scripts'] = True
            logger.info(" All scripts available")
            
    except Exception as e:
        error_msg = f"Environment validation failed: {str(e)}"
        validation_results['errors'].append(error_msg)
        logger.error(f" {error_msg}")
        raise AirflowException(error_msg)
    
    if validation_results['errors']:
        raise AirflowException(f"Environment validation failed: {validation_results['errors']}")
    
    logger.info(" Environment validation completed successfully")
    return validation_results

@task(dag=dag)
def plan_processing(dataset_ids: Optional[List[str]] = None, **context) -> Dict[str, Any]:
    """
    Create processing plan using configuration
    
    Args:
        dataset_ids: Optional list of specific datasets to process
    
    Returns:
        processing plan with shared configuration
    """
    sys.path.append('/opt/airflow/scripts')
    
    from shared.config import HEALTH_DATASETS, is_health_dataset
    
    # Determine datasets to process
    # Initialize processing_remarks as an empty dict
    processing_remarks_from_conf = {}

    if not dataset_ids:
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            for key in ['dataset_ids', 'dataset_id', 'datasets']:
                if key in dag_run.conf:
                    config_value = dag_run.conf[key]
                    if isinstance(config_value, str):
                        dataset_ids = [config_value]
                    elif isinstance(config_value, list):
                        dataset_ids = config_value
                    break
            # Also retrieve processing_remarks if passed in conf
            if 'processing_remarks' in dag_run.conf and isinstance(dag_run.conf['processing_remarks'], dict):
                processing_remarks_from_conf = dag_run.conf['processing_remarks']
                logger.info(f"Received processing_remarks from dag_run.conf: {processing_remarks_from_conf}")
        
        if not dataset_ids: # Fallback to shared health datasets only if no specific datasets requested
            try:
                use_health_datasets = Variable.get("use_shared_health_datasets", deserialize_json=True)
            except KeyError:
                use_health_datasets = True
                
            if use_health_datasets:
                dataset_ids = HEALTH_DATASETS[:5]  # Process first 5 for demo
                logger.info(f" Using shared health datasets: {len(dataset_ids)} datasets")
            else:
                try:
                    dataset_ids = Variable.get("eurostat_target_datasets", deserialize_json=True)
                except KeyError:
                    dataset_ids = ["hlth_cd_ainfo", "hlth_dh010"]
    
    # Create  processing plan
    processing_plan = {
        'datasets': dataset_ids,
        'run_id': context['run_id'],
        'data_dir': f"/opt/airflow/temp_enhanced_downloads/{context['run_id']}",
        'use_shared_config': True,
        'health_datasets_count': len([d for d in dataset_ids if is_health_dataset(d)]),
        'batch_size': _get_variable_with_default("enhanced_batch_size", 2000),
        'enable_streaming': _get_variable_with_default("enable_streaming_load", True),
        'generate_marts': _get_variable_with_default("auto_generate_marts", True),
        'processing_remarks': processing_remarks_from_conf,
        'successful_loads': []  # Initialize empty list for successful loads
    }
    
    # Create data directory
    os.makedirs(processing_plan['data_dir'], exist_ok=True)
    
    logger.info(f" Processing plan created for {len(dataset_ids)} datasets")
    logger.info(f" Health datasets: {processing_plan['health_datasets_count']}")
    
    return processing_plan

@task(dag=dag)
def enhanced_download_datasets(processing_plan: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Download datasets using SourceData.py with shared configuration
    
    Args:
        processing_plan: Processing plan from plan_processing task
    
    Returns:
        Download results with metrics
    """
    import subprocess
    import time
    sys.path.append('/opt/airflow/scripts')
    
    from shared.config import HEALTH_DATASETS
    
    dataset_ids = processing_plan['datasets']
    data_dir = processing_plan['data_dir']
    
    logger.info(f" Starting download for {len(dataset_ids)} datasets")
    
    # Build download command
    dataset_list = ','.join(dataset_ids)
    cmd = [
        'python', '/opt/airflow/scripts/SourceData.py',
        '--action', 'download-datasets',
        '--dataset-ids', dataset_list,
        '--data-dir', data_dir,
        '--enable-db-tracking'  # Optional database tracking
    ]
    
    # Only use shared health datasets if we're processing the default shared list
    # Don't use it when specific custom datasets are requested
    if dataset_ids == processing_plan.get('datasets', []) and all(ds.lower() in [h.lower() for h in HEALTH_DATASETS] for ds in dataset_ids):
        cmd.append('--use-shared-health-datasets')
        logger.info(" Using shared health datasets configuration")
    else:
        logger.info(" Processing custom datasets - bypassing shared list")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        download_time = time.time() - start_time
        
        logger.info(f" Download completed in {download_time:.2f} seconds")
        logger.info(f" Download output: {result.stdout}")
        
        # Verify downloads and collect metrics
        download_results = {
            'successful_downloads': [],
            'failed_downloads': [],
            'total_size_mb': 0,
            'download_time_seconds': download_time,
            'files_info': {}
        }
        
        for dataset_id in dataset_ids:
            json_path = f"{data_dir}/{dataset_id}.json"
            if Path(json_path).exists():
                file_size = Path(json_path).stat().st_size
                download_results['successful_downloads'].append(dataset_id)
                download_results['total_size_mb'] += file_size / (1024 * 1024)
                download_results['files_info'][dataset_id] = {
                    'path': json_path,
                    'size_mb': file_size / (1024 * 1024)
                }
                logger.info(f" {dataset_id}: {file_size / (1024 * 1024):.2f} MB")
            else:
                download_results['failed_downloads'].append(dataset_id)
                logger.error(f" Failed to download {dataset_id}")
        
        if download_results['failed_downloads']:
            raise AirflowException(f"Failed to download: {download_results['failed_downloads']}")
        
        logger.info(f"Total downloaded: {download_results['total_size_mb']:.2f} MB")
        return download_results
        
    except subprocess.CalledProcessError as e:
        error_msg = f"download failed: {e.stderr}"
        logger.error(f" {error_msg}")
        raise AirflowException(error_msg)

@task(dag=dag)
def enhanced_load_to_database(processing_plan: Dict[str, Any], download_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Load datasets using json_to_postgres_loader.py with streaming
    
    Args:
        processing_plan: Processing plan
        download_results: Results from download task
    
    Returns:
        Loading results with performance metrics
    """
    import subprocess
    import time
    
    successful_downloads = download_results['successful_downloads']
    batch_size = processing_plan['batch_size']
    
    logger.info(f" Starting database loading for {len(successful_downloads)} datasets")
    
    loading_results = {
        'successful_loads': [],
        'failed_loads': [],
        'total_rows_loaded': 0,
        'loading_time_seconds': 0,
        'performance_metrics': {}
    }
    
    start_time = time.time()
    
    for dataset_id in successful_downloads:
        file_info = download_results['files_info'][dataset_id]
        json_path = file_info['path']
        table_name = dataset_id.lower()
        
        logger.info(f"Loading {dataset_id} to table {table_name}")
        
        # Build loading command
        cmd = [
            'python', '/opt/airflow/scripts/json_to_postgres_loader.py',
            '--input-file', json_path,
            '--table-name', table_name,
            '--source-dataset-id', dataset_id,
            '--batch-size', str(batch_size),
            '--log-level', 'DEBUG'
        ]
        
        dataset_start_time = time.time()
        
        try:
            # Log the command being run
            logger.info(f"Executing command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            dataset_load_time = time.time() - dataset_start_time
            
            # Log stdout and stderr from the script
            logger.info(f"Output from json_to_postgres_loader.py for {dataset_id}:\\nSTDOUT:\\n{result.stdout}\\nSTDERR:\\n{result.stderr}")

            # Extract row count from output (loader provides this)
            output_lines = result.stderr.split('\n')
            rows_loaded = 0
            for line in output_lines:
                # Match the new log format from json_to_postgres_loader.py
                if 'Successfully loaded' in line and 'total rows into' in line: 
                    try:
                        rows_loaded_str = line.split('Successfully loaded')[1].split('total rows into')[0].strip()
                        rows_loaded = int(rows_loaded_str)
                        logger.info(f"Extracted {rows_loaded} rows from log line: {line}")
                        break # Found the line, no need to parse further
                    except Exception as e_parse:
                        logger.warning(f"Could not parse row count from log line: '{line}'. Error: {e_parse}")
                        pass
            
            loading_results['successful_loads'].append(dataset_id)
            loading_results['total_rows_loaded'] += rows_loaded
            loading_results['performance_metrics'][dataset_id] = {
                'rows_loaded': rows_loaded,
                'load_time_seconds': dataset_load_time,
                'rows_per_second': rows_loaded / dataset_load_time if dataset_load_time > 0 else 0,
                'file_size_mb': file_info['size_mb']
            }
            
            logger.info(f" {dataset_id}: {rows_loaded:,} rows in {dataset_load_time:.2f}s ({rows_loaded/dataset_load_time:.0f} rows/sec)")
            
        except subprocess.CalledProcessError as e:
            # Log stdout and stderr on error as well
            logger.error(f" Failed to load {dataset_id}. Return code: {e.returncode}")
            logger.error(f"Output from json_to_postgres_loader.py for {dataset_id} (on error):\\nSTDOUT:\\n{e.stdout}\\nSTDERR:\\n{e.stderr}")
            loading_results['failed_loads'].append(dataset_id)
    
    loading_results['loading_time_seconds'] = time.time() - start_time
    
    if loading_results['failed_loads']:
        raise AirflowException(f"Failed to load datasets: {loading_results['failed_loads']}")
    
    # Update the processing plan with successful loads
    processing_plan['successful_loads'] = loading_results['successful_loads']
    
    logger.info(f" loading completed: {loading_results['total_rows_loaded']:,} total rows")
    logger.info(f" Average performance: {loading_results['total_rows_loaded']/loading_results['loading_time_seconds']:.0f} rows/sec")
    
    return loading_results

@task(dag=dag)
def extract_source_metadata_task(loading_results: Dict[str, Any], processing_plan: Dict[str, Any], **context) -> Dict[str, Dict[str, Any]]:
    """
    Extracts source_data_updated_at and title from the downloaded JSON file
    for each successfully loaded dataset.
    """
    logger.info(" Extracting source metadata for successfully loaded datasets...")
    datasets_metadata = {}
    
    # Ensure shared modules can be imported
    # This might already be handled by PYTHONPATH in the execution environment or global setup
    scripts_dir = '/opt/airflow/scripts'
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)
    from shared.catalog_utils import get_metadata_from_dataset_json

    data_dir = processing_plan.get('data_dir', '/opt/airflow/temp_enhanced_downloads/unknown_run')

    for dataset_id in loading_results.get('successful_loads', []):
        # Construct path to the JSON file. Assumes structure from 'enhanced_download_datasets' task.
        # e.g., data_dir / dataset_id.json
        json_file_path = Path(data_dir) / f"{dataset_id}.json"
        
        if json_file_path.exists():
            metadata = get_metadata_from_dataset_json(str(json_file_path))
            if metadata:
                datasets_metadata[dataset_id] = metadata
                logger.info(f"Extracted metadata for {dataset_id}: {metadata}")
            else:
                logger.warning(f"Could not extract metadata for {dataset_id} from {json_file_path}")
                datasets_metadata[dataset_id] = {"title": None, "update_data_date": None} # Ensure entry exists
        else:
            logger.warning(f"JSON file not found for metadata extraction: {json_file_path}")
            datasets_metadata[dataset_id] = {"title": None, "update_data_date": None} # Ensure entry exists

    return datasets_metadata

@task(dag=dag)
def generate_dbt_star_schema_files_task(
    processing_plan: Dict[str, Any], 
    loading_results: Dict[str, Any], 
    source_metadata: Dict[str, Any], 
    **context
) -> Dict[str, Any]:
    """
    Generates dbt dimension and fact models, and updates sources.yml.
    Dynamically scopes generation to successfully loaded datasets.
    """
    scripts_dir = '/opt/airflow/scripts'
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)
    
    import generate_dbt_star_schema # Your script

    successfully_loaded_dataset_ids = loading_results.get('successful_loads', [])
    
    if not successfully_loaded_dataset_ids:
        logger.info("No datasets successfully loaded. Skipping dbt star schema file generation.")
        return {"status": "skipped_no_loaded_data", "generated_files_count": 0}

    logger.info(f"Starting dbt star schema generation, filtered for: {successfully_loaded_dataset_ids}")

    try:
        # Assuming generate_dbt_star_schema.main() is modified to accept this filter
        # and uses it to scope its operations.
        generate_dbt_star_schema.main(
            processed_dataset_ids_filter=successfully_loaded_dataset_ids
        )
        logger.info(f"generate_dbt_star_schema.py executed for loaded datasets.")
        return {"status": "completed", "processed_filter_used": True, "dataset_count": len(successfully_loaded_dataset_ids)}
    except Exception as e:
        logger.error(f"Error during dbt star schema file generation: {e}", exc_info=True)
        raise AirflowException(f"generate_dbt_star_schema_files_task failed: {e}")

@task(dag=dag)
def generate_topic_marts(processing_plan: Dict[str, Any], star_schema_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Generate topic-based mart models using topic_mart_generator.py
    
    Args:
        processing_plan: Processing plan
        star_schema_results: Results from star schema generation
    
    Returns:
        Topic mart generation results
    """
    import subprocess
    import os
    
    if not processing_plan.get('generate_marts', True):
        logger.info("Topic mart generation disabled")
        return {'marts_generated': [], 'generation_skipped': True}
    
    logger.info("Generating topic-based mart models...")
    
    mart_results = {
        'marts_generated': [],
        'failed_generations': [],
        'generation_skipped': False
    }
    
    script_path = '/opt/airflow/scripts/topic_mart_generator.py'

    try:
        # Get the list of successfully loaded datasets from the processing plan
        successful_loads = processing_plan.get('successful_loads', [])
        if not successful_loads:
            logger.warning("No datasets were successfully loaded. Skipping mart generation.")
            return {'marts_generated': [], 'generation_skipped': True}

        # Build command with dataset filter
        cmd = ['python', script_path, '--datasets', ','.join(successful_loads)]
        
        # Set PYTHONPATH to include the shared modules directory
        env = os.environ.copy()
        env['PYTHONPATH'] = f"{env.get('PYTHONPATH', '')}:{os.path.dirname(script_path)}/shared"

        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env, cwd=os.path.dirname(script_path))
        
        # Extract generated mart names from output
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            if 'Generated' in line and 'mart models' in line:
                try:
                    count = int(line.split('Generated')[1].split('mart models')[0].strip())
                    logger.info(f" Generated {count} mart models")
                except:
                    pass
            elif 'Mart models:' in line:
                models_part = line.split('Mart models:')[1].strip()
                mart_results['marts_generated'] = [m.strip() for m in models_part.split(',')]
        
        logger.info(f" Topic marts generated: {mart_results['marts_generated']}")
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Topic mart generation failed: {e.stderr}"
        logger.error(f" {error_msg}")
        # Don't fail the entire DAG for mart generation issues
        mart_results['failed_generations'].append(error_msg)
        logger.warning(" Continuing without topic marts")
    
    return mart_results

@task(dag=dag)
def run_dbt_pipeline(
    star_schema_results: Dict[str, Any], 
    mart_generation_results: Dict[str, Any],
    loading_results: Dict[str, Any],  # Contains 'successful_loads'
    processing_plan: Dict[str, Any], 
    **context
) -> Dict[str, Any]:
    """
    Run dbt models, tests, and generate documentation.
    Dynamically selects models based on successfully loaded datasets.
    """
    import subprocess
    import os

    logger.info(" Running dbt pipeline (full-refresh & test)...")
    
    DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
    DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
    dbt_results = {
        "run_success": False,
        "run_output": "",
        "run_error": "",
        "test_success": False,
        "test_output": "",
        "test_error": "",
        "docs_success": False,
        "docs_error": ""
    }

    successful_dataset_ids = loading_results.get('successful_loads', [])
    base_dbt_args = ['--project-dir', DBT_PROJECT_DIR, '--profiles-dir', DBT_PROFILES_DIR]

    # Build source selectors for the successfully loaded datasets
    source_selectors = [f"source:eurostat_raw.{dataset_id.lower()}+" for dataset_id in successful_dataset_ids]
    
    if not source_selectors:
        logger.info("No datasets were successfully loaded. Skipping dbt operations.")
        return dbt_results

    def run_dbt_command(cmd, step_name):
        """Helper function to run dbt commands with consistent error handling"""
        try:
            logger.info(f"Executing command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"{step_name} completed successfully")
            logger.debug(f"Command output:\n{result.stdout}")
            return True, result.stdout, ""
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to {step_name.lower()}:"
            if e.stdout:
                error_msg += f"\nSTDOUT:\n{e.stdout}"
            if e.stderr:
                error_msg += f"\nSTDERR:\n{e.stderr}"
            logger.error(error_msg)
            return False, e.stdout, error_msg

    # Step 1: Run dimensions first
    logger.info("Step 1: Running dimension models...")
    dim_cmd = ['dbt', 'run', '--full-refresh', '--select', 'tag:dimension'] + base_dbt_args
    success, output, error = run_dbt_command(dim_cmd, "Build dimension models")
    if not success:
        raise AirflowException(error)

    # Step 2: Run fact models
    logger.info("Step 2: Running fact models...")
    fact_selectors = ' '.join(source_selectors)
    fact_cmd = ['dbt', 'run', '--full-refresh', '--select', fact_selectors, '--exclude', 'tag:mart'] + base_dbt_args
    success, output, error = run_dbt_command(fact_cmd, "Build fact models")
    if not success:
        raise AirflowException(error)

    # Step 3: Run mart models - only for the datasets we loaded
    logger.info("Step 3: Running mart models...")
    # Build a selector that includes both the source tables and their downstream mart models
    mart_selectors = []
    for dataset_id in successful_dataset_ids:
        # Add the source and all its downstream models (including marts)
        mart_selectors.append(f"source:eurostat_raw.{dataset_id.lower()}+")
    
    mart_cmd = ['dbt', 'run', '--full-refresh', '--select', ' '.join(mart_selectors), '--select', 'tag:mart'] + base_dbt_args
    success, output, error = run_dbt_command(mart_cmd, "Build mart models")
    if not success:
        raise AirflowException(error)
    dbt_results["run_success"] = True

    # Step 4: Run tests - only for the models we built
    logger.info("Running dbt tests...")
    test_selectors = []
    # Test dimensions
    test_selectors.append('tag:dimension')
    # Test facts for loaded datasets
    test_selectors.extend(source_selectors)
    # Test marts that depend on our loaded datasets
    test_selectors.extend(mart_selectors)
    test_selectors.append('tag:mart')

    test_cmd = ['dbt', 'test', '--select', ' '.join(test_selectors)] + base_dbt_args
    success, output, error = run_dbt_command(test_cmd, "Run tests")
    if success:
        dbt_results["test_success"] = True
        dbt_results["test_output"] = output
    else:
        dbt_results["test_error"] = error
        # Don't fail the DAG for test failures, just log them

    # Step 5: Generate documentation - only for the models we built
    logger.info("Generating dbt documentation...")
    docs_cmd = ['dbt', 'docs', 'generate', '--select', ' '.join(test_selectors)] + base_dbt_args
    success, output, error = run_dbt_command(docs_cmd, "Generate documentation")
    if success:
        dbt_results["docs_success"] = True
    else:
        dbt_results["docs_error"] = error
        # Don't fail the DAG for docs generation failures

    # Fail the task if critical dbt steps failed
    if not dbt_results["run_success"]:
        error_summary = "dbt pipeline execution failed:"
        if dbt_results["run_error"]:
            error_summary += f"\nRUN ERRORS:\n{dbt_results['run_error']}"
        if dbt_results["test_error"]:
            error_summary += f"\nTEST ERRORS:\n{dbt_results['test_error']}"
        raise AirflowException(error_summary)

    logger.info("dbt pipeline execution finished.")
    return dbt_results

@task(dag=dag)
def update_processed_log_db_task(dbt_execution_results: Dict[str, Any], 
                                 source_metadata_results: Dict[str, Dict[str, Any]], 
                                 loading_results: Dict[str, Any],
                                 processing_plan: Dict[str, Any],
                                 **context) -> None:
    """
    Updates the processed_dataset_log table for successfully processed datasets.
    """
    logger.info(" Updating processed_dataset_log database table...")
    
    scripts_dir = '/opt/airflow/scripts'
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)
    from shared.db_utils import update_processed_dataset_log_db

    # Get airflow_run_id from processing_plan or context as fallback
    airflow_run_id = processing_plan.get('run_id', context.get('run_id', 'unknown_run_id'))
    # Get processing_remarks_map from processing_plan
    processing_remarks_map = processing_plan.get('processing_remarks', {})

    if not dbt_execution_results.get('run_success', False):
        logger.warning("dbt run command did not complete successfully (based on run_success flag). Skipping update to processed_dataset_log.")
        return

    # We need the list of datasets that were part of this successful run.
    # loading_results['successful_loads'] gives us the dataset_ids handled by this run.
    successfully_loaded_ids = loading_results.get('successful_loads', [])

    for dataset_id in successfully_loaded_ids:
        metadata = source_metadata_results.get(dataset_id)
        if metadata:
            source_update_date = metadata.get('update_data_date')
            title = metadata.get('title')
            
            # Get specific remark for this dataset_id, or a default
            specific_remark = processing_remarks_map.get(dataset_id, f"Successfully processed in run {airflow_run_id}.")
            
            update_success = update_processed_dataset_log_db(
                dataset_id=dataset_id,
                source_data_updated_at=source_update_date, 
                dataset_title=title,
                airflow_run_id=airflow_run_id,
                remarks=specific_remark # Use specific or default remark
            )
            if not update_success:
                logger.error(f"Failed to log {dataset_id} to processed_dataset_log table.")
        else:
            logger.warning(f"No source metadata found for successfully loaded dataset {dataset_id}. Cannot update log.")

    logger.info("Database log update process finished.")

@task(dag=dag)
def generate_pipeline_summary(
    validation_results: Dict[str, Any],
    processing_plan: Dict[str, Any], 
    download_results: Dict[str, Any],
    loading_results: Dict[str, Any],
    dbt_results: Dict[str, Any],
    mart_results: Dict[str, Any],
    execution_results: Dict[str, Any],
    **context
) -> Dict[str, Any]:
    """
    Generate comprehensive pipeline execution summary
    
    Returns:
        Complete pipeline summary with metrics and results
    """
    
    logger.info(" Starting pipeline summary generation...")
    
    try:
        # Simple summary with basic metrics
        summary = {
            'pipeline_id': context.get('run_id', 'unknown'),
            'datasets_processed': len(processing_plan.get('datasets', [])),
            'total_rows_loaded': loading_results.get('total_rows_loaded', 0),
            'dbt_success': execution_results.get('run_success', False),
            'pipeline_success': True
        }
        
        logger.info(" ENHANCED PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f" Datasets Processed: {summary['datasets_processed']}")
        logger.info(f" Rows Loaded: {summary['total_rows_loaded']:,}")
        logger.info(f" dbt Success: {summary['dbt_success']}")
        logger.info(f" Pipeline Success: {summary['pipeline_success']}")
        logger.info("=" * 50)
        
        return summary
        
    except Exception as e:
        logger.error(f" Error in pipeline summary: {str(e)}")
        return {
            'pipeline_id': context.get('run_id', 'unknown'),
            'pipeline_success': False,
            'error': str(e)
        }

# Define task dependencies
env_validation = validate_environment()
plan = plan_processing()
downloads = enhanced_download_datasets(plan)
loading = enhanced_load_to_database(plan, downloads)
source_metadata_results = extract_source_metadata_task(loading_results=loading, processing_plan=plan)
star_schema_files = generate_dbt_star_schema_files_task(processing_plan=plan, loading_results=loading, source_metadata=source_metadata_results)
topic_mart_files = generate_topic_marts(processing_plan=loading, star_schema_results=star_schema_files)
dbt_execution = run_dbt_pipeline(star_schema_results=star_schema_files, mart_generation_results=topic_mart_files, loading_results=loading, processing_plan=plan)

# update_processed_log_db_task after dbt_execution
log_update = update_processed_log_db_task(
    dbt_execution_results=dbt_execution, 
    source_metadata_results=source_metadata_results, 
    loading_results=loading,
    processing_plan=plan
)

pipeline_summary = generate_pipeline_summary(
    validation_results=env_validation,
    processing_plan=plan,
    download_results=downloads,
    loading_results=loading,
    dbt_results=star_schema_files,
    mart_results=topic_mart_files,
    execution_results=dbt_execution,
)

# Define dependencies
env_validation >> plan >> downloads >> loading >> source_metadata_results >> star_schema_files >> topic_mart_files >> dbt_execution >> log_update >> pipeline_summary 