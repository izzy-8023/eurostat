#!/usr/bin/env python3
"""
Dynamic Eurostat Dataset Processor DAG

This DAG automatically detects dataset schemas and processes them accordingly.
It can handle any Eurostat dataset structure without manual configuration.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import yaml
import logging
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sdk import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG Configuration
default_args = {
    'owner': 'eurostat-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dynamic_eurostat_processor',
    default_args=default_args,
    description='Dynamic Eurostat dataset processor with automatic schema detection',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['eurostat', 'dynamic', 'schema-detection'],
)

@task(dag=dag)
def detect_schemas_and_plan(dataset_ids: List[str] = None, **context) -> Dict[str, Any]:
    """
    Detect schemas for specified datasets and create processing plan
    
    Args:
        dataset_ids: List of dataset IDs to process. If None, uses default list.
    
    Returns:
        Processing plan with schema information
    """
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from eurostat_schema_detector import EurostatSchemaDetector
    from batch_schema_processor import BatchSchemaProcessor
    
    # Default datasets if none specified
    if not dataset_ids:
        # Try to get from DAG run configuration first
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
            # Support multiple key variations for user convenience
            for key in ['dataset_ids', 'dataset_id', 'datasets_id', 'datasets']:
                if key in dag_run.conf:
                    config_value = dag_run.conf[key]
                    # Handle both list and string inputs
                    if isinstance(config_value, str):
                        dataset_ids = [config_value]
                    elif isinstance(config_value, list):
                        dataset_ids = config_value
                    logger.info(f"Found datasets in DAG config under key '{key}': {dataset_ids}")
                    break
        
        if not dataset_ids:
            # Fall back to Airflow variable or default
            dataset_ids = Variable.get("eurostat_target_datasets", 
                                     default_value=["HLTH_CD_AINFO", "HLTH_CD_YINFO"], 
                                     deserialize_json=True)
    
    logger.info(f"Processing schemas for datasets: {dataset_ids}")
    
    # Initialize schema detector
    detector = EurostatSchemaDetector()
    processor = BatchSchemaProcessor()
    
    processing_plan = {
        'datasets': {},
        'processing_groups': {},
        'download_tasks': [],
        'load_tasks': [],
        'dbt_tasks': []
    }
    
    # Process each dataset
    for dataset_id in dataset_ids:
        try:
            # Check if JSON file exists (from previous download)
            json_path = f"/opt/airflow/temp_processor_downloads/{context['run_id']}/{dataset_id}.json"
            
            if Path(json_path).exists():
                # Detect schema from existing file
                schema = detector.detect_schema(json_path)
                
                # Add to processing plan
                processing_plan['datasets'][dataset_id] = {
                    'schema': schema.to_dict(),
                    'table_name': dataset_id.lower(),
                    'dbt_model': f"stg_{dataset_id.lower()}",
                    'processing_group': processor._get_processing_group(schema),
                    'has_temporal': schema.is_temporal_dataset(),
                    'has_geographic': schema.is_geographic_dataset()
                }
                
                # Add tasks to plan
                processing_plan['download_tasks'].append(dataset_id)
                processing_plan['load_tasks'].append({
                    'dataset_id': dataset_id,
                    'table_name': dataset_id.lower(),
                    'json_path': json_path
                })
                processing_plan['dbt_tasks'].append(f"stg_{dataset_id.lower()}")
                
                logger.info(f"✓ Schema detected for {dataset_id}: {len(schema.dimensions)} dimensions")
            else:
                logger.warning(f"JSON file not found for {dataset_id}, will download first")
                processing_plan['download_tasks'].append(dataset_id)
                
        except Exception as e:
            logger.error(f"Failed to process schema for {dataset_id}: {e}")
            # Still add to download tasks to attempt processing
            processing_plan['download_tasks'].append(dataset_id)
    
    # Group datasets by similarity for optimized processing
    processing_plan['processing_groups'] = processor._group_datasets_by_similarity()
    
    logger.info(f"Processing plan created: {len(processing_plan['datasets'])} datasets")
    return processing_plan

@task(dag=dag)
def download_datasets(processing_plan: Dict[str, Any], **context) -> List[str]:
    """
    Download datasets based on processing plan
    
    Args:
        processing_plan: Plan from detect_schemas_and_plan task
    
    Returns:
        List of successfully downloaded dataset IDs
    """
    import subprocess
    import os
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from eurostat_schema_detector import EurostatSchemaDetector
    
    dataset_ids = processing_plan['download_tasks']
    run_id = context['run_id']
    
    if not dataset_ids:
        logger.info("No datasets to download")
        return []
    
    logger.info(f"Downloading {len(dataset_ids)} datasets")
    
    # Prepare download directory
    download_dir = f"/opt/airflow/temp_processor_downloads/{run_id}"
    os.makedirs(download_dir, exist_ok=True)
    
    # Build download command using SourceData.py
    dataset_list = ','.join(dataset_ids)
    cmd = [
        'python', '/opt/airflow/scripts/SourceData.py',
        '--action', 'download-datasets',
        '--dataset-ids', dataset_list,
        '--data-dir', download_dir,
        '--skip-download'  # Skip catalog download since we're targeting specific datasets
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"Download completed successfully")
        logger.info(f"Download output: {result.stdout}")
        
        # Verify downloads and detect schemas
        successful_downloads = []
        detector = EurostatSchemaDetector()
        
        for dataset_id in dataset_ids:
            json_path = f"{download_dir}/{dataset_id}.json"
            if Path(json_path).exists():
                try:
                    # Detect schema and update processing plan
                    schema = detector.detect_schema(json_path)
                    successful_downloads.append(dataset_id)
                    logger.info(f"✓ Downloaded and validated {dataset_id}")
                except Exception as e:
                    logger.error(f"Schema detection failed for {dataset_id}: {e}")
            else:
                logger.error(f"Download failed for {dataset_id}")
        
        return successful_downloads
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Download failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        raise

@task(dag=dag)
def generate_dynamic_sql_schemas(processing_plan: Dict[str, Any], downloaded_datasets: List[str], **context) -> Dict[str, str]:
    """
    Generate SQL schemas for downloaded datasets
    
    Args:
        processing_plan: Processing plan
        downloaded_datasets: List of successfully downloaded datasets
    
    Returns:
        Dictionary mapping dataset_id to SQL schema
    """
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from eurostat_schema_detector import EurostatSchemaDetector
    
    detector = EurostatSchemaDetector()
    run_id = context['run_id']
    sql_schemas = {}
    
    for dataset_id in downloaded_datasets:
        try:
            json_path = f"/opt/airflow/temp_processor_downloads/{run_id}/{dataset_id}.json"
            schema = detector.detect_schema(json_path)
            
            # Generate SQL schema
            sql_schema = detector.generate_sql_schema(schema, dataset_id.lower())
            sql_schemas[dataset_id] = sql_schema
            
            logger.info(f"✓ Generated SQL schema for {dataset_id}")
            
        except Exception as e:
            logger.error(f"Failed to generate SQL schema for {dataset_id}: {e}")
    
    return sql_schemas

@task(dag=dag)
def create_database_tables(sql_schemas: Dict[str, str]) -> List[str]:
    """
    Skip table creation - let the data loader handle it
    
    Args:
        sql_schemas: Dictionary of dataset_id -> SQL schema (unused)
    
    Returns:
        List of datasets ready for loading (all downloaded datasets)
    """
    # Return all dataset IDs from sql_schemas as "created" tables
    # The actual table creation will be handled by json_to_postgres_loader.py
    created_tables = [dataset_id.lower() for dataset_id in sql_schemas.keys()]
    
    for dataset_id in created_tables:
        logger.info(f"✓ Prepared for table creation: {dataset_id}")
    
    return created_tables

@task(dag=dag)
def load_datasets_to_db(processing_plan: Dict[str, Any], downloaded_datasets: List[str], created_tables: List[str], **context) -> List[str]:
    """
    Load datasets into database tables
    
    Args:
        processing_plan: Processing plan
        downloaded_datasets: Successfully downloaded datasets
        created_tables: Successfully created tables
    
    Returns:
        List of successfully loaded datasets
    """
    import subprocess
    
    run_id = context['run_id']
    loaded_datasets = []
    
    # Only process datasets that were both downloaded and have tables created
    datasets_to_load = set(downloaded_datasets) & set([t.upper() for t in created_tables])
    
    for dataset_id in datasets_to_load:
        try:
            json_path = f"/opt/airflow/temp_processor_downloads/{run_id}/{dataset_id}.json"
            table_name = dataset_id.lower()
            
            # Load using existing loader script
            cmd = [
                'python', '/opt/airflow/scripts/json_to_postgres_loader.py',
                '--input-file', json_path,
                '--table-name', table_name,
                '--dataset-id', dataset_id
            ]
            
            # Set up environment variables for the subprocess
            import os
            env = os.environ.copy()
            env.update({
                'POSTGRES_HOST': os.getenv('EUROSTAT_POSTGRES_HOST', 'eurostat_postgres_db'),
                'POSTGRES_PORT': os.getenv('EUROSTAT_POSTGRES_PORT', '5432'),
                'POSTGRES_DB': os.getenv('EUROSTAT_POSTGRES_DB', 'eurostat_data'),
                'POSTGRES_USER': os.getenv('EUROSTAT_POSTGRES_USER', 'eurostat_user'),
                'POSTGRES_PASSWORD': os.getenv('EUROSTAT_POSTGRES_PASSWORD', 'mysecretpassword')
            })
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)
            loaded_datasets.append(dataset_id)
            logger.info(f"✓ Loaded {dataset_id} to table {table_name}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to load {dataset_id}: {e}")
            logger.error(f"Error output: {e.stderr}")
    
    return loaded_datasets

@task(dag=dag)
def generate_dynamic_dbt_models(processing_plan: Dict[str, Any], loaded_datasets: List[str], **context) -> List[str]:
    """
    Generate dbt models dynamically for loaded datasets
    
    Args:
        processing_plan: Processing plan
        loaded_datasets: Successfully loaded datasets
    
    Returns:
        List of generated dbt models
    """
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from dbt_model_generator import DbtModelGenerator
    
    if not loaded_datasets:
        logger.info("No datasets loaded, skipping dbt model generation")
        return []
    
    logger.info(f"Generating dynamic dbt models for {len(loaded_datasets)} datasets")
    
    # Initialize the dynamic model generator
    generator = DbtModelGenerator()
    
    # Generate models for all loaded datasets
    models_dir = "/opt/airflow/dbt_project/models/staging"
    generated_models = generator.generate_models_for_datasets(loaded_datasets, models_dir)
    
    logger.info(f"✓ Generated {len(generated_models)} dbt models dynamically")
    return generated_models

@task(dag=dag)
def run_dbt_models(generated_models: List[str]) -> Dict[str, str]:
    """
    Run generated dbt models
    
    Args:
        generated_models: List of generated model names
    
    Returns:
        Dictionary of model -> status
    """
    import subprocess
    import os
    
    if not generated_models:
        logger.info("No dbt models to run")
        return {}
    
    model_results = {}
    
    # Change to dbt project directory
    dbt_dir = "/opt/airflow/dbt_project"
    
    for model in generated_models:
        try:
            cmd = ['dbt', 'run', '--models', model]
            
            # Set up environment variables for dbt
            env = os.environ.copy()
            env.update({
                'DBT_PROFILES_DIR': '/home/airflow/.dbt',
                'EUROSTAT_POSTGRES_HOST': os.getenv('EUROSTAT_POSTGRES_HOST', 'eurostat_postgres_db'),
                'EUROSTAT_POSTGRES_PORT': os.getenv('EUROSTAT_POSTGRES_PORT', '5432'),
                'EUROSTAT_POSTGRES_DB': os.getenv('EUROSTAT_POSTGRES_DB', 'eurostat_data'),
                'EUROSTAT_POSTGRES_USER': os.getenv('EUROSTAT_POSTGRES_USER', 'eurostat_user'),
                'EUROSTAT_POSTGRES_PASSWORD': os.getenv('EUROSTAT_POSTGRES_PASSWORD', 'mysecretpassword')
            })
            
            result = subprocess.run(
                cmd, 
                cwd=dbt_dir,
                capture_output=True, 
                text=True, 
                check=True,
                env=env
            )
            
            model_results[model] = "success"
            logger.info(f"✓ dbt model {model} completed successfully")
            
        except subprocess.CalledProcessError as e:
            model_results[model] = f"failed: {e}"
            logger.error(f"dbt model {model} failed: {e}")
            logger.error(f"Error output: {e.stderr}")
    
    return model_results

@task(dag=dag)
def generate_topic_marts(dbt_results: Dict[str, str]) -> Dict[str, Any]:
    """
    Generate topic-based mart models that combine related datasets
    
    Args:
        dbt_results: Results from dbt model execution
    
    Returns:
        Dictionary with mart generation results
    """
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from topic_mart_generator import TopicMartGenerator
    
    # Only proceed if dbt models were successful
    successful_models = [model for model, status in dbt_results.items() if status == "success"]
    
    if not successful_models:
        logger.info("No successful dbt models, skipping topic mart generation")
        return {'generated_marts': [], 'errors': ['No successful staging models available']}
    
    logger.info(f"Generating topic marts based on {len(successful_models)} successful staging models")
    
    # Initialize topic mart generator
    generator = TopicMartGenerator()
    
    # Generate marts for all topics
    csv_file = "/opt/airflow/grouped_datasets_summary.csv"
    models_dir = "/opt/airflow/dbt_project/models"
    
    try:
        results = generator.generate_topic_marts(csv_file, models_dir)
        
        logger.info(f"✓ Generated {len(results['generated_marts'])} topic marts")
        if results['errors']:
            logger.warning(f"Encountered {len(results['errors'])} errors during mart generation")
            for error in results['errors']:
                logger.warning(f"  - {error}")
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to generate topic marts: {e}")
        return {'generated_marts': [], 'errors': [str(e)]}

@task(dag=dag)
def run_topic_marts(mart_results: Dict[str, Any]) -> Dict[str, str]:
    """
    Run generated topic mart models
    
    Args:
        mart_results: Results from topic mart generation
    
    Returns:
        Dictionary of mart -> status
    """
    import subprocess
    import os
    
    generated_marts = mart_results.get('generated_marts', [])
    
    if not generated_marts:
        logger.info("No topic marts to run")
        return {}
    
    mart_execution_results = {}
    dbt_dir = "/opt/airflow/dbt_project"
    
    for mart in generated_marts:
        try:
            cmd = ['dbt', 'run', '--models', mart]
            
            # Set up environment variables for dbt
            env = os.environ.copy()
            env.update({
                'DBT_PROFILES_DIR': '/home/airflow/.dbt',
                'EUROSTAT_POSTGRES_HOST': os.getenv('EUROSTAT_POSTGRES_HOST', 'eurostat_postgres_db'),
                'EUROSTAT_POSTGRES_PORT': os.getenv('EUROSTAT_POSTGRES_PORT', '5432'),
                'EUROSTAT_POSTGRES_DB': os.getenv('EUROSTAT_POSTGRES_DB', 'eurostat_data'),
                'EUROSTAT_POSTGRES_USER': os.getenv('EUROSTAT_POSTGRES_USER', 'eurostat_user'),
                'EUROSTAT_POSTGRES_PASSWORD': os.getenv('EUROSTAT_POSTGRES_PASSWORD', 'mysecretpassword')
            })
            
            result = subprocess.run(
                cmd, 
                cwd=dbt_dir,
                capture_output=True, 
                text=True, 
                check=True,
                env=env
            )
            
            mart_execution_results[mart] = "success"
            logger.info(f"✓ Topic mart {mart} completed successfully")
            
        except subprocess.CalledProcessError as e:
            mart_execution_results[mart] = f"failed: {e}"
            logger.error(f"Topic mart {mart} failed: {e}")
            logger.error(f"Error output: {e.stderr}")
    
    return mart_execution_results

# Define task dependencies
schema_detection = detect_schemas_and_plan()
download_task = download_datasets(schema_detection)
sql_generation = generate_dynamic_sql_schemas(schema_detection, download_task)
table_creation = create_database_tables(sql_generation)
data_loading = load_datasets_to_db(schema_detection, download_task, table_creation)
dbt_generation = generate_dynamic_dbt_models(schema_detection, data_loading)
dbt_execution = run_dbt_models(dbt_generation)
topic_mart_generation = generate_topic_marts(dbt_execution)
topic_mart_execution = run_topic_marts(topic_mart_generation)

# Set up dependencies
schema_detection >> download_task >> sql_generation >> table_creation >> data_loading >> dbt_generation >> dbt_execution >> topic_mart_generation >> topic_mart_execution 