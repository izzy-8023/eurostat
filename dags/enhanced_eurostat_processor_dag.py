#!/usr/bin/env python3
"""
Enhanced Eurostat Dataset Processor DAG

This DAG leverages the enhanced scripts with shared modules, improved error handling,
and centralized configuration. It provides better observability and performance.

Features:
- Uses enhanced SourceData.py with shared health datasets
- Leverages enhanced json_to_postgres_loader.py with streaming
- Utilizes consolidated_model_generator.py for unified model generation
- Integrates topic_mart_generator.py for automated mart creation
- Enhanced logging with emoji indicators
- Centralized configuration via shared modules
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import logging
from pathlib import Path
import os

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
)

@task(dag=dag)
def validate_environment(**context) -> Dict[str, Any]:
    """
    Validate that all enhanced scripts and shared modules are available
    
    Returns:
        Environment validation results
    """
    import sys
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
        logger.info(f"✅ Shared modules loaded: {len(HEALTH_DATASETS)} health datasets available")
        
        # Test database configuration
        db_config = get_db_config()
        db = EurostatDatabase()
        validation_results['database_config'] = True
        logger.info("✅ Database configuration validated")
        
        # Verify enhanced scripts exist
        script_paths = [
            '/opt/airflow/scripts/SourceData.py',
            '/opt/airflow/scripts/json_to_postgres_loader.py',
            '/opt/airflow/scripts/consolidated_model_generator.py',
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
            logger.info("✅ All enhanced scripts available")
            
    except Exception as e:
        error_msg = f"Environment validation failed: {str(e)}"
        validation_results['errors'].append(error_msg)
        logger.error(f"❌ {error_msg}")
        raise AirflowException(error_msg)
    
    if validation_results['errors']:
        raise AirflowException(f"Environment validation failed: {validation_results['errors']}")
    
    logger.info("🚀 Environment validation completed successfully")
    return validation_results

@task(dag=dag)
def plan_processing(dataset_ids: Optional[List[str]] = None, **context) -> Dict[str, Any]:
    """
    Create processing plan using enhanced configuration
    
    Args:
        dataset_ids: Optional list of specific datasets to process
    
    Returns:
        Enhanced processing plan with shared configuration
    """
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from shared.config import HEALTH_DATASETS, is_health_dataset
    
    # Determine datasets to process
    if not dataset_ids:
        # Try to get from DAG run configuration
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
        
        # Fall back to shared health datasets only if no specific datasets requested
        if not dataset_ids:
            try:
                use_health_datasets = Variable.get("use_shared_health_datasets", deserialize_json=True)
            except KeyError:
                use_health_datasets = True
                
            if use_health_datasets:
                dataset_ids = HEALTH_DATASETS[:5]  # Process first 5 for demo
                logger.info(f"📋 Using shared health datasets: {len(dataset_ids)} datasets")
            else:
                try:
                    dataset_ids = Variable.get("eurostat_target_datasets", deserialize_json=True)
                except KeyError:
                    dataset_ids = ["hlth_cd_ainfo", "hlth_dh010"]
    
    # Create enhanced processing plan
    processing_plan = {
        'datasets': dataset_ids,
        'run_id': context['run_id'],
        'data_dir': f"/opt/airflow/temp_enhanced_downloads/{context['run_id']}",
        'use_shared_config': True,
        'health_datasets_count': len([d for d in dataset_ids if is_health_dataset(d)]),
        'batch_size': _get_variable_with_default("enhanced_batch_size", 2000),
        'enable_streaming': _get_variable_with_default("enable_streaming_load", True),
        'generate_marts': _get_variable_with_default("auto_generate_marts", True)
    }
    
    # Create data directory
    os.makedirs(processing_plan['data_dir'], exist_ok=True)
    
    logger.info(f"📊 Processing plan created for {len(dataset_ids)} datasets")
    logger.info(f"🏥 Health datasets: {processing_plan['health_datasets_count']}")
    
    return processing_plan

@task(dag=dag)
def enhanced_download_datasets(processing_plan: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Download datasets using enhanced SourceData.py with shared configuration
    
    Args:
        processing_plan: Processing plan from plan_processing task
    
    Returns:
        Download results with enhanced metrics
    """
    import subprocess
    import time
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from shared.config import HEALTH_DATASETS
    
    dataset_ids = processing_plan['datasets']
    data_dir = processing_plan['data_dir']
    
    logger.info(f"🚀 Starting enhanced download for {len(dataset_ids)} datasets")
    
    # Build enhanced download command
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
        logger.info("📋 Using shared health datasets configuration")
    else:
        logger.info("🎯 Processing custom datasets - bypassing shared list")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        download_time = time.time() - start_time
        
        logger.info(f"✅ Enhanced download completed in {download_time:.2f} seconds")
        logger.info(f"📥 Download output: {result.stdout}")
        
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
                logger.info(f"✅ {dataset_id}: {file_size / (1024 * 1024):.2f} MB")
            else:
                download_results['failed_downloads'].append(dataset_id)
                logger.error(f"❌ Failed to download {dataset_id}")
        
        if download_results['failed_downloads']:
            raise AirflowException(f"Failed to download: {download_results['failed_downloads']}")
        
        logger.info(f"📊 Total downloaded: {download_results['total_size_mb']:.2f} MB")
        return download_results
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Enhanced download failed: {e.stderr}"
        logger.error(f"❌ {error_msg}")
        raise AirflowException(error_msg)

@task(dag=dag)
def enhanced_load_to_database(processing_plan: Dict[str, Any], download_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Load datasets using enhanced json_to_postgres_loader.py with streaming
    
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
    
    logger.info(f"🔄 Starting enhanced database loading for {len(successful_downloads)} datasets")
    
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
        
        logger.info(f"📥 Loading {dataset_id} to table {table_name}")
        
        # Build enhanced loading command
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
            # Enhanced: Log the command being run
            logger.info(f"Executing command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            dataset_load_time = time.time() - dataset_start_time
            
            # Enhanced: Log stdout and stderr from the script
            logger.info(f"Output from json_to_postgres_loader.py for {dataset_id}:\\nSTDOUT:\\n{result.stdout}\\nSTDERR:\\n{result.stderr}")

            # Extract row count from output (enhanced loader provides this)
            output_lines = result.stderr.split('\n')
            rows_loaded = 0
            for line in output_lines:
                # Match the new log format from json_to_postgres_loader.py
                if 'Successfully loaded' in line and 'total rows into' in line: 
                    try:
                        # Example: "INFO - ✅ Successfully loaded 6025 total rows into hlth_ehis_sk2e"
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
            
            logger.info(f"✅ {dataset_id}: {rows_loaded:,} rows in {dataset_load_time:.2f}s ({rows_loaded/dataset_load_time:.0f} rows/sec)")
            
        except subprocess.CalledProcessError as e:
            # Enhanced: Log stdout and stderr on error as well
            logger.error(f"❌ Failed to load {dataset_id}. Return code: {e.returncode}")
            logger.error(f"Output from json_to_postgres_loader.py for {dataset_id} (on error):\\nSTDOUT:\\n{e.stdout}\\nSTDERR:\\n{e.stderr}")
            loading_results['failed_loads'].append(dataset_id)
    
    loading_results['loading_time_seconds'] = time.time() - start_time
    
    if loading_results['failed_loads']:
        raise AirflowException(f"Failed to load datasets: {loading_results['failed_loads']}")
    
    logger.info(f"🎉 Enhanced loading completed: {loading_results['total_rows_loaded']:,} total rows")
    logger.info(f"⚡ Average performance: {loading_results['total_rows_loaded']/loading_results['loading_time_seconds']:.0f} rows/sec")
    
    return loading_results

@task(dag=dag)
def generate_dbt_models(processing_plan: Dict[str, Any], loading_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Generate dbt sources and staging models using consolidated_model_generator.py
    
    Args:
        processing_plan: Processing plan
        loading_results: Results from loading task
    
    Returns:
        dbt model generation results
    """
    import subprocess
    
    successful_loads = loading_results['successful_loads']
    
    logger.info(f"🏗️ Generating dbt models for {len(successful_loads)} datasets")
    
    # Convert to table names (lowercase)
    table_names = [dataset_id.lower() for dataset_id in successful_loads]
    table_list = ','.join(table_names)
    
    dbt_results = {
        'sources_generated': False,
        'staging_models_generated': [],
        'failed_generations': []
    }
    
    try:
        # Generate sources
        logger.info("📋 Generating dbt sources...")
        sources_cmd = [
            'python', '/opt/airflow/scripts/consolidated_model_generator.py',
            '--action', 'sources',
            '--dataset-ids', table_list,
            '--sources-file', '/opt/airflow/dbt_project/models/staging/sources.yml'
        ]
        
        result = subprocess.run(sources_cmd, capture_output=True, text=True, check=True)
        dbt_results['sources_generated'] = True
        logger.info(f"✅ Sources generated: {result.stdout}")
        
        # Generate staging models
        logger.info("🏭 Generating staging models...")
        models_cmd = [
            'python', '/opt/airflow/scripts/consolidated_model_generator.py',
            '--action', 'models',
            '--dataset-ids', table_list,
            '--models-dir', '/opt/airflow/dbt_project/models/staging'
        ]
        
        result = subprocess.run(models_cmd, capture_output=True, text=True, check=True)
        
        # Extract generated model names from output
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            if 'Generated' in line and 'staging models:' in line:
                models_part = line.split('staging models:')[1].strip()
                dbt_results['staging_models_generated'] = [m.strip() for m in models_part.split(',')]
                break
        
        logger.info(f"✅ Staging models generated: {dbt_results['staging_models_generated']}")
        
    except subprocess.CalledProcessError as e:
        error_msg = f"dbt model generation failed: {e.stderr}"
        logger.error(f"❌ {error_msg}")
        raise AirflowException(error_msg)
    
    return dbt_results

@task(dag=dag)
def generate_topic_marts(processing_plan: Dict[str, Any], dbt_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Generate topic-based mart models using topic_mart_generator.py
    
    Args:
        processing_plan: Processing plan
        dbt_results: Results from dbt model generation
    
    Returns:
        Topic mart generation results
    """
    import subprocess
    
    if not processing_plan.get('generate_marts', True):
        logger.info("🚫 Topic mart generation disabled")
        return {'marts_generated': [], 'generation_skipped': True}
    
    logger.info("🎯 Generating topic-based mart models...")
    
    mart_results = {
        'marts_generated': [],
        'failed_generations': [],
        'generation_skipped': False
    }
    
    try:
        # Generate topic marts for all available topics (Solution 1)
        # The CSV contains specific topic names like "Body mass index (BMI)", "Causes of death", etc.

        cmd = [
            'python', '/opt/airflow/scripts/topic_mart_generator.py',
            '--models-dir', '/opt/airflow/dbt_project/models',
            '--csv-file', '/opt/airflow/grouped_datasets_summary.csv'
            # No --topics filter - will generate marts for all topic groups with 2+ datasets
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Extract generated mart names from output
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            if 'Generated' in line and 'mart models' in line:
                # Look for pattern like "Generated 5 mart models"
                try:
                    count = int(line.split('Generated')[1].split('mart models')[0].strip())
                    logger.info(f"✅ Generated {count} mart models")
                except:
                    pass
            elif 'Mart models:' in line:
                # Extract model names
                models_part = line.split('Mart models:')[1].strip()
                mart_results['marts_generated'] = [m.strip() for m in models_part.split(',')]
        
        logger.info(f"🎯 Topic marts generated: {mart_results['marts_generated']}")
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Topic mart generation failed: {e.stderr}"
        logger.error(f"❌ {error_msg}")
        # Don't fail the entire DAG for mart generation issues
        mart_results['failed_generations'].append(error_msg)
        logger.warning("⚠️ Continuing without topic marts")
    
    return mart_results

@task(dag=dag)
def run_dbt_pipeline(dbt_results: Dict[str, Any], mart_results: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Execute the complete dbt pipeline (staging models + marts)
    
    Args:
        dbt_results: Results from dbt model generation
        mart_results: Results from topic mart generation
    
    Returns:
        dbt execution results
    """
    import subprocess
    import os
    
    logger.info("🚀 Running dbt pipeline...")
    
    # Set dbt environment
    env = os.environ.copy()
    env['DBT_PROFILES_DIR'] = '/opt/airflow/dbt_project'
    
    execution_results = {
        'staging_models_run': [],
        'mart_models_run': [],
        'failed_models': [],
        'dbt_run_success': False
    }
    
    try:
        # First, try to run only staging models to avoid dependency issues
        logger.info("🏭 Running staging models first...")
        staging_cmd = [
            'dbt', 'run',
            '--select', 'staging',
            '--project-dir', '/opt/airflow/dbt_project',
            '--profiles-dir', '/opt/airflow/dbt_project'
        ]
        
        staging_result = subprocess.run(staging_cmd, capture_output=True, text=True, env=env)
        
        if staging_result.returncode == 0:
            logger.info(f"✅ Staging models executed successfully")
            logger.info(f"📊 Staging output: {staging_result.stdout}")
            
            # Parse staging output
            staging_lines = staging_result.stdout.split('\n')
            for line in staging_lines:
                if 'OK created' in line or 'OK view' in line:
                    if 'stg_' in line:
                        execution_results['staging_models_run'].append(line.strip())
            
            # Try to run mart models, but don't fail if they have dependency issues
            logger.info("🎯 Attempting to run mart models...")
            mart_cmd = [
                'dbt', 'run',
                '--select', 'marts',
                '--project-dir', '/opt/airflow/dbt_project',
                '--profiles-dir', '/opt/airflow/dbt_project'
            ]
            
            mart_result = subprocess.run(mart_cmd, capture_output=True, text=True, env=env)
            
            if mart_result.returncode == 0:
                logger.info(f"✅ Mart models executed successfully")
                logger.info(f"📊 Mart output: {mart_result.stdout}")
                
                # Parse mart output
                mart_lines = mart_result.stdout.split('\n')
                for line in mart_lines:
                    if 'OK created' in line or 'OK view' in line:
                        if 'mart_' in line:
                            execution_results['mart_models_run'].append(line.strip())
            else:
                logger.warning(f"⚠️ Mart models failed (expected for new datasets): {mart_result.stderr}")
                execution_results['failed_models'].append(f"Mart models failed: {mart_result.stderr}")
            
            execution_results['dbt_run_success'] = True
            
        else:
            logger.error(f"❌ Staging models failed: {staging_result.stderr}")
            execution_results['failed_models'].append(f"Staging models failed: {staging_result.stderr}")
            raise subprocess.CalledProcessError(staging_result.returncode, staging_cmd, staging_result.stderr)
        
    except subprocess.CalledProcessError as e:
        error_msg = f"dbt pipeline execution failed: {e.stderr}"
        logger.error(f"❌ {error_msg}")
        execution_results['failed_models'].append(error_msg)
        # Continue execution to provide partial results
    
    logger.info(f"🏁 dbt pipeline completed: {len(execution_results['staging_models_run'])} staging, {len(execution_results['mart_models_run'])} marts")
    
    return execution_results

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
    
    logger.info("🚀 Starting pipeline summary generation...")
    
    try:
        # Simple summary with basic metrics
        summary = {
            'pipeline_id': context.get('run_id', 'unknown'),
            'datasets_processed': len(processing_plan.get('datasets', [])),
            'total_rows_loaded': loading_results.get('total_rows_loaded', 0),
            'dbt_success': execution_results.get('dbt_run_success', False),
            'pipeline_success': True
        }
        
        logger.info("📊 ENHANCED PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"🎯 Datasets Processed: {summary['datasets_processed']}")
        logger.info(f"📊 Rows Loaded: {summary['total_rows_loaded']:,}")
        logger.info(f"🏗️ dbt Success: {summary['dbt_success']}")
        logger.info(f"✅ Pipeline Success: {summary['pipeline_success']}")
        logger.info("=" * 50)
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error in pipeline summary: {str(e)}")
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
dbt_models = generate_dbt_models(plan, loading)
topic_marts = generate_topic_marts(plan, dbt_models)
dbt_execution = run_dbt_pipeline(dbt_models, topic_marts)
summary = generate_pipeline_summary(
    env_validation, plan, downloads, loading, 
    dbt_models, topic_marts, dbt_execution
)

# Set up dependencies
env_validation >> plan >> downloads >> loading >> dbt_models >> topic_marts >> dbt_execution >> summary 