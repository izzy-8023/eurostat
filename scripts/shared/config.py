#!/usr/bin/env python3
"""
Shared Configuration Module for Eurostat Pipeline

Centralizes all configuration constants, dataset lists, and patterns
to eliminate hardcoded values scattered across multiple scripts.
"""

import os
from typing import List, Dict, Any
from pathlib import Path

# Database Configuration
DEFAULT_DB_CONFIG = {
    'host': 'eurostat_postgres_db',
    'port': '5432',
    'database': 'eurostat_data',
    'user': 'eurostat_user',
    'password': 'mysecretpassword'
}

# Environment Variable Names (standardized)
DB_ENV_VARS = {
    'host': 'EUROSTAT_POSTGRES_HOST',
    'port': 'EUROSTAT_POSTGRES_PORT',
    'database': 'EUROSTAT_POSTGRES_DB',
    'user': 'EUROSTAT_POSTGRES_USER',
    'password': 'EUROSTAT_POSTGRES_PASSWORD'
}

# Health Datasets (consolidated from multiple files)
HEALTH_DATASETS = [
    'hlth_cd_ainfo', 'hlth_cd_ainfr', 'hlth_cd_yinfo', 'hlth_cd_yinfr',
    'hlth_dh010', 'hlth_dh030', 'hlth_silc_02', 'hlth_silc_04', 
    'hlth_silc_05', 'hlth_silc_07', 'hlth_silc_09', 'hlth_silc_11',
    'hlth_silc_12', 'hlth_silc_18', 'hlth_silc_19', 'hlth_silc_20',
    'hlth_silc_24', 'hlth_silc_27', 'hlth_silc_28'
]

# Default Paths
DEFAULT_PATHS = {
    'dbt_project': '/opt/airflow/dbt_project',
    'staging_models': '/opt/airflow/dbt_project/models/staging',
    'marts_models': '/opt/airflow/dbt_project/models/marts',
    'sources_file': '/opt/airflow/dbt_project/models/staging/sources.yml',
    'scripts': '/opt/airflow/scripts',
    'temp_downloads': '/opt/airflow/temp_processor_downloads',
    'grouped_datasets_csv': '/opt/airflow/grouped_datasets_summary.csv',
    'health_datasets_csv': '/opt/airflow/health_datasets.csv'
}

# Column Patterns for Eurostat Data
EUROSTAT_COLUMN_PATTERNS = {
    'dimension_key_suffix': '_key',
    'dimension_label_suffix': '_label',
    'standard_columns': ['value', 'status_code', 'status_label', 'linear_index', 'source_dataset_id'],
    'metadata_columns': ['dbt_updated_at', 'dataset_id']
}

# Column Description Templates
COLUMN_DESCRIPTIONS = {
    'dimension_key': "Code for {dimension} dimension",
    'dimension_label': "Label for {dimension} dimension",
    'value': "Numeric value for the indicator",
    'linear_index': "Sequential index for the record",
    'status_code': "Status code for the value",
    'status_label': "Status description for the value",
    'source_dataset_id': "Source dataset identifier",
    'dbt_updated_at': "Timestamp when record was processed by dbt",
    'dataset_id': "Eurostat dataset identifier"
}

# dbt Configuration
DBT_CONFIG = {
    'staging_materialization': 'view',
    'mart_materialization': 'table',
    'profiles_dir': '/home/airflow/.dbt'
}

# API Configuration
EUROSTAT_API_CONFIG = {
    'host': 'https://ec.europa.eu/eurostat/api/dissemination',
    'service': 'sdmx',
    'version': '2.1',
    'response_type': 'data',
    'format': 'json',
    'lang': 'EN'
}

def get_db_config() -> Dict[str, str]:
    """
    Get database configuration from environment variables with fallbacks
    
    Returns:
        Dictionary with database connection parameters
    """
    return {
        key: os.getenv(env_var, default_value)
        for key, (env_var, default_value) in zip(
            DEFAULT_DB_CONFIG.keys(),
            zip(DB_ENV_VARS.values(), DEFAULT_DB_CONFIG.values())
        )
    }

def get_path(path_key: str) -> Path:
    """
    Get a configured path as a Path object
    
    Args:
        path_key: Key from DEFAULT_PATHS
        
    Returns:
        Path object for the requested path
    """
    if path_key not in DEFAULT_PATHS:
        raise ValueError(f"Unknown path key: {path_key}")
    return Path(DEFAULT_PATHS[path_key])

def get_column_description(column_name: str, dimension: str = None) -> str:
    """
    Get standardized column description
    
    Args:
        column_name: Name of the column
        dimension: Dimension name for dimension-specific columns
        
    Returns:
        Standardized description string
    """
    if column_name.endswith('_key') and dimension:
        return COLUMN_DESCRIPTIONS['dimension_key'].format(dimension=dimension)
    elif column_name.endswith('_label') and dimension:
        return COLUMN_DESCRIPTIONS['dimension_label'].format(dimension=dimension)
    elif column_name in COLUMN_DESCRIPTIONS:
        return COLUMN_DESCRIPTIONS[column_name]
    else:
        return f"Column {column_name}"

def is_health_dataset(dataset_id: str) -> bool:
    """
    Check if a dataset ID is in the health datasets list
    
    Args:
        dataset_id: Dataset identifier to check
        
    Returns:
        True if dataset is a health dataset
    """
    return dataset_id.lower() in [ds.lower() for ds in HEALTH_DATASETS]

def get_staging_model_name(dataset_id: str) -> str:
    """
    Get standardized staging model name for a dataset
    
    Args:
        dataset_id: Dataset identifier
        
    Returns:
        Standardized staging model name
    """
    return f"stg_{dataset_id.lower()}"

def get_table_name(dataset_id: str) -> str:
    """
    Get standardized table name for a dataset
    
    Args:
        dataset_id: Dataset identifier
        
    Returns:
        Standardized table name
    """
    return dataset_id.lower() 