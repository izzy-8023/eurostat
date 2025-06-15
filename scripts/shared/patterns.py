#!/usr/bin/env python3
"""
Shared Pattern Detection Module for Eurostat Pipeline

Centralizes column pattern detection and Eurostat-specific logic
to eliminate redundancy across multiple scripts.
"""

from typing import List, Tuple, Dict, Any
from .config import EUROSTAT_COLUMN_PATTERNS, get_column_description

def detect_column_patterns(columns: List[Tuple[str, str]]) -> Dict[str, Any]:
    """
    Detect common patterns in Eurostat table columns
    
    Args:
        columns: List of tuples (column_name, data_type)
        
    Returns:
        Dictionary with detected patterns and metadata
    """
    patterns = {
        'dimensions': [],
        'has_linear_index': False,
        'has_value': False,
        'has_status': False,
        'has_source_dataset': False,
        'key_label_pairs': [],
        'status_columns': [],
        'all_columns': []
    }
    
    column_names = [col[0] for col in columns]
    patterns['all_columns'] = column_names
    
    # Check for standard columns
    patterns['has_linear_index'] = 'linear_index' in column_names
    patterns['has_value'] = 'value' in column_names
    patterns['has_status'] = any('status' in col for col in column_names)
    patterns['has_source_dataset'] = 'source_dataset_id' in column_names
    
    # Find status columns
    patterns['status_columns'] = [col for col in column_names if 'status' in col]
    
    # Find key-label pairs 
    for col_name, col_type in columns:
        if col_name.endswith(EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix']):
            dimension = col_name[:-len(EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix'])]
            label_col = f"{dimension}{EUROSTAT_COLUMN_PATTERNS['dimension_label_suffix']}"
            if label_col in column_names:
                patterns['key_label_pairs'].append(dimension)
                patterns['dimensions'].append(dimension)
    
    return patterns

def generate_column_mappings(columns: List[Tuple[str, str]], dataset_id: str) -> List[str]:
    """
    Generate standardized column mappings for staging models
    
    Args:
        columns: List of tuples (column_name, data_type)
        dataset_id: Dataset identifier for metadata
        
    Returns:
        List of SQL column mapping strings
    """
    patterns = detect_column_patterns(columns)
    column_mappings = []
    
    # Add linear_index if present (useful for debugging)
    if patterns['has_linear_index']:
        column_mappings.append("    linear_index")
    
    # Add dimension key-label pairs with meaningful aliases
    for dimension in patterns['key_label_pairs']:
        column_mappings.extend([
            f"    {dimension}_key as {dimension}_code",
            f"    {dimension}_label as {dimension}_name"
        ])
    
    # Add value column
    if patterns['has_value']:
        column_mappings.append("    value")
    
    # Add status columns with standardized naming
    for status_col in patterns['status_columns']:
        if status_col == 'status_code':
            column_mappings.append("    status_code")
        elif status_col == 'status_label':
            column_mappings.append("    status_label as status_description")
        else:
            column_mappings.append(f"    {status_col}")
    
    # Add source dataset ID
    if patterns['has_source_dataset']:
        column_mappings.append("    source_dataset_id")
    
    # Add metadata columns
    column_mappings.extend([
        "    current_timestamp as dbt_updated_at",
        f"    '{dataset_id}' as dataset_id"
    ])
    
    return column_mappings

def create_source_table_entry(table_name: str, columns: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Create a source table entry for sources.yml
    
    Args:
        table_name: Name of the table
        columns: List of column dictionaries with 'column_name' and 'data_type'
        
    Returns:
        Dictionary representing the source table entry
    """
    entry = {
        'name': table_name,
        'description': f'Eurostat dataset {table_name.upper()}',
        'columns': []
    }
    
    for col in columns:
        col_name = col['column_name']
        
        # Determine dimension name for key/label columns
        dimension = None
        if col_name.endswith('_key'):
            dimension = col_name.replace('_key', '')
        elif col_name.endswith('_label'):
            dimension = col_name.replace('_label', '')
        
        description = get_column_description(col_name, dimension)
        
        entry['columns'].append({
            'name': col_name,
            'description': description
        })
    
    return entry

def generate_staging_model_sql(table_name: str, columns: List[Tuple[str, str]], dataset_id: str) -> str:
    """
    Generate complete staging model SQL
    
    Args:
        table_name: Name of the source table
        columns: List of tuples (column_name, data_type)
        dataset_id: Dataset identifier
        
    Returns:
        Complete SQL for the staging model
    """
    column_mappings = generate_column_mappings(columns, dataset_id)
    
    sql_template = f"""{{{{ config(materialized='view') }}}}

-- Staging model for {dataset_id}
-- Generated automatically based on table schema
-- Standardizes column names and adds metadata

select
{',\n'.join(column_mappings)}
from {{{{ source('eurostat_raw', '{table_name}') }}}}

-- Add data quality filters
where value is not null
  or status_code is not null  -- Keep rows with status even if no value
"""
    
    return sql_template

def is_eurostat_dimension_column(column_name: str) -> bool:
    """
    Check if a column name follows Eurostat dimension patterns
    
    Args:
        column_name: Name of the column to check
        
    Returns:
        True if column follows dimension patterns
    """
    return (column_name.endswith(EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix']) or 
            column_name.endswith(EUROSTAT_COLUMN_PATTERNS['dimension_label_suffix']))

def get_dimension_from_column(column_name: str) -> str:
    """
    Extract dimension name from a dimension column
    
    Args:
        column_name: Name of the dimension column
        
    Returns:
        Dimension name without suffix
    """
    if column_name.endswith(EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix']):
        return column_name[:-len(EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix'])]
    elif column_name.endswith(EUROSTAT_COLUMN_PATTERNS['dimension_label_suffix']):
        return column_name[:-len(EUROSTAT_COLUMN_PATTERNS['dimension_label_suffix'])]
    else:
        return column_name 