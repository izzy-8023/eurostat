#!/usr/bin/env python3
"""
Eurostat Dataset Schema Detector

Automatically detects and extracts schema information from Eurostat JSON files
to enable dynamic processing of any dataset structure.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class DimensionInfo:
    """Information about a single dimension"""
    id: str
    label: str
    position: int
    size: int
    categories: Dict[str, str]  # code -> label mapping
    category_indices: Dict[str, int]  # code -> index mapping
    data_type: str = "categorical"  # categorical, temporal, numeric

@dataclass
class DatasetSchema:
    """Complete schema information for a dataset"""
    dataset_id: str
    label: str
    source: str
    updated: str
    version: str
    language: str
    
    # Dimension information
    dimensions: List[DimensionInfo]
    dimension_order: List[str]  # ["freq", "sex", "age", ...]
    dimension_sizes: List[int]  # [1, 3, 9, ...]
    
    # Value information
    value_unit: str
    
    # Metadata
    total_observations: int
    time_periods: List[str]
    geographic_coverage: List[str]
    
    # Fields with defaults must come last
    value_type: str = "numeric"
    status_codes: Dict[str, str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    def get_dimension_by_id(self, dim_id: str) -> Optional[DimensionInfo]:
        """Get dimension info by ID"""
        for dim in self.dimensions:
            if dim.id == dim_id:
                return dim
        return None
    
    def is_temporal_dataset(self) -> bool:
        """Check if dataset has temporal dimension"""
        return any(dim.id in ['time', 'freq'] for dim in self.dimensions)
    
    def is_geographic_dataset(self) -> bool:
        """Check if dataset has geographic dimension"""
        return any(dim.id in ['geo', 'country', 'region'] for dim in self.dimensions)

class EurostatSchemaDetector:
    """Detects schema from Eurostat JSON files"""
    
    def __init__(self):
        self.temporal_dimensions = {'time', 'freq', 'period'}
        self.geographic_dimensions = {'geo', 'country', 'region', 'nuts'}
        self.unit_dimensions = {'unit', 'measure'}
        
    def detect_schema(self, json_file_path: str) -> DatasetSchema:
        """
        Detect schema from Eurostat JSON file
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            DatasetSchema object with complete schema information
        """
        logger.info(f"Detecting schema for: {json_file_path}")
        
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract basic dataset information
        dataset_info = self._extract_dataset_info(data)
        
        # Extract dimension information
        dimensions = self._extract_dimensions(data)
        
        # Extract value information
        value_info = self._extract_value_info(data, dimensions)
        
        # Extract temporal and geographic coverage
        temporal_coverage = self._extract_temporal_coverage(data, dimensions)
        geographic_coverage = self._extract_geographic_coverage(data, dimensions)
        
        # Extract status information
        status_codes = self._extract_status_codes(data)
        
        # Count observations
        obs_count = len(data.get('value', {}))
        
        schema = DatasetSchema(
            dataset_id=dataset_info['id'],
            label=dataset_info['label'],
            source=dataset_info['source'],
            updated=dataset_info['updated'],
            version=dataset_info['version'],
            language=dataset_info['language'],
            dimensions=dimensions,
            dimension_order=data.get('id', []),
            dimension_sizes=data.get('size', []),
            value_unit=value_info['unit'],
            value_type=value_info['type'],
            total_observations=obs_count,
            time_periods=temporal_coverage,
            geographic_coverage=geographic_coverage,
            status_codes=status_codes
        )
        
        logger.info(f"Schema detected: {len(dimensions)} dimensions, {obs_count} observations")
        return schema
    
    def _extract_dataset_info(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Extract basic dataset information"""
        extension = data.get('extension', {})
        
        return {
            'id': extension.get('id', 'unknown'),
            'label': data.get('label', 'Unknown Dataset'),
            'source': data.get('source', 'ESTAT'),
            'updated': data.get('updated', ''),
            'version': extension.get('version', '1.0'),
            'language': extension.get('lang', 'EN')
        }
    
    def _extract_dimensions(self, data: Dict[str, Any]) -> List[DimensionInfo]:
        """Extract dimension information"""
        dimensions = []
        dimension_data = data.get('dimension', {})
        dimension_order = data.get('id', [])
        dimension_sizes = data.get('size', [])
        
        for i, dim_id in enumerate(dimension_order):
            if dim_id in dimension_data:
                dim_info = dimension_data[dim_id]
                
                # Extract categories
                categories = {}
                category_indices = {}
                if 'category' in dim_info:
                    categories = dim_info['category'].get('label', {})
                    category_indices = dim_info['category'].get('index', {})
                
                # Determine data type
                data_type = self._determine_dimension_type(dim_id, categories)
                
                dimension = DimensionInfo(
                    id=dim_id,
                    label=dim_info.get('label', dim_id),
                    position=i,
                    size=dimension_sizes[i] if i < len(dimension_sizes) else 0,
                    categories=categories,
                    category_indices=category_indices,
                    data_type=data_type
                )
                dimensions.append(dimension)
        
        return dimensions
    
    def _determine_dimension_type(self, dim_id: str, categories: Dict[str, str]) -> str:
        """Determine the type of dimension"""
        if dim_id in self.temporal_dimensions:
            return "temporal"
        elif dim_id in self.geographic_dimensions:
            return "geographic"
        elif dim_id in self.unit_dimensions:
            return "unit"
        elif self._is_numeric_categories(categories):
            return "numeric"
        else:
            return "categorical"
    
    def _is_numeric_categories(self, categories: Dict[str, str]) -> bool:
        """Check if categories represent numeric values"""
        if not categories:
            return False
        
        # Check if category keys are numeric
        try:
            for key in list(categories.keys())[:5]:  # Check first 5
                float(key)
            return True
        except ValueError:
            return False
    
    def _extract_value_info(self, data: Dict[str, Any], dimensions: List[DimensionInfo]) -> Dict[str, str]:
        """Extract information about the value field"""
        # Find unit dimension
        unit = "unknown"
        for dim in dimensions:
            if dim.id in self.unit_dimensions and dim.categories:
                # Get the first (and usually only) unit
                unit = list(dim.categories.values())[0]
                break
        
        # Determine value type based on actual values
        values = data.get('value', {})
        value_type = "numeric"
        
        if values:
            sample_values = list(values.values())[:10]
            if all(isinstance(v, (int, float)) for v in sample_values):
                value_type = "numeric"
            else:
                value_type = "mixed"
        
        return {
            'unit': unit,
            'type': value_type
        }
    
    def _extract_temporal_coverage(self, data: Dict[str, Any], dimensions: List[DimensionInfo]) -> List[str]:
        """Extract temporal coverage information"""
        time_periods = []
        
        for dim in dimensions:
            if dim.data_type == "temporal":
                time_periods.extend(dim.categories.keys())
        
        return sorted(time_periods)
    
    def _extract_geographic_coverage(self, data: Dict[str, Any], dimensions: List[DimensionInfo]) -> List[str]:
        """Extract geographic coverage information"""
        geo_entities = []
        
        for dim in dimensions:
            if dim.data_type == "geographic":
                geo_entities.extend(dim.categories.keys())
        
        return sorted(geo_entities)
    
    def _extract_status_codes(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Extract status code definitions"""
        extension = data.get('extension', {})
        status_info = extension.get('status', {})
        
        if 'label' in status_info:
            return status_info['label']
        
        return {}
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote PostgreSQL identifier if it's a reserved word or contains special characters"""
        # PostgreSQL reserved words that need quoting
        reserved_words = {
            'unit', 'value', 'time', 'date', 'year', 'month', 'day', 'order', 'group',
            'select', 'from', 'where', 'insert', 'update', 'delete', 'create', 'drop',
            'table', 'index', 'view', 'user', 'role', 'grant', 'revoke', 'all', 'any',
            'some', 'exists', 'in', 'not', 'and', 'or', 'between', 'like', 'is', 'null',
            'true', 'false', 'case', 'when', 'then', 'else', 'end', 'union', 'intersect',
            'except', 'distinct', 'having', 'limit', 'offset', 'join', 'inner', 'outer',
            'left', 'right', 'full', 'cross', 'on', 'using', 'natural', 'as', 'desc',
            'asc', 'nulls', 'first', 'last', 'primary', 'foreign', 'key', 'references',
            'unique', 'check', 'constraint', 'default', 'auto_increment', 'serial'
        }
        
        # Check if identifier needs quoting
        if (identifier.lower() in reserved_words or 
            not identifier.replace('_', '').isalnum() or 
            identifier[0].isdigit()):
            return f'"{identifier}"'
        return identifier

    def generate_sql_schema(self, schema: DatasetSchema, table_name: str = None) -> str:
        """Generate SQL CREATE TABLE statement from schema"""
        if not table_name:
            table_name = schema.dataset_id.lower()
        
        columns = []
        
        # Add dimension columns
        for dim in schema.dimensions:
            if dim.data_type == "temporal":
                col_type = "DATE"
            elif dim.data_type == "numeric":
                col_type = "NUMERIC"
            else:
                col_type = "VARCHAR(50)"
            
            quoted_id = self._quote_identifier(dim.id)
            columns.append(f"    {quoted_id} {col_type}")
        
        # Add value column
        quoted_value = self._quote_identifier("value")
        columns.append(f"    {quoted_value} {schema.value_type.upper()}")
        
        # Add status column if status codes exist 
        # TODO: to be deleted later
        if schema.status_codes:
            quoted_status = self._quote_identifier("status")
            columns.append(f"    {quoted_status} VARCHAR(10)")
        
        # Add metadata columns
        columns.extend([
            f"    {self._quote_identifier('dataset_id')} VARCHAR(50)",
            f"    {self._quote_identifier('created_at')} TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            f"    {self._quote_identifier('updated_at')} TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ])
        
        quoted_table = self._quote_identifier(table_name)
        sql = f"""CREATE TABLE IF NOT EXISTS {quoted_table} (
{(',' + chr(10)).join(columns)}
);

-- Add indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_{table_name}_dataset ON {quoted_table}({self._quote_identifier('dataset_id')});
"""
        
        # Add indexes for temporal and geographic dimensions
        for dim in schema.dimensions:
            if dim.data_type in ["temporal", "geographic"]:
                quoted_dim_id = self._quote_identifier(dim.id)
                sql += f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{dim.id} ON {quoted_table}({quoted_dim_id});\n"
        
        return sql
    
    def generate_dbt_model(self, schema: DatasetSchema, model_name: str = None) -> str:
        """Generate dbt model template from schema"""
        if not model_name:
            model_name = f"stg_{schema.dataset_id.lower()}"
        
        # Generate column list with descriptions
        columns = []
        for dim in schema.dimensions:
            quoted_id = self._quote_identifier(dim.id)
            columns.append(f"    {quoted_id}, -- {dim.label}")
        
        quoted_value = self._quote_identifier("value")
        columns.append(f"    {quoted_value}, -- {schema.value_unit}")
        
        # TODO: status codes will be deleted later
        if schema.status_codes:
            quoted_status = self._quote_identifier("status")
            columns.append(f"    {quoted_status}, -- Data quality status")
        
        dbt_model = f"""{{{{
  config(
    materialized='table',
    description='{schema.label}'
  )
}}}}

-- Staging model for {schema.dataset_id}
-- Source: {schema.source}
-- Updated: {schema.updated}

SELECT
{chr(10).join(columns)}
    '{{{{ var("run_date") }}}}' as processed_date,
    '{schema.dataset_id}' as dataset_id
FROM {{{{ source('eurostat_raw', '{schema.dataset_id.lower()}') }}}}

-- Data quality checks
{{{{ config(
    post_hook="{{{{ check_not_null('{model_name}', ['{quoted_value}']) }}}}"
) }}}}
"""
        
        return dbt_model

def main():
    parser = argparse.ArgumentParser(description='Detect schema from Eurostat JSON files')
    parser.add_argument('input_file', help='Path to Eurostat JSON file')
    parser.add_argument('--output', '-o', help='Output file for schema (JSON format)')
    parser.add_argument('--sql', help='Generate SQL schema file')
    parser.add_argument('--dbt', help='Generate dbt model file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Detect schema
    detector = EurostatSchemaDetector()
    schema = detector.detect_schema(args.input_file)
    
    # Print summary
    print(f"\n=== Schema Detection Results ===")
    print(f"Dataset: {schema.dataset_id}")
    print(f"Label: {schema.label}")
    print(f"Dimensions: {len(schema.dimensions)}")
    print(f"Observations: {schema.total_observations}")
    print(f"Time Coverage: {', '.join(schema.time_periods) if schema.time_periods else 'None'}")
    print(f"Geographic Coverage: {len(schema.geographic_coverage)} entities")
    
    print(f"\nDimensions:")
    for dim in schema.dimensions:
        print(f"  - {dim.id}: {dim.label} ({dim.size} categories, {dim.data_type})")
    
    # Save schema to JSON
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(schema.to_dict(), f, indent=2, ensure_ascii=False)
        print(f"\nSchema saved to: {args.output}")
    
    # Generate SQL schema
    if args.sql:
        sql_schema = detector.generate_sql_schema(schema)
        with open(args.sql, 'w', encoding='utf-8') as f:
            f.write(sql_schema)
        print(f"SQL schema saved to: {args.sql}")
    
    # Generate dbt model
    if args.dbt:
        dbt_model = detector.generate_dbt_model(schema)
        with open(args.dbt, 'w', encoding='utf-8') as f:
            f.write(dbt_model)
        print(f"dbt model saved to: {args.dbt}")

if __name__ == "__main__":
    main() 