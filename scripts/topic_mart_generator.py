#!/usr/bin/env python3
"""
Topic-Based Mart Generator for Eurostat Datasets

This script automatically generates mart models that combine related datasets
with different dimensions into unified analytical views for topic-based analysis.
"""

import sys
import os
import yaml
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Any, Set
import logging
from collections import defaultdict

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

from shared.database import EurostatDatabase
from shared.config import get_db_config, get_path

logger = logging.getLogger(__name__)

class TopicMartGenerator:
    """Generate topic-based mart models by combining related datasets"""
    
    def __init__(self, db_config: Dict[str, str] = None):
        """Initialize with database configuration"""
        self.db = EurostatDatabase(db_config or get_db_config())
    
    def load_topic_groups(self, csv_file: str) -> Dict[str, List[str]]:
        """Load topic groups from CSV file"""
        topic_groups = {}
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    group_name = row['group_name']
                    dataset_count = int(row['dataset_count'])
                    
                    # Skip single-dataset groups and special groups
                    if (dataset_count < 2 or 
                        group_name.startswith('datasets_with_') or
                        group_name in ['unknown_grouping_issue']):
                        continue
                    
                    dataset_ids = [d.strip() for d in row['dataset_ids'].split(',')]
                    topic_groups[group_name] = dataset_ids
                    
        except Exception as e:
            logger.error(f"Failed to load topic groups from {csv_file}: {e}")
            
        return topic_groups
    
    def get_table_schema(self, table_name: str) -> List[Tuple[str, str]]:
        """Get table schema from database"""
        return self.db.get_table_schema(table_name)
    
    def check_table_exists(self, table_name: str) -> bool:
        """Check if table exists in database"""
        return self.db.check_table_exists(table_name)
    
    def analyze_topic_dimensions(self, topic_name: str, dataset_ids: List[str]) -> Dict[str, Any]:
        """Analyze dimensions across datasets in a topic"""
        topic_analysis = {
            'topic_name': topic_name,
            'datasets': [],
            'common_dimensions': set(),
            'unique_dimensions': defaultdict(list),
            'all_dimensions': set(),
            'dimension_coverage': {},
            'available_datasets': []
        }
        
        dataset_dimensions = {}
        
        # Analyze each dataset
        for dataset_id in dataset_ids:
            table_name = dataset_id.lower()
            
            if not self.check_table_exists(table_name):
                logger.warning(f"Table {table_name} not found, skipping from topic analysis")
                continue
                
            columns = self.get_table_schema(table_name)
            if not columns:
                continue
                
            # Extract dimensions (key-label pairs)
            dimensions = set()
            for col_name, col_type in columns:
                if col_name.endswith('_key'):
                    dimension = col_name[:-4]  # Remove '_key' suffix
                    label_col = f"{dimension}_label"
                    if any(c[0] == label_col for c in columns):
                        dimensions.add(dimension)
            
            dataset_dimensions[dataset_id] = dimensions
            topic_analysis['datasets'].append({
                'dataset_id': dataset_id,
                'table_name': table_name,
                'dimensions': list(dimensions)
            })
            topic_analysis['available_datasets'].append(dataset_id)
            topic_analysis['all_dimensions'].update(dimensions)
        
        if not dataset_dimensions:
            logger.warning(f"No valid datasets found for topic: {topic_name}")
            return topic_analysis
        
        # Find common dimensions (present in all datasets)
        if dataset_dimensions:
            topic_analysis['common_dimensions'] = set.intersection(*dataset_dimensions.values())
        
        # Find unique dimensions (present in some but not all datasets)
        for dimension in topic_analysis['all_dimensions']:
            datasets_with_dim = [ds for ds, dims in dataset_dimensions.items() if dimension in dims]
            if len(datasets_with_dim) < len(dataset_dimensions):
                topic_analysis['unique_dimensions'][dimension] = datasets_with_dim
            
            # Calculate coverage percentage
            coverage = len(datasets_with_dim) / len(dataset_dimensions) * 100
            topic_analysis['dimension_coverage'][dimension] = {
                'coverage_percent': coverage,
                'datasets': datasets_with_dim
            }
        
        return topic_analysis
    
    def generate_mart_model_sql(self, topic_analysis: Dict[str, Any]) -> str:
        """Generate SQL for a topic-based mart model"""
        topic_name = topic_analysis['topic_name']
        datasets = topic_analysis['datasets']
        common_dimensions = topic_analysis['common_dimensions']
        unique_dimensions = topic_analysis['unique_dimensions']
        
        if not datasets:
            raise ValueError(f"No datasets available for topic: {topic_name}")
        
        # Create a safe model name
        model_name = "mart_" + "".join(c if c.isalnum() or c == '_' else '_' for c in topic_name.lower())
        
        # Build UNION ALL query
        union_parts = []
        
        for dataset_info in datasets:
            dataset_id = dataset_info['dataset_id']
            table_name = dataset_info['table_name']
            dataset_dimensions = set(dataset_info['dimensions'])
            
            select_parts = []
            
            # Add common dimensions (always present)
            for dim in sorted(common_dimensions):
                select_parts.extend([
                    f"    {dim}_code,",
                    f"    {dim}_name,"
                ])
            
            # Add unique dimensions (with NULL for datasets that don't have them)
            for dim in sorted(unique_dimensions.keys()):
                if dim in dataset_dimensions:
                    select_parts.extend([
                        f"    {dim}_code,",
                        f"    {dim}_name,"
                    ])
                else:
                    select_parts.extend([
                        f"    null as {dim}_code,",
                        f"    null as {dim}_name,"
                    ])
            
            # Add standard columns
            select_parts.extend([
                "    value,",
                "    status_code,",
                "    status_description,",
                f"    '{dataset_id}' as source_dataset_id,",
                f"    'stg_{table_name}' as source_model"
            ])
            
            # Build the SELECT for this dataset
            dataset_sql = f"""
    select
{chr(10).join(select_parts)}
    from {{{{ ref('stg_{table_name}') }}}}
    where value is not null or status_code is not null"""
            
            union_parts.append(dataset_sql)
        
        # Combine all parts
        full_sql = f"""{{{{ config(materialized='table') }}}}

-- Topic-based mart for: {topic_name}
-- Combines {len(datasets)} related datasets with unified dimensions
-- Generated automatically based on topic grouping and schema analysis

-- Common dimensions across all datasets: {', '.join(sorted(common_dimensions))}
-- Unique dimensions (dataset-specific): {', '.join(sorted(unique_dimensions.keys()))}

{' union all '.join(union_parts)}
"""
        
        return full_sql
    
    def generate_topic_documentation(self, topic_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate documentation for a topic mart"""
        topic_name = topic_analysis['topic_name']
        
        doc = {
            'name': f"mart_{topic_name.lower().replace(' ', '_')}",
            'description': f"Unified mart for {topic_name} combining {len(topic_analysis['datasets'])} related datasets",
            'columns': []
        }
        
        # Document common dimensions
        for dim in sorted(topic_analysis['common_dimensions']):
            doc['columns'].extend([
                {
                    'name': f"{dim}_code",
                    'description': f"Code for {dim} dimension (common across all datasets)"
                },
                {
                    'name': f"{dim}_name", 
                    'description': f"Label for {dim} dimension (common across all datasets)"
                }
            ])
        
        # Document unique dimensions
        for dim in sorted(topic_analysis['unique_dimensions'].keys()):
            coverage = topic_analysis['dimension_coverage'][dim]
            datasets_with_dim = ', '.join(coverage['datasets'])
            doc['columns'].extend([
                {
                    'name': f"{dim}_code",
                    'description': f"Code for {dim} dimension (available in: {datasets_with_dim})"
                },
                {
                    'name': f"{dim}_name",
                    'description': f"Label for {dim} dimension (available in: {datasets_with_dim})"
                }
            ])
        
        # Document standard columns
        doc['columns'].extend([
            {
                'name': 'value',
                'description': 'Numeric value for the indicator'
            },
            {
                'name': 'status_code',
                'description': 'Status code for the data point'
            },
            {
                'name': 'status_description',
                'description': 'Status description for the data point'
            },
            {
                'name': 'source_dataset_id',
                'description': 'Original Eurostat dataset ID'
            },
            {
                'name': 'source_model',
                'description': 'Source dbt staging model'
            }
        ])
        
        return doc
    
    def generate_topic_marts(self, csv_file: str, models_dir: str, 
                           topic_filter: List[str] = None) -> Dict[str, Any]:
        """Generate mart models for all topics or filtered topics"""
        models_dir = Path(models_dir)
        marts_dir = models_dir / "marts"
        marts_dir.mkdir(parents=True, exist_ok=True)
        
        # Load topic groups
        topic_groups = self.load_topic_groups(csv_file)
        
        if topic_filter:
            # Filter to only specified topics (exact matching)
            filtered_topic_groups = {}
            for group_name, dataset_ids in topic_groups.items():
                # Check if the group_name exactly matches any keyword in topic_filter (case-insensitive)
                if any(keyword.lower() == group_name.lower() for keyword in topic_filter):
                    filtered_topic_groups[group_name] = dataset_ids
            topic_groups = filtered_topic_groups
        
        results = {
            'generated_marts': [],
            'topic_analyses': {},
            'errors': []
        }
        
        logger.info(f"Generating marts for {len(topic_groups)} topics")
        
        for topic_name, dataset_ids in topic_groups.items():
            try:
                logger.info(f"Analyzing topic: {topic_name}")
                
                # Analyze topic dimensions
                topic_analysis = self.analyze_topic_dimensions(topic_name, dataset_ids)
                
                if not topic_analysis['available_datasets']:
                    logger.warning(f"No available datasets for topic: {topic_name}")
                    continue
                
                results['topic_analyses'][topic_name] = topic_analysis
                
                # Generate mart model SQL
                mart_sql = self.generate_mart_model_sql(topic_analysis)
                
                # Create safe filename
                safe_topic_name = "".join(c if c.isalnum() or c == '_' else '_' for c in topic_name.lower())
                mart_file = marts_dir / f"mart_{safe_topic_name}.sql"
                
                # Write mart model
                with open(mart_file, 'w', encoding='utf-8') as f:
                    f.write(mart_sql)
                
                results['generated_marts'].append(f"mart_{safe_topic_name}")
                logger.info(f"✓ Generated mart model: mart_{safe_topic_name}")
                
            except Exception as e:
                error_msg = f"Failed to generate mart for topic '{topic_name}': {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Generate schema.yml for marts
        if results['generated_marts']:
            self.generate_marts_schema(results, marts_dir)
        
        return results
    
    def generate_marts_schema(self, results: Dict[str, Any], marts_dir: Path):
        """Generate schema.yml file for mart models"""
        schema_config = {
            'version': 2,
            'models': []
        }
        
        for topic_name, topic_analysis in results['topic_analyses'].items():
            if topic_analysis['available_datasets']:
                doc = self.generate_topic_documentation(topic_analysis)
                schema_config['models'].append(doc)
        
        schema_file = marts_dir / "schema.yml"
        with open(schema_file, 'w', encoding='utf-8') as f:
            yaml.dump(schema_config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"✓ Generated schema.yml with {len(schema_config['models'])} mart models")

def main():
    """CLI interface for the topic mart generator"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate topic-based mart models for Eurostat datasets")
    parser.add_argument("--csv-file", default="grouped_datasets_summary.csv", 
                       help="CSV file with topic groupings")
    parser.add_argument("--models-dir", default="/opt/airflow/dbt_project/models", 
                       help="Directory to write dbt models")
    parser.add_argument("--topics", help="Comma-separated list of specific topics to process")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    # Parse topics filter
    topic_filter = None
    if args.topics:
        topic_filter = [t.strip() for t in args.topics.split(',')]
    
    # Generate marts
    generator = TopicMartGenerator()
    results = generator.generate_topic_marts(args.csv_file, args.models_dir, topic_filter)
    
    print(f"Generated {len(results['generated_marts'])} mart models")
    if results['generated_marts']:
        print(f"Mart models: {', '.join(results['generated_marts'])}")
    
    if results['errors']:
        print(f"Errors encountered: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")

if __name__ == "__main__":
    main() 