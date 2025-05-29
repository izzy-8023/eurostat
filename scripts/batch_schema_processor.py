#!/usr/bin/env python3
"""
Batch Schema Processor for Eurostat Datasets

Processes multiple Eurostat JSON files to detect schemas and generate
processing configurations for dynamic pipeline execution.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml

from eurostat_schema_detector import EurostatSchemaDetector, DatasetSchema

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchSchemaProcessor:
    """Process multiple datasets and generate configurations"""
    
    def __init__(self, max_workers: int = 4):
        self.detector = EurostatSchemaDetector()
        self.max_workers = max_workers
        self.schemas: Dict[str, DatasetSchema] = {}
        
    def process_directory(self, input_dir: str, pattern: str = "*.json") -> Dict[str, DatasetSchema]:
        """Process all JSON files in a directory"""
        input_path = Path(input_dir)
        json_files = list(input_path.glob(pattern))
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._process_single_file, str(file_path)): file_path 
                for file_path in json_files
            }
            
            # Collect results
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    schema = future.result()
                    if schema:
                        self.schemas[schema.dataset_id] = schema
                        logger.info(f"✓ Processed {schema.dataset_id}")
                except Exception as e:
                    logger.error(f"✗ Failed to process {file_path}: {e}")
        
        return self.schemas
    
    def _process_single_file(self, file_path: str) -> Optional[DatasetSchema]:
        """Process a single JSON file"""
        try:
            return self.detector.detect_schema(file_path)
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return None
    
    def generate_pipeline_config(self, output_file: str = "pipeline_config.yaml"):
        """Generate Airflow/pipeline configuration from detected schemas"""
        config = {
            'datasets': {},
            'processing_groups': self._group_datasets_by_similarity(),
            'dbt_models': {},
            'table_schemas': {}
        }
        
        for dataset_id, schema in self.schemas.items():
            # Dataset configuration
            config['datasets'][dataset_id] = {
                'label': schema.label,
                'dimensions': len(schema.dimensions),
                'observations': schema.total_observations,
                'has_temporal': schema.is_temporal_dataset(),
                'has_geographic': schema.is_geographic_dataset(),
                'processing_group': self._get_processing_group(schema),
                'table_name': dataset_id.lower(),
                'dbt_model': f"stg_{dataset_id.lower()}"
            }
            
            # dbt model configuration
            config['dbt_models'][f"stg_{dataset_id.lower()}"] = {
                'materialized': 'table',
                'description': schema.label,
                'columns': [
                    {'name': dim.id, 'description': dim.label, 'type': dim.data_type}
                    for dim in schema.dimensions
                ] + [
                    {'name': 'value', 'description': f'Value in {schema.value_unit}', 'type': 'numeric'}
                ]
            }
            
            # Table schema
            config['table_schemas'][dataset_id.lower()] = {
                'columns': self._generate_column_definitions(schema),
                'indexes': self._generate_index_definitions(schema)
            }
        
        # Save configuration
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Pipeline configuration saved to {output_file}")
        return config
    
    def _group_datasets_by_similarity(self) -> Dict[str, List[str]]:
        """Group datasets by structural similarity"""
        groups = {}
        
        for dataset_id, schema in self.schemas.items():
            # Create a signature based on dimensions
            dim_signature = tuple(sorted([dim.id for dim in schema.dimensions]))
            group_key = f"group_{hash(dim_signature) % 1000:03d}"
            
            if group_key not in groups:
                groups[group_key] = {
                    'datasets': [],
                    'common_dimensions': list(dim_signature),
                    'processing_template': 'standard'
                }
            
            groups[group_key]['datasets'].append(dataset_id)
        
        return groups
    
    def _get_processing_group(self, schema: DatasetSchema) -> str:
        """Determine processing group for a schema"""
        dim_signature = tuple(sorted([dim.id for dim in schema.dimensions]))
        return f"group_{hash(dim_signature) % 1000:03d}"
    
    def _generate_column_definitions(self, schema: DatasetSchema) -> List[Dict[str, str]]:
        """Generate column definitions for SQL schema"""
        columns = []
        
        for dim in schema.dimensions:
            if dim.data_type == "temporal":
                col_type = "DATE"
            elif dim.data_type == "numeric":
                col_type = "NUMERIC"
            else:
                col_type = "VARCHAR(50)"
            
            columns.append({
                'name': dim.id,
                'type': col_type,
                'description': dim.label
            })
        
        # Add value column
        columns.append({
            'name': 'value',
            'type': 'NUMERIC',
            'description': f'Value in {schema.value_unit}'
        })
        
        # Add status column if needed
        if schema.status_codes:
            columns.append({
                'name': 'status',
                'type': 'VARCHAR(10)',
                'description': 'Data quality status'
            })
        
        return columns
    
    def _generate_index_definitions(self, schema: DatasetSchema) -> List[str]:
        """Generate index definitions"""
        indexes = ['dataset_id']  # Always index by dataset_id
        
        for dim in schema.dimensions:
            if dim.data_type in ["temporal", "geographic"]:
                indexes.append(dim.id)
        
        return indexes
    
    def generate_summary_report(self, output_file: str = "schema_analysis_report.md"):
        """Generate a markdown report of the analysis"""
        report = []
        report.append("# Eurostat Dataset Schema Analysis Report\n")
        report.append(f"**Total Datasets Analyzed:** {len(self.schemas)}\n")
        
        # Summary statistics
        total_obs = sum(schema.total_observations for schema in self.schemas.values())
        avg_dimensions = sum(len(schema.dimensions) for schema in self.schemas.values()) / len(self.schemas)
        
        report.append("## Summary Statistics\n")
        report.append(f"- **Total Observations:** {total_obs:,}")
        report.append(f"- **Average Dimensions per Dataset:** {avg_dimensions:.1f}")
        report.append(f"- **Temporal Datasets:** {sum(1 for s in self.schemas.values() if s.is_temporal_dataset())}")
        report.append(f"- **Geographic Datasets:** {sum(1 for s in self.schemas.values() if s.is_geographic_dataset())}\n")
        
        # Dimension analysis
        all_dimensions = {}
        for schema in self.schemas.values():
            for dim in schema.dimensions:
                if dim.id not in all_dimensions:
                    all_dimensions[dim.id] = {
                        'count': 0,
                        'label': dim.label,
                        'type': dim.data_type
                    }
                all_dimensions[dim.id]['count'] += 1
        
        report.append("## Common Dimensions\n")
        report.append("| Dimension | Label | Type | Frequency |\n")
        report.append("|-----------|-------|------|----------|\n")
        
        for dim_id, info in sorted(all_dimensions.items(), key=lambda x: x[1]['count'], reverse=True):
            report.append(f"| {dim_id} | {info['label']} | {info['type']} | {info['count']}/{len(self.schemas)} |\n")
        
        # Dataset details
        report.append("\n## Dataset Details\n")
        for dataset_id, schema in sorted(self.schemas.items()):
            report.append(f"### {dataset_id}\n")
            report.append(f"**Label:** {schema.label}\n")
            report.append(f"**Observations:** {schema.total_observations:,}\n")
            report.append(f"**Dimensions:** {', '.join([dim.id for dim in schema.dimensions])}\n")
            if schema.time_periods:
                report.append(f"**Time Coverage:** {', '.join(schema.time_periods)}\n")
            if schema.geographic_coverage:
                report.append(f"**Geographic Coverage:** {len(schema.geographic_coverage)} entities\n")
            report.append("\n")
        
        # Save report
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logger.info(f"Analysis report saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Batch process Eurostat JSON files for schema detection')
    parser.add_argument('input_dir', help='Directory containing JSON files')
    parser.add_argument('--pattern', default='*.json', help='File pattern to match (default: *.json)')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--config', default='pipeline_config.yaml', help='Output configuration file')
    parser.add_argument('--report', default='schema_analysis_report.md', help='Output analysis report')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Process datasets
    processor = BatchSchemaProcessor(max_workers=args.workers)
    schemas = processor.process_directory(args.input_dir, args.pattern)
    
    if not schemas:
        logger.error("No schemas were successfully processed")
        return 1
    
    logger.info(f"Successfully processed {len(schemas)} datasets")
    
    # Generate outputs
    processor.generate_pipeline_config(args.config)
    processor.generate_summary_report(args.report)
    
    # Print summary
    print(f"\n=== Batch Processing Complete ===")
    print(f"Datasets processed: {len(schemas)}")
    print(f"Configuration saved: {args.config}")
    print(f"Report saved: {args.report}")
    
    return 0

if __name__ == "__main__":
    exit(main()) 