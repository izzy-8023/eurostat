#!/usr/bin/env python3
"""
Consolidated Model Generator for Eurostat Datasets

This script consolidates the functionality of:
- add_missing_sources.py
- generate_missing_staging_models.py

It uses shared modules to eliminate code redundancy and provides
a unified interface for managing dbt sources and staging models.
"""

import sys
import os
import yaml
import argparse
from pathlib import Path
from typing import List, Dict, Any
import logging

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

from shared.database import EurostatDatabase
from shared.config import get_db_config, get_path, HEALTH_DATASETS, get_staging_model_name, get_table_name
from shared.patterns import create_source_table_entry, generate_staging_model_sql

logger = logging.getLogger(__name__)

class ConsolidatedModelGenerator:
    """Unified generator for dbt sources and staging models"""
    
    def __init__(self, db_config: Dict[str, str] = None):
        """Initialize with database configuration"""
        self.db = EurostatDatabase(db_config or get_db_config())
    
    def add_missing_sources(self, dataset_ids: List[str] = None, sources_file: str = None) -> int:
        """
        Add missing source tables to sources.yml
        
        Args:
            dataset_ids: List of dataset IDs to add. If None, uses HEALTH_DATASETS
            sources_file: Path to sources.yml file. If None, uses default path
            
        Returns:
            Number of sources added
        """
        if dataset_ids is None:
            dataset_ids = HEALTH_DATASETS
        
        if sources_file is None:
            sources_file = get_path('sources_file')
        else:
            sources_file = Path(sources_file)
        
        logger.info(f"Adding missing sources for {len(dataset_ids)} datasets")
        
        # Load existing sources.yml
        try:
            with open(sources_file, 'r') as f:
                sources_data = yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Sources file {sources_file} not found, creating new one")
            sources_data = {
                'version': 2,
                'sources': [{
                    'name': 'eurostat_raw',
                    'description': 'Raw Eurostat data loaded from JSON files into PostgreSQL',
                    'database': "{{ env_var('EUROSTAT_POSTGRES_DB') }}",
                    'schema': 'public',
                    'tables': []
                }]
            }
        
        # Get existing table names
        existing_tables = set()
        eurostat_source = None
        for source in sources_data['sources']:
            if source['name'] == 'eurostat_raw':
                eurostat_source = source
                for table in source.get('tables', []):
                    existing_tables.add(table['name'])
                break
        
        if not eurostat_source:
            logger.error("eurostat_raw source not found in sources.yml")
            return 0
        
        # Add missing tables
        added_count = 0
        for dataset_id in dataset_ids:
            table_name = get_table_name(dataset_id)
            
            if table_name not in existing_tables:
                try:
                    logger.info(f"Adding source table {table_name}...")
                    
                    # Check if table exists in database
                    if not self.db.check_table_exists(table_name):
                        logger.warning(f"Table {table_name} does not exist in database, skipping")
                        continue
                    
                    columns = self.db.get_table_columns_detailed(table_name)
                    if columns:
                        table_entry = create_source_table_entry(table_name, columns)
                        eurostat_source['tables'].append(table_entry)
                        added_count += 1
                        logger.info(f"  ✓ Added {table_name}")
                    else:
                        logger.warning(f"  ✗ No columns found for {table_name}")
                except Exception as e:
                    logger.error(f"  ✗ Failed to add {table_name}: {e}")
            else:
                logger.info(f"  - {table_name} already exists")
        
        if added_count > 0:
            # Write back to sources.yml
            with open(sources_file, 'w') as f:
                yaml.dump(sources_data, f, default_flow_style=False, sort_keys=False, indent=2)
            logger.info(f"✓ Added {added_count} new source tables to sources.yml")
        else:
            logger.info("No new tables to add")
        
        return added_count
    
    def generate_staging_models(self, dataset_ids: List[str] = None, models_dir: str = None) -> List[str]:
        """
        Generate staging models for datasets
        
        Args:
            dataset_ids: List of dataset IDs to process. If None, uses HEALTH_DATASETS
            models_dir: Directory to write models. If None, uses default path
            
        Returns:
            List of generated model names
        """
        if dataset_ids is None:
            dataset_ids = HEALTH_DATASETS
        
        if models_dir is None:
            models_dir = get_path('staging_models')
        else:
            models_dir = Path(models_dir)
        
        models_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Generating staging models for {len(dataset_ids)} datasets")
        
        generated_models = []
        
        for dataset_id in dataset_ids:
            try:
                table_name = get_table_name(dataset_id)
                model_name = get_staging_model_name(dataset_id)
                
                logger.info(f"Generating staging model for {dataset_id}...")
                
                # Check if table exists in database
                if not self.db.check_table_exists(table_name):
                    logger.warning(f"Table {table_name} does not exist in database, skipping")
                    continue
                
                # Get table columns
                columns = self.db.get_table_schema(table_name)
                if not columns:
                    logger.warning(f"  Warning: No columns found for {table_name}")
                    continue
                
                # Generate SQL
                sql_content = generate_staging_model_sql(table_name, columns, dataset_id)
                
                # Write to file
                model_file = models_dir / f"{model_name}.sql"
                with open(model_file, 'w') as f:
                    f.write(sql_content)
                
                generated_models.append(model_name)
                logger.info(f"  ✓ Created {model_file}")
                
            except Exception as e:
                logger.error(f"  ✗ Failed to generate model for {dataset_id}: {e}")
        
        logger.info(f"✓ Generated {len(generated_models)} staging models")
        return generated_models
    
    def process_all(self, dataset_ids: List[str] = None, sources_file: str = None, models_dir: str = None) -> Dict[str, Any]:
        """
        Process both sources and staging models in one operation
        
        Args:
            dataset_ids: List of dataset IDs to process. If None, uses HEALTH_DATASETS
            sources_file: Path to sources.yml file. If None, uses default path
            models_dir: Directory to write models. If None, uses default path
            
        Returns:
            Dictionary with results summary
        """
        logger.info("Starting consolidated processing...")
        
        # Add sources first
        sources_added = self.add_missing_sources(dataset_ids, sources_file)
        
        # Then generate staging models
        models_generated = self.generate_staging_models(dataset_ids, models_dir)
        
        results = {
            'sources_added': sources_added,
            'models_generated': len(models_generated),
            'model_names': models_generated,
            'datasets_processed': dataset_ids or HEALTH_DATASETS
        }
        
        logger.info(f"✓ Consolidated processing complete: {sources_added} sources added, {len(models_generated)} models generated")
        return results

def main():
    """CLI interface for the consolidated model generator"""
    parser = argparse.ArgumentParser(description="Consolidated dbt sources and staging model generator")
    parser.add_argument("--action", choices=['sources', 'models', 'all'], default='all',
                       help="Action to perform: sources, models, or all")
    parser.add_argument("--dataset-ids", help="Comma-separated list of dataset IDs (default: health datasets)")
    parser.add_argument("--sources-file", help="Path to sources.yml file")
    parser.add_argument("--models-dir", help="Directory to write staging models")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    # Parse dataset IDs
    dataset_ids = None
    if args.dataset_ids:
        dataset_ids = [d.strip() for d in args.dataset_ids.split(',')]
    
    # Initialize generator
    generator = ConsolidatedModelGenerator()
    
    # Execute requested action
    if args.action == 'sources':
        sources_added = generator.add_missing_sources(dataset_ids, args.sources_file)
        print(f"Added {sources_added} source tables")
    elif args.action == 'models':
        models_generated = generator.generate_staging_models(dataset_ids, args.models_dir)
        print(f"Generated {len(models_generated)} staging models: {', '.join(models_generated)}")
    else:  # all
        results = generator.process_all(dataset_ids, args.sources_file, args.models_dir)
        print(f"Sources added: {results['sources_added']}")
        print(f"Models generated: {results['models_generated']}")
        if results['model_names']:
            print(f"Model names: {', '.join(results['model_names'])}")

if __name__ == "__main__":
    main() 