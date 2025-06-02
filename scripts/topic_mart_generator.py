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
from typing import Dict, List, Tuple, Any, Set, Optional
import logging
from collections import defaultdict

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

from shared.config import get_path

logger = logging.getLogger(__name__)

# --- Configuration ---
DBT_PROJECT_ROOT = Path(os.path.dirname(__file__)).parent / 'dbt_project'
SOURCES_YML_PATH = DBT_PROJECT_ROOT / 'models' / 'sources' / 'sources.yml'
MART_MODELS_OUTPUT_DIR = DBT_PROJECT_ROOT / 'models' / 'marts'
# Path to the CSV defining topic groups.
# Using get_path for now, assuming it resolves correctly or will be made configurable.
# If running locally and not in Airflow, this path might need adjustment.
GROUPED_DATASETS_CSV_PATH = Path(os.path.dirname(__file__)).parent / 'grouped_datasets_summary.csv'


# Stems to exclude when auto-discovering dimensions from sources.yml
# Should align with generate_dbt_star_schema.py
EXCLUDED_DIMENSION_STEMS = ['value', 'status', 'source_dataset', 'linear_index', 'time_format']

# Define the standard set of columns expected in the output topic marts
# These will guide the SELECT statements from the fact views
MART_MEASURE_COLUMNS = ['value']
MART_METADATA_COLUMNS = ['status_code', 'status_label', 'linear_index'] # Assuming these are in fct views
MART_SOURCE_ID_COLUMN = 'source_dataset_code' # To identify original dataset

class TopicMartGenerator:
    """Generate topic-based mart models from star schema fact views"""

    def __init__(self):
        """Initialize generator. Database connection no longer directly needed here."""
        self.sources_data: Optional[Dict[str, Any]] = None
        self.discovered_stems_config: Optional[Dict[str, Dict[str, str]]] = None
        pass

    def _load_sources_yml(self) -> bool:
        """Loads and parses the sources.yml file."""
        try:
            with open(SOURCES_YML_PATH, 'r') as f:
                self.sources_data = yaml.safe_load(f)
            if not self.sources_data or 'sources' not in self.sources_data or \
               not self.sources_data['sources'][0].get('tables'):
                logger.error(f"Invalid or empty sources.yml structure in {SOURCES_YML_PATH}")
                self.sources_data = None
                return False
            logger.info(f"Successfully loaded sources.yml from {SOURCES_YML_PATH}")
            return True
        except FileNotFoundError:
            logger.error(f"sources.yml not found at {SOURCES_YML_PATH}. Run generate_dbt_star_schema.py first.")
            return False
        except yaml.YAMLError as e:
            logger.error(f"Error parsing sources.yml: {e}")
            return False

    def _discover_dimension_stems_from_sources(self) -> bool:
        """
        Discovers potential dimension stems from the loaded sources.yml data.
        This method populates self.discovered_stems_config.
        Returns True if successful, False otherwise.
        """
        if not self.sources_data:
            logger.error("Cannot discover dimension stems, sources.yml data not loaded.")
            return False

        all_tables = self.sources_data['sources'][0]['tables']
        potential_stems: Dict[str, Dict[str, Any]] = {}

        for table in all_tables:
            columns = {col['name'] for col in table.get('columns', [])}
            for col_name in columns:
                if col_name.endswith('_key'):
                    stem = col_name[:-4] # Remove '_key'
                    label_col_v1 = f"{stem}_label"
                    label_col_v2 = f"{stem}_name" # Allow for _name as well

                    if stem not in EXCLUDED_DIMENSION_STEMS and \
                       (label_col_v1 in columns or label_col_v2 in columns):
                        actual_label_col = label_col_v1 if label_col_v1 in columns else label_col_v2
                        if stem not in potential_stems:
                            potential_stems[stem] = {
                                "key_col": col_name, # e.g. geo_key
                                "label_col": actual_label_col, # e.g. geo_label
                                "description": f"Conformed dimension for {stem}"
                                # Add other relevant info if needed by this script
                            }
        
        if not potential_stems:
            logger.warning("No potential dimension stems discovered from sources.yml.")
            # This might be okay if data is purely factual with no shared dimensions
            
        self.discovered_stems_config = potential_stems
        logger.info(f"Discovered {len(self.discovered_stems_config)} potential dimension stems: {list(self.discovered_stems_config.keys())}")
        return True

    def load_topic_groups(self, csv_file_path: Path) -> Dict[str, List[str]]:
        """Load topic groups from CSV file"""
        topic_groups = {}
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    group_name = row['group_name']
                    # Ensure dataset_count is present and valid before trying to convert
                    dataset_count_str = row.get('dataset_count')
                    if not dataset_count_str or not dataset_count_str.isdigit():
                        logger.warning(f"Skipping group '{group_name}' due to missing or invalid 'dataset_count'.")
                        continue
                    # dataset_count = int(dataset_count_str) # No longer strictly needed for filtering here

                    # REMOVED Filtering Logic: Process all groups defined in the CSV.
                    # Previously, we skipped single-dataset groups and special groups.
                    # if (dataset_count < 2 or
                    #     group_name.startswith('datasets_with_') or
                    #     group_name in ['unknown_grouping_issue']):
                    #     continue

                    dataset_ids_str = row.get('dataset_ids')
                    if not dataset_ids_str:
                        logger.warning(f"Skipping group '{group_name}' due to missing 'dataset_ids'.")
                        continue
                    
                    dataset_ids = [d.strip() for d in dataset_ids_str.split(',') if d.strip()]
                    if dataset_ids: # Only add if there are actual dataset_ids
                        topic_groups[group_name] = dataset_ids
                    else:
                        logger.warning(f"Group '{group_name}' has no valid dataset_ids after parsing.")

        except FileNotFoundError:
            logger.error(f"Topic groups CSV file not found: {csv_file_path}")
            # return {} # Return empty if file not found, or raise error
        except Exception as e:
            logger.error(f"Failed to load topic groups from {csv_file_path}: {e}")
            # return {} # Return empty on other errors
        
        logger.info(f"Loaded {len(topic_groups)} topic groups from {csv_file_path} to be processed.")
        return topic_groups

    def _generate_star_topic_mart_sql(self, topic_name: str, dataset_ids_in_topic: List[str]) -> Optional[str]:
        """
        Generates the SQL for a single topic-based mart model using star schema fact views.

        Args:
            topic_name: The name of the topic.
            dataset_ids_in_topic: List of dataset IDs belonging to this topic.

        Returns:
            The SQL string for the mart model, or None if generation is not possible.
        """
        if not self.sources_data or not self.discovered_stems_config:
            logger.error(f"Sources data or dimension stems not loaded. Cannot generate mart for topic '{topic_name}'.")
            return None

        # Find all source table definitions from sources.yml
        source_table_definitions = {tbl['name']: tbl for tbl in self.sources_data['sources'][0]['tables']}

        superset_of_dim_keys_for_topic: Set[str] = set()
        dataset_to_its_dim_keys_map: Dict[str, Set[str]] = {}
        valid_dataset_ids_for_topic: List[str] = []

        for dataset_id in dataset_ids_in_topic:
            dataset_id_lower = dataset_id.lower()
            if dataset_id_lower not in source_table_definitions:
                logger.warning(f"Dataset ID '{dataset_id}' from topic '{topic_name}' not found in sources.yml. Skipping.")
                continue
            
            valid_dataset_ids_for_topic.append(dataset_id)
            current_dataset_dim_keys: Set[str] = set()
            raw_table_cols = {col['name'] for col in source_table_definitions[dataset_id_lower].get('columns', [])}

            for stem, stem_config in self.discovered_stems_config.items():
                original_key_col = stem_config["key_col"] # This is from raw source, e.g. geo_key
                # The column in the fact view will now be stem_code, e.g., geo_code
                dim_code_col_in_fact = f"{stem}_code"
                if original_key_col in raw_table_cols: # Check if original source had this dimension
                    # We expect the fct view to have transformed original_key_col to dim_code_col_in_fact
                    superset_of_dim_keys_for_topic.add(dim_code_col_in_fact)
                    current_dataset_dim_keys.add(dim_code_col_in_fact)
            dataset_to_its_dim_keys_map[dataset_id] = current_dataset_dim_keys

        if not valid_dataset_ids_for_topic:
            logger.warning(f"No valid (found in sources.yml) datasets for topic '{topic_name}'. Mart not generated.")
            return None

        union_parts = []
        # Ensure consistent ordering of dimension keys in SELECT
        ordered_superset_dim_keys = sorted(list(superset_of_dim_keys_for_topic))

        for dataset_id in valid_dataset_ids_for_topic:
            select_clauses = []
            # Add dimension foreign keys (now _code columns)
            for dim_key_to_select in ordered_superset_dim_keys: # This list now contains _code column names
                if dim_key_to_select in dataset_to_its_dim_keys_map[dataset_id]:
                    select_clauses.append(f"    {dim_key_to_select}")
                else:
                    select_clauses.append(f"    NULL AS {dim_key_to_select}")
            
            # Add measure columns
            for measure_col in MART_MEASURE_COLUMNS:
                # Assuming measures are always present in fact views if the view exists
                select_clauses.append(f"    {measure_col}")
            
            # Add metadata columns from fact views
            for meta_col in MART_METADATA_COLUMNS:
                select_clauses.append(f"    {meta_col}")

            # Add source dataset identifier
            select_clauses.append(f"    \'{dataset_id.lower()}\' AS {MART_SOURCE_ID_COLUMN}")
            
            # Clean up commas for the actual SELECT list (last non-comment line should not have a comma)
            final_select_lines = []
            for i, clause in enumerate(select_clauses):
                is_last_selectable_line = True
                for next_clause in select_clauses[i+1:]:
                     is_last_selectable_line = False
                     break
                if is_last_selectable_line:
                    final_select_lines.append(clause) # No comma
                else:
                    final_select_lines.append(f"{clause},")

            dataset_sql_select_block = "\n".join(final_select_lines)

            dataset_sql = f"SELECT\n{dataset_sql_select_block}\nFROM {{{{ ref('fct_{dataset_id.lower()}') }}}}\nWHERE value IS NOT NULL OR status_code IS NOT NULL" # Retaining original WHERE clause logic
            union_parts.append(dataset_sql)

        if not union_parts:
            logger.warning(f"No SQL parts generated for topic mart '{topic_name}'. Mart not generated.")
            return None

        model_name = "mart_" + "".join(c if c.isalnum() or c == '_' else '_' for c in topic_name.lower())
        header = f"-- Topic-based mart for: {topic_name}\n"
        header += f"-- Combines {len(valid_dataset_ids_for_topic)} fact views related to this topic.\n"
        header += "-- Generated automatically by topic_mart_generator.py from star schema components.\n"
        header += f"{{{{ config(materialized='table') }}}}\n"
        
        full_sql = f"{header}\n{chr(10).join(union_parts)}"
        if len(union_parts) > 1:
            full_sql = f"{header}\n{'\nUNION ALL\n'.join(union_parts)}"
        else: # Single dataset topic mart (if load_topic_groups allows it, or for robustness)
            full_sql = f"{header}\n{union_parts[0]}"
            
        return full_sql

    def generate_topic_documentation(self, topic_name: str, dataset_ids: List[str], mart_sql: str) -> Dict[str, Any]:
        """Generates a basic description for the topic mart model."""
        # This is a simplified version. Could be expanded based on actual columns.
        # The `mart_sql` argument isn't directly used here now but could be for more advanced parsing.
        
        # Identify dimension keys from the superset created in _generate_star_topic_mart_sql
        # This requires access to similar logic or the result of it.
        # For simplicity, we'll just list that it contains dimension keys and measures.
        
        dim_keys_present_desc = "various dimension foreign keys (e.g., geo_dim_key, unit_dim_key)"
        if self.discovered_stems_config:
            dim_keys_present_desc = ", ".join([f"{stem}_dim_key" for stem in self.discovered_stems_config.keys() if f"{stem}_dim_key" in mart_sql]) # Crude check
            if not dim_keys_present_desc:  # Fallback if crude check fails
                 dim_keys_present_desc = "various conformed dimension foreign keys"


        description = (
            f"Topic mart for '{topic_name}'.\n"
            f"Consolidates data from {len(dataset_ids)} Eurostat fact views related to this topic: "
            f"{', '.join(dataset_ids)}.\n"
            f"Provides a unified view with harmonized dimension foreign keys and measures.\n"
            f"Columns include: {dim_keys_present_desc}, {', '.join(MART_MEASURE_COLUMNS)}, "
            f"{', '.join(MART_METADATA_COLUMNS)}, and {MART_SOURCE_ID_COLUMN}.\n"
            f"Materialized as a table."
        )
        return {
            "name": "mart_" + "".join(c if c.isalnum() or c == '_' else '_' for c in topic_name.lower()),
            "description": description,
            "columns": [] # Placeholder, to be filled by generate_marts_schema
        }

    def generate_topic_marts(
        self,
        grouped_datasets_csv_path: Path,
        topic_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Generates all topic mart .sql files and collects schema information."""
        results = {"generated_marts": [], "failed_marts": [], "schemas": []}

        if not self._load_sources_yml() or not self._discover_dimension_stems_from_sources():
            logger.error("Failed to initialize sources or stems. Aborting mart generation.")
            return results

        topic_groups = self.load_topic_groups(grouped_datasets_csv_path)
        if not topic_groups:
            logger.warning("No topic groups loaded. No marts will be generated.")
            return results

        os.makedirs(MART_MODELS_OUTPUT_DIR, exist_ok=True)

        for topic_name, dataset_ids in topic_groups.items():
            if topic_filter and topic_name not in topic_filter:
                continue

            logger.info(f"--- Generating mart for topic: {topic_name} ---")
            mart_sql = self._generate_star_topic_mart_sql(topic_name, dataset_ids)

            if mart_sql:
                model_name = "mart_" + "".join(c if c.isalnum() or c == '_' else '_' for c in topic_name.lower())
                file_path = MART_MODELS_OUTPUT_DIR / f"{model_name}.sql"
                try:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(mart_sql)
                    logger.info(f"Successfully generated mart model: {file_path}")
                    results["generated_marts"].append(model_name)
                    
                    # Generate basic documentation/schema entry
                    # The mart_sql is passed here, which is a bit of a change in flow.
                    doc_info = self.generate_topic_documentation(topic_name, dataset_ids, mart_sql)
                    # Further schema details (columns with tests) will be added by generate_marts_schema
                    results["schemas"].append(doc_info) 

                except IOError as e:
                    logger.error(f"Failed to write mart model {file_path}: {e}")
                    results["failed_marts"].append(topic_name)
            else:
                logger.warning(f"SQL generation failed for topic '{topic_name}'. Mart not created.")
                results["failed_marts"].append(topic_name)
        
        return results

    def generate_marts_schema(
        self, 
        mart_generation_results: Dict[str, Any], 
        # discovered_stems_config: Dict[str, Dict[str, str]] # Now uses self.discovered_stems_config
    ):
        """Generates a schema.yml content suggestion for the created mart models."""
        if not self.discovered_stems_config:
            logger.warning("Dimension stems not available, cannot generate detailed schema for marts.")
            # Fallback to use schema entries already partially populated by generate_topic_documentation
            if mart_generation_results.get("schemas"):
                schema_models = mart_generation_results["schemas"]
            else:
                logger.error("No schema information available to generate schema.yml for marts.")
                return
        else:
            # Enhance the schema entries with column details
            schema_models = []
            for mart_schema_entry in mart_generation_results.get("schemas", []):
                model_name = mart_schema_entry["name"]
                columns_for_schema = []
                
                # Attempt to get actual columns from a generated mart SQL (if needed and robustly possible)
                # For now, rely on discovered_stems and standard columns
                
                # Add FKs based on discovered stems that would likely be in the mart
                # This needs to be more precise by checking against the actual superset_of_dim_keys for that specific mart
                # For now, let's assume all discovered stems *could* be in any mart, which is an oversimplification.
                # A better approach would be to pass the `superset_of_dim_keys_for_topic` to documentation/schema generation.
                for stem in self.discovered_stems_config.keys():
                    # FK in mart is the _code column, e.g. geo_code
                    fk_code_col = f"{stem}_code"
                    dim_table_ref = f"dim_{stem}"
                    
                    # Relationship test joins mart's _code col to dimension's _code col
                    relationship_test_dict = {"relationships": {"to": dim_table_ref, "field": fk_code_col}}
                    
                    columns_for_schema.append({
                        "name": fk_code_col,
                        "description": f"Foreign key to {dim_table_ref}.{fk_code_col}, linking to {stem} dimension.",
                        "tests": [
                            relationship_test_dict
                        ]
                    })
                
                # Add measure columns
                for col_name in MART_MEASURE_COLUMNS:
                    columns_for_schema.append({"name": col_name, "description": "Measure value.", "tests": ["not_null"] if col_name == 'value' else [] })
                
                # Add metadata columns
                for col_name in MART_METADATA_COLUMNS:
                    columns_for_schema.append({"name": col_name, "description": f"Metadata column: {col_name}."})
                
                # Add source dataset ID column
                columns_for_schema.append({"name": MART_SOURCE_ID_COLUMN, "description": "Identifier of the original Eurostat dataset for this row.", "tests": ["not_null"]})
                
                mart_schema_entry["columns"] = columns_for_schema
                schema_models.append(mart_schema_entry)

        if not schema_models:
            logger.info("No mart schemas to generate.")
            return

        schema_yml_content = {"version": 2, "models": schema_models}
        try:
            output_path = MART_MODELS_OUTPUT_DIR / "schema_topic_marts.yml"
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(schema_yml_content, f, sort_keys=False, indent=2, allow_unicode=True)
            logger.info(f"--- Suggested schema for Topic Marts written to: {output_path} ---")
            logger.info("Review this file and integrate its contents into your dbt project's schema definitions (e.g., in models/marts/your_marts_schema.yml).")
            # print("\n" + yaml.dump(schema_yml_content, sort_keys=False, indent=2, allow_unicode=True))
        except Exception as e:
            logger.error(f"Could not generate YAML for topic marts schema.yml: {e}")

def main():
    """Main function to generate topic marts"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Argument parsing (simplified for now, can be expanded)
    # Defaulting to the configured GROUPED_DATASETS_CSV_PATH
    # In a real CLI, you'd use argparse to allow overriding this.
    csv_file_path = GROUPED_DATASETS_CSV_PATH 
    # topic_filter = None # Example: ['Some Specific Topic']
    topic_filter = [] # Process all topics by default

    logger.info(f"Starting topic mart generation using CSV: {csv_file_path}")
    if topic_filter:
        logger.info(f"Filtering for topics: {topic_filter}")

    generator = TopicMartGenerator()
    
    # The generate_topic_marts method now handles most of the orchestration.
    # It loads sources.yml, discovers stems, loads topic groups, generates SQLs, and collects schema info.
    mart_generation_results = generator.generate_topic_marts(
        grouped_datasets_csv_path=csv_file_path, 
        topic_filter=topic_filter if topic_filter else None
    )

    generated_count = len(mart_generation_results.get("generated_marts", []))
    failed_count = len(mart_generation_results.get("failed_marts", []))

    logger.info(f"Mart generation complete. Generated: {generated_count}, Failed: {failed_count}.")

    if generated_count > 0:
        logger.info(f"Generated mart models: {mart_generation_results['generated_marts']}")
        # Call schema generation if any marts were successfully created and schema info was collected
        if mart_generation_results.get("schemas"):
            logger.info("Proceeding to generate schema.yml for created marts.")
            generator.generate_marts_schema(mart_generation_results)
        else:
            logger.warning("No schema information was collected during mart generation, skipping schema.yml output.")
    
    if failed_count > 0:
        logger.warning(f"Failed to generate marts for topics: {mart_generation_results['failed_marts']}")

    logger.info("Topic mart generation process finished.")

if __name__ == '__main__':
    # For direct execution. 
    # Consider adding proper CLI argument parsing here (e.g., using argparse) 
    # if you need to specify CSV path or topic filters dynamically from command line.
    main() 