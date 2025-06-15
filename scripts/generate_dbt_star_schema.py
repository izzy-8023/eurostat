import yaml
import os
import logging
import psycopg2 
from pathlib import Path
import glob 

# --- Script Configuration ---
DB_NAME = os.environ.get("DB_NAME", "eurostat_data")
DB_USER = os.environ.get("DB_USER", "eurostat_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "mysecretpassword")
DB_HOST = os.environ.get("DB_HOST", "db")  
DB_PORT = os.environ.get("DB_PORT", "5432")

# Schema in PostgreSQL where the raw Eurostat tables reside
DB_RAW_SCHEMA = "public"
# Pattern to identify Eurostat tables (all start with 'hlth_')
# Use None or a very general pattern like '%' to discover all tables in DB_RAW_SCHEMA, Example: EUROSTAT_TABLE_PATTERN = "hlth_%"
EUROSTAT_TABLE_PATTERN = "%" # Discover all tables in the schema

DBT_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', 'dbt_project')
SOURCES_DIR_PATH = os.path.join(DBT_PROJECT_ROOT, 'models', 'sources') # New path for sources.yml
SOURCES_FILE_PATH = os.path.join(SOURCES_DIR_PATH, 'sources.yml')
DIM_MODELS_PATH = os.path.join(DBT_PROJECT_ROOT, 'models', 'dimensions')
FACT_MODELS_PATH = os.path.join(DBT_PROJECT_ROOT, 'models', 'facts')
STG_MODELS_PATH = os.path.join(DBT_PROJECT_ROOT, 'models', 'staging') # Added staging models path

# Stems to exclude when auto-discovering dimensions
# These are typically measure names or generic metadata not suitable for conformed dimensions
EXCLUDED_DIMENSION_STEMS = ['value', 'status', 'source_dataset', 'linear_index', 'time_format'] 

MEASURE_COLUMNS = ['value']
METADATA_FACT_COLUMNS = ['source_dataset_id'] # Removed status_code, status_label, linear_index

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper function to clean directories and to prevent stale models
def clean_generated_model_files(directory_path, patterns):
    """Removes files matching specified patterns from a directory."""
    logging.info(f"Cleaning patterns {patterns} from directory {directory_path}...")
    for pattern in patterns:
        files_to_delete = glob.glob(os.path.join(directory_path, pattern))
        if not files_to_delete:
            logging.info(f"No files found for pattern '{pattern}' in {directory_path}.")
        for f_path in files_to_delete:
            try:
                os.remove(f_path)
                logging.info(f"Removed stale file: {f_path}")
            except OSError as e:
                logging.error(f"Error removing file {f_path}: {e}")

# --- Database Interaction & sources.yml Generation ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("Successfully connected to PostgreSQL.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise

def introspect_database_schema(conn, processed_dataset_ids_filter=None):
    """Introspects the database to find tables and their columns."""
    tables_schema = {}
    try:
        with conn.cursor() as cur:
            params = [DB_RAW_SCHEMA]
            if processed_dataset_ids_filter and isinstance(processed_dataset_ids_filter, list) and len(processed_dataset_ids_filter) > 0:
                # Convert dataset_ids to lowercase 
                lower_case_ids = [dataset_id.lower() for dataset_id in processed_dataset_ids_filter]
                table_query = f""" 
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = ANY(%s);
                """
                params.append(lower_case_ids)
                logging.info(f"Introspecting specific tables: {lower_case_ids} in schema '{DB_RAW_SCHEMA}'.")
            else:
                table_query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name LIKE %s;
                """
                params.append(EUROSTAT_TABLE_PATTERN)
                logging.info(f"Introspecting tables in schema '{DB_RAW_SCHEMA}' matching pattern '{EUROSTAT_TABLE_PATTERN}'.")

            cur.execute(table_query, tuple(params))
            tables = [row[0] for row in cur.fetchall()]

            # Confirm the number of tables found after filtering.
            logging.info(f"Found {len(tables)} tables matching criteria.")

            for table_name in tables:
                column_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{DB_RAW_SCHEMA}'
                  AND table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
                cur.execute(column_query)
                columns = []
                for row in cur.fetchall():
                    columns.append({"name": row[0], "description": f"Raw column {row[0]} from {table_name} (type: {row[1]})"})
                tables_schema[table_name] = columns
                logging.debug(f"Introspected {table_name}: {len(columns)} columns.")
        return tables_schema
    except psycopg2.Error as e:
        logging.error(f"Database introspection error: {e}")
        raise

def generate_sources_yml_content(tables_schema):
    """Generates the content for sources.yml based on introspected schema."""
    sources_list = []
    for table_name, columns in tables_schema.items():
        table_entry = {
            "name": table_name,
            "description": f"Raw Eurostat table '{table_name}' loaded from source.",
            "columns": columns
        }
        sources_list.append(table_entry)

    content = {
        "version": 2,
        "sources": [
            {
                "name": "eurostat_raw", # Keep a consistent source name for dbt
                "schema": DB_RAW_SCHEMA,
                "description": f"Raw Eurostat data tables loaded into the '{DB_RAW_SCHEMA}' schema.",
                "tables": sources_list
            }
        ]
    }
    return content

def write_sources_yml(content):
    """Writes the content to the sources.yml file."""
    os.makedirs(SOURCES_DIR_PATH, exist_ok=True)
    try:
        with open(SOURCES_FILE_PATH, 'w') as f:
            yaml.dump(content, f, sort_keys=False, indent=2)
        logging.info(f"Successfully generated/updated {SOURCES_FILE_PATH}")
    except IOError as e:
        logging.error(f"Error writing {SOURCES_FILE_PATH}: {e}")
        raise

# --- Dimension and Fact Model Generation Logic ---
def discover_dimension_stems(sources_data):
    """Discovers potential dimension stems from parsed sources.yml data."""
    if not sources_data.get('sources') or not sources_data['sources'][0].get('tables'):
        logging.warning("Invalid sources data structure, cannot discover dimension stems.")
        return {}

    all_tables = sources_data['sources'][0]['tables']
    potential_stems = {} # Using dict to store stem and its original key/label names

    for table in all_tables:
        columns = {col['name'] for col in table.get('columns', [])}
        for col_name in columns:
            if col_name.endswith('_key'):
                stem = col_name[:-4] # Remove '_key'
                label_col_v1 = f"{stem}_label"
                label_col_v2 = f"{stem}_name" # Allow for _name as well if that's a pattern

                if stem not in EXCLUDED_DIMENSION_STEMS and (label_col_v1 in columns or label_col_v2 in columns):
                    actual_label_col = label_col_v1 if label_col_v1 in columns else label_col_v2
                    if stem not in potential_stems:
                        potential_stems[stem] = {"key_col": col_name, "label_col": actual_label_col, "tables_count": 0, "description": f"Conformed dimension for {stem}"}
                    potential_stems[stem]["tables_count"] +=1
    

    logging.info(f"Discovered {len(potential_stems)} potential dimension stems: {list(potential_stems.keys())}")
    return potential_stems

def generate_dim_model_sql(dim_stem, dim_config, all_source_tables_with_columns):
    original_key_col = dim_config["key_col"]
    original_label_col = dim_config["label_col"]
    
    dim_code_col = f"{dim_stem}_code"
    dim_name_col = f"{dim_stem}_name"
    surrogate_key_col = f"{dim_stem}_dim_key"
    dim_description = dim_config.get("description", f"Dimension for {dim_stem}")

    union_parts = []
    for table_def in all_source_tables_with_columns:
        table_name = table_def['name']
        table_columns = {col['name'] for col in table_def.get('columns', [])}
        if original_key_col in table_columns and original_label_col in table_columns:
            union_parts.append(
                f"    SELECT DISTINCT CAST({original_key_col} AS VARCHAR) AS {dim_code_col}, CAST({original_label_col} AS VARCHAR) AS {dim_name_col} FROM {{{{ source('eurostat_raw', '{table_name}') }}}} WHERE {original_key_col} IS NOT NULL"
            )
    
    if not union_parts:
        logging.warning(f"No source tables supply both '{original_key_col}' and '{original_label_col}' for dimension '{dim_stem}'. Model will be empty or fail.")
        return "-- Dimension model skipped: No source tables found with required key/label columns."

    unioned_sql = "\n    UNION ALL\n".join(union_parts)

    sql = f"""{{{{ config(
    materialized='incremental',
    unique_key='{dim_code_col}'
) }}}}

-- {dim_description}
-- Generated automatically by generate_dbt_star_schema.py

WITH source_values AS (
{unioned_sql}
),

distinct_values AS (
    SELECT
        {dim_code_col},
        MAX({dim_name_col}) AS {dim_name_col} -- Ensure one name per code if slight variations exist post-union
    FROM source_values
    GROUP BY {dim_code_col}
)

SELECT
    {dim_code_col}, -- This is now the primary key
    {dim_name_col}
FROM distinct_values

{{% if is_incremental() %}}
-- this filter will only be applied on an incremental run
WHERE {dim_code_col} NOT IN (SELECT {dim_code_col} FROM {{{{ this }}}})
{{% endif %}}

ORDER BY {dim_code_col} -- Optional for easier inspection
"""
    return sql

def generate_fact_model_sql(source_table_name, source_table_columns_info, discovered_dim_stems):
    select_clauses = ["    -- Dimension Codes from Source"]
    
    source_column_names = {col_info['name'] for col_info in source_table_columns_info}

    for stem, stem_config in discovered_dim_stems.items():
        original_key_col = stem_config["key_col"] # e.g., geo_key
        target_dim_code_col = f"{stem}_code" # e.g., geo_code
        
        if original_key_col in source_column_names: # Only include the column if the original key exists
            select_clauses.append(f"    source_table.{original_key_col} AS {target_dim_code_col},"
            )

    select_clauses.append("\n    -- Measures")
    for col_name in MEASURE_COLUMNS:
        if col_name in source_column_names:
            select_clauses.append(f"    source_table.{col_name},")
    
    select_clauses.append("\n    -- Metadata from source")
    for col_name in METADATA_FACT_COLUMNS:
        if col_name in source_column_names:
            select_clauses.append(f"    source_table.{col_name},")
            
    # Clean up trailing commas from the select_clauses list before joining
    processed_select_parts = []
    last_data_clause_idx = -1
    for i in range(len(select_clauses) - 1, -1, -1):
        if not select_clauses[i].strip().startswith("--"):
            last_data_clause_idx = i
            break

    for i, clause in enumerate(select_clauses):
        stripped_clause_content = clause.strip()
        if not stripped_clause_content.startswith("--") and stripped_clause_content: # It's a data clause and not empty
            # Remove any existing trailing comma, then add one back if not the last data clause
            current_clause_main_part = clause.rstrip(',')
            if i < last_data_clause_idx:
                 processed_select_parts.append(current_clause_main_part + ",")
            else:
                 processed_select_parts.append(current_clause_main_part)
        else: # It's a comment or an empty line (though select_clauses shouldn't have fully empty lines)
            processed_select_parts.append(clause)
            
    select_statement = "\n".join(processed_select_parts)

    sql = f"""{{{{ config(materialized='view') }}}}

-- Fact view for {source_table_name}, linking to conformed dimensions.
-- Generated automatically by generate_dbt_star_schema.py

SELECT
{select_statement}
FROM {{{{ source('eurostat_raw', '{source_table_name}') }}}} source_table

"""
    return sql

def generate_schema_yml_suggestions(dim_models_generated_config, fact_models_generated_sources, discovered_dimension_stems_param):
    logging.info("\n--- Suggested schema.yml updates ---")
    dim_schema_models = []
    fact_schema_models = []

    for dim_model_name, dim_config in dim_models_generated_config.items():
        dim_stem = dim_model_name.replace("dim_", "")
        # Surrogate key no longer exists
        code_col = f"{dim_stem}_code"
        name_col = f"{dim_stem}_name"
        dim_schema_models.append({
            "name": dim_model_name,
            "description": dim_config.get("description", f"Conformed dimension for {dim_stem}."),
            "columns": [
                # Code column is now the primary key
                {"name": code_col, "description": f"Primary key ({code_col}) for {dim_stem}.", "tests": ["unique", "not_null"]},
                {"name": name_col, "description": f"Descriptive name for {dim_stem} ({name_col})."}
            ]
        })

    for fact_model_name, source_table_name in fact_models_generated_sources.items():
        cols_for_fact_schema = []
        for stem in discovered_dimension_stems_param: 
             # FK column in fact is now the _code column itself, e.g., geo_code
             fk_code_col = f"{stem}_code" 
             dim_table_ref = f"dim_{stem}"
             # Relationship test joins fact's _code col to dimension's _code col
             cols_for_fact_schema.append({
                 "name": fk_code_col, # e.g., geo_code
                 "description": f"Foreign key to {dim_table_ref}.{fk_code_col}, linking to the {stem} dimension.",
                 "tests": [
                     {"relationships": {"to": dim_table_ref, "field": fk_code_col}} # fact.geo_code -> dim_geo.geo_code
                 ]
             })
        
        # Add measure columns (example, assuming 'value' is a common measure)
        # This should be driven by MEASURE_COLUMNS constant or actual columns in fct view
        if "value" in MEASURE_COLUMNS: # Check against the global constant
             cols_for_fact_schema.append({
                 "name": "value", 
                 "description": "Measure value from source table.",
                 "tests": ["not_null"] # Example test
             })
        # Add other metadata columns defined in METADATA_FACT_COLUMNS
        for meta_col in METADATA_FACT_COLUMNS:
            cols_for_fact_schema.append({
                "name": meta_col,
                "description": f"Metadata column {meta_col} from source table."
            })


        fact_schema_models.append({
            "name": fact_model_name,
            "description": f"Fact view for source table '{source_table_name}'. Contains measures and foreign keys to conformed dimensions.",
            "columns": cols_for_fact_schema
        })
    
    # Write dimension schemas
    if dim_schema_models:
        dim_schema_yml_content = {"version": 2, "models": dim_schema_models}
        dim_schema_path = Path(DIM_MODELS_PATH) / "schema_dimensions.yml"
        try:
            with open(dim_schema_path, 'w') as f:
                yaml.dump(dim_schema_yml_content, f, sort_keys=False, indent=2, allow_unicode=True)
            logging.info(f"Dimension model schemas written to: {dim_schema_path}")
        except Exception as e:
            logging.error(f"Could not write dimension schemas to {dim_schema_path}: {e}")
    else:
        logging.info("No dimension model schemas to generate.")

    # Write fact schemas
    if fact_schema_models:
        fact_schema_yml_content = {"version": 2, "models": fact_schema_models}
        fact_schema_path = Path(FACT_MODELS_PATH) / "schema_facts.yml"
        try:
            with open(fact_schema_path, 'w') as f:
                yaml.dump(fact_schema_yml_content, f, sort_keys=False, indent=2, allow_unicode=True)
            logging.info(f"Fact model schemas written to: {fact_schema_path}")
        except Exception as e:
            logging.error(f"Could not write fact schemas to {fact_schema_path}: {e}")
    else:
        logging.info("No fact model schemas to generate.")

# --- Main Orchestration ---
def main(processed_dataset_ids_filter=None):
    if processed_dataset_ids_filter:
        logging.info(f"Starting dbt star schema generation, filtered for: {processed_dataset_ids_filter}")
    else:
        logging.info("Starting dbt star schema generation process...")

    # Create necessary directories
    os.makedirs(SOURCES_DIR_PATH, exist_ok=True)
    os.makedirs(DIM_MODELS_PATH, exist_ok=True)
    os.makedirs(FACT_MODELS_PATH, exist_ok=True)
    os.makedirs(STG_MODELS_PATH, exist_ok=True) # Ensure staging directory exists

    # Clean previously generated models and schema files to prevent stale models
    # when filters are applied or after a database reset.
    clean_generated_model_files(DIM_MODELS_PATH, ["dim_*.sql", "schema_dimensions.yml"])
    clean_generated_model_files(FACT_MODELS_PATH, ["fct_*.sql", "schema_facts.yml"])
    # clean staging models too, as they may be stale from previous runs
    # and cause "source not found" errors if the database has been reset.
    clean_generated_model_files(STG_MODELS_PATH, ["stg_*.sql", "schema_staging.yml"])

    # Note: sources.yml is in SOURCES_DIR_PATH and is overwritten, so no specific cleaning needed there.

    # 1. Introspect DB and generate/update sources.yml
    conn = None
    try:
        conn = get_db_connection()
        tables_schema = introspect_database_schema(conn, processed_dataset_ids_filter)
        if not tables_schema:
            logging.warning("No tables found in the database matching criteria. Exiting model generation.")
            return
        sources_yml_content = generate_sources_yml_content(tables_schema)
        write_sources_yml(sources_yml_content)
    except Exception as e:
        logging.error(f"Failed during database introspection or sources.yml generation: {e}")
        return # Critical step, exit if it fails
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    # Load the freshly generated sources.yml for subsequent steps
    try:
        with open(SOURCES_FILE_PATH, 'r') as f:
            current_sources_data = yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Generated sources file not found: {SOURCES_FILE_PATH}. Cannot proceed.")
        return
    except yaml.YAMLError as e:
        logging.error(f"Error parsing freshly generated YAML from {SOURCES_FILE_PATH}: {e}. Cannot proceed.")
        return

    # 2. Discover Dimension Stems from the current sources.yml
    discovered_stems = discover_dimension_stems(current_sources_data)
    if not discovered_stems:
        logging.warning("No dimension stems discovered. Only fact views (without dimensional links) might be generated if any.")
        # Still proceed to generate fact views, they might just not have FKs if no dims are made

    all_source_tables_with_columns = current_sources_data['sources'][0]['tables']


    # 3. Generate Dimension Models
    logging.info("\n--- Generating Dimension Models ---")
    dim_models_generated_config = {} # For schema.yml
    for dim_stem, dim_config_details in discovered_stems.items():
        dim_sql = generate_dim_model_sql(dim_stem, dim_config_details, all_source_tables_with_columns)
        dim_model_name = f"dim_{dim_stem}"
        dim_file_path = os.path.join(DIM_MODELS_PATH, f"{dim_model_name}.sql")
        try:
            with open(dim_file_path, 'w') as f_dim:
                f_dim.write(dim_sql)
            logging.info(f"Generated dimension model: {dim_file_path}")
            dim_models_generated_config[dim_model_name] = dim_config_details
        except IOError as e:
            logging.error(f"Failed to write dimension model {dim_file_path}: {e}")

    # 4. Generate Fact Models (as Views)
    logging.info("\n--- Generating Fact Views ---")
    fact_models_generated_sources = {} # For schema.yml
    for table_def in all_source_tables_with_columns:
        table_name = table_def['name']
        table_columns_info = table_def.get('columns', [])
        if not table_columns_info: # Should not happen if sources.yml is well-formed
            logging.warning(f"Skipping fact view for {table_name} due to missing column definitions.")
            continue
            
        fact_sql = generate_fact_model_sql(table_name, table_columns_info, discovered_stems)
        fact_model_name = f"fct_{table_name}"
        fact_file_path = os.path.join(FACT_MODELS_PATH, f"{fact_model_name}.sql")
        try:
            with open(fact_file_path, 'w') as f_fact:
                f_fact.write(fact_sql)
            logging.info(f"Generated fact view: {fact_file_path}")
            fact_models_generated_sources[fact_model_name] = table_name
        except IOError as e:
            logging.error(f"Failed to write fact view {fact_file_path}: {e}")
    
    # 5. Suggest schema.yml updates
    if dim_models_generated_config or fact_models_generated_sources:
        generate_schema_yml_suggestions(dim_models_generated_config, fact_models_generated_sources, discovered_stems)

    logging.info("\nStar schema dbt model generation process complete.")
    logging.warning("Reminder: Obsolete models (e.g., stg_*.sql from previous logic) and generator scripts "
                    "(e.g., consolidated_model_generator.py) may need to be manually removed from your dbt project.")
    logging.warning("Ensure 'dbt-utils' is in your packages.yml and run 'dbt deps' if you haven't already.")


if __name__ == '__main__':
    # For direct execution, no filter is passed by default.
    main() 