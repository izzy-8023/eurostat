import json
import math
import os
import argparse
import ijson # For streaming JSON
import psycopg2
# from psycopg2.extras import execute_values # No longer needed
from psycopg2 import sql # For safe SQL construction
import io # For in-memory text buffer (StringIO)
import csv # For robust CSV formatting
import logging # For logging
import time # For timing, if we want to add it later

# --- Logger Setup ---
# Configure logger. Can be further customized (e.g., file output, format) as needed.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Core Logic from jsonParser.py (get_category_info_from_linear_index_core_logic) ---
# This function remains largely the same as it's about interpreting the JSON structure.
def pre_calculate_reverse_indices(dimension_details_meta):
    """Pre-calculates _reverse_index for all categories to avoid modifying during streaming."""
    if dimension_details_meta:
        for dim_id, dim_content in dimension_details_meta.items():
            category_section = dim_content.get('category', {})
            if isinstance(category_section, dict):
                index_map = category_section.get('index', {})
                if isinstance(index_map, dict):
                    category_section['_reverse_index'] = {v: k for k, v in index_map.items()}

def get_category_info_from_linear_index_core_logic(
        dimension_ids_meta, dimension_sizes_meta, dimension_details_meta,
        linear_index_str, value_content,
        current_status_code,
        status_labels_meta
    ):
    """
    Core logic to interpret a linear index, using pre-loaded metadata and streamed value/status.
    Reverse indices are expected to be pre-calculated in dimension_details_meta.
    """
    try:
        linear_index = int(linear_index_str)
    except ValueError:
        logger.warning(f"Skipping non-numeric index '{linear_index_str}'.")
        return None

    if not (dimension_ids_meta and dimension_sizes_meta and dimension_details_meta):
        logger.error("Essential dimension information is missing (passed to core logic function).")
        return None

    max_possible_index = 1
    for size in dimension_sizes_meta:
        max_possible_index *= size
    if linear_index < 0 or linear_index >= max_possible_index:
        logger.warning(f"Skipping out-of-bounds index {linear_index}. Max is {max_possible_index -1}.")
        return None

    row_dict = {'Linear_Index': linear_index} # Store as int
    temp_index = linear_index

    for i in range(len(dimension_ids_meta) - 1, -1, -1):
        dim_id = dimension_ids_meta[i]
        dim_size = dimension_sizes_meta[i]
        category_numeric_index = temp_index % dim_size
        temp_index = math.floor(temp_index / dim_size)

        category_section = dimension_details_meta.get(dim_id, {}).get('category', {})
        label_map = category_section.get('label', {})
        category_key = "Unknown Key"
        if isinstance(category_section, dict) and '_reverse_index' in category_section:
             category_key = category_section['_reverse_index'].get(category_numeric_index, "Unknown Key")
        category_label = label_map.get(category_key, f"Unknown Label (Index: {category_numeric_index})")

        row_dict[f"{dim_id}_key"] = category_key
        row_dict[f"{dim_id}_label"] = category_label

    # Attempt to convert value_content to float if possible, otherwise keep as is (or None)
    try:
        # Handle common Eurostat data flags explicitly as None (SQL NULL)
        if isinstance(value_content, str) and value_content.strip().lower() in [':', 'z', 'c', 'u', 'n/a', 'na', '-']: # Add or modify flags as needed
            logger.debug(f"Eurostat flag or common NA string '{value_content}' found for value at index {linear_index_str}. Storing as None (SQL NULL).")
            row_dict['Value'] = None
        elif value_content is not None:
            row_dict['Value'] = float(value_content)
        else: # value_content is already Python None
            row_dict['Value'] = None
    except (ValueError, TypeError):
        logger.warning(f"Could not convert value '{value_content}' to float for index {linear_index_str}. Storing as None (SQL NULL).")
        row_dict['Value'] = None # Ensure conversion errors also result in None for SQL NULL

    row_dict['Status_Code'] = current_status_code if current_status_code else ""
    
    status_label_text = ""
    if current_status_code and isinstance(status_labels_meta, dict):
        status_label_text = status_labels_meta.get(current_status_code, 'Unknown status code')
    row_dict['Status_Label'] = status_label_text
    return row_dict

# --- Main Parsing and Loading Function (using COPY) ---
def parse_json_and_load_to_db(input_json_path, table_name, source_dataset_id, db_copy_batch_lines=10000):
    """
    Streams a JSON-stat file, parses it, and loads data directly into PostgreSQL using COPY.
    """
    logger.info(f"Processing file: {input_json_path} for direct DB load into {table_name} using COPY.")

    # Database connection details from environment variables
    db_host = os.getenv('POSTGRES_HOST', 'db')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')

    if not all([db_name, db_user, db_password]):
        logger.error("DB credentials (POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) not set.")
        return False

    conn_str = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"
    conn = None
    cur = None

    dimension_ids = None
    dimension_details = None
    statuses_map = {}
    status_labels = {}

    try:
        logger.info("Phase 1: Loading metadata from JSON...")
        with open(input_json_path, 'rb') as f:
            full_data_except_value = {
                k: v for k, v in ijson.kvitems(f, '') if k != 'value'
            }
        
        dimension_ids = full_data_except_value.get('id')
        dimension_sizes = full_data_except_value.get('size')
        dimension_details = full_data_except_value.get('dimension')
        statuses_map = full_data_except_value.get('status', {})
        ext_data = full_data_except_value.get('extension', {})
        if isinstance(ext_data, dict):
            status_ext = ext_data.get('status', {})
            if isinstance(status_ext, dict):
                status_labels = status_ext.get('label', {})

        if not (dimension_ids and isinstance(dimension_ids, list) and
                dimension_sizes and isinstance(dimension_sizes, list) and
                dimension_details and isinstance(dimension_details, dict)):
            logger.error(f"Essential dimension info missing in {input_json_path}.")
            return False
        
        pre_calculate_reverse_indices(dimension_details)
        logger.info("Metadata loaded and reverse indices pre-calculated.")

        header_columns = ['linear_index']
        pg_column_sql_parts = [sql.SQL("linear_index INTEGER")]

        for dim_id in dimension_ids:
            dim_id_lower = dim_id.lower()
            header_columns.append(f"{dim_id_lower}_key")
            pg_column_sql_parts.append(sql.SQL("{} TEXT").format(sql.Identifier(f"{dim_id_lower}_key")))
            header_columns.append(f"{dim_id_lower}_label")
            pg_column_sql_parts.append(sql.SQL("{} TEXT").format(sql.Identifier(f"{dim_id_lower}_label")))
        
        header_columns.extend(['value', 'status_code', 'status_label', 'source_dataset_id'])
        pg_column_sql_parts.extend([
            sql.SQL("value FLOAT"), 
            sql.SQL("status_code TEXT"), 
            sql.SQL("status_label TEXT"), 
            sql.SQL("source_dataset_id TEXT")
        ])

        conn = psycopg2.connect(conn_str)
        conn.autocommit = False
        cur = conn.cursor()
        
        _raw_safe_table_name = "".join(c if c.isalnum() or c == '_' else '' for c in table_name).lower()
        if not _raw_safe_table_name: _raw_safe_table_name = 'eurostat_data' # fallback
        safe_table_identifier = sql.Identifier(_raw_safe_table_name)

        # Define column names for the COPY command
        col_names_sql = sql.SQL(', ').join(map(sql.Identifier, header_columns))

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
            safe_table_identifier,
            sql.SQL(', ').join(pg_column_sql_parts)
        )
        logger.info(f"Ensuring table exists using query: {create_table_query.as_string(conn)}")
        cur.execute(create_table_query)
        conn.commit()
        logger.info(f"Table {safe_table_identifier.strings[0] if safe_table_identifier.strings else _raw_safe_table_name} ensured.")

        # Prepare the COPY SQL statement for CSV format
        copy_sql_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '', DELIMITER ',', QUOTE '\"')").format(
            safe_table_identifier,
            col_names_sql
        )
        logger.info(f"Using COPY statement for loading: {copy_sql_stmt.as_string(conn)}")

        logger.info(f"Phase 2: Streaming 'value' items and loading to {safe_table_identifier.strings[0] if safe_table_identifier.strings else _raw_safe_table_name} using COPY (CSV format)...")
        
        total_rows_loaded = 0
        processed_count = 0
        skipped_count = 0
        lines_in_current_batch = 0

        csv_buffer = io.StringIO()
        # QUOTE_MINIMAL is good. Forcing quote_char to be default for less confusion with copy_from.
        # csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL, lineterminator='\n') # Old
        # csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL, lineterminator='\n') # Old: Quote all fields
        csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n') # New: Quote non-numeric

        with open(input_json_path, 'rb') as f_values:
            for linear_index_str, value_content in ijson.kvitems(f_values, 'value'):
                current_status_code = statuses_map.get(linear_index_str)
                interpreted_row_dict = get_category_info_from_linear_index_core_logic(
                    dimension_ids, dimension_sizes, dimension_details,
                    linear_index_str, value_content,
                    current_status_code, status_labels
                )
                processed_count += 1

                if interpreted_row_dict:
                    interpreted_row_dict['source_dataset_id'] = source_dataset_id
                    interpreted_row_lower_keys = {k.lower(): v for k, v in interpreted_row_dict.items()}
                    
                    try:
                        row_values_for_csv = [interpreted_row_lower_keys.get(col.lower()) for col in header_columns]
                        
                        csv_writer.writerow(row_values_for_csv)
                        lines_in_current_batch += 1
                    except Exception as e:
                        logger.error(f"Error formatting row for CSV: {e}. Row data: {interpreted_row_lower_keys}. Headers: {header_columns}", exc_info=True)
                        skipped_count += 1
                        continue
                else:
                    skipped_count += 1
                    continue

                if lines_in_current_batch >= db_copy_batch_lines:
                    csv_buffer.seek(0)
                    try:
                        # --- BEGIN NEW DEBUG BLOCK ---
                        current_batch_data_sample = csv_buffer.getvalue()[:1000] # Get a larger sample of the batch head
                        logger.debug(f"DEBUG_BATCH: Attempting to COPY. Python num columns: {len(header_columns)}. Batch head (first ~1000 chars): {current_batch_data_sample!r}")
                        # --- END NEW DEBUG BLOCK ---
                        # cur.copy_from(csv_buffer, safe_table_identifier.strings[0], sep=',', null='', columns=header_columns) # OLD METHOD
                        cur.copy_expert(copy_sql_stmt, csv_buffer) # NEW METHOD using CSV format
                        total_rows_loaded += lines_in_current_batch
                        logger.info(f"DB Load (COPY CSV): Sent batch of {lines_in_current_batch} lines. Total loaded: {total_rows_loaded}")
                        conn.commit()
                    except psycopg2.Error as db_batch_err:
                        conn.rollback() # Rollback this batch
                        logger.error(f"Database error during COPY batch: {db_batch_err}. Batch for table {safe_table_identifier.strings[0]} will be skipped. First few lines of batch: {csv_buffer.getvalue()[:500]}", exc_info=True)
                        # Decide if you want to stop or continue. For now, we skip and continue.
                    finally:
                        csv_buffer.close()
                        csv_buffer = io.StringIO()
                        # csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL, lineterminator='\n') # Old
                        # csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL, lineterminator='\n') # Old
                        csv_writer = csv.writer(csv_buffer, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n') # New: Quote non-numeric
                        lines_in_current_batch = 0

            if lines_in_current_batch > 0:
                csv_buffer.seek(0)
                try:
                    # --- BEGIN NEW DEBUG BLOCK (for final batch) ---
                    final_batch_data_sample = csv_buffer.getvalue()[:1000]
                    logger.debug(f"DEBUG_BATCH: Attempting to COPY FINAL batch. Python num columns: {len(header_columns)}. Batch head (first ~1000 chars): {final_batch_data_sample!r}")
                    # --- END NEW DEBUG BLOCK (for final batch) ---
                    # cur.copy_from(csv_buffer, safe_table_identifier.strings[0], sep=',', null='', columns=header_columns) # OLD METHOD
                    cur.copy_expert(copy_sql_stmt, csv_buffer) # NEW METHOD using CSV format
                    total_rows_loaded += lines_in_current_batch
                    logger.info(f"DB Load (COPY CSV): Sent final batch of {lines_in_current_batch} lines. Total loaded: {total_rows_loaded}")
                    conn.commit()
                except psycopg2.Error as db_final_batch_err:
                    conn.rollback()
                    logger.error(f"Database error during final COPY batch: {db_final_batch_err}. Final batch for table {safe_table_identifier.strings[0]} will be lost. First few lines of batch: {csv_buffer.getvalue()[:500]}", exc_info=True)
            
        csv_buffer.close()
        logger.info(f"Finished processing {input_json_path}.")
        logger.info(f"  Total items processed from JSON 'value' field: {processed_count}")
        logger.info(f"  Rows loaded to DB: {total_rows_loaded}, Skipped due to formatting errors: {skipped_count}")
        return True

    except FileNotFoundError:
        logger.error(f"Input file not found: {input_json_path}")
        return False
    except psycopg2.Error as db_err:
        logger.error(f"Database error: {db_err}", exc_info=True)
        if conn: conn.rollback()
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        if conn: conn.rollback()
        return False
    finally:
        if cur: cur.close()
        if conn: conn.close()
        logger.info("DB connection closed if it was opened.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Eurostat JSON and load directly to PostgreSQL using COPY.")
    parser.add_argument("-i", "--input-file", required=True, help="Path to input JSON-stat file.")
    parser.add_argument("-t", "--table-name", required=True, help="Target PostgreSQL table name.")
    parser.add_argument("-d", "--dataset-id", required=True, help="Eurostat dataset ID for source_dataset_id column.")
    parser.add_argument("--batch-size", type=int, default=10000, help="Number of lines per COPY batch.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    
    args = parser.parse_args()

    # Reconfigure logger level based on CLI argument
    logging.getLogger().setLevel(args.log_level.upper())

    success = parse_json_and_load_to_db(
        args.input_file, 
        args.table_name, 
        args.dataset_id, 
        args.batch_size
    )

    if success:
        logger.info(f"Successfully processed {args.input_file} and loaded to {args.table_name}.")
    else:
        logger.error(f"Failed to process {args.input_file}.")
        exit(1) 