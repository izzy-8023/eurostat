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

        dim_data = dimension_details_meta.get(dim_id, {})
        category_section = dim_data.get('category', {})
        label_map = category_section.get('label', {})
        category_key = "Unknown Key"

        # _reverse_index is now expected to be pre-calculated in dimension_details_meta
        # No on-demand creation here.
        if isinstance(category_section, dict) and '_reverse_index' in category_section:
             category_key = category_section['_reverse_index'].get(category_numeric_index, "Unknown Key")
        else:
            # This case should be less common if metadata parsing is robust and all dimensions have categories.
            # It could happen if a dimension has no 'index' in its category, so '_reverse_index' wasn't created.
            logger.debug(f"Dimension {dim_id} category either not a dict or no '_reverse_index'. Using numeric index: {category_numeric_index}")
            # Fallback or decide how to handle - for now, this will lead to "Unknown Key" if not in label_map directly by numeric index
            # which is unlikely for JSON-stat. The original on-demand logic also had this path implicitly.

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
    dimension_sizes = None # Added for consistency, was missing from previous declaration block
    dimension_details_meta = {} # Initialize as empty dict
    statuses_map = {} # Default to empty dict
    status_labels = {} # Default to empty dict

    try:
        # Phase 1: Loading metadata from JSON (with streaming for 'dimension' object)
        logger.info("Phase 1: Loading metadata from JSON (streaming 'dimension' object)...")

        # Load 'id' (dimension_ids)
        try:
            with open(input_json_path, 'rb') as f_id:
                # Assuming 'id' is a top-level array of strings
                dimension_ids = list(ijson.items(f_id, 'id.item'))
            if not dimension_ids:
                logger.warning(f"Parsed 'id' (dimension_ids) is empty or None from {input_json_path}")
        except Exception as e:
            logger.error(f"Error loading 'id' (dimension_ids) metadata: {e}", exc_info=True)
            dimension_ids = None # Ensure it's None on error

        # Load 'size' (dimension_sizes)
        try:
            with open(input_json_path, 'rb') as f_size:
                # Assuming 'size' is a top-level array of numbers
                dimension_sizes = list(ijson.items(f_size, 'size.item'))
            if not dimension_sizes:
                 logger.warning(f"Parsed 'size' (dimension_sizes) is empty or None from {input_json_path}")
        except Exception as e:
            logger.error(f"Error loading 'size' (dimension_sizes) metadata: {e}", exc_info=True)
            dimension_sizes = None # Ensure it's None on error

        # Stream-load 'dimension' object and build dimension_details_meta
        try:
            all_dimension_data = None
            with open(input_json_path, 'rb') as f_dim_obj: # f_dim_obj is the main file stream
                # Parse the entire "dimension": {...} object into a Python dictionary
                all_dimension_data = next(ijson.items(f_dim_obj, 'dimension'), None)

            if not all_dimension_data:
                logger.warning(f"Parsed 'dimension' object is empty or None from {input_json_path}. No dimension details will be processed.")
                dimension_details_meta = {} # Ensure it's empty
            else:
                for dim_id, current_dim_details_built in all_dimension_data.items():
                    # current_dim_details_built is now a Python dict for the dimension's content
                    # No further ijson parsing or ObjectBuilder needed here for this part.
                    
                    category_data = current_dim_details_built.get('category', {})
                    index_map = {}
                    label_map = {}

                    if isinstance(category_data, dict):
                        index_map = category_data.get('index', {})
                        label_map = category_data.get('label', {})
                    
                    # Ensure index_map and label_map are dicts
                    index_map = index_map if isinstance(index_map, dict) else {}
                    label_map = label_map if isinstance(label_map, dict) else {}
                        
                    reverse_index = {}
                    if index_map: # Only if 'index' map exists and is not empty
                        try:
                            reverse_index = {v: k for k, v in index_map.items()}
                        except TypeError as te: # Handle non-integer values in index_map if they occur
                            logger.warning(f"TypeError creating reverse_index for dim '{dim_id}'. Index map values might not be hashable: {index_map}. Error: {te}")
                            # reverse_index remains empty
                        except Exception as e_rev:
                            logger.warning(f"Generic error creating reverse_index for dim '{dim_id}'. Error: {e_rev}")
                            # reverse_index remains empty

                    dimension_details_meta[dim_id] = {
                        'category': {
                            'index': index_map,
                            'label': label_map,
                            '_reverse_index': reverse_index
                        }
                    }
            if not dimension_details_meta: # Check if, after processing, it's still empty
                logger.warning(f"After processing, 'dimension' (dimension_details_meta) is effectively empty from {input_json_path}")

        except Exception as e: # Catch errors from opening file or parsing the 'dimension' object
            logger.error(f"Error loading the entire 'dimension' metadata object: {e}", exc_info=True)
            dimension_details_meta = {} # Ensure it's empty on error
        
        # Load 'status' (statuses_map)
        try:
            with open(input_json_path, 'rb') as f_stat:
                # Assuming 'status' is a top-level object (map from linear index to status code)
                statuses_map = dict(ijson.kvitems(f_stat, 'status'))
        except Exception as e:
            logger.error(f"Error loading 'status' (statuses_map) metadata: {e}. Defaulting to empty status map.", exc_info=True)
            statuses_map = {} # Default to empty

        # Load 'extension.status.label' (status_labels)
        try:
            with open(input_json_path, 'rb') as f_ext:
                # Navigate to extension -> status -> label
                # This is a bit more complex with ijson.items or kvitems for deep paths.
                # A simpler way is to load 'extension' then navigate, if 'extension' isn't huge.
                ext_data = dict(ijson.kvitems(f_ext, 'extension')) # Load 'extension' object
                status_ext_data = ext_data.get('status', {})
                status_labels = status_ext_data.get('label', {})
        except Exception as e:
            logger.error(f"Error loading 'extension' or 'status_labels' metadata: {e}. Defaulting to empty status labels.", exc_info=True)
            status_labels = {} # Default to empty

        if not (dimension_ids and isinstance(dimension_ids, list) and
                dimension_sizes and isinstance(dimension_sizes, list) and len(dimension_ids) == len(dimension_sizes) and
                dimension_details_meta and isinstance(dimension_details_meta, dict)):
            logger.error(f"Essential dimension info (id, size, dimension details) missing, mismatched, or not parsed correctly in {input_json_path}. " +
                         f"IDs loaded: {dimension_ids is not None and isinstance(dimension_ids, list)}, " +
                         f"Sizes loaded: {dimension_sizes is not None and isinstance(dimension_sizes, list)}, " +
                         f"Details loaded: {dimension_details_meta is not None and isinstance(dimension_details_meta, dict)}. " +
                         f"ID count: {len(dimension_ids or [])}, Size count: {len(dimension_sizes or [])}.")
            if cur: cur.close()
            if conn: conn.close()
            return False
        
        statuses_map = statuses_map if isinstance(statuses_map, dict) else {}
        status_labels = status_labels if isinstance(status_labels, dict) else {}

        # pre_calculate_reverse_indices(dimension_details_meta) # This function is removed.
        logger.info("Metadata loaded (dimension details streamed) and reverse indices pre-calculated.")

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
                    dimension_ids, dimension_sizes, dimension_details_meta,
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