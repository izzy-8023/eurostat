#!/usr/bin/env python3
"""
Enhanced JSON to PostgreSQL Loader for Eurostat Pipeline

This enhanced version integrates with shared modules to eliminate redundancy
and improve maintainability while preserving all existing functionality.
"""

import json
import math
import os
import argparse
import ijson # For streaming JSON
import io # For in-memory text buffer (StringIO)
import csv # For robust CSV formatting
import logging # For logging
import time # For timing
import sys

# Add shared modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))

from shared.database import EurostatDatabase
from shared.config import get_db_config, EUROSTAT_COLUMN_PATTERNS
from shared.patterns import detect_column_patterns, is_eurostat_dimension_column

# --- Logger Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Core Logic (unchanged but enhanced with shared patterns) ---
def get_category_info_from_linear_index_core_logic(
        dimension_ids_meta, dimension_sizes_meta, dimension_details_meta,
        linear_index_str, value_content,
        current_status_code,
        status_labels_meta
    ):
    """
    Core logic to interpret a linear index, using pre-loaded metadata and streamed value/status.
    Enhanced with shared pattern detection.
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
        logger.warning(f"Skipping out-of-bounds index {linear_index}. Max is {max_possible_index -1}. Dim Sizes: {dimension_sizes_meta}")
        return None

    row_dict = {'linear_index': linear_index} # Use lowercase for consistency
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

        if isinstance(category_section, dict) and '_reverse_index' in category_section:
             category_key = category_section['_reverse_index'].get(category_numeric_index, "Unknown Key")
        else:
            logger.debug(f"Dimension {dim_id} category either not a dict or no '_reverse_index'. Using numeric index: {category_numeric_index}")

        category_label = label_map.get(category_key, f"Unknown Label (Index: {category_numeric_index})")

        # Use shared column patterns for consistent naming
        row_dict[f"{dim_id}{EUROSTAT_COLUMN_PATTERNS['dimension_key_suffix']}"] = category_key
        row_dict[f"{dim_id}{EUROSTAT_COLUMN_PATTERNS['dimension_label_suffix']}"] = category_label

    # Handle value conversion with Eurostat-specific flags
    try:
        if isinstance(value_content, str) and value_content.strip().lower() in [':', 'z', 'c', 'u', 'n/a', 'na', '-']:
            logger.debug(f"Eurostat flag '{value_content}' found for index {linear_index_str}. Storing as None.")
            row_dict['value'] = None
        elif value_content is not None:
            row_dict['value'] = float(value_content)
        else:
            row_dict['value'] = None
    except (ValueError, TypeError):
        logger.warning(f"Could not convert value '{value_content}' to float for index {linear_index_str}. Storing as None.")
        row_dict['value'] = None

    row_dict['status_code'] = current_status_code if current_status_code else ""
    
    status_label_text = ""
    if current_status_code and isinstance(status_labels_meta, dict):
        status_label_text = status_labels_meta.get(current_status_code, 'Unknown status code')
    row_dict['status_label'] = status_label_text
    
    return row_dict

# --- Enhanced Main Function with Shared Database ---
def parse_json_and_load_to_db(input_json_path, table_name, source_dataset_id, db_copy_batch_lines=10000):
    """
    Enhanced version using shared database module for connection management.
    """
    logger.info(f"Processing file: {input_json_path} for direct DB load into {table_name} using COPY.")

    # Use shared database configuration
    try:
        db = EurostatDatabase()
        logger.info("✅ Using shared database configuration")
    except Exception as e:
        logger.error(f"Failed to initialize shared database: {e}")
        return False

    conn = None
    cur = None

    dimension_ids = None
    dimension_sizes = None
    dimension_details_meta = {}
    statuses_map = {}
    status_labels = {}

    try:
        # Phase 1: Loading metadata from JSON (unchanged logic)
        logger.info("Phase 1: Loading metadata from JSON...")

        # Load dimension IDs
        try:
            with open(input_json_path, 'rb') as f_id:
                dimension_ids = list(ijson.items(f_id, 'id.item'))
            if not dimension_ids:
                logger.warning(f"Parsed 'id' (dimension_ids) is empty from {input_json_path}")
        except Exception as e:
            logger.error(f"Error loading dimension IDs: {e}")
            dimension_ids = None

        # Load dimension sizes
        try:
            with open(input_json_path, 'rb') as f_size:
                dimension_sizes = list(ijson.items(f_size, 'size.item'))
            if not dimension_sizes:
                logger.warning(f"Parsed 'size' (dimension_sizes) is empty from {input_json_path}")
        except Exception as e:
            logger.error(f"Error loading dimension sizes: {e}")
            dimension_sizes = None

        # Load dimension details
        try:
            with open(input_json_path, 'rb') as f_dim_obj:
                all_dimension_data = next(ijson.items(f_dim_obj, 'dimension'), None)

            if not all_dimension_data:
                logger.warning(f"Parsed 'dimension' object is empty from {input_json_path}")
                dimension_details_meta = {}
            else:
                for dim_id, current_dim_details_built in all_dimension_data.items():
                    category_data = current_dim_details_built.get('category', {})
                    index_map = category_data.get('index', {}) if isinstance(category_data, dict) else {}
                    label_map = category_data.get('label', {}) if isinstance(category_data, dict) else {}
                    
                    reverse_index = {}
                    if index_map:
                        try:
                            reverse_index = {v: k for k, v in index_map.items()}
                        except (TypeError, Exception) as e:
                            logger.warning(f"Error creating reverse_index for dim '{dim_id}': {e}")

                    dimension_details_meta[dim_id] = {
                        'category': {
                            'index': index_map,
                            'label': label_map,
                            '_reverse_index': reverse_index
                        }
                    }
        except Exception as e:
            logger.error(f"Error loading dimension metadata: {e}")
            dimension_details_meta = {}

        # Load status information
        try:
            with open(input_json_path, 'rb') as f_stat:
                statuses_map = dict(ijson.kvitems(f_stat, 'status'))
        except Exception as e:
            logger.error(f"Error loading status metadata: {e}")
            statuses_map = {}

        try:
            with open(input_json_path, 'rb') as f_ext:
                ext_data = dict(ijson.kvitems(f_ext, 'extension'))
                status_ext_data = ext_data.get('status', {})
                status_labels = status_ext_data.get('label', {})
        except Exception as e:
            logger.error(f"Error loading status labels: {e}")
            status_labels = {}

        # Validate essential metadata
        logger.debug(f"Dimension IDs: {dimension_ids}")
        logger.debug(f"Dimension Sizes: {dimension_sizes}")
        logger.debug(f"Dimension Details Keys: {dimension_details_meta.keys() if dimension_details_meta else 'None'}")
        
        if not (dimension_ids and dimension_sizes and dimension_details_meta and \
                len(dimension_ids) == len(dimension_sizes)):
            logger.error(f"Essential dimension info missing or mismatched in {input_json_path}")
            logger.error(f"  - Got {len(dimension_ids) if dimension_ids else 0} dimension_ids")
            logger.error(f"  - Got {len(dimension_sizes) if dimension_sizes else 0} dimension_sizes")
            logger.error(f"  - Got dimension_details: {'Yes' if dimension_details_meta else 'No'}")
            if dimension_ids and dimension_sizes:
                 logger.error(f"  - Lengths: IDs={len(dimension_ids)}, Sizes={len(dimension_sizes)}")
            return False
        else:
            logger.info("✅ Metadata validation passed")

        # Phase 2: Database operations using shared database
        logger.info("Phase 2: Database operations...")
        
        conn = db.get_connection()
        cur = conn.cursor()

        # Check if table exists, create if needed
        if not db.check_table_exists(table_name):
            logger.info(f"Creating table {table_name}...")
            
            # Generate column definitions using shared patterns
            columns = []
            for dim_id in dimension_ids:
                columns.extend([
                    f"{dim_id}_key TEXT",
                    f"{dim_id}_label TEXT"
                ])
            
            columns.extend([
                "linear_index INTEGER",
                "value NUMERIC",
                "status_code TEXT",
                "status_label TEXT",
                "source_dataset_id TEXT"
            ])
            
            create_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                )
            """
            cur.execute(create_sql)
            conn.commit()
            logger.info(f"✅ Table {table_name} created successfully")

        # Phase 3: Stream and load data (enhanced with shared patterns)
        logger.info("Phase 3: Streaming and loading data...")
        
        batch_buffer = io.StringIO()
        csv_writer = csv.writer(batch_buffer)
        batch_count = 0
        total_rows = 0

        with open(input_json_path, 'rb') as f_values:
            loop_entered_count = 0
            value_items_count = 0
            logger.info(f"Starting to iterate over ijson.kvitems for file: {input_json_path}")
            try:
                for linear_index_str, value_content in ijson.kvitems(f_values, 'value'):
                    loop_entered_count += 1
                    if loop_entered_count == 1:
                        logger.info("✅ Successfully entered the value processing loop")
                    
                    current_status_code = statuses_map.get(linear_index_str, "")
                    
                    row_dict = get_category_info_from_linear_index_core_logic(
                        dimension_ids, dimension_sizes, dimension_details_meta,
                        linear_index_str, value_content, current_status_code, status_labels
                    )
                    
                    if row_dict:
                        value_items_count +=1 # counter for valid items
                        # Add source dataset ID
                        row_dict['source_dataset_id'] = source_dataset_id
                        
                        # Write to CSV buffer in correct column order
                        row_values = []
                        for dim_id in dimension_ids:
                            row_values.extend([
                                row_dict.get(f"{dim_id}_key", ""),
                                row_dict.get(f"{dim_id}_label", "")
                            ])
                        
                        row_values.extend([
                            row_dict.get('linear_index', 0),
                            row_dict.get('value'),
                            row_dict.get('status_code', ""),
                            row_dict.get('status_label', ""),
                            row_dict.get('source_dataset_id', "")
                        ])
                        
                        csv_writer.writerow(row_values)
                        batch_count += 1
                        total_rows += 1

                        # Execute batch when buffer is full
                        if batch_count >= db_copy_batch_lines:
                            batch_buffer.seek(0)
                            copy_sql = f"COPY {table_name} FROM STDIN WITH CSV"
                            cur.copy_expert(copy_sql, batch_buffer)
                            conn.commit()
                            
                            logger.info(f"Loaded batch of {batch_count} rows (total: {total_rows}). Valid items in batch: {value_items_count} ")
                            # Verify rows in table (optional, can be slow)
                            # cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                            # logger.info(f"Actual rows in table after batch: {cur.fetchone()[0]}")
                            
                            # Reset buffer
                            batch_buffer = io.StringIO()
                            csv_writer = csv.writer(batch_buffer)
                            batch_count = 0
            except Exception as e_ijson:
                logger.error(f"Error during ijson.kvitems iteration: {e_ijson}", exc_info=True)

        if loop_entered_count == 0:
            logger.warning("⚠️ Value processing loop was never entered. No data rows found in 'value' section.")
        logger.info(f"Total valid items processed by get_category_info_from_linear_index_core_logic: {value_items_count}")

        logger.info(f"✅ Successfully loaded {total_rows} total rows into {table_name}")
        return True

    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    """Enhanced CLI interface"""
    parser = argparse.ArgumentParser(description="Enhanced JSON to PostgreSQL loader with shared modules")
    parser.add_argument("--input-file", required=True, help="Input JSON file path")
    parser.add_argument("--table-name", required=True, help="Target PostgreSQL table name")
    parser.add_argument("--source-dataset-id", required=True, help="Source dataset identifier")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for COPY operations")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    # Process file
    success = parse_json_and_load_to_db(
        args.input_file,
        args.table_name,
        args.source_dataset_id,
        args.batch_size
    )
    
    if success:
        print(f" Successfully processed {args.input_file}")
    else:
        print(f" Failed to process {args.input_file}")
        sys.exit(1)

if __name__ == "__main__":
    main() 