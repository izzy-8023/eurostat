import json
import math
import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ijson

def pre_calculate_reverse_indices(dimension_details_meta):
    """Pre-calculates _reverse_index for all categories to avoid modifying during streaming."""
    if dimension_details_meta:
        for dim_id, dim_content in dimension_details_meta.items():
            category_section = dim_content.get('category', {})
            if isinstance(category_section, dict): # Ensure category_section is a dict
                index_map = category_section.get('index', {})
                if isinstance(index_map, dict): # Ensure index_map is a dict
                    category_section['_reverse_index'] = {v: k for k, v in index_map.items()}

def get_category_info_from_linear_index_core_logic(
        dimension_ids_meta, dimension_sizes_meta, dimension_details_meta,
        linear_index_str, value_content,
        current_status_code, # Specific status code for this linear_index
        status_labels_meta
    ):
    """
    Core logic to interpret a linear index, using pre-loaded metadata and streamed value/status.
    Reverse indices are expected to be pre-calculated in dimension_details_meta.
    """
    try:
        linear_index = int(linear_index_str)
    except ValueError:
        print(f"Warning: Skipping non-numeric index '{linear_index_str}'.")
        return None

    if not (dimension_ids_meta and dimension_sizes_meta and dimension_details_meta):
        print("Error: Essential dimension information is missing (passed to core logic function).")
        return None

    max_possible_index = 1
    for size in dimension_sizes_meta:
        max_possible_index *= size
    if linear_index < 0 or linear_index >= max_possible_index: # Should be < max_possible_index
        print(f"Warning: Skipping out-of-bounds index {linear_index}. Max is {max_possible_index -1}.")
        return None

    row_dict = {'Linear_Index': linear_index_str}
    temp_index = linear_index

    for i in range(len(dimension_ids_meta) - 1, -1, -1):
        dim_id = dimension_ids_meta[i]
        dim_size = dimension_sizes_meta[i]
        category_numeric_index = temp_index % dim_size
        temp_index = math.floor(temp_index / dim_size)

        category_section = dimension_details_meta.get(dim_id, {}).get('category', {})
        # Uses pre-calculated '_reverse_index'
        label_map = category_section.get('label', {})
        
        category_key = "Unknown Key"
        if isinstance(category_section, dict) and '_reverse_index' in category_section:
             category_key = category_section['_reverse_index'].get(category_numeric_index, "Unknown Key")
        
        category_label = label_map.get(category_key, f"Unknown Label (Index: {category_numeric_index})")

        row_dict[f"{dim_id}_key"] = category_key
        row_dict[f"{dim_id}_label"] = category_label

    row_dict['Value'] = value_content if value_content is not None else None
    row_dict['Status_Code'] = current_status_code if current_status_code else ""
    
    status_label_text = ""
    if current_status_code and isinstance(status_labels_meta, dict):
        status_label_text = status_labels_meta.get(current_status_code, 'Unknown status code')
    row_dict['Status_Label'] = status_label_text

    return row_dict

# --- Main Parsing Function (Modified) ---
def parse_json_stat_file(input_json_path, output_parquet_path, batch_size=50000):
    """
    Loads a JSON-stat file, parses it (streaming 'value' field), 
    and writes the interpretation to a Parquet file in batches.
    WARNING: The 'status' field is still loaded entirely into memory.
    """
    print(f"Processing file: {input_json_path} using ijson streaming.")

    dimension_ids = None
    dimension_sizes = None
    dimension_details = None
    statuses_map = {}  # WARNING: This will hold all statuses in memory.
    status_labels = {}

    try:
        print("Phase 1: Loading metadata (excluding 'value' field initially)...")
        # Load all top-level items except 'value'. 'status' will be loaded fully here.
        with open(input_json_path, 'rb') as f: # ijson prefers binary mode
            full_data_except_value = {
                k: v for k, v in ijson.kvitems(f, '') if k != 'value'
            }
        
        dimension_ids = full_data_except_value.get('id')
        dimension_sizes = full_data_except_value.get('size')
        dimension_details = full_data_except_value.get('dimension')
        statuses_map = full_data_except_value.get('status', {}) # Loaded fully
        
        ext_data = full_data_except_value.get('extension', {})
        if isinstance(ext_data, dict):
            status_ext = ext_data.get('status', {})
            if isinstance(status_ext, dict):
                status_labels = status_ext.get('label', {})

        if not (dimension_ids and isinstance(dimension_ids, list) and
                dimension_sizes and isinstance(dimension_sizes, list) and
                dimension_details and isinstance(dimension_details, dict)):
            print(f"Error: Essential dimension information ('id', 'size', 'dimension') is missing or invalid in {input_json_path}.")
            return False
        
        # Pre-calculate reverse indices on the loaded dimension_details
        pre_calculate_reverse_indices(dimension_details)
        print("Metadata loaded and reverse indices pre-calculated.")

    except FileNotFoundError:
        print(f"Error: Input file not found: {input_json_path}")
        return False
    except Exception as e:
        print(f"Error during initial metadata load from {input_json_path}: {e}")
        # import traceback
        # traceback.print_exc()
        return False

    header = ['Linear_Index']
    for dim_id in dimension_ids: # Assumes dimension_ids is now populated
        header.append(f"{dim_id}_key")
        header.append(f"{dim_id}_label")
    header.extend(['Value', 'Status_Code', 'Status_Label'])

    processed_count = 0
    skipped_count = 0
    parquet_writer = None
    schema = None
    
    output_directory = os.path.dirname(output_parquet_path)
    if output_directory: # Ensure output directory exists
        os.makedirs(output_directory, exist_ok=True)

    print(f"Phase 2: Streaming 'value' items and writing to Parquet: {output_parquet_path}")
    batch_data = []

    try:
        with open(input_json_path, 'rb') as f_values: # Open again for streaming 'value'
            # Assuming 'value' is a top-level dictionary/map
            for linear_index_str, value_content in ijson.kvitems(f_values, 'value'):
                current_status_code = statuses_map.get(linear_index_str) # Uses the in-memory statuses_map

                interpreted_row = get_category_info_from_linear_index_core_logic(
                    dimension_ids, dimension_sizes, dimension_details,
                    linear_index_str, value_content,
                    current_status_code, status_labels
                )

                if interpreted_row:
                    # Ensure all header columns are present, filling with None if missing
                    ordered_row = {col: interpreted_row.get(col) for col in header}
                    batch_data.append(ordered_row)
                    processed_count += 1
                else:
                    skipped_count += 1

                if len(batch_data) >= batch_size:
                    if not batch_data: continue
                    try:
                        df_batch = pd.DataFrame(batch_data) # Let pandas infer schema initially or use header
                        # Ensure consistent column order matching the schema for subsequent batches
                        df_batch = df_batch[header]
                        table_batch = pa.Table.from_pandas(df_batch, schema=schema, preserve_index=False)
                        
                        if parquet_writer is None:
                            schema = table_batch.schema # Get schema from the first batch
                            parquet_writer = pq.ParquetWriter(output_parquet_path, schema, compression='snappy')
                        
                        parquet_writer.write_table(table_batch)
                        # print(f"Wrote batch of {len(batch_data)} records. Total processed: {processed_count}")
                        batch_data = [] # Reset batch
                    except Exception as e:
                        print(f"Error writing batch to Parquet for {input_json_path}: {e}")
                        if parquet_writer: parquet_writer.close()
                        return False
            
            # After loop, write any remaining data in batch_data
            if batch_data:
                try:
                    df_batch = pd.DataFrame(batch_data)
                    df_batch = df_batch[header] # Ensure column order
                    table_batch = pa.Table.from_pandas(df_batch, schema=schema, preserve_index=False)
                    if parquet_writer is None and schema is None and not table_batch.schema.names: # Edge case: only one batch and it's empty of columns
                         # If schema wasn't set and table_batch is also schemaless, create schema from header
                         if not schema and header:
                             fields = [(col_name, pa.string()) for col_name in header] # Default to string, adjust if needed
                             schema = pa.schema(fields)

                    if parquet_writer is None: # If schema was just created or first batch
                        if schema is None: schema = table_batch.schema # Fallback if not created above
                        parquet_writer = pq.ParquetWriter(output_parquet_path, schema, compression='snappy')
                    
                    parquet_writer.write_table(table_batch)
                    # print(f"Wrote final batch of {len(batch_data)} records. Total processed: {processed_count}")
                except Exception as e:
                    print(f"Error writing final batch to Parquet for {input_json_path}: {e}")
                    if parquet_writer: parquet_writer.close()
                    return False

    except Exception as e:
        print(f"Error streaming 'value' items from {input_json_path}: {e}")
        # import traceback
        # traceback.print_exc()
        if parquet_writer: parquet_writer.close()
        return False
    finally:
        if parquet_writer:
            parquet_writer.close()
            print(f"Finished writing Parquet file for {input_json_path}.")
            print(f"  Total Processed: {processed_count}, Skipped: {skipped_count}")
        
        # Handle cases where no data was written (e.g., empty input 'value' field)
        if processed_count == 0:
            if not os.path.exists(output_parquet_path) or os.path.getsize(output_parquet_path) == 0:
                print(f"No data values processed or all skipped. Writing empty Parquet file with schema: {output_parquet_path}")
                # Create an empty DataFrame with the determined header to define the schema
                empty_df = pd.DataFrame(columns=header) 
                final_schema = schema
                if final_schema is None: # If schema was never determined (e.g. no batches)
                    fields = [(col_name, pa.string()) for col_name in header] # Default to string type
                    final_schema = pa.schema(fields)
                
                empty_table = pa.Table.from_pandas(empty_df, schema=final_schema, preserve_index=False)
                pq.write_table(empty_table, output_parquet_path, compression='snappy')
        elif not parquet_writer: # Should not happen if processed_count > 0
             print(f"Warning: Processed {processed_count} records but Parquet writer was not initialized. Output might be missing.")


    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse a single Eurostat JSON-stat file to a Parquet file using ijson for streaming.")
    parser.add_argument(
        "-i", "--input-file",
        required=True,
        help="Path to the input JSON-stat file."
    )
    parser.add_argument(
        "-o", "--output-file",
        required=True,
        help="Path for the output Parquet file (e.g., data.parquet)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000, # Consider making this smaller if memory per row is high
        help="Number of records to process in each batch before writing to Parquet."
    )
    args = parser.parse_args()

    if not args.output_file.lower().endswith('.parquet'):
        print("Warning: Output filename does not end with .parquet. Ensure downstream tools expect this format.")

    success = parse_json_stat_file(args.input_file, args.output_file, args.batch_size)
    if success:
        print(f"Successfully parsed {args.input_file} to {args.output_file}")
    else:
        print(f"Failed to parse {args.input_file}")
        exit(1) # Exit with error for Airflow
