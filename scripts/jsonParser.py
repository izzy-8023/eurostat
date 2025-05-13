import json
import math
import os
import argparse
import pandas as pd

def get_category_info_from_linear_index(data, linear_index_str):
    """
    Interprets a linear index from a JSON-stat file to find its
    corresponding dimension categories, value, and status.
    Returns a dictionary containing the interpreted data or None on error.
    """
    try:
        linear_index = int(linear_index_str)
    except ValueError:
        print(f"Warning: Skipping non-numeric index '{linear_index_str}'.")
        return None

    dimension_ids = data.get('id')
    dimension_sizes = data.get('size')
    dimension_details = data.get('dimension')
    values = data.get('value', {})
    statuses = data.get('status', {})
    # Handle potential absence of 'extension' or nested status label info
    status_extension = data.get('extension', {}).get('status', {})
    status_labels = status_extension.get('label', {}) if isinstance(status_extension, dict) else {}


    if not (dimension_ids and dimension_sizes and dimension_details):
        print("Error: Essential dimension information is missing from the JSON.")
        return None

    max_possible_index = 1
    for size in dimension_sizes:
        max_possible_index *= size
    if linear_index < 0 or linear_index >= max_possible_index:
        print(f"Warning: Skipping out-of-bounds index {linear_index}. Max is {max_possible_index - 1}.")
        return None

    row_dict = {'Linear_Index': linear_index_str}
    temp_index = linear_index

    for i in range(len(dimension_ids) - 1, -1, -1):
        dim_id = dimension_ids[i]
        dim_size = dimension_sizes[i]
        category_numeric_index = temp_index % dim_size
        temp_index = math.floor(temp_index / dim_size)

        category_section = dimension_details.get(dim_id, {}).get('category', {})
        index_map = category_section.get('index', {})
        label_map = category_section.get('label', {})

        if not hasattr(category_section, '_reverse_index'):
            category_section['_reverse_index'] = {v: k for k, v in index_map.items()}
        
        category_key = category_section['_reverse_index'].get(category_numeric_index, "Unknown Key")
        category_label = label_map.get(category_key, f"Unknown Label (Index: {category_numeric_index})")

        row_dict[f"{dim_id}_key"] = category_key
        row_dict[f"{dim_id}_label"] = category_label

    value = values.get(linear_index_str)
    row_dict['Value'] = value if value is not None else None

    status_code = statuses.get(linear_index_str)
    row_dict['Status_Code'] = status_code if status_code else ""
    status_label_text = ""
    if status_code:
        status_label_text = status_labels.get(status_code, 'Unknown status code')
    row_dict['Status_Label'] = status_label_text

    return row_dict

# --- Reusable Parsing Function ---
def parse_json_stat_file(input_json_path, output_parquet_path):
    """
    Loads a JSON-stat file, parses it, and writes the interpretation
    to an efficient Parquet file.
    """
    print(f"Processing file: {input_json_path}")
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            eurostat_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file not found: {input_json_path}")
        return False
    except json.JSONDecodeError:
        print(f"Error: Input file is not valid JSON: {input_json_path}")
        return False
    except Exception as e:
        print(f"Error loading file {input_json_path}: {e}")
        return False

    if 'id' not in eurostat_data or not isinstance(eurostat_data.get('id'), list):
        print(f"Error: 'id' field missing or not a list in {input_json_path}, cannot determine dimensions.")
        return False

    # Determine header dynamically (needed for DataFrame columns)
    dimension_ids = eurostat_data['id']
    header = ['Linear_Index']
    for dim_id in dimension_ids:
        header.append(f"{dim_id}_key")
        header.append(f"{dim_id}_label")
    header.extend(['Value', 'Status_Code', 'Status_Label'])

    available_indices = list(eurostat_data.get('value', {}).keys())
    if not available_indices:
         print(f"Warning: No 'value' data found in {input_json_path}. Output Parquet file might be empty or schema-only.")

    processed_count = 0
    skipped_count = 0
    all_rows_data = [] # List to hold all row dictionaries

    print(f"Found {len(available_indices)} data points. Preparing data for Parquet: {output_parquet_path}")

    # 1. Collect all interpreted rows
    for index_str in available_indices:
        interpreted_row = get_category_info_from_linear_index(eurostat_data, index_str)
        if interpreted_row:
            # Ensure all keys exist even if value is None/empty, matching header order
            ordered_row = {col: interpreted_row.get(col) for col in header}
            all_rows_data.append(ordered_row)
            processed_count += 1
        else:
            skipped_count += 1

    if not all_rows_data:
        print(f"No processable data found in {input_json_path}. Skipping Parquet file creation.")
        return True # Or False depending on desired behavior for empty files

    # 2. Convert to Pandas DataFrame
    try:
        df = pd.DataFrame(all_rows_data, columns=header) # Specify column order

        # Optional: Infer better dtypes for memory/storage efficiency
        # df = df.convert_dtypes()
        # Be cautious with convert_dtypes, check results if using

    except Exception as e:
        print(f"Error creating Pandas DataFrame for {input_json_path}: {e}")
        return False

    # 3. Write DataFrame to Parquet
    try:
        output_directory = os.path.dirname(output_parquet_path)
        if output_directory:
            os.makedirs(output_directory, exist_ok=True)

        # Use pyarrow engine by default, compression='snappy' is common and fast
        df.to_parquet(output_parquet_path, engine='pyarrow', compression='snappy', index=False)

        print(f"Finished writing Parquet file for {input_json_path}.")
        print(f"  Processed: {processed_count}, Skipped: {skipped_count}")
        return True
    except ImportError:
         print("Error: 'pyarrow' library not found. Please install it: pip install pyarrow")
         return False
    except Exception as e:
        print(f"Error writing Parquet file {output_parquet_path}: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse a single Eurostat JSON-stat file to a Parquet file.")
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
    args = parser.parse_args()

    if not args.output_file.lower().endswith('.parquet'):
        print("Warning: Output filename does not end with .parquet. Ensure downstream tools expect this format.")

    parse_json_stat_file(args.input_file, args.output_file)
