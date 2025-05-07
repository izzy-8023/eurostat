import json
import math
import csv
import os # Added for directory handling

def get_category_info_from_linear_index(data, linear_index_str):
    """
    Interprets a linear index from a JSON-stat file to find its
    corresponding dimension categories, value, and status.
    Returns a dictionary ready for CSV writing or None on error.
    """
    try:
        linear_index = int(linear_index_str)
    except ValueError:
        print(f"Warning: Skipping non-numeric index '{linear_index_str}'.")
        return None # Skip non-numeric indices

    dimension_ids = data.get('id')
    dimension_sizes = data.get('size')
    dimension_details = data.get('dimension')
    values = data.get('value', {})
    statuses = data.get('status', {})
    status_labels = data.get('extension', {}).get('status', {}).get('label', {})

    if not (dimension_ids and dimension_sizes and dimension_details):
        print("Error: Essential dimension information is missing from the JSON.")
        return None # Cannot proceed

    # --- Sanity check for index bounds ---
    max_possible_index = 1
    for size in dimension_sizes:
        max_possible_index *= size
    if linear_index < 0 or linear_index >= max_possible_index:
        print(f"Warning: Skipping out-of-bounds index {linear_index}. Max is {max_possible_index - 1}.")
        return None # Skip out-of-bounds indices
    # ---

    # Prepare the row dictionary for CSV
    csv_row = {'Linear_Index': linear_index_str}

    temp_index = linear_index

    # Iterate through dimensions in reverse order
    for i in range(len(dimension_ids) - 1, -1, -1):
        dim_id = dimension_ids[i]
        dim_size = dimension_sizes[i]

        category_numeric_index = temp_index % dim_size
        temp_index = math.floor(temp_index / dim_size)

        # Precompute the reverse index map if it doesn't exist
        if not hasattr(dimension_details[dim_id]['category'], '_reverse_index'):
             dimension_details[dim_id]['category']['_reverse_index'] = {v: k for k, v in dimension_details[dim_id]['category']['index'].items()}
        
        category_key = dimension_details[dim_id]['category']['_reverse_index'].get(category_numeric_index, "Unknown Key")
        category_label = dimension_details[dim_id]['category']['label'].get(category_key, f"Unknown Label (Index: {category_numeric_index})")

        # Add dimension key and label to the row
        csv_row[f"{dim_id}_key"] = category_key
        csv_row[f"{dim_id}_label"] = category_label

    # Get the value
    value = values.get(linear_index_str)
    csv_row['Value'] = value if value is not None else "" # Use empty string for missing value in CSV

    # Get the status
    status_code = statuses.get(linear_index_str)
    csv_row['Status_Code'] = status_code if status_code else ""
    status_label_text = ""
    if status_code:
        status_label_text = status_labels.get(status_code, 'Unknown status code')
    csv_row['Status_Label'] = status_label_text

    return csv_row


if __name__ == "__main__":
    input_file_path = "Data_Directory/HLTH_EHIS_HC7.json"
    output_directory = "Output_Directory" # Name for the output directory
    output_csv_path = os.path.join(output_directory, "HLTH_EHIS_HC7_interpreted.csv")

    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    try:
        with open(input_file_path, 'r') as f:
            eurostat_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The input file {input_file_path} was not found.")
        exit()
    except json.JSONDecodeError:
        print(f"Error: The input file {input_file_path} is not valid JSON.")
        exit()

    # --- Prepare CSV Header ---
    if 'id' not in eurostat_data:
        print("Error: 'id' field missing in JSON, cannot determine dimensions for CSV header.")
        exit()
        
    dimension_ids = eurostat_data['id']
    header = ['Linear_Index']
    for dim_id in dimension_ids:
        header.append(f"{dim_id}_key")
        header.append(f"{dim_id}_label")
    header.extend(['Value', 'Status_Code', 'Status_Label'])

    # Get all available linear indices from the 'value' dictionary
    available_indices = list(eurostat_data.get('value', {}).keys())
    processed_count = 0
    skipped_count = 0

    print(f"Found {len(available_indices)} data points in the JSON.")
    print(f"Writing interpretations to: {output_csv_path}")

    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()

            # --- Loop through all indices ---
            for index_str in available_indices:
                interpreted_row = get_category_info_from_linear_index(eurostat_data, index_str)

                if interpreted_row:
                    # Reorder the dictionary to match the header order for writing
                    # This ensures columns are written correctly even if interpretation order changes
                    ordered_row = {col: interpreted_row.get(col, '') for col in header}
                    writer.writerow(ordered_row)
                    processed_count += 1
                else:
                    skipped_count += 1 # Count if interpretation function returned None (error/skip)
            
            print("\nFinished writing CSV.")
            print(f"Successfully processed and wrote {processed_count} data points.")
            if skipped_count > 0:
                 print(f"Skipped {skipped_count} invalid or out-of-bounds indices.")

    except IOError as e:
        print(f"Error writing to CSV file {output_csv_path}: {e}")
    except Exception as e:
         print(f"An unexpected error occurred during CSV writing: {e}")
