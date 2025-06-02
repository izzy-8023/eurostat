import csv

descriptive_groups = {}
# Define fallback group names for clarity
# NO_PRIMARY_BY_SEPARATOR_GROUP = "datasets_with_label_missing_primary_by_separator" # Will be replaced by using full label
# EMPTY_PRIMARY_GROUP_KEY = "datasets_with_empty_primary_group_key" # Will be replaced by using full label
DATASET_WITH_EMPTY_LABEL = "dataset_with_empty_label" # For rows where the label itself is empty
UNKNOWN_GROUPING_ISSUE = "unknown_grouping_issue" # Safeguard

try:
    with open("health_datasets.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)  
        except StopIteration:
            print("Warning: CSV file 'health_datasets.csv' is empty or contains only a header.")
            exit()

        for i, row in enumerate(reader, 1):
            if not row or len(row) < 2:
                print(f"Warning: Skipping malformed or empty row at line {i+1}: {row}")
                continue

            dataset_id = row[0].strip()
            label_str = row[1].strip()

            if not dataset_id:
                print(f"Warning: Skipping row at line {i+1} with empty dataset ID. Label: '{label_str}'")
                continue

            final_group_name = None
            label_parts = label_str.split(" by ", 1)
            
            primary_part_candidate = None
            if len(label_parts) == 2: # " by " was found
                primary_part_candidate = label_parts[0].strip()

            if primary_part_candidate: # Non-empty part before " by "
                final_group_name = primary_part_candidate
            elif label_str: # No valid primary part, but label_str itself is not empty. Use full label.
                final_group_name = label_str
                if len(label_parts) == 2 and not primary_part_candidate : # Original label started with " by "
                    print(f"Info: Label '{label_str}' (ID: '{dataset_id}', line: {i+1}) has ' by ' but an empty primary part. Using full label as group name.")
                elif len(label_parts) == 1 : # Original label did not contain " by "
                     print(f"Info: Label '{label_str}' (ID: '{dataset_id}', line: {i+1}) does not contain ' by ' separator. Using full label as group name.")
                # else: label_str is non-empty but label_parts logic didn't fit above, unusual case.
            else: # label_str is empty
                final_group_name = DATASET_WITH_EMPTY_LABEL
                print(f"Warning: Label for ID '{dataset_id}' (line: {i+1}) is empty. Placing in '{DATASET_WITH_EMPTY_LABEL}'.")
            
            if final_group_name is None: # Safeguard, should ideally not be hit if logic above is comprehensive
                print(f"Critical Warning: final_group_name evaluated to None for label '{label_str}' (ID: '{dataset_id}', line {i+1}). Using fallback '{UNKNOWN_GROUPING_ISSUE}'.")
                final_group_name = UNKNOWN_GROUPING_ISSUE

            descriptive_groups.setdefault(final_group_name, []).append(dataset_id)

except FileNotFoundError:
    print("Error: The file 'health_datasets.csv' was not found in the current directory.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit()

print(f"Total number of distinct group descriptions found: {len(descriptive_groups)}")

output_csv_file = "grouped_datasets_summary.csv"

try:
    with open(output_csv_file, "w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write header
        csv_writer.writerow(["group_name", "dataset_count", "dataset_ids"])

        # Write data rows
        for group_name_key, items_list in sorted(descriptive_groups.items()):
            dataset_ids_str = ",".join(items_list)
            csv_writer.writerow([group_name_key, len(items_list), dataset_ids_str])
    print(f"Successfully wrote grouped data to '{output_csv_file}'")

except IOError:
    print(f"Error: Could not write to CSV file '{output_csv_file}'. Check permissions or path.")
except Exception as e:
    print(f"An unexpected error occurred while writing the CSV: {e}")