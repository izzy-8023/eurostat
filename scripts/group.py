import csv

descriptive_groups = {}
# Define fallback group names for clarity
NO_PRIMARY_BY_SEPARATOR_GROUP = "datasets_with_label_missing_primary_by_separator"
EMPTY_DERIVED_GROUP_KEY = "datasets_with_empty_derived_group_key"
EMPTY_PRIMARY_GROUP_KEY = "datasets_with_empty_primary_group_key"

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

            label_parts = label_str.split(" by ", 1)
            
            final_group_name = None

            if len(label_parts) == 2:
                primary_group_candidate = label_parts[0].strip()
                if primary_group_candidate:
                    final_group_name = primary_group_candidate
                else:
                    final_group_name = EMPTY_PRIMARY_GROUP_KEY
                    print(f"Warning: Label '{label_str}' (ID: '{dataset_id}', line: {i+1}) has an empty part before the first ' by '. Placing in '{EMPTY_PRIMARY_GROUP_KEY}'.")
            else:
                final_group_name = NO_PRIMARY_BY_SEPARATOR_GROUP
                print(f"Warning: Label '{label_str}' (ID: '{dataset_id}', line: {i+1}) does not contain the primary ' by ' separator. Placing in '{NO_PRIMARY_BY_SEPARATOR_GROUP}'.")
            
            if final_group_name is None:
                print(f"Critical Warning: final_group_name evaluated to None for label '{label_str}' (ID: '{dataset_id}', line {i+1}). Using fallback 'unknown_grouping_issue'.")
                final_group_name = "unknown_grouping_issue"

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