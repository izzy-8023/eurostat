import ijson
import json
# inspect the structure of the json file
# curl -X GET "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?format=json" -o eurostat_catalog.json\

def inspect_json_structure(file_path, max_events=50):
    """Prints the first few events from parsing a JSON file using ijson."""
    try:
        with open(file_path, "rb") as f:
            parser = ijson.parse(f)
            event_count = 0
            for prefix, event, value in parser:
                print(f"Prefix: {prefix}, Event: {event}, Value: {value}")
                event_count += 1
                if event_count >= max_events:
                    break
    except FileNotFoundError:
        print(f"Error: Catalog file '{file_path}' not found for inspection.")
    except Exception as e:
        print(f"Error during inspection: {e}")

inspect_json_structure("eurostat_catalog.json")


def explore_json(d, indent=0, max_depth=3):
    if indent >= max_depth:
        print('  ' * indent + '... (max depth reached)')
        return

    if isinstance(d, dict):
        for k, v in d.items():
            print('  ' * indent + f"- {k}")
            explore_json(v, indent + 1, max_depth)
    elif isinstance(d, list):
        print('  ' * indent + f"[list of {type(d[0]).__name__}]" if d else "[empty list]")
        if d:
            explore_json(d[0], indent + 1, max_depth)

# usage


with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)

explore_json(data, max_depth=8)  # limit the max depth
