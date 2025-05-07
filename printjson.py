import ijson
# inspect the structure of the json file
def inspect_json_structure(file_path, max_events=50):
    with open(file_path, "rb") as f:
        parser = ijson.parse(f)
        event_count = 0
        for prefix, event, value in parser:
            print(f"Prefix: {prefix}, Event: {event}, Value: {value}")
            event_count += 1
            if event_count >= max_events:
                break

# usage
inspect_json_structure("eurostat_catalog.json")

import json

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
import json

with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)

explore_json(data, max_depth=8)  # limit the max depth
