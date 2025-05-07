import ijson
import json
import requests

# curl -X GET "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?format=json" -o eurostat_catalog.json\
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
#inspect_json_structure("eurostat_catalog.json")


# 获取包含 HLTH 的 id 值
def health_dataset_list(obj, keyword="HLTH"):
    matches = []
    seen = set()  # 用于去重，防止重复添加

    def recurse(o, parent_label=None):
        if isinstance(o, dict):
            # 提前获取当前层级的 label
            label = o.get("label", parent_label)

            for k, v in o.items():
                # 如果当前键是 'id' 且值包含关键词
                if k == "id" and isinstance(v, str) and keyword in v and "$" not in v:
                    if v not in seen:
                        matches.append((v, label))  # 保存 id 和它的 label
                        seen.add(v)
                elif isinstance(v, (dict, list)):
                    # 递归进入更深层级，同时传入当前的 label
                    recurse(v, label)

        elif isinstance(o, list):
            for item in o:
                recurse(item, parent_label)

    recurse(obj)
    return matches


# usage
with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)
results = health_dataset_list(data)
# for id_value, label in results: print(f"ID: {id_value}, Label: {label}")

# for id_value, _ in results: print(id_value) # 打印仅 id

print(f"Total number of id-label combinations: {len(results)}")



# Get the data through the EUROSTAT API
# {host_url}/{service}/{version}/{response_type}/{datasetCode}?{format}&{lang}&{filters}
# https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_hlye?format=TSV&compressed=true
HOST_URL = "https://ec.europa.eu/eurostat/api/dissemination"
SERVICE = "statistics"
VERSION = "1.0"
RESPONSE_TYPE = "data"
FORMAT = "JSON"  
LANG = "EN"

# After getting results from health_dataset_list
for id_value, _ in results[0:2]:
    # Construct URL for each dataset
    url = f"{HOST_URL}/{SERVICE}/{VERSION}/{RESPONSE_TYPE}/{id_value}?format={FORMAT}&lang={LANG}"
    print(f"\nFetching data for {id_value}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"Successfully retrieved data for {id_value}")
            # Save the response to a file
            filename = f"{id_value}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Data saved to {filename}")
        else:
            print(f"Failed to retrieve data for {id_value}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {id_value}: {e}")


