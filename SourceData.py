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

    if isinstance(obj, dict):
        for k, v in obj.items():
            # 如果当前键是 'id' 且值包含 'HLTH'
            if k == "id" and isinstance(v, str) and keyword in v:
                matches.append(v)  # 保存符合条件的 id 值
            elif isinstance(v, (dict, list)):  # 递归进入更深层级
                matches.extend(health_dataset_list(v, keyword))
    elif isinstance(obj, list):
        for v in obj:
            matches.extend(health_dataset_list(v, keyword))

    return matches

# 使用示例
with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)

matches = health_dataset_list(data)
for val in matches:
    print(f"Value: {val}")


# Get the data through the EUROSTAT API
# {host_url}/{service}/{version}/{response_type}/{datasetCode}?{format}&{lang}&{filters}
# https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_hlye?format=TSV&compressed=true
HOST_URL = "https://ec.europa.eu/eurostat/api/dissemination"
SERVICE = "statistics"
VERSION = "1.0"
RESPONSE_TYPE = "data"
DATASET_CODE = "HLTH_EHIS_HA1I"
FORMAT = "CSV"  
LANG = "EN"



# https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/HLTH_EHIS_HA1I?format=TSV&compressed=true