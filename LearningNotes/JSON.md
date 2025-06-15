# JSON
https://www.json.org/json-en.html
## Components
| JSON 类型      | 举例                    | 类似的 Python 类型   |
| ------------ | --------------------- | --------------- |
| 对象（object）   | `{ "name": "Alice" }` | `dict`          |
| 数组（array）    | `[1, 2, 3]`           | `list`          |
| 字符串（string）  | `"hello"`             | `str`           |
| 数字（number）   | `123`, `3.14`         | `int`, `float`  |
| 布尔值（boolean） | `true`, `false`       | `True`, `False` |
| 空值（null）     | `null`                | `None`          |
## Python Library
### `import json`
1. **Main Functions**:
```python
# 1. Reading JSON (from string)
json_string = '{"name": "John", "age": 30}'
data = json.loads(json_string)  # Convert JSON string to Python object
print(data)  # {'name': 'John', 'age': 30}

# 2. Writing JSON (to string)
python_dict = {"name": "John", "age": 30}
json_string = json.dumps(python_dict)  # Convert Python object to JSON string
print(json_string)  # '{"name": "John", "age": 30}'

# 3. Reading JSON from file
with open('data.json', 'r') as f:
    data = json.load(f)  # Read JSON from file

# 4. Writing JSON to file
with open('data.json', 'w') as f:
    json.dump(data, f)  # Write JSON to file
```
2. **JSON to Python Type Mapping**:
```python
# JSON types -> Python types
json_data = {
    "string": "Hello",      # -> str
    "number": 42,           # -> int/float
    "boolean": True,        # -> bool
    "null": None,          # -> None
    "array": [1, 2, 3],    # -> list
    "object": {            # -> dict
        "key": "value"
    }
}
```
3. **Common Options**:
```python
# Pretty printing (indentation)
json_string = json.dumps(data, indent=4)
print(json_string)
# Output:
# {
#     "name": "John",
#     "age": 30
# }

# Sorting keys
json_string = json.dumps(data, sort_keys=True)

# Handling non-ASCII characters
json_string = json.dumps(data, ensure_ascii=False)  # Preserve non-ASCII characters
```

5. **Error Handling**:
```python
try:
    data = json.loads(json_string)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
```

6. **Practical Example**:
```python
import json

# Create a sample data structure
data = {
    "name": "John Doe",
    "age": 30,
    "is_student": False,
    "courses": ["Python", "JSON", "API"],
    "address": {
        "street": "123 Main St",
        "city": "Boston"
    }
}

# Write to file with pretty printing
with open('student.json', 'w') as f:
    json.dump(data, f, indent=4)

# Read from file
with open('student.json', 'r') as f:
    loaded_data = json.load(f)

# Access data
print(loaded_data['name'])  # John Doe
print(loaded_data['courses'][0])  # Python
print(loaded_data['address']['city'])  # Boston
```

7. **Custom JSON Encoding/Decoding**:
```python
import json
from datetime import datetime

# Custom encoder
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Using custom encoder
data = {
    "name": "John",
    "created_at": datetime.now()
}
json_string = json.dumps(data, cls=CustomEncoder)
```

8. **Best Practices**:
```python
# 1. Always use 'with' statement for file operations
with open('data.json', 'r') as f:
    data = json.load(f)

# 2. Handle encoding when working with files
with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)

# 3. Use pretty printing for readability
json.dump(data, f, indent=4)

# 4. Validate JSON before processing
def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False
```


### `import ijson`
#### Events
When using `ijson.parse()`, it generates a stream of events that tell you what type of JSON element is being processed. Here are the main events you'll encounter:
1. `"map_key"`: 
   - Occurs when encountering a key in a JSON object
   - The `value` will be the key name (as a string)
   - Example: In `{"name": "John"}`, you'll get a `map_key` event with value `"name"`
2. `"start_map"` and `"end_map"`:
   - `start_map`: Signals the beginning of a JSON object `{`
   - `end_map`: Signals the end of a JSON object `}`
   - Example: For `{"name": "John"}`, you'll get:
     ```
     start_map
     map_key (value="name")
     string (value="John")
     end_map
     ```
3. `"start_array"` and `"end_array"`:
   - `start_array`: Signals the beginning of a JSON array `[`
   - `end_array`: Signals the end of a JSON array `]`
   - Example: For `["apple", "banana"]`, you'll get:
     ```
     start_array
     string (value="apple")
     string (value="banana")
     end_array
     ```
4. Value events:
   - `"string"`: For string values
   - `"number"`: For numeric values
   - `"boolean"`: For true/false values
   - `"null"`: For null values
   - 
Here's a complete example to illustrate the event sequence:

For this JSON:
```json
{
    "name": "John",
    "age": 30,
    "is_student": false,
    "scores": [85, 90, 95]
}
```

The events would come in this order:
```
start_map
map_key (value="name")
string (value="John")
map_key (value="age")
number (value=30)
map_key (value="is_student")
boolean (value=false)
map_key (value="scores")
start_array
number (value=85)
number (value=90)
number (value=95)
end_array
end_map
```
This event-based parsing is what makes `ijson` memory-efficient - instead of loading the entire JSON file into memory, it processes it one event at a time, allowing you to handle very large JSON files without running out of memory.
### Summary

Key differences between `json` and `ijson`:
1. `json` loads the entire file into memory
2. `ijson` parses the file incrementally (better for large files)
3. `json` is simpler to use for small files
4. `ijson` is more memory-efficient for large files

When to use which:
- Use `json` for:
  - Small to medium-sized JSON files
  - When you need the entire file in memory
  - Simple JSON operations
- Use `ijson` for:
  - Large JSON files
  - When memory is a concern
  - When you only need to process parts of the file

### EDA with ijson Sample Code
```python
import ijson

def inspect_json_structure(file_path, max_depth=5, sample_size=5):
    """
    Inspect the structure of a large JSON file without loading it entirely into memory.
    Args:
        file_path: Path to the JSON file
        max_depth: Maximum depth to explore (default: 5)
        sample_size: Number of sample values to show (default: 5)
    """
    with open(file_path, "rb") as f:
        parser = ijson.parse(f)
        path = []  # Track current path
        indent = 0  # Indentation level
        current_key = None
        value_samples = {}  # Store sample values for each path
        sample_counts = {}  # Track how many samples we've collected for each path
        in_item_array = False  # Flag to track if we're in the item array

        print(f"\nExploring structure of {file_path} (max depth: {max_depth}):")
        print("=" * 50)

        for prefix, event, value in parser:
            if event == "map_key":
                current_key = value
                if indent < max_depth:
                    print("  " * indent + f"├─ {current_key}")
                # Check if we're entering the item array
                if current_key == "item" and "link" in path:
                    in_item_array = True
            elif event == "start_map":
                if indent < max_depth:
                    print("  " * indent + "└─ {")
                indent += 1
                if path:
                    path.append("{}")
            elif event == "start_array":
                if indent < max_depth:
                    print("  " * indent + "└─ [")
                indent += 1
                path.append("[]")
            elif event == "end_map":
                indent = max(0, indent - 1)
                if indent < max_depth:
                    print("  " * indent + "}")
                if path and path[-1] == "{}":
                    path.pop()
            elif event == "end_array":
                indent = max(0, indent - 1)
                if indent < max_depth:
                    print("  " * indent + "]")
                if path and path[-1] == "[]":
                    path.pop()
                    if in_item_array and "item" in path:
                        in_item_array = False
            elif event in ["string", "number", "boolean", "null"]:
                if indent < max_depth:
                    value_type = type(value).__name__ if value is not None else "null"
                    current_path = " → ".join(path + [current_key]) if current_key else " → ".join(path)
                    
                    # Initialize sample tracking for this path if not exists
                    if current_path not in value_samples:
                        value_samples[current_path] = []
                        sample_counts[current_path] = 0
                    
                    # Only add sample if we haven't reached the limit
                    if sample_counts[current_path] < sample_size:
                        value_samples[current_path].append(str(value))
                        sample_counts[current_path] += 1
                    
                    print("  " * indent + f"├─ {value_type}")

        print("=" * 50)
        print("\nSample values found:")
        print("=" * 50)
        # Sort paths to show item array samples first
        sorted_paths = sorted(value_samples.keys(), 
                            key=lambda x: (not x.startswith("link → item"), x))
        for path in sorted_paths:
            samples = value_samples[path]
            if samples:
                print(f"\nPath: {path}")
                print(f"Sample values: {', '.join(samples)}")

# Example usage
inspect_json_structure("eurostat_catalog.json", max_depth=5, sample_size=3)
```
**Simple Version with ijson**
```python
import ijson  # 导入 ijson 库，用于流式解析大型 JSON 文件

def inspect_json_structure(file_path, max_events=1000):
    # 定义函数，参数：
    # - file_path: JSON 文件路径
    # - max_events: 最大解析事件数（默认 1000），用于控制输出量

    with open(file_path, "rb") as f:  # 以二进制模式打开文件（ijson 要求字节流输入）
        parser = ijson.parse(f)      # 创建解析器对象，生成事件流（三元组：prefix, event, value）
        
        event_count = 0              # 初始化事件计数器
        for prefix, event, value in parser:  # 遍历解析器的每个事件
            print(f"路径: {prefix}, 事件类型: {event}, 值: {value}")  # 打印事件详情
            event_count += 1         # 计数器递增
            if event_count >= max_events:  # 如果事件数超过设定的最大值
                break               # 终止循环，避免处理过久
```
**Simple Version with json**
```python
import json
# 定义 explore_json 函数，用于探索 JSON 的嵌套结构
def explore_json(d, indent=0, max_depth=3):
    # 如果当前缩进（即深度）超过最大层级，停止递归
    if indent >= max_depth:
        print('  ' * indent + '... (max depth reached)')  # 打印提示
        return
    # 如果当前对象是一个字典（即 JSON 对象）
    if isinstance(d, dict):
        for k, v in d.items():  # 遍历字典的每一个键值对
            print('  ' * indent + f"- {k}")  # 打印键名，前面加缩进
            explore_json(v, indent + 1, max_depth)  # 递归处理对应的值
# === 实际用法 ===

# 打开并读取名为 "eurostat_catalog.json" 的 JSON 文件
with open("eurostat_catalog.json", "r") as f:
    data = json.load(f)  # 将 JSON 内容解析为 Python 对象（通常是 dict）
# 使用 explore_json 函数探索该 JSON 的结构，最多递归 8 层
explore_json(data, max_depth=8)

#explore_json 函数的输入 data 是通过 json.load() 或 json.loads() 解析出来的，是一个 标准的嵌套 Python 对象（dict / list）
# 适用于比较小的