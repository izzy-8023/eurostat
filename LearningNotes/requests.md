
# 🌐 Python `requests` 模块使用笔记

`requests` 是一个功能强大且使用简单的 HTTP 库，用于发送各种 HTTP 请求（如 GET、POST、PUT、DELETE 等），常用于与 Web API 交互。

📦 安装：

```bash
pip install requests
```

📚 官方文档：https://docs.python-requests.org/

---

## ✉️ 基本用法

### GET 请求

```python
import requests

response = requests.get("https://api.example.com/data")
print(response.status_code)
print(response.text)
```

### POST 请求

```python
payload = {"username": "ziwei", "password": "123456"}
response = requests.post("https://api.example.com/login", data=payload)
print(response.json())
```

---

## 📮 请求方法汇总

| 方法              | 用途                 |
|-------------------|----------------------|
| `requests.get()`     | 获取资源（读取）    |
| `requests.post()`    | 提交数据（创建）    |
| `requests.put()`     | 更新资源           |
| `requests.delete()`  | 删除资源           |
| `requests.head()`    | 只获取响应头信息   |
| `requests.patch()`   | 局部更新资源       |

---

## 📦 常用参数说明

### `params`（用于 GET 查询参数）

```python
params = {"q": "python", "page": 2}
requests.get("https://api.example.com/search", params=params)
```

### `data`（用于 POST 表单）

```python
data = {"username": "ziwei", "password": "123"}
requests.post("https://api.example.com/login", data=data)
```

### `json`（用于发送 JSON 数据）

```python
payload = {"name": "Ziwei", "age": 26}
requests.post("https://api.example.com/users", json=payload)
```

### `headers`（自定义请求头）

```python
headers = {"Authorization": "Bearer TOKEN"}
requests.get("https://api.example.com/profile", headers=headers)
```

---

## 🗂️ 处理响应内容

```python
res = requests.get("https://httpbin.org/get")

res.status_code
res.text
res.json()
res.headers
res.cookies
res.content
```

---

## 🕵️‍♂️ 异常处理

```python
try:
    res = requests.get("https://api.example.com/data", timeout=5)
    res.raise_for_status()
except requests.exceptions.HTTPError as e:
    print("HTTP 错误:", e)
except requests.exceptions.RequestException as e:
    print("请求异常:", e)
```

---

## ⌛ 超时设置

```python
requests.get("https://api.example.com", timeout=10)
```

---

## 🔁 会话管理（自动保持 cookie）

```python
session = requests.Session()
session.get("https://api.example.com/login")
session.post("https://api.example.com/update", data={"foo": "bar"})
```

---

## 💾 下载文件示例

```python
url = "https://example.com/image.png"
response = requests.get(url)

with open("image.png", "wb") as f:
    f.write(response.content)
```

---

## 🗃️ 上传文件示例

```python
files = {'file': open('report.pdf', 'rb')}
response = requests.post("https://api.example.com/upload", files=files)
```

---

## 🔐 认证（Basic Auth / Token）

### Basic Auth

```python
from requests.auth import HTTPBasicAuth

response = requests.get("https://api.example.com", auth=HTTPBasicAuth("user", "pass"))
```

### Bearer Token

```python
headers = {"Authorization": "Bearer your_token"}
response = requests.get("https://api.example.com", headers=headers)
```

---

## ⚙️ 设置 User-Agent

```python
headers = {"User-Agent": "ZiweiBot/1.0"}
requests.get("https://api.example.com", headers=headers)
```

---

## 🧵 多线程下载（示意）

```python
import threading
import requests

def download(url, filename):
    r = requests.get(url)
    with open(filename, "wb") as f:
        f.write(r.content)

urls = [("https://example.com/a", "a.html"),
        ("https://example.com/b", "b.html")]

threads = [threading.Thread(target=download, args=(u, f)) for u, f in urls]
[t.start() for t in threads]
[t.join() for t in threads]
```

---

## 🧹 常用函数速查表

| 功能           | 用法                                     |
|----------------|------------------------------------------|
| 发送 GET 请求   | `requests.get(url, params=...)`         |
| 发送 POST 请求  | `requests.post(url, data=... / json=...)` |
| 添加请求头     | `headers={"Authorization": "token"}`     |
| 设置超时       | `timeout=5`                              |
| 解析 JSON 响应 | `response.json()`                        |
| 抛出异常       | `response.raise_for_status()`            |
| 会话保持       | `requests.Session()`                     |

---
