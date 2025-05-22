Python 的 `subprocess` 模块用于创建和管理子进程，允许你在 Python 程序中执行系统命令（例如在终端中运行的命令）。它是一个非常强大且灵活的工具，可以用来替代旧的 `os.system()` 方法。




## ✅ 常用函数

### 1. `subprocess.run()`

最推荐使用的方法，从 Python 3.5 起加入，简单且强大。

```python
import subprocess

result = subprocess.run(["ls", "-l"], capture_output=True, text=True)
print(result.stdout)
```

参数说明：

* `["ls", "-l"]`：要执行的命令和参数，推荐用列表方式而不是字符串。
* `capture_output=True`：捕获标准输出和错误。
* `text=True`：输出为字符串（否则是字节）。
* `check=True`：命令出错时报错。

---

### 2. `subprocess.Popen()`

更底层、更灵活，但也更复杂。适合需要与子进程持续交互的场景。

```python
process = subprocess.Popen(["ping", "www.google.com"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

for line in process.stdout:
    print(line.strip())
```

`Popen`参数：

* `stdout=subprocess.PIPE`：将子进程的输出重定向。
* `stderr=subprocess.PIPE`：同理，重定向错误。
* `stdin=subprocess.PIPE`：用于向子进程输入。
* `text=True`：输出文本模式。

---

### 3. `subprocess.call()`

运行命令并返回返回码（exit code），已过时，推荐用 `run()` 替代。

```python
retcode = subprocess.call(["ls", "-l"])
```

---

### 4. `subprocess.check_output()`

运行命令并获取标准输出，如果命令失败会抛出异常。

```python
output = subprocess.check_output(["ls", "-l"], text=True)
print(output)
```

---

## 🧠 常见用法场景

### ✅ 运行 shell 命令

```python
subprocess.run("echo hello", shell=True)
```

注意：`shell=True` 是允许你像 bash 一样运行命令字符串，但有**安全风险**，不要在其中包含不可信用户输入！

---

### ✅ 带输入的命令

```python
result = subprocess.run(["python3"], input="print('Hello')\n", capture_output=True, text=True)
print(result.stdout)
```

---

### ✅ 异常处理

```python
try:
    subprocess.run(["ls", "/nonexistent"], check=True)
except subprocess.CalledProcessError as e:
    print("命令执行失败:", e)
```

---

## 🚨 安全建议

* **尽量使用列表方式传递命令，而非字符串。**
* **避免 `shell=True`，除非你确定输入来源是安全的。**
* **处理异常 `subprocess.CalledProcessError`。**

---

## 🛠 实用技巧

| 需求        | 推荐方法                                                     |
| --------- | -------------------------------------------------------- |
| 简单运行命令    | `subprocess.run([...])`                                  |
| 获取输出      | `subprocess.run([...], capture_output=True)`             |
| 获取输出并处理错误 | `subprocess.run([...], capture_output=True, check=True)` |
| 与子进程持续交互  | `subprocess.Popen()`                                     |

