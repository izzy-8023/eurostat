
# 🐍 Python `os` 模块使用笔记

`os` 模块是 Python 的标准库之一，用于与操作系统进行交互。它提供了文件和目录操作、环境变量访问、路径处理等功能。

---

## 📦 基本导入

```python
import os
```

---

## 📁 目录操作

### 获取当前工作目录

```python
os.getcwd()
```

### 改变当前工作目录

```python
os.chdir('/path/to/directory')
```

### 列出当前目录下的所有文件和文件夹

```python
os.listdir()
```

### 创建新目录

```python
os.mkdir('new_folder')            # 创建一个目录
os.makedirs('a/b/c')              # 递归创建多层目录
```

### 删除目录

```python
os.rmdir('folder')                # 删除空目录
os.removedirs('a/b/c')            # 递归删除空目录
```

---

## 📄 文件操作

### 删除文件

```python
os.remove('file.txt')
```

### 重命名文件或目录

```python
os.rename('old_name.txt', 'new_name.txt')
```

---

## 🔍 路径操作（配合 `os.path`）

```python
os.path.join('folder', 'file.txt')          # 拼接路径
os.path.exists('path/to/file')              # 路径是否存在
os.path.isfile('path/to/file')              # 是否为文件
os.path.isdir('path/to/dir')                # 是否为目录
os.path.basename('/path/to/file.txt')       # 获取文件名
os.path.dirname('/path/to/file.txt')        # 获取文件所在目录
os.path.abspath('file.txt')                 # 获取绝对路径
```

---

## 🌍 环境变量

### 获取环境变量

```python
os.environ['HOME']               # 获取特定变量，不存在时抛错
os.environ.get('HOME')           # 获取变量，不存在时返回 None
```

### 设置环境变量（仅当前进程有效）

```python
os.environ['MY_VAR'] = 'value'
```

---

## ⚙️ 执行系统命令

```python
os.system('ls -la')              # 执行 shell 命令
```

> ✅ 推荐使用 `subprocess` 模块替代 `os.system`，更安全更强大。

---

## 🧹 清单：常用函数速查表

| 功能      | 函数                                     |
| ------- | -------------------------------------- |
| 当前目录    | `os.getcwd()`                          |
| 切换目录    | `os.chdir(path)`                       |
| 列出文件    | `os.listdir(path)`                     |
| 创建目录    | `os.mkdir()` / `os.makedirs()`         |
| 删除文件/目录 | `os.remove()` / `os.rmdir()`           |
| 路径拼接    | `os.path.join()`                       |
| 路径判断    | `os.path.exists()` / `os.path.isdir()` |
| 环境变量    | `os.environ.get()`                     |
| 执行命令    | `os.system()`                          |

---

## 🧪 示例代码

```python
import os

# 创建目录并进入
if not os.path.exists('test_folder'):
    os.mkdir('test_folder')
os.chdir('test_folder')

# 创建文件
with open('example.txt', 'w') as f:
    f.write('Hello, os module!')

# 重命名
os.rename('example.txt', 'new_example.txt')

# 删除文件和目录
os.remove('new_example.txt')
os.chdir('..')
os.rmdir('test_folder')
```

---

如需了解更多详细内容，也可以查阅官方文档：
📚 [https://docs.python.org/3/library/os.html](https://docs.python.org/3/library/os.html)

