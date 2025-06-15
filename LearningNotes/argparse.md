
# 🐍 Python `argparse` 模块使用笔记

`argparse` 是 Python 标准库中用于解析命令行参数的模块。它能帮助我们为脚本添加参数并自动生成帮助文档。

---

## 📦 模块导入

```python
import argparse
```

---

## 🛠️ 基本用法

### 1. 创建解析器对象

```python
parser = argparse.ArgumentParser(description="脚本功能描述")
```

### 2. 添加参数

```python
parser.add_argument("name", help="用户名")                     # 必填位置参数
parser.add_argument("--age", type=int, help="年龄")            # 可选参数
```

### 3. 解析命令行参数

```python
args = parser.parse_args()
print(args.name)
print(args.age)
```

🧪 **运行示例**

```bash
python script.py Alice --age 25
```

---

## 🧩 参数类型说明

| 类型      | 示例                                    |
| ------- | ------------------------------------- |
| `str`   | `--name Alice`                        |
| `int`   | `--age 25`                            |
| `float` | `--rate 0.8`                          |
| `bool`  | `action='store_true'` 或 `store_false` |

---

## ⚙️ 常用参数选项

### 位置参数（必须传）

```python
parser.add_argument("input_file", help="输入文件")
```

### 可选参数（以 -- 开头）

```python
parser.add_argument("--verbose", help="显示详细信息")
```

### 设置参数类型

```python
parser.add_argument("--count", type=int, help="数量")
```

### 设置默认值

```python
parser.add_argument("--count", type=int, default=1)
```

### 指定可选值范围

```python
parser.add_argument("--color", choices=['red', 'green', 'blue'])
```

### 开关参数（布尔值）

```python
parser.add_argument("--debug", action="store_true")
```

### 接收多个值（nargs）

```python
parser.add_argument("--nums", nargs="+", type=int)
```

---

## 📄 示例代码：完整示例

```python
import argparse

parser = argparse.ArgumentParser(description="简单计算器")
parser.add_argument("x", type=int, help="第一个数字")
parser.add_argument("y", type=int, help="第二个数字")
parser.add_argument("--operation", choices=['add', 'sub', 'mul', 'div'], default='add', help="运算类型")

args = parser.parse_args()

if args.operation == 'add':
    result = args.x + args.y
elif args.operation == 'sub':
    result = args.x - args.y
elif args.operation == 'mul':
    result = args.x * args.y
elif args.operation == 'div':
    result = args.x / args.y

print(f"结果: {result}")
```

🧪 **运行示例**

```bash
python calculator.py 4 2 --operation mul
# 输出：结果: 8
```

---

## 🆘 自动生成帮助文档

使用 `-h` 或 `--help` 自动输出帮助信息：

```bash
python script.py -h
```

输出示例：

```
usage: script.py [-h] [--age AGE] name

脚本功能描述

positional arguments:
  name         用户名

optional arguments:
  -h, --help   show this help message and exit
  --age AGE    年龄
```

---

## 🧹 常用方法速查表

| 方法 / 参数                  | 用途          |
| ------------------------ | ----------- |
| `ArgumentParser()`       | 创建解析器       |
| `add_argument()`         | 添加参数        |
| `type=int / float / str` | 指定参数类型      |
| `choices=[...]`          | 限定参数取值范围    |
| `default=value`          | 设置默认值       |
| `nargs='+'`              | 接收多个值（空格分隔） |
| `action='store_true'`    | 布尔值开关       |
| `parse_args()`           | 开始解析        |

---

## 🔗 官方文档

📚 [https://docs.python.org/3/library/argparse.html](https://docs.python.org/3/library/argparse.html)

---
