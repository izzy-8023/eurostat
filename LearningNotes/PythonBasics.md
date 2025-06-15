# 1. `with`
1. **Basic Syntax**:
```python
with open('filename.txt', 'mode') as file_variable:
    # do something with file_variable
```

2. **File Modes**:
```python
# Common modes:
'r'  # Read mode (default)
'w'  # Write mode (overwrites existing file)
'a'  # Append mode (adds to existing file)
'x'  # Exclusive creation (fails if file exists)
'b'  # Binary mode (e.g., 'rb' for reading binary)
't'  # Text mode (default)
'+'  # Update mode (read and write)
```

3. **Basic Examples**:
```python
# Reading a file
with open('example.txt', 'r') as file:
    content = file.read()
    print(content)

# Writing to a file
with open('example.txt', 'w') as file:
    file.write('Hello, World!')

# Appending to a file
with open('example.txt', 'a') as file:
    file.write('\nNew line added')
```

4. **File Methods**:
```python
with open('example.txt', 'r') as file:
    # Read entire file
    content = file.read()
    
    # Read line by line
    line = file.readline()
    
    # Read all lines into a list
    lines = file.readlines()
    
    # Iterate through lines
    for line in file:
        print(line.strip())
```

5. **With Encoding**:
```python
# Reading with specific encoding
with open('example.txt', 'r', encoding='utf-8') as file:
    content = file.read()

# Writing with specific encoding
with open('example.txt', 'w', encoding='utf-8') as file:
    file.write('Hello, World!')
```

6. **Error Handling**:
```python
try:
    with open('example.txt', 'r') as file:
        content = file.read()
except FileNotFoundError:
    print("File not found!")
except PermissionError:
    print("Permission denied!")
except Exception as e:
    print(f"An error occurred: {e}")
```

7. **Multiple Files**:
```python
# Reading from one file and writing to another
with open('input.txt', 'r') as input_file, open('output.txt', 'w') as output_file:
    content = input_file.read()
    output_file.write(content.upper())
```

8. **Context Manager Benefits**:
```python
# The 'with' statement automatically:
# 1. Opens the file
# 2. Handles the file operations
# 3. Closes the file properly, even if an error occurs

# This is better than:
file = open('example.txt', 'r')
try:
    content = file.read()
finally:
    file.close()  # Must remember to close!
```

9. **Practical Examples**:

```python
# Example 1: Reading a CSV file
with open('data.csv', 'r') as file:
    for line in file:
        values = line.strip().split(',')
        print(values)

# Example 2: Writing JSON data
import json
data = {'name': 'John', 'age': 30}
with open('data.json', 'w') as file:
    json.dump(data, file, indent=4)

# Example 3: Appending logs
with open('app.log', 'a') as file:
    file.write(f'[{datetime.now()}] User logged in\n')

# Example 4: Reading binary file
with open('image.jpg', 'rb') as file:
    image_data = file.read()
```

10. **Best Practices**:
```python
# 1. Always use 'with' statement
# 2. Specify encoding when working with text files
# 3. Use appropriate error handling
# 4. Close files as soon as possible
# 5. Use context managers for multiple files

# Good practice:
with open('file.txt', 'r', encoding='utf-8') as file:
    content = file.read()
    # File is automatically closed after this block

# Bad practice:
file = open('file.txt', 'r')
content = file.read()
file.close()  # Might be forgotten if an error occurs
```

11. **Common Use Cases**:
```python
# Reading configuration
with open('config.json', 'r') as file:
    config = json.load(file)

# Writing logs
with open('app.log', 'a') as file:
    file.write(f'[{datetime.now()}] {message}\n')

# Processing large files
with open('large_file.txt', 'r') as file:
    for line in file:  # Memory efficient
        process_line(line)

# Binary file operations
with open('image.jpg', 'rb') as file:
    image_data = file.read()
```

The `with` statement is a context manager that ensures proper resource management. It's particularly useful for file operations because it:
1. Automatically handles file opening and closing
2. Ensures files are closed even if an error occurs
3. Makes code cleaner and more readable
4. Prevents resource leaks


# 2. Magic Methods / Dunder Methods
Python 中 **“双下划线包裹的名称”**，也叫做 **“魔法方法（Magic Methods）”或“dunder methods（double underscore）”**，全名是 **“特殊方法”**。这些方法或变量以 `__name__` 的形式存在，在 Python 中有特殊用途，比如控制运算符行为、对象的创建与销毁、字符串表示、上下文管理等。

## 🔹 1. 对象生命周期相关

| 方法                    | 用途              |
| --------------------- | --------------- |
| `__init__(self, ...)` | 初始化对象           |
| `__new__(cls, ...)`   | 创建对象（一般不重写）     |
| `__del__(self)`       | 对象被销毁时调用（不推荐依赖） |

## 🔹 2. 字符串表示

| 方法                              | 用途                       |
| ------------------------------- | ------------------------ |
| `__str__(self)`                 | `str(obj)`，用于打印时展示       |
| `__repr__(self)`                | `repr(obj)`，用于开发调试，推荐更准确 |
| `__format__(self, format_spec)` | 格式化支持：`format(obj)`      |

## 🔹 3. 运算符重载（Arithmetic and Comparison）

| 方法                          | 用途        |
| --------------------------- | --------- |
| `__add__(self, other)`      | 加号 `+`    |
| `__sub__(self, other)`      | 减号 `-`    |
| `__mul__(self, other)`      | 乘号 `*`    |
| `__truediv__(self, other)`  | 除法 `/`    |
| `__floordiv__(self, other)` | 地板除 `//`  |
| `__mod__(self, other)`      | 取模 `%`    |
| `__pow__(self, other)`      | 幂 `**`    |
| `__eq__(self, other)`       | 相等 `==`   |
| `__ne__(self, other)`       | 不等 `!=`   |
| `__lt__(self, other)`       | 小于 `<`    |
| `__le__(self, other)`       | 小于等于 `<=` |
| `__gt__(self, other)`       | 大于 `>`    |
| `__ge__(self, other)`       | 大于等于 `>=` |

还有：

| 方法          | 含义         |
| ----------- | ---------- |
| `__neg__`   | `-a`       |
| `__pos__`   | `+a`       |
| `__abs__`   | `abs(a)`   |
| `__round__` | `round(a)` |

## 🔹 4. 容器相关（Sequence/Mapping）

| 方法                              | 用途                 |
| ------------------------------- | ------------------ |
| `__len__(self)`                 | `len(obj)`         |
| `__getitem__(self, key)`        | `obj[key]`         |
| `__setitem__(self, key, value)` | `obj[key] = value` |
| `__delitem__(self, key)`        | `del obj[key]`     |
| `__contains__(self, item)`      | `item in obj`      |
| `__iter__(self)`                | 可迭代对象（用于 `for`）    |
| `__next__(self)`                | 迭代器的下一个值           |
| `__reversed__(self)`            | `reversed(obj)`    |

## 🔹 5. 上下文管理器（`with` 语句）

| 方法                                          | 用途         |
| ------------------------------------------- | ---------- |
| `__enter__(self)`                           | 进入上下文      |
| `__exit__(self, exc_type, exc_val, exc_tb)` | 离开上下文，处理异常 |

## 🔹 6. 属性访问控制

| 方法                               | 用途            |
| -------------------------------- | ------------- |
| `__getattr__(self, name)`        | 当找不到属性时调用     |
| `__getattribute__(self, name)`   | 获取属性时总会调用（危险） |
| `__setattr__(self, name, value)` | 设置属性时调用       |
| `__delattr__(self, name)`        | 删除属性时调用       |

### 🔹 7. 可调用对象

| 方法                    | 用途                |
| --------------------- | ----------------- |
| `__call__(self, ...)` | `obj()` 形式调用对象时触发 |


## 🔹 8. 类型和类相关

| 方法          | 用途          |
| ----------- | ----------- |
| `__class__` | 返回对象所属类     |
| `__bases__` | 返回类的父类（元类用） |
| `__mro__`   | 方法解析顺序      |

### 🔹 9. 反射与元编程

| 方法                                  | 用途                 |
| ----------------------------------- | ------------------ |
| `__dir__(self)`                     | `dir(obj)` 返回的成员列表 |
| `__instancecheck__(self, instance)` | `isinstance()`     |
| `__subclasscheck__(self, subclass)` | `issubclass()`     |


### 🔹 10. 其他常见内置变量

| 名称          | 用途                            |
| ----------- | ----------------------------- |
| `__name__`  | 模块名称，若为 `"__main__"` 表示作为脚本执行 |
| `__file__`  | 当前 Python 文件的路径               |
| `__doc__`   | 文档字符串                         |
| `__dict__`  | 返回对象的属性字典                     |
| `__slots__` | 限定对象可以拥有的属性                   |

---

## ✳️ 示例

```python
class Dog:
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"This is a dog named {self.name}"

    def __len__(self):
        return len(self.name)

d = Dog("Buddy")
print(d)          # 调用 __str__
print(len(d))     # 调用 __len__
```

输出：

```
This is a dog named Buddy
5
```

---

Tips:

* 你**通常不需要手动调用**这些方法，比如你不会写 `obj.__str__()`，而是写 `str(obj)` 或 `print(obj)`。
* 它们的主要作用是**让你自定义类行为，像内置类型一样自然使用**。



# 3. 带下划线的变量
Python 约定俗成使用下划线（`_`）来区分变量或方法的“访问权限”和“特殊用途”。不同位置和数量的下划线代表不同含义。


## 1. 单个下划线 `_var`

* **含义**：约定俗成的“内部使用”变量或方法，外部不建议直接访问。
* **作用**：无实际语言限制，仅作为提示“这是内部成员”。
* **示例**：

```python
class MyClass:
    def __init__(self):
        self._internal = 42  # 内部变量，外部不建议访问
```


## 2. 单个下划线 `_`

* **作为变量名**：

  * 用于表示临时变量，或“不关心”的变量名。
  * 交互式解释器中，`_`保存上一次表达式的结果。
* **示例**：

```python
for _ in range(3):  # 不关心循环变量
    print("Hello")

result = 10 + 5
print(_)  # 交互式环境中，打印上一次结果 15
```

## 3. 双下划线开头 `__var`

* **名称重整（Name Mangling）**：

  * Python自动把`__var`变成`_ClassName__var`，防止子类覆盖和外部访问。
  * 用于“类私有变量”，但不是绝对私有。
* **注意**：不以双下划线结尾的变量才会重整。
* **示例**：

```python
class MyClass:
    def __init__(self):
        self.__private = 99  # 内部变量名会被改写

obj = MyClass()
print(obj._MyClass__private)  # 可以访问
```

## 4. 双下划线开头和结尾 `__var__`

* **魔术方法 / 特殊方法（Magic Methods / Dunder Methods）**：

  * Python内置方法，具有特殊功能。
  * 例如：`__init__`, `__str__`, `__call__`，都是被 Python 自动调用的特殊方法。
* **不要自己随便创建这种命名**，除非真的想实现特殊协议。
* **示例**：

```python
class MyClass:
    def __init__(self):
        pass  # 构造函数

    def __str__(self):
        return "MyClass instance"
```


## 5. 单下划线结尾 `var_`

* **解决命名冲突**：

  * 当变量名与Python关键字冲突时，用单下划线后缀避免冲突。
* **示例**：

```python
class_ = "MyClass"  # class 是关键字，改成 class_ 避免冲突
```

## 6. 多个下划线（不常用）

* 例如 `___var`，Python没有特殊含义，和普通变量名没区别，只是命名习惯。
* 不建议使用。


## 总结表格

| 形式        | 含义             | 示例                    | 备注                          |
| --------- | -------------- | --------------------- | --------------------------- |
| `_var`    | 内部使用变量，约定不外部访问 | `self._name`          | 只是约定，语言无强制限制                |
| `__var`   | 名称重整，类私有变量     | `self.__id`           | 实际变成`_ClassName__id`，防止子类覆盖 |
| `__var__` | 魔术方法或特殊方法      | `__init__`, `__str__` | Python 内置调用，别乱写             |
| `var_`    | 避免与关键字冲突       | `class_`              | 用于避免和关键字冲突                  |
| `_`       | 临时变量或上次结果      | `for _ in range(5)`   | 交互式下，`_`保存上个表达式结果           |
