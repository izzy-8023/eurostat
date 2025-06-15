`concurrent.futures` 是 Python 标准库中的一个高层并发编程模块，常用于 **多线程** 和 **多进程** 编程，它比 `threading` 和 `multiprocessing` 更易用、更直观。

常用的两个执行器：

* `ThreadPoolExecutor`: 用于 IO 密集型任务（如网络请求、文件读写）
* `ProcessPoolExecutor`: 用于 CPU 密集型任务（如大计算、图像处理）

---

## ✅ 基本用法一览

### 示例：多线程执行任务

```python
import concurrent.futures
import time

def task(n):
    print(f"Task {n} starting...")
    time.sleep(1)
    return f"Task {n} done"

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(task, i) for i in range(5)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
```

说明：

* `submit(func, arg)`: 提交任务，返回 `Future` 对象。
* `as_completed(futures)`: 哪个任务先完成就先返回。
* `max_workers=3`: 同时运行最多 3 个线程。

---

## ✅ `map()` 和 `submit()` 区别

### `map()`（结果有序）

```python
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(task, [0, 1, 2])
    for res in results:
        print(res)
```

* 类似 `map(func, iterable)`
* 返回值顺序与输入顺序相同
* 不抛异常，异常会在迭代结果时抛出

---

### `submit()` + `as_completed()`（结果无序）

```python
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(task, i) for i in range(3)]
    for future in concurrent.futures.as_completed(futures):
        print(future.result())
```

* `submit` 是异步提交
* `as_completed` 会在任务完成后立即获取结果（顺序可能不同）

---

## ✅ 多进程版：ProcessPoolExecutor

适合 CPU 密集型任务，如数学计算

```python
import concurrent.futures

def square(n):
    return n * n

with concurrent.futures.ProcessPoolExecutor() as executor:
    results = executor.map(square, range(10))
    print(list(results))
```

注意：

* Windows/macOS 下，`ProcessPoolExecutor` 需要放在 `if __name__ == '__main__':` 语句中运行！

---

## ✅ 获取异常与超时

```python
try:
    result = future.result(timeout=2)
except concurrent.futures.TimeoutError:
    print("任务超时")
except Exception as e:
    print("任务出错", e)
```

---

## 🚀 实战总结表

| 用法                           | 场景             |
| ---------------------------- | -------------- |
| `ThreadPoolExecutor`         | 网络爬虫、读写文件      |
| `ProcessPoolExecutor`        | 图像处理、科学计算      |
| `submit(func, arg)`          | 更灵活、可以自定义逻辑    |
| `map(func, iterable)`        | 简单批量任务、顺序处理结果  |
| `as_completed(futures)`      | 哪个任务先完成先处理     |
| `future.result(timeout=...)` | 获取结果时设置超时或异常捕获 |

