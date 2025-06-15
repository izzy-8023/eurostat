
# 📘 Apache Airflow 

> 用于构建、调度和监控数据工程工作流的开源平台。

---

## 🧠 目录

- [📘 Apache Airflow](#-apache-airflow)
  - [🧠 目录](#-目录)
  - [Airflow 是什么？](#airflow-是什么)
  - [核心概念](#核心概念)
    - [1. DAG (Directed Acyclic Graph)](#1-dag-directed-acyclic-graph)
    - [2. Task](#2-task)
    - [3. Operator](#3-operator)
    - [4. Scheduler](#4-scheduler)
    - [5. Webserver](#5-webserver)
    - [6. Metadata Database](#6-metadata-database)
  - [安装与环境配置](#安装与环境配置)
    - [快速安装（本地开发）](#快速安装本地开发)
    - [设置环境变量](#设置环境变量)
  - [DAG 文件结构与语法](#dag-文件结构与语法)
    - [一个典型 DAG 文件](#一个典型-dag-文件)
  - [常见 Operator 用法](#常见-operator-用法)
    - [PythonOperator](#pythonoperator)
    - [BashOperator](#bashoperator)
    - [BranchPythonOperator](#branchpythonoperator)
  - [任务依赖与执行流程](#任务依赖与执行流程)
  - [Airflow 命令大全](#airflow-命令大全)
  - [调试技巧与日志查看](#调试技巧与日志查看)
  - [高级功能](#高级功能)
    - [1. Trigger Rules](#1-trigger-rules)
    - [2. TaskGroup（任务分组）](#2-taskgroup任务分组)
    - [3. Branching（分支）](#3-branching分支)
    - [4. XCom（任务间通信）](#4-xcom任务间通信)
    - [5. SLA（服务级别协议）](#5-sla服务级别协议)
  - [与 Docker/Azure 的结合](#与-dockerazure-的结合)
  - [常见问题排查](#常见问题排查)

---

## Airflow 是什么？

Apache Airflow 是一个 **编排工具**，用于设计、调度和监控数据管道。

* 支持 Python 编写的工作流脚本
* 原生支持定时调度、依赖管理、重试机制、监控
* 可扩展，支持插件、自定义 Operator
* UI 监控界面清晰直观

---

## 核心概念

### 1. DAG (Directed Acyclic Graph)

* 有向无环图，是 Airflow 的基本单元
* 定义任务之间的执行顺序
* 不执行任务，只定义结构
* 用 Python 脚本表示，必须保存在 `$AIRFLOW_HOME/dags/` 目录中

```python
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="example_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)
```

### 2. Task

* DAG 中的每个节点就是一个 Task
* 每个 Task 是 Operator 的一个实例

### 3. Operator

Operator 是 Task 的模板：

| 类型                     | 描述           |
| ---------------------- | ------------ |
| `PythonOperator`       | 执行 Python 函数 |
| `BashOperator`         | 执行 Bash 命令   |
| `EmailOperator`        | 发送邮件         |
| `DummyOperator`        | 占位符          |
| `BranchPythonOperator` | 条件分支         |
| `DockerOperator`       | 执行 Docker 容器 |
| `PostgresOperator`     | 执行 SQL       |

### 4. Scheduler

* 不断扫描 DAGs，并根据 schedule\_interval 执行任务
* 是 Airflow 的“大脑”

### 5. Webserver

* 提供可视化界面，查看 DAG 状态、触发运行、查看日志等

### 6. Metadata Database

* 存储 DAG 的状态、任务执行结果、日志等元信息
* 默认使用 SQLite（生产建议使用 PostgreSQL 或 MySQL）

---

## 安装与环境配置

### 快速安装（本地开发）

```bash
# 建议使用 Python 3.8+
pip install apache-airflow==2.8.1 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"
```

或使用 Docker：

```bash
git clone https://github.com/apache/airflow.git
cd airflow
docker-compose up airflow-init
docker-compose up
```

### 设置环境变量

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

---

## DAG 文件结构与语法

### 一个典型 DAG 文件

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "my_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example"]
) as dag:

    task1 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello Airflow!'"
    )

    task2 = BashOperator(
        task_id="say_goodbye",
        bash_command="echo 'Goodbye!'"
    )

    task1 >> task2  # task1 先执行，然后是 task2
```

---

## 常见 Operator 用法

### PythonOperator

```python
from airflow.operators.python import PythonOperator

def my_function():
    print("Running Python Task")

task = PythonOperator(
    task_id="python_task",
    python_callable=my_function
)
```

### BashOperator

```python
BashOperator(
    task_id="list_files",
    bash_command="ls -l /"
)
```

### BranchPythonOperator

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch():
    return "task_a" if datetime.now().day % 2 == 0 else "task_b"

branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch
)
```

---

## 任务依赖与执行流程

```python
t1 >> t2  # t1 先执行，再执行 t2
t2.set_upstream(t1)
t1.set_downstream(t2)

# 多任务依赖
[t1, t2] >> t3  # t3 要等 t1 和 t2 都完成
```

---

## Airflow 命令大全

| 命令                                                 | 作用                   |
| -------------------------------------------------- | -------------------- |
| `airflow db init`                                  | 初始化数据库               |
| `airflow webserver`                                | 启动 Web UI（默认端口 8080） |
| `airflow scheduler`                                | 启动调度器                |
| `airflow dags list`                                | 查看所有 DAG             |
| `airflow tasks list dag_id`                        | 查看某个 DAG 中的任务        |
| `airflow tasks test dag_id task_id execution_date` | 测试单个任务（不记录数据库）       |

---

## 调试技巧与日志查看

* Web UI 中点击 DAG → 点击 Task 查看日志
* 日志路径：`$AIRFLOW_HOME/logs/`
* 使用 `airflow tasks test` 来验证 PythonOperator 函数是否正常

---

## 高级功能

### 1. Trigger Rules

控制任务是否执行的策略：

```python
trigger_rule="all_success"  # 默认值
trigger_rule="all_failed"
trigger_rule="one_success"
trigger_rule="none_failed"
```

### 2. TaskGroup（任务分组）

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("etl_tasks") as etl:
    extract = BashOperator(...)
    transform = BashOperator(...)
    load = BashOperator(...)

etl >> another_task
```

### 3. Branching（分支）

* 使用 `BranchPythonOperator` 实现条件逻辑

### 4. XCom（任务间通信）

```python
# 推送
task_instance.xcom_push(key='value', value=123)

# 拉取
task_instance.xcom_pull(task_ids='my_task', key='value')
```

### 5. SLA（服务级别协议）

```python
DAG(
    dag_id="example_sla",
    sla_miss_callback=notify_func,
    ...
)

PythonOperator(
    task_id='slow_task',
    python_callable=slow_func,
    sla=timedelta(minutes=5)
)
```

---

## 与 Docker/Azure 的结合

* 可用 Docker Compose 快速搭建本地开发环境
* Azure 提供 Airflow 托管服务（例如通过 Azure Data Factory 触发）
* 可通过 Azure Container Instances / Kubernetes + Helm 部署生产环境

---

## 常见问题排查

| 问题          | 原因                  | 解决方案                           |
| ----------- | ------------------- | ------------------------------ |
| DAG 无法识别    | Python 文件有语法错误      | 运行 `python my_dag.py` 看是否报错    |
| 任务卡住不动      | Scheduler 没有启动或资源不足 | 确保 `airflow scheduler` 正常运行    |
| 日志无法查看      | 没有配置日志路径            | 配置 `airflow.cfg` 中的 logging 设置 |
| Web UI 登陆失败 | 用户没创建或密码错误          | 使用 `airflow users create` 创建账户 |

---

