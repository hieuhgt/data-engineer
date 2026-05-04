# Apache Airflow - Complete Deep Dive

## 📚 Welcome to Airflow Mastery

This directory contains a **comprehensive journey from Junior to Senior Airflow engineer** with:

- 📖 **Complete guide** covering DAG design to enterprise operations
- 💻 **Runnable DAG examples** for each level
- 🎯 **Structured learning path** (week-by-week progression)
- 🏗️ **Production patterns** (error handling, monitoring, scheduling)
- 🚀 **Integration examples** (Spark, Kafka, Data Warehouse)
- 🔗 **Real-world orchestration** (multi-job workflows)

---

## 🎯 Quick Start

### For Beginners (Junior)
```bash
# 1. Install Airflow
pip install apache-airflow

# 2. Initialize database
airflow db init

# 3. Read the guide (start with Level 1)
grep -A 200 "LEVEL 1: JUNIOR" AIRFLOW_COMPLETE_GUIDE.md

# 4. Run examples
cd dags/
# Copy level1_junior_dags.py to ~/airflow/dags/
airflow dags list  # Should see your DAGs
```

### For Learning Path
```bash
# See the complete learning roadmap
cat AIRFLOW_LEARNING_PATH.md
```

### For Understanding Concepts
```bash
# Deep understanding of all topics
cat AIRFLOW_COMPLETE_GUIDE.md
```

---

## 📂 What's Here

```
airflow/
├── AIRFLOW_COMPLETE_GUIDE.md    ← Read this first! (Comprehensive)
├── AIRFLOW_LEARNING_PATH.md     ← Your learning roadmap
├── README.md                    ← This file
│
├── dags/
│   ├── 01_level1_basics.py       ← Simple DAGs (operators, tasks)
│   ├── 02_level2_intermediate.py ← Error handling, retries, dependencies
│   ├── 03_level3_advanced.py     ← Spark integration, dynamic DAGs
│   └── 04_level4_senior.py       ← Multi-tenant, complex workflows
│
├── plugins/
│   ├── custom_operators.py       ← Custom operators
│   └── hooks.py                  ← Database/API connections
│
├── config/
│   ├── airflow.cfg              ← Configuration
│   └── docker-compose.yml       ← Local Airflow setup
│
└── jobs/
    ├── spark_job.py             ← Spark job wrapper
    └── data_validation.py        ← Validation logic
```

---

## 📖 Learning Levels

### Level 1: Junior (Weeks 1-2)
**Goal:** Understand Airflow fundamentals and create working DAGs

**Topics:**
- What is Airflow? Architecture
- DAGs and tasks
- Operators (BashOperator, PythonOperator, BranchOperator)
- Task dependencies (>> operator)
- Scheduling (cron, intervals)
- Running DAGs locally

**You'll learn:**
- ✅ Create simple DAG
- ✅ Understand task dependencies
- ✅ Use basic operators
- ✅ Schedule execution

---

### Level 2: Intermediate (Weeks 3-5)
**Goal:** Build robust, production-ready DAGs

**Topics:**
- Error handling and retries
- Task groups and subDAGs
- XComs (passing data between tasks)
- Sensors (waiting for conditions)
- Branching logic
- Task monitoring

**You'll learn:**
- ✅ Handle failures gracefully
- ✅ Organize complex workflows
- ✅ Pass data between tasks
- ✅ Dynamic task creation

---

### Level 3: Advanced (Weeks 6-12)
**Goal:** Orchestrate complex data pipelines

**Topics:**
- Spark job orchestration
- Kafka monitoring
- Dynamic DAG generation
- SLA monitoring and alerting
- Multi-environment deployment
- Backfill and data recovery

**You'll learn:**
- ✅ Orchestrate Spark jobs at scale
- ✅ Monitor data pipeline health
- ✅ Handle SLA violations
- ✅ Deploy to production

---

### Level 4: Senior (Weeks 13-20)
**Goal:** Build enterprise-scale orchestration platform

**Topics:**
- Multi-tenant architecture
- Custom operators and plugins
- Cost optimization
- High availability and disaster recovery
- Security and access control
- Performance optimization

**You'll learn:**
- ✅ Design scalable platforms
- ✅ Implement security policies
- ✅ Optimize cost and performance
- ✅ Handle complex scenarios

---

## 💡 Key Concepts at a Glance

### DAG (Directed Acyclic Graph)
```
Think of it as a workflow blueprint:

Task A
  ↓
Task B → Task C
  ↓
Task D

Rules:
- Tasks execute in dependency order
- No cycles (Task A can't depend on Task D if D depends on A)
- Can run in parallel (B and C run together)
```

### Operators
```
Operators are types of tasks:

BashOperator:    Run shell command
PythonOperator:  Run Python function
SparkSubmitOperator: Submit Spark job
KubernetesPodOperator: Run in Kubernetes
BranchOperator:  Conditional logic
```

### Scheduling
```
schedule_interval: How often DAG runs

'@daily'             → Every day at midnight
'@hourly'            → Every hour
'0 2 * * *'          → 2 AM every day (cron)
'*/15 * * * *'       → Every 15 minutes (cron)
timedelta(hours=1)   → Every 1 hour
```

---

## 🔧 Local Setup

### Docker Compose
```bash
# Start Airflow
docker-compose -f config/docker-compose.yml up -d

# Initialize database
docker-compose exec airflow airflow db init

# Create user
docker-compose exec airflow airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@airflow.local

# Access UI
# http://localhost:8080
```

### Manual Installation
```bash
# Install
pip install apache-airflow

# Initialize
export AIRFLOW_HOME=~/airflow
airflow db init

# Create user
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin \
  --email admin@airflow.local

# Start scheduler and webserver
airflow scheduler  # Terminal 1
airflow webserver  # Terminal 2
```

---

## 📊 Learning Outcomes

### After Week 2 (JUNIOR)
- [ ] Understand Airflow architecture
- [ ] Create DAG with multiple tasks
- [ ] Define task dependencies
- [ ] Use basic operators
- [ ] Schedule execution

### After Week 5 (INTERMEDIATE)
- [ ] Handle errors and retries
- [ ] Create complex workflows
- [ ] Pass data between tasks
- [ ] Monitor DAG execution
- [ ] Use sensors effectively

### After Week 12 (ADVANCED)
- [ ] Orchestrate Spark jobs
- [ ] Monitor data pipeline SLAs
- [ ] Deploy to production
- [ ] Recover from failures
- [ ] Backfill data

### After Week 20 (SENIOR)
- [ ] Design multi-tenant systems
- [ ] Build custom operators
- [ ] Optimize costs
- [ ] Implement HA/DR
- [ ] Lead platform initiatives

---

## 🚨 Common Mistakes (Don't Make These!)

```python
# ❌ DON'T: Hard-code paths
task = PythonOperator(
    python_callable=my_func,
    op_kwargs={'path': '/home/user/data'}  # Will fail on different server!
)

# ✅ DO: Use variables and environment
from airflow.models import Variable
path = Variable.get("data_path", "/data")
task = PythonOperator(
    python_callable=my_func,
    op_kwargs={'path': path}
)

# ❌ DON'T: No error handling
def process_data():
    df = load_data()
    df.to_parquet("output/")  # What if file doesn't exist?

# ✅ DO: Handle errors
def process_data():
    try:
        df = load_data()
        df.to_parquet("output/")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise  # Let Airflow know about the failure

# ❌ DON'T: Schedule too frequently
dag = DAG('my_dag', schedule_interval='* * * * *')  # Every minute!
# Result: Massive overhead, usually not needed

# ✅ DO: Use appropriate frequency
dag = DAG('my_dag', schedule_interval='@daily')  # Once per day
```

---

## 📞 Quick Reference

### Create Simple DAG
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
)

def print_hello():
    print("Hello from Airflow!")

task1 = PythonOperator(
    task_id='print_task',
    python_callable=print_hello,
    dag=dag,
)

# task1 runs after task2
task2 >> task1
```

### Common Operators
```python
# Python
PythonOperator(python_callable=my_func)

# Bash
BashOperator(bash_command='echo "Hello"')

# Spark
SparkSubmitOperator(
    application='job.py',
    conf={'spark.executor.memory': '4g'}
)

# SQL
PostgresOperator(sql='SELECT * FROM table')

# Sensor (wait for something)
S3KeySensor(
    bucket_name='my-bucket',
    bucket_key='data/file.parquet',
    poke_interval=30,  # Check every 30s
    timeout=3600,  # Give up after 1 hour
)

# Branch (conditional)
@task(provide_context=True)
def decide_branch(context):
    if some_condition:
        return 'task_a'
    else:
        return 'task_b'
```

### XCom (Pass Data Between Tasks)
```python
# Task 1: Push data
def task1_func():
    return {'user_count': 100, 'status': 'success'}

task1 = PythonOperator(
    task_id='task1',
    python_callable=task1_func,
)

# Task 2: Pull data
def task2_func(context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='task1')
    print(f"Received: {data}")

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2_func,
    provide_context=True,
)

task1 >> task2
```

---

## 🔗 Integration Examples

### With Spark
See: `dags/03_level3_advanced.py`
```python
spark_task = SparkSubmitOperator(
    task_id='run_spark',
    application='spark_job.py',
    conf={'spark.executor.memory': '4g'}
)
```

### With Data Warehouse
```python
create_table = PostgresOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS results (id INT, value STRING)'
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_to_warehouse,
)

create_table >> load_data
```

### Monitoring SLA
```python
dag = DAG(
    'sla_dag',
    default_args={
        'owner': 'data-team',
        'sla': timedelta(hours=4),  # Must finish in 4 hours
    },
    schedule_interval='@daily',
)

# On SLA miss: Airflow sends alert
```

---

## 🎓 Interview Preparation

### Common Questions
1. **What is a DAG?** Directed acyclic graph representing workflow tasks
2. **Why Airflow?** Orchestrates complex workflows, handles failures, monitors SLAs
3. **How to handle failures?** Retries, alerts, SLA monitoring, task dependencies
4. **Difference: Airflow vs Cron?** Airflow has dependencies, monitoring, UI, error handling
5. **XCom use case?** Pass data between tasks without external storage

---

## 🎯 Next Steps

1. **Start with Level 1**: Read AIRFLOW_COMPLETE_GUIDE.md LEVEL 1
2. **Follow the path**: Use AIRFLOW_LEARNING_PATH.md as guide
3. **Build DAGs**: Create simple DAG, then complex one
4. **Integrate with Spark**: Combine with Spark jobs
5. **Deploy**: Move from local to production

---

## 📚 Additional Resources

- **Airflow Official Docs**: https://airflow.apache.org/docs/
- **Astronomer Academy**: https://academy.astronomer.io/
- **This Guide**: AIRFLOW_COMPLETE_GUIDE.md
- **Learning Path**: AIRFLOW_LEARNING_PATH.md
- **Examples**: `dags/` directory

---

**Ready? Start with AIRFLOW_COMPLETE_GUIDE.md LEVEL 1!** 🚀

**Questions? Check the complete guide for detailed explanations.**

**Want structure? Follow AIRFLOW_LEARNING_PATH.md for week-by-week guidance.**
