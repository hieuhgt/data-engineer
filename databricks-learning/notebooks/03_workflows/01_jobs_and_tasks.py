# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Databricks Workflows: Jobs and Tasks
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC You've built orchestration with Airflow. Databricks Workflows is the built-in alternative —
# MAGIC no scheduler server to host, no DAG files to deploy, no pip packages to maintain.
# MAGIC It lives inside your Databricks workspace.
# MAGIC
# MAGIC **Airflow → Databricks Workflows mapping:**
# MAGIC | Airflow concept | Databricks Workflows equivalent |
# MAGIC |---|---|
# MAGIC | DAG file (`daily_batch_pipeline.py`) | Job definition (UI or JSON/Terraform) |
# MAGIC | Task | Task |
# MAGIC | `BashOperator`, `PythonOperator` | Notebook task, Python script task |
# MAGIC | `SparkSubmitOperator` | Notebook task or Spark JAR task |
# MAGIC | `trigger_rule="all_done"` | `run_if: "ALL_DONE"` |
# MAGIC | `depends_on_past=True` | Task dependencies in the job |
# MAGIC | `{{ ds }}` | `{{job.parameters.run_date}}` |
# MAGIC | Airflow Variables | Job parameters |
# MAGIC | Airflow Connections | Databricks secrets |
# MAGIC | `email_on_failure=True` | Job notification settings |
# MAGIC | `retries=3, retry_delay=timedelta(minutes=5)` | `max_retries: 3, min_retry_interval_millis: 300000` |
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. Job anatomy: tasks, dependencies, schedules
# MAGIC 2. Job JSON definition (equivalent of a DAG file)
# MAGIC 3. Equivalent of your `daily_batch_pipeline.py` Airflow DAG
# MAGIC 4. How to run notebooks programmatically from a task
# MAGIC 5. Task types: notebooks, Python scripts, dbt, SQL
# MAGIC 6. Monitoring and notifications
# MAGIC
# MAGIC > **Note**: Workflows UI is not available in Community Edition.
# MAGIC > This notebook explains the concepts and shows the JSON definitions.
# MAGIC > Create the job via the Jobs API or in a paid workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Workflow Anatomy
# MAGIC
# MAGIC A Databricks Workflow (Job) has this structure:
# MAGIC
# MAGIC ```
# MAGIC Job
# MAGIC ├── name: "daily_users_pipeline"
# MAGIC ├── schedule: "0 6 * * *"   (cron: every day at 6am)
# MAGIC ├── parameters: {run_date: "{{current_date}}"}
# MAGIC ├── clusters: [shared_job_cluster]
# MAGIC └── tasks:
# MAGIC     ├── task: ingest_bronze
# MAGIC     │   ├── type: notebook_task
# MAGIC     │   ├── notebook_path: /Users/.../02_etl/01_batch_pipeline
# MAGIC     │   └── depends_on: []
# MAGIC     ├── task: transform_silver
# MAGIC     │   ├── type: notebook_task
# MAGIC     │   └── depends_on: [ingest_bronze]
# MAGIC     └── task: aggregate_gold
# MAGIC         ├── type: notebook_task
# MAGIC         └── depends_on: [transform_silver]
# MAGIC ```
# MAGIC
# MAGIC This is your `daily_batch_pipeline.py` Airflow DAG, but defined in JSON instead of Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Your Airflow DAG → Databricks Workflow JSON
# MAGIC
# MAGIC Here's your production `daily_batch_pipeline.py` DAG reimplemented as a Databricks Workflow.
# MAGIC This JSON can be imported via the Workflows UI, REST API, or Terraform.

# COMMAND ----------

# This is what your Airflow DAG looks like as a Databricks Workflow JSON
# You'd paste this into the Jobs API or the Workflow JSON editor in the UI

workflow_definition = {
    "name": "daily_users_pipeline",

    # Schedule: every day at 06:00 UTC — same as your Airflow schedule_interval
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",   # Quartz format (not Unix cron)
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"                  # "PAUSED" to disable without deleting
    },

    # Job-level parameters — equivalent of Airflow's default_args and params
    # These are passed to every task automatically
    "parameters": [
        {"name": "run_date",   "default": "{{job.start_time.iso_date}}"},  # like {{ ds }}
        {"name": "env",        "default": "prod"},
        {"name": "api_limit",  "default": "100"},
    ],

    # Shared cluster — reused across tasks (cheaper than a new cluster per task)
    # In Airflow terms: this is your Spark cluster configuration
    "job_clusters": [
        {
            "job_cluster_key": "shared_compute",
            "new_cluster": {
                "spark_version": "17.0.x-scala2.13",
                "node_type_id":  "i3.xlarge",
                "num_workers":   2,
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled":   "true"
                }
            }
        }
    ],

    # Email notifications on failure — same as Airflow's email_on_failure
    "email_notifications": {
        "on_failure": ["trunghieu280999@gmail.com"],
        "on_success": [],
        "no_alert_for_skipped_runs": True
    },

    # Tasks — equivalent of Airflow tasks
    "tasks": [
        # Task 1: Ingest Bronze (equivalent of your fetch_users + load_bronze Airflow tasks)
        {
            "task_key": "ingest_bronze",
            "description": "Fetch users from DummyJSON API and write to Bronze Delta table",

            # Notebook task — runs a Databricks notebook
            "notebook_task": {
                "notebook_path": "/Users/trunghieu280999@gmail.com/databricks-learning/notebooks/02_etl/01_batch_pipeline",
                # Override job parameters for this specific task if needed
                "base_parameters": {
                    "layer": "bronze"
                }
            },

            "job_cluster_key": "shared_compute",
            "depends_on": [],  # no dependencies — this is the first task

            # Retry configuration — equivalent of Airflow's retries + retry_delay
            "max_retries": 3,
            "min_retry_interval_millis": 300_000,  # 5 minutes between retries
            "retry_on_timeout": False,

            "timeout_seconds": 1800,  # 30 minutes max — fail if longer
        },

        # Task 2: Transform Silver
        # (equivalent of your transform_users Airflow task using SparkSubmitOperator)
        {
            "task_key": "transform_silver",
            "description": "Cleanse Bronze data and merge into Silver Delta table",

            "notebook_task": {
                "notebook_path": "/Users/trunghieu280999@gmail.com/databricks-learning/notebooks/02_etl/01_batch_pipeline",
                "base_parameters": {
                    "layer": "silver"
                }
            },

            "job_cluster_key": "shared_compute",
            "depends_on": [{"task_key": "ingest_bronze"}],  # runs after Bronze succeeds

            "max_retries": 2,
            "min_retry_interval_millis": 60_000,
            "timeout_seconds": 3600,
        },

        # Task 3: Aggregate Gold
        {
            "task_key": "aggregate_gold",
            "description": "Aggregate Silver into company/department stats for Gold layer",

            "notebook_task": {
                "notebook_path": "/Users/trunghieu280999@gmail.com/databricks-learning/notebooks/02_etl/01_batch_pipeline",
                "base_parameters": {
                    "layer": "gold"
                }
            },

            "job_cluster_key": "shared_compute",
            "depends_on": [{"task_key": "transform_silver"}],

            "max_retries": 1,
            "timeout_seconds": 1800,
        },

        # Task 4: Data Quality Check (runs after all layers complete)
        # This is like an Airflow task with trigger_rule="all_success"
        {
            "task_key": "data_quality_check",
            "description": "Validate row counts and data quality across all layers",

            "python_wheel_task": {
                # Can also be a Python script task or a SQL task
                "package_name": "pipeline_dq",
                "entry_point":  "run_checks",
                "parameters":   ["--run-date", "{{job.parameters.run_date}}"]
            },

            # Alternative: use a notebook task
            # "notebook_task": {
            #     "notebook_path": "/Users/.../dq_checks",
            # },

            "job_cluster_key": "shared_compute",
            "depends_on": [{"task_key": "aggregate_gold"}],

            # run_if: equivalent of Airflow trigger_rule
            # ALL_SUCCESS (default), ALL_DONE, AT_LEAST_ONE_SUCCESS, NONE_FAILED, etc.
            "run_if": "ALL_SUCCESS",
        }
    ]
}

import json
print("Workflow JSON definition (equivalent of your Airflow DAG):")
print(json.dumps(workflow_definition, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Side-by-Side: Airflow DAG vs Databricks Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Airflow DAG (`daily_batch_pipeline.py`)**
# MAGIC
# MAGIC ```python
# MAGIC from airflow import DAG
# MAGIC from airflow.operators.python import PythonOperator
# MAGIC from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC default_args = {
# MAGIC     "owner": "data-team",
# MAGIC     "retries": 3,
# MAGIC     "retry_delay": timedelta(minutes=5),
# MAGIC     "email_on_failure": True,
# MAGIC     "email": ["trunghieu280999@gmail.com"],
# MAGIC }
# MAGIC
# MAGIC with DAG(
# MAGIC     "daily_batch_pipeline",
# MAGIC     default_args=default_args,
# MAGIC     schedule_interval="0 6 * * *",
# MAGIC     start_date=datetime(2024, 1, 1),
# MAGIC     catchup=False,
# MAGIC ) as dag:
# MAGIC
# MAGIC     fetch_users = PythonOperator(
# MAGIC         task_id="fetch_users",
# MAGIC         python_callable=fetch_users_from_api,
# MAGIC         op_kwargs={"run_date": "{{ ds }}"},
# MAGIC     )
# MAGIC
# MAGIC     transform_users = SparkSubmitOperator(
# MAGIC         task_id="transform_users",
# MAGIC         application="spark/transform_daily.py",
# MAGIC         conf={"spark.executor.memory": "2g"},
# MAGIC     )
# MAGIC
# MAGIC     fetch_users >> transform_users
# MAGIC ```
# MAGIC
# MAGIC **Equivalent Databricks Workflow (JSON excerpt):**
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "daily_users_pipeline",
# MAGIC   "schedule": {"quartz_cron_expression": "0 0 6 * * ?"},
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "fetch_users",
# MAGIC       "notebook_task": {"notebook_path": "/Users/.../ingest"},
# MAGIC       "max_retries": 3,
# MAGIC       "min_retry_interval_millis": 300000
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "transform_users",
# MAGIC       "notebook_task": {"notebook_path": "/Users/.../transform"},
# MAGIC       "depends_on": [{"task_key": "fetch_users"}]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Key difference**: In Airflow, the DAG file is Python code that Airflow imports every 30 seconds
# MAGIC to discover tasks. In Databricks, the Workflow definition is stored in the platform — no file to
# MAGIC deploy, no scheduler process to keep running.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Task Types in Databricks Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Notebook Task (most common)
# MAGIC
# MAGIC Runs a Databricks notebook. Parameters become `dbutils.widgets` inside the notebook.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "ingest_bronze",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/Users/me/pipeline/ingest_bronze",
# MAGIC     "base_parameters": {
# MAGIC       "run_date": "{{job.parameters.run_date}}",
# MAGIC       "env": "prod"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Inside the notebook:
# MAGIC ```python
# MAGIC run_date = dbutils.widgets.get("run_date")   # "2024-01-15"
# MAGIC env      = dbutils.widgets.get("env")        # "prod"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Python Script Task
# MAGIC
# MAGIC Runs a plain Python script (not a notebook). Good for code that lives in a git repo
# MAGIC and shouldn't be in a notebook format.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "data_quality",
# MAGIC   "spark_python_task": {
# MAGIC     "python_file": "dbfs:/FileStore/scripts/dq_check.py",
# MAGIC     "parameters": ["--run-date", "2024-01-15"]
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Or from a git repo (Git integration):
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "data_quality",
# MAGIC   "spark_python_task": {
# MAGIC     "python_file": "scripts/dq_check.py"
# MAGIC   },
# MAGIC   "git_source": {
# MAGIC     "git_url": "https://github.com/myorg/pipeline",
# MAGIC     "git_branch": "main"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 SQL Task
# MAGIC
# MAGIC Runs a SQL query or a file of SQL queries against Databricks SQL.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "create_gold_view",
# MAGIC   "sql_task": {
# MAGIC     "query": {"query_id": "abc123"},
# MAGIC     "warehouse_id": "abc456"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 dbt Task
# MAGIC
# MAGIC Runs dbt models as a Workflow task — native dbt integration.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "run_dbt_models",
# MAGIC   "dbt_task": {
# MAGIC     "project_directory": "dbt/",
# MAGIC     "commands": ["dbt run --select silver+", "dbt test --select silver+"],
# MAGIC     "schema": "silver",
# MAGIC     "catalog": "my_catalog"
# MAGIC   },
# MAGIC   "git_source": {
# MAGIC     "git_url": "https://github.com/myorg/dbt-project",
# MAGIC     "git_branch": "main"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Trigger Modes and Parameters

# COMMAND ----------

# Quartz cron expressions — Databricks uses Quartz format, NOT Unix cron
# Quartz has 6-7 fields: second minute hour day-of-month month day-of-week [year]
# Unix cron has 5 fields: minute hour day-of-month month day-of-week

quartz_examples = {
    "Every day at 6am UTC":        "0 0 6 * * ?",
    "Every hour":                   "0 0 * * * ?",
    "Every 15 minutes":             "0 0/15 * * * ?",
    "Weekdays at 8am":              "0 0 8 ? * MON-FRI",
    "First day of month at midnight": "0 0 0 1 * ?",
    "Every Sunday at 2am":          "0 0 2 ? * SUN",
}

print("Quartz cron expression examples:")
print("(Databricks uses Quartz format — add a leading '0' for seconds vs Unix cron)")
print()
for desc, expr in quartz_examples.items():
    print(f"  {desc:<35} → {expr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job parameters and dynamic values
# MAGIC
# MAGIC Databricks Workflows support dynamic parameter substitution — like Airflow Jinja templates:
# MAGIC
# MAGIC | Airflow template | Databricks equivalent |
# MAGIC |---|---|
# MAGIC | `{{ ds }}` | `{{job.start_time.iso_date}}` |
# MAGIC | `{{ ds_nodash }}` | `{{job.start_time.iso_date_basic}}` |
# MAGIC | `{{ ts }}` | `{{job.start_time.iso_datetime}}` |
# MAGIC | `{{ params.my_var }}` | `{{job.parameters.my_var}}` |
# MAGIC | `{{ run_id }}` | `{{job.run_id}}` |
# MAGIC
# MAGIC ```json
# MAGIC "parameters": [
# MAGIC   {"name": "run_date", "default": "{{job.start_time.iso_date}}"},
# MAGIC   {"name": "run_id",   "default": "{{job.run_id}}"}
# MAGIC ]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Creating a Workflow via the REST API
# MAGIC
# MAGIC You can create and manage Workflows programmatically via the Databricks REST API.
# MAGIC This is the equivalent of deploying Airflow DAGs via CI/CD.

# COMMAND ----------

# Example: create a job via the REST API
# In a real setup you'd store the token in a secret
# This is read-only — just shows the pattern, doesn't actually create the job

def create_databricks_workflow(workspace_url, token, job_definition):
    """
    Create a Databricks Workflow via the REST API.

    In production:
    - workspace_url comes from an environment variable or secret
    - token comes from dbutils.secrets.get("databricks", "api-token")
    - job_definition is the JSON dict you saw above
    """
    import requests

    endpoint = f"{workspace_url}/api/2.1/jobs/create"
    headers  = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }

    response = requests.post(endpoint, headers=headers, json=job_definition)
    response.raise_for_status()
    job_id = response.json()["job_id"]
    print(f"Created job with ID: {job_id}")
    return job_id

# Example usage (not executed — replace with real values)
print("To create a workflow via API:")
print("""
  job_id = create_databricks_workflow(
      workspace_url = "https://adb-1234567890.azuredatabricks.net",
      token         = dbutils.secrets.get("databricks", "api-token"),
      job_definition = workflow_definition  # the JSON dict from cell above
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Triggering a Run Manually (with parameters)
# MAGIC
# MAGIC Equivalent of clicking "Trigger DAG" in Airflow UI, or using:
# MAGIC `airflow dags trigger daily_batch_pipeline --conf '{"run_date": "2024-01-15"}'`

# COMMAND ----------

# Trigger a one-off run via the REST API
def trigger_job_run(workspace_url, token, job_id, parameters=None):
    """
    Trigger a job run with optional parameter overrides.
    Equivalent of: airflow dags trigger <dag_id> --conf '{"key": "value"}'
    """
    import requests

    endpoint = f"{workspace_url}/api/2.1/jobs/run-now"
    headers  = {"Authorization": f"Bearer {token}"}
    payload  = {"job_id": job_id}

    if parameters:
        payload["job_parameters"] = parameters  # overrides job-level defaults

    response = requests.post(endpoint, headers=headers, json=payload)
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"Triggered run: {run_id}")
    return run_id

print("To trigger a manual run:")
print("""
  run_id = trigger_job_run(
      workspace_url = workspace_url,
      token         = token,
      job_id        = job_id,
      parameters    = {"run_date": "2024-01-15", "env": "prod"}
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monitoring and Observability
# MAGIC
# MAGIC In Airflow you monitor runs via the Airflow UI or query the metadata database.
# MAGIC In Databricks Workflows, monitoring is built in:

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you get out of the box:
# MAGIC
# MAGIC 1. **Job Run History**: Every run is logged with status, duration, start/end times
# MAGIC 2. **Task-level logs**: Each task shows Spark UI, stdout/stderr, and cluster logs
# MAGIC 3. **Gantt chart**: Visual timeline of task execution (like Airflow's Gantt view)
# MAGIC 4. **Email/Slack notifications**: On failure, success, or both
# MAGIC 5. **Metrics API**: Query run history programmatically
# MAGIC
# MAGIC ### Get run status via API:
# MAGIC
# MAGIC ```python
# MAGIC def get_run_status(workspace_url, token, run_id):
# MAGIC     import requests
# MAGIC     response = requests.get(
# MAGIC         f"{workspace_url}/api/2.1/jobs/runs/get",
# MAGIC         headers={"Authorization": f"Bearer {token}"},
# MAGIC         params={"run_id": run_id}
# MAGIC     )
# MAGIC     return response.json()["state"]
# MAGIC
# MAGIC state = get_run_status(workspace_url, token, run_id)
# MAGIC print(f"Run {run_id} state: {state['life_cycle_state']} / {state['result_state']}")
# MAGIC # life_cycle_state: PENDING, RUNNING, TERMINATED, SKIPPED
# MAGIC # result_state: SUCCESS, FAILED, TIMEDOUT, CANCELED
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Workflow Differences from Airflow
# MAGIC
# MAGIC | Feature | Airflow | Databricks Workflows |
# MAGIC |---|---|---|
# MAGIC | **DAG backfill** | `airflow dags backfill` | Manual re-runs per date (no native backfill) |
# MAGIC | **XCom (task outputs)** | `xcom_push/pull` | `dbutils.notebook.exit()` / task values |
# MAGIC | **Sensor tasks** | `FileSensor`, `HttpSensor` | Use a notebook with a polling loop |
# MAGIC | **Branch operator** | `BranchPythonOperator` | `if/else` logic inside a notebook |
# MAGIC | **Pools** | Airflow pools for concurrency | Job concurrency settings |
# MAGIC | **Custom operators** | Python class extending `BaseOperator` | Python script task or notebook |
# MAGIC | **Git sync** | `git pull` on scheduler | Native Git integration in Workflows |
# MAGIC | **Multiple DAGs** | Separate .py files | Separate Jobs |
# MAGIC | **Cross-DAG trigger** | `TriggerDagRunOperator` | Job API trigger, or `dbutils.notebook.run()` |
# MAGIC
# MAGIC **When Airflow is still better:**
# MAGIC - You need cross-platform orchestration (Spark + non-Spark tasks together)
# MAGIC - You have complex custom sensors or operators
# MAGIC - You want a single orchestrator for Databricks + AWS Glue + Redshift + etc.
# MAGIC - Your team is very experienced with Airflow and the migration cost isn't worth it
# MAGIC
# MAGIC **When Databricks Workflows is better:**
# MAGIC - All your pipelines run on Databricks (notebooks/Spark jobs)
# MAGIC - You want zero infrastructure to maintain
# MAGIC - You want native Delta table integration (e.g., trigger based on file arrival)
# MAGIC - You're building a Delta Live Tables pipeline (DLT has its own built-in scheduler)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Your `daily_batch_pipeline.py` Airflow DAG translates to a Databricks Workflow where:
# MAGIC - The DAG → a Job definition (JSON, created via UI or API)
# MAGIC - Tasks → Notebook tasks (same `dbutils.widgets.get()` for parameters)
# MAGIC - `>> ` task dependencies → `"depends_on": [{"task_key": "prev_task"}]`
# MAGIC - `{{ ds }}` → `{{job.start_time.iso_date}}`
# MAGIC - `retries=3` → `"max_retries": 3`
# MAGIC - `email_on_failure` → `"email_notifications": {"on_failure": [...]}`
# MAGIC
# MAGIC **Next**: `04_advanced/01_delta_optimization.py` — OPTIMIZE, ZORDER, VACUUM, Liquid clustering
