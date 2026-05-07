# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Databricks Utilities (dbutils)
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC `dbutils` is the Databricks utility library — it has no equivalent in vanilla Spark.
# MAGIC It gives you platform-level capabilities that you'd otherwise need separate tools for:
# MAGIC secrets management (like AWS SSM / HashiCorp Vault), notebook parameters (like Airflow template vars),
# MAGIC and notebook orchestration (like Airflow's `TriggerDagRunOperator`).
# MAGIC
# MAGIC **What you already know → What's new:**
# MAGIC - Airflow `{{ ds }}`, `{{ params.table }}` template variables → `dbutils.widgets`
# MAGIC - AWS SSM / .env files for secrets → `dbutils.secrets`
# MAGIC - Airflow `TriggerDagRunOperator` → `dbutils.notebook.run()`
# MAGIC - `aws s3 ls`, `boto3.client("s3")` → `dbutils.fs`
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. `dbutils.fs` — full filesystem API
# MAGIC 2. `dbutils.secrets` — vault integration
# MAGIC 3. `dbutils.widgets` — notebook parameters (like Airflow template variables)
# MAGIC 4. `dbutils.notebook.run()` — chain notebooks (like Airflow task chaining)
# MAGIC 5. `dbutils.data` — sampling utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dbutils.fs — Filesystem Operations (Full Reference)
# MAGIC
# MAGIC You already saw the basics in notebook 02. Here's the complete picture.

# COMMAND ----------

# All dbutils modules are pre-available — no import needed
# Run this to see everything dbutils can do:
dbutils.fs.help()

# COMMAND ----------

DEMO_DIR = "/FileStore/databricks-learning/dbutils-demo"
dbutils.fs.mkdirs(DEMO_DIR)

# Create some test files
for i in range(3):
    # Write directly to /dbfs path (Python's local filesystem view of DBFS)
    with open(f"/dbfs{DEMO_DIR}/file_{i}.txt", "w") as f:
        f.write(f"This is file {i}\nLine 2 of file {i}\n")

print("Created test files:")
for f in dbutils.fs.ls(DEMO_DIR):
    print(f"  {f.name:<20} {f.size} bytes  modificationTime={f.modificationTime}")

# COMMAND ----------

# head() — preview the beginning of a file (useful for debugging large files)
print("Preview of file_0.txt:")
print(dbutils.fs.head(f"{DEMO_DIR}/file_0.txt"))

# COMMAND ----------

# cp() — copy files or directories
dbutils.fs.mkdirs(f"{DEMO_DIR}/archive")
dbutils.fs.cp(f"{DEMO_DIR}/file_0.txt", f"{DEMO_DIR}/archive/file_0_backup.txt")

# cp with recurse=True copies entire directories
dbutils.fs.cp(DEMO_DIR, f"{DEMO_DIR}_copy", recurse=True)
print("After copy:")
for f in dbutils.fs.ls(f"{DEMO_DIR}_copy"):
    print(f"  {f.name}")

# COMMAND ----------

# mv() — move (rename) files
dbutils.fs.mv(f"{DEMO_DIR}/file_1.txt", f"{DEMO_DIR}/file_1_renamed.txt")
print("After rename:")
for f in dbutils.fs.ls(DEMO_DIR):
    print(f"  {f.name}")

# COMMAND ----------

# rm() — remove files or directories
dbutils.fs.rm(f"{DEMO_DIR}_copy", recurse=True)  # always use recurse=True for directories
print(f"Removed {DEMO_DIR}_copy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Programmatic ls() — useful in pipeline code
# MAGIC
# MAGIC A common pattern: list files in a landing zone, process only new ones.

# COMMAND ----------

# List all .txt files and filter/process programmatically
all_files = dbutils.fs.ls(DEMO_DIR)
txt_files = [f for f in all_files if f.name.endswith(".txt")]

print(f"Found {len(txt_files)} .txt files:")
for f in txt_files:
    print(f"  Path: {f.path}")
    print(f"  Size: {f.size} bytes")
    print(f"  Modified: {f.modificationTime}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dbutils.secrets — Vault Integration
# MAGIC
# MAGIC In your Docker setup you use `.env` files or environment variables for credentials.
# MAGIC In Databricks, secrets are stored in a **secret scope** (backed by Databricks or Azure Key Vault
# MAGIC / AWS Secrets Manager). You never hardcode credentials in notebooks.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Create a secret scope: `databricks secrets create-scope --scope my-scope` (CLI)
# MAGIC 2. Add a secret: `databricks secrets put --scope my-scope --key api-key` (CLI)
# MAGIC 3. Fetch it in notebook: `dbutils.secrets.get("my-scope", "api-key")`
# MAGIC
# MAGIC The secret value is **never printed** — Databricks redacts it from notebook output automatically.

# COMMAND ----------

# List available secret scopes (in Community Edition there may be none)
try:
    scopes = dbutils.secrets.listScopes()
    if scopes:
        print("Available secret scopes:")
        for scope in scopes:
            print(f"  {scope.name}")
    else:
        print("No secret scopes configured (expected in Community Edition).")
        print("In a real workspace you'd create scopes via the Databricks CLI.")
except Exception as e:
    print(f"Error listing scopes: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How you'd use secrets in a pipeline (production pattern)
# MAGIC
# MAGIC ```python
# MAGIC # Fetch database credentials from a secret scope
# MAGIC db_host     = dbutils.secrets.get(scope="prod-secrets", key="postgres-host")
# MAGIC db_password = dbutils.secrets.get(scope="prod-secrets", key="postgres-password")
# MAGIC
# MAGIC # Connect to a JDBC source — password never visible in notebook output
# MAGIC df = (spark.read
# MAGIC     .format("jdbc")
# MAGIC     .option("url", f"jdbc:postgresql://{db_host}:5432/mydb")
# MAGIC     .option("dbtable", "users")
# MAGIC     .option("user", "pipeline_user")
# MAGIC     .option("password", db_password)   # redacted in output automatically
# MAGIC     .load())
# MAGIC
# MAGIC # Fetch API key for DummyJSON or other services
# MAGIC api_key = dbutils.secrets.get(scope="prod-secrets", key="dummyjson-api-key")
# MAGIC headers = {"Authorization": f"Bearer {api_key}"}
# MAGIC ```
# MAGIC
# MAGIC **In Community Edition**: Create a scope via the CLI or REST API before using secrets.
# MAGIC See: https://docs.databricks.com/security/secrets/secret-scopes.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dbutils.widgets — Notebook Parameters
# MAGIC
# MAGIC Widgets are the Databricks equivalent of Airflow's template variables (`{{ ds }}`, `{{ params.table }}`).
# MAGIC They let you pass parameters to a notebook at runtime — either from the UI or from another
# MAGIC notebook/workflow calling this one.
# MAGIC
# MAGIC **Airflow → Databricks mapping:**
# MAGIC - `{{ ds }}` (execution date) → `dbutils.widgets.get("run_date")`
# MAGIC - `{{ params.batch_size }}` → `dbutils.widgets.get("batch_size")`
# MAGIC - `conf={"table": "users"}` in TriggerDagRunOperator → `arguments={"table": "users"}` in `notebook.run()`
# MAGIC
# MAGIC After running the cells below, you'll see a widget UI appear at the top of the notebook.
# MAGIC You can change values there and re-run cells — useful for interactive exploration.

# COMMAND ----------

# Remove any existing widgets first (clean slate)
dbutils.widgets.removeAll()

# COMMAND ----------

# Text widget — free-form input, like Airflow's {{ ds }}
dbutils.widgets.text(
    name="run_date",           # widget name (key to retrieve value)
    defaultValue="2024-01-15", # default if not provided
    label="Run Date"           # display label in the UI
)

# Dropdown widget — constrained choices
dbutils.widgets.dropdown(
    name="environment",
    defaultValue="dev",
    choices=["dev", "staging", "prod"],
    label="Environment"
)

# Combobox — dropdown with free-form option
dbutils.widgets.combobox(
    name="table_name",
    defaultValue="users",
    choices=["users", "products", "orders"],
    label="Table Name"
)

# Multiselect — choose multiple values
dbutils.widgets.multiselect(
    name="departments",
    defaultValue="Engineering",
    choices=["Engineering", "Data", "Product", "Analytics", "Sales"],
    label="Departments"
)

print("Widgets created! Look at the top of the notebook for the widget UI.")
print("Change values there, then run the next cell to see them take effect.")

# COMMAND ----------

# Read widget values — this is what you'd use in your pipeline logic
run_date    = dbutils.widgets.get("run_date")
environment = dbutils.widgets.get("environment")
table_name  = dbutils.widgets.get("table_name")
departments = dbutils.widgets.get("departments")  # returns comma-separated string

print(f"run_date:    {run_date}")
print(f"environment: {environment}")
print(f"table_name:  {table_name}")
print(f"departments: {departments}")

# Parse multi-select into a list
dept_list = [d.strip() for d in departments.split(",")]
print(f"dept_list:   {dept_list}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using widgets in pipeline code
# MAGIC
# MAGIC Here's how widgets replace Airflow template variables in a real pipeline:

# COMMAND ----------

from pyspark.sql.functions import lit, to_date
from datetime import datetime

# Simulate using widget values in pipeline logic — same as using {{ ds }} in Airflow
run_date = dbutils.widgets.get("run_date")
env      = dbutils.widgets.get("environment")

# Construct paths based on parameters — same as Airflow's date-partitioned paths
if env == "prod":
    base_path = "/mnt/prod-datalake"
else:
    base_path = f"/FileStore/databricks-learning/{env}"

bronze_path = f"{base_path}/bronze/users/run_date={run_date}"
print(f"Processing pipeline for date: {run_date}, env: {env}")
print(f"Bronze output path: {bronze_path}")

# Create sample data representing a daily batch
daily_users = [
    (1, "Emily",   "Johnson",  run_date),
    (2, "Michael", "Williams", run_date),
    (3, "Sophia",  "Martinez", run_date),
]
df_daily = spark.createDataFrame(daily_users, "id INT, firstName STRING, lastName STRING, run_date STRING")

print(f"\nWould write {df_daily.count()} records to {bronze_path}")
display(df_daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dbutils.notebook.run() — Chain Notebooks
# MAGIC
# MAGIC `dbutils.notebook.run()` is the Databricks equivalent of Airflow's `TriggerDagRunOperator`
# MAGIC or calling a Python function from another Airflow task.
# MAGIC
# MAGIC Use it to:
# MAGIC - Break large pipelines into modular notebooks
# MAGIC - Pass parameters between notebooks
# MAGIC - Handle notebook failures with timeouts
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```
# MAGIC orchestrator_notebook.py
# MAGIC   → dbutils.notebook.run("01_extract", 300, {"run_date": "2024-01-15"})
# MAGIC   → dbutils.notebook.run("02_transform", 600, {"run_date": "2024-01-15"})
# MAGIC   → dbutils.notebook.run("03_load", 300, {"run_date": "2024-01-15"})
# MAGIC ```
# MAGIC
# MAGIC This is similar to your Airflow DAG:
# MAGIC ```python
# MAGIC fetch_users >> transform_users >> load_to_silver >> aggregate_gold
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to pass return values between notebooks
# MAGIC
# MAGIC A child notebook can return a value using `dbutils.notebook.exit()`.
# MAGIC The parent receives it as a string from `dbutils.notebook.run()`.
# MAGIC
# MAGIC **Child notebook (e.g., "extract_users"):**
# MAGIC ```python
# MAGIC # ... do work ...
# MAGIC rows_written = df.count()
# MAGIC dbutils.notebook.exit(str(rows_written))   # must be a string
# MAGIC ```
# MAGIC
# MAGIC **Parent / orchestrator notebook:**
# MAGIC ```python
# MAGIC result = dbutils.notebook.run(
# MAGIC     path="/Users/me/extract_users",
# MAGIC     timeout_seconds=300,
# MAGIC     arguments={"run_date": "2024-01-15", "source_table": "users"}
# MAGIC )
# MAGIC rows_written = int(result)
# MAGIC print(f"Extract wrote {rows_written} rows")
# MAGIC
# MAGIC # Chain the next step
# MAGIC dbutils.notebook.run(
# MAGIC     path="/Users/me/transform_users",
# MAGIC     timeout_seconds=600,
# MAGIC     arguments={"run_date": "2024-01-15"}
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### %run vs dbutils.notebook.run()
# MAGIC
# MAGIC There are TWO ways to call another notebook:
# MAGIC
# MAGIC | `%run ./other_notebook` | `dbutils.notebook.run("path", timeout, args)` |
# MAGIC |---|---|
# MAGIC | Runs inline in the **same session** | Runs in a **separate session** |
# MAGIC | Variables from the other notebook are available here | No variable sharing — communicate via exit() |
# MAGIC | No timeout control | You set a timeout |
# MAGIC | Good for: shared utility functions | Good for: modular pipeline steps |
# MAGIC | Like: `from utils import *` | Like: `subprocess.run()` or `TriggerDagRunOperator` |
# MAGIC
# MAGIC Use `%run` for shared setup/config notebooks.
# MAGIC Use `dbutils.notebook.run()` for pipeline orchestration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulated orchestrator pattern
# MAGIC
# MAGIC Here's what a real orchestrator notebook looks like. In practice each `run()` call
# MAGIC is a separate notebook in your workspace:

# COMMAND ----------

from datetime import date

# In a real orchestrator, this comes from a widget
run_date = dbutils.widgets.get("run_date")

# Simulate the orchestration flow — each step would be a real notebook.run() call
pipeline_steps = [
    {"name": "01_extract_bronze",  "path": "/Users/me/batch_pipeline/01_extract",   "timeout": 300},
    {"name": "02_transform_silver","path": "/Users/me/batch_pipeline/02_transform",  "timeout": 600},
    {"name": "03_aggregate_gold",  "path": "/Users/me/batch_pipeline/03_aggregate",  "timeout": 300},
]

print(f"Starting pipeline for run_date={run_date}")
print("=" * 50)

for i, step in enumerate(pipeline_steps):
    print(f"Step {i+1}/{len(pipeline_steps)}: {step['name']}")
    print(f"  Would call: dbutils.notebook.run('{step['path']}', {step['timeout']}, {{'run_date': '{run_date}'}})")
    # In real code:
    # result = dbutils.notebook.run(step["path"], step["timeout"], {"run_date": run_date})
    # print(f"  Result: {result}")
    print(f"  Status: SIMULATED OK")

print("=" * 50)
print("Pipeline complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dbutils.data — Sampling Utilities
# MAGIC
# MAGIC `dbutils.data` has a useful `summarize()` function — like pandas' `describe()` but for Spark DataFrames:

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

users_data = [
    (1, "Emily",   "Johnson",  "emily.johnson@example.com",   28, "New York",      "NY", "Acme Corp",     "Engineering"),
    (2, "Michael", "Williams", "michael.williams@example.com", 35, "San Francisco", "CA", "TechStart Inc", "Product"),
    (3, "Sophia",  "Martinez", "sophia.martinez@example.com",  31, "Austin",        "TX", "Acme Corp",     "Data"),
    (4, "James",   "Brown",    "james.brown@example.com",      42, "Chicago",       "IL", "DataFlow LLC",  "Engineering"),
    (5, "Olivia",  "Davis",    "olivia.davis@example.com",     26, "Seattle",       "WA", "TechStart Inc", "Data"),
]

schema = StructType([
    StructField("id",           IntegerType(), False),
    StructField("firstName",    StringType(),  True),
    StructField("lastName",     StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("age",          IntegerType(), True),
    StructField("city",         StringType(),  True),
    StructField("state",        StringType(),  True),
    StructField("company_name", StringType(),  True),
    StructField("department",   StringType(),  True),
])

df = spark.createDataFrame(users_data, schema)

# summarize() gives a statistical summary — very useful for data quality checks in pipelines
dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.fs.rm(DEMO_DIR, recurse=True)
print("Widgets removed and demo files cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — dbutils Reference Card
# MAGIC
# MAGIC ### dbutils.fs
# MAGIC | Method | Description |
# MAGIC |---|---|
# MAGIC | `dbutils.fs.ls(path)` | List files, returns list of `FileInfo` objects |
# MAGIC | `dbutils.fs.mkdirs(path)` | Create directory (like `mkdir -p`) |
# MAGIC | `dbutils.fs.cp(src, dst, recurse=False)` | Copy file or directory |
# MAGIC | `dbutils.fs.mv(src, dst)` | Move / rename |
# MAGIC | `dbutils.fs.rm(path, recurse=False)` | Delete |
# MAGIC | `dbutils.fs.head(path, maxBytes=65536)` | Preview first N bytes |
# MAGIC | `dbutils.fs.put(path, contents)` | Write a string to a file |
# MAGIC | `dbutils.fs.mount(source, mount_point, extra_configs)` | Mount cloud storage |
# MAGIC
# MAGIC ### dbutils.secrets
# MAGIC | Method | Description |
# MAGIC |---|---|
# MAGIC | `dbutils.secrets.listScopes()` | List all secret scopes |
# MAGIC | `dbutils.secrets.list(scope)` | List keys in a scope (not values) |
# MAGIC | `dbutils.secrets.get(scope, key)` | Fetch a secret value (redacted in output) |
# MAGIC
# MAGIC ### dbutils.widgets
# MAGIC | Method | Description |
# MAGIC |---|---|
# MAGIC | `dbutils.widgets.text(name, default, label)` | Free-text input |
# MAGIC | `dbutils.widgets.dropdown(name, default, choices, label)` | Dropdown |
# MAGIC | `dbutils.widgets.combobox(name, default, choices, label)` | Dropdown + free input |
# MAGIC | `dbutils.widgets.multiselect(name, default, choices, label)` | Multi-select |
# MAGIC | `dbutils.widgets.get(name)` | Get current widget value |
# MAGIC | `dbutils.widgets.remove(name)` | Remove one widget |
# MAGIC | `dbutils.widgets.removeAll()` | Remove all widgets |
# MAGIC
# MAGIC ### dbutils.notebook
# MAGIC | Method | Description |
# MAGIC |---|---|
# MAGIC | `dbutils.notebook.run(path, timeout, arguments)` | Run notebook, return exit value |
# MAGIC | `dbutils.notebook.exit(value)` | Return a string value to the caller |
# MAGIC
# MAGIC **Next**: `02_etl/01_batch_pipeline.py` — Full Bronze→Silver→Gold medallion pipeline
