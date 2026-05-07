# Databricks Learning Path

A hands-on learning project for data engineers who already know PySpark, Airflow, Kafka, and MinIO.
These notebooks run on **Databricks Community Edition** (free) — not locally.

---

## How Databricks Relates to What You Already Know

| What you know | Databricks equivalent | Key difference |
|---|---|---|
| Apache Spark (standalone/YARN) | **Databricks Runtime (DBR)** | Pre-optimized Spark + Photon engine. No cluster setup. Delta Lake built in. |
| MinIO (S3-compatible object store) | **DBFS / S3 / Azure Data Lake** | DBFS is a virtual filesystem layered on cloud storage. Same S3 APIs work. |
| Airflow DAGs + operators | **Databricks Workflows** | Built into the platform. Runs notebooks, Python scripts, dbt, SQL. No scheduler to host. |
| Airflow `{{ ds }}` template vars | **dbutils.widgets** | Notebook parameters injected at runtime — same concept, Databricks syntax. |
| Spark Structured Streaming (Kafka) | **Auto Loader** | Reads new files from S3/DBFS as they land. Exactly-once. No Kafka required for file sources. |
| Parquet + manual schema management | **Delta Lake** | ACID transactions, time travel, schema enforcement, MERGE/UPSERT built in. |
| Hive metastore | **Unity Catalog** | 3-level namespace (catalog.schema.table). Column-level security, data lineage. |
| Your `transform_daily.py` Spark job | **Databricks notebook or Job** | Same PySpark code, runs managed on DBR. `display()` replaces `.show()`. |
| Your medallion pipeline (Bronze/Silver/Gold) | **Delta Lake medallion** | Same pattern, Delta tables instead of Parquet. MERGE makes Bronze→Silver idempotent. |

**Mental model**: Databricks = managed Spark + Delta Lake + Airflow + data catalog, all in one platform.
Your existing PySpark knowledge transfers almost 1:1. The new things to learn are Delta-specific APIs,
`dbutils`, and the platform's managed services.

---

## Setup: Getting a Databricks Environment

> **Community Edition no longer provides all-purpose clusters (interactive compute).**
> As of 2025, Community Edition only offers a Serverless SQL Warehouse, which runs SQL queries
> but does not support PySpark notebooks, `dbutils`, `display()`, or streaming jobs.
> To actually run these notebooks, use one of the options below.

### Option A — Databricks Free Trial (Recommended)

The 14-day free trial on AWS/Azure/GCP gives you full Databricks with all-purpose clusters:

1. Go to [databricks.com/try-databricks](https://www.databricks.com/try-databricks)
2. Sign up — no credit card required for the trial
3. Once logged in, create a cluster:
   - Click **Compute** → **Create compute**
   - Name it `learning-cluster`
   - Runtime: pick **Databricks Runtime 17.x LTS**
   - Single node (cheapest option for learning)
   - Click **Create compute** — first start takes ~5 minutes
4. Import notebooks:
   - Click **Workspace** → **Home** → your username folder
   - Click the three dots → **Import**
   - Upload `.py` files from the `notebooks/` directory in this project
   - Databricks reads the `# MAGIC %md` and `# COMMAND ----------` markers automatically

### Option B — Community Edition (SQL Only)

Community Edition is still free but limited to Serverless SQL Warehouse:

- You **can** run: `%sql` cells, Delta SQL queries, Unity Catalog browsing
- You **cannot** run: PySpark code, `dbutils`, `display()`, Auto Loader, streaming
- Useful for: `01_delta_lake_intro.py` (SQL sections), `04_advanced/02_unity_catalog.py` (SQL sections)

To use Community Edition: Go to [community.cloud.databricks.com](https://community.cloud.databricks.com),
sign in, and use the SQL Editor instead of notebooks for SQL-only exploration.

### What works where

| Notebook | Full trial (cluster) | Community Edition (SQL only) |
|---|---|---|
| `01_delta_lake_intro.py` | Full | SQL sections only |
| `02_spark_on_databricks.py` | Full | No (needs PySpark) |
| `03_databricks_utilities.py` | Full | No (needs dbutils) |
| `02_etl/01_batch_pipeline.py` | Full | No |
| `02_etl/02_autoloader_streaming.py` | Full | No |
| `03_workflows/01_jobs_and_tasks.py` | Full | No |
| `04_advanced/01_delta_optimization.py` | Full | SQL sections only |
| `04_advanced/02_unity_catalog.py` | Full | SQL sections only |

> **Study tip**: Even without a running environment, reading these notebooks teaches you the APIs
> and patterns. The code is self-documented — you can learn the concepts without running every cell.

---

## Learning Path

Work through these in order. Each notebook builds on the previous.

### Level 1 — Junior: Platform Basics (Week 1)

| Notebook | What you learn | Time |
|---|---|---|
| `01_basics/01_delta_lake_intro.py` | Delta Lake: ACID, MERGE, time travel | 1-2h |
| `01_basics/02_spark_on_databricks.py` | DBR vs vanilla Spark, display(), dbutils.fs | 1h |
| `01_basics/03_databricks_utilities.py` | dbutils.fs, secrets, widgets, notebook chaining | 1h |

### Level 2 — Mid: ETL Patterns (Week 2)

| Notebook | What you learn | Time |
|---|---|---|
| `02_etl/01_batch_pipeline.py` | Full medallion pipeline (same DummyJSON data as production) | 2h |
| `02_etl/02_autoloader_streaming.py` | Auto Loader: file-based streaming without Kafka | 1-2h |

### Level 3 — Senior: Platform Mastery (Week 3-4)

| Notebook | What you learn | Time |
|---|---|---|
| `03_workflows/01_jobs_and_tasks.py` | Databricks Workflows vs Airflow DAGs | 1-2h |
| `04_advanced/01_delta_optimization.py` | OPTIMIZE, ZORDER, VACUUM, Liquid clustering, Photon | 2h |
| `04_advanced/02_unity_catalog.py` | 3-level namespace, access control, data lineage | 1-2h |

---

## Key Concepts Reference

### Delta Lake
Delta Lake is the storage layer that makes Databricks tables reliable. Think of it as Parquet +
a transaction log (the `_delta_log/` directory). Every write is atomic, you can roll back to any
previous version, and MERGE/UPSERT is a first-class operation.

```python
# Write a Delta table (replaces: df.write.parquet(...))
df.write.format("delta").mode("overwrite").save("/mnt/datalake/bronze/users")

# Or as a managed table (preferred)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.users")

# Time travel — read yesterday's version
spark.read.format("delta").option("versionAsOf", 1).load("/mnt/datalake/bronze/users")
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(...)
```

### DBFS (Databricks File System)
DBFS is a virtual filesystem. `/dbfs/` on the driver maps to cloud storage (S3, ADLS, GCS).
In Community Edition, files in `/FileStore/` persist between cluster restarts.

```python
dbutils.fs.ls("/FileStore/")          # list files
dbutils.fs.cp("/FileStore/a", "/FileStore/b")   # copy
dbutils.fs.rm("/FileStore/old", recurse=True)   # delete
```

### dbutils — The Swiss Army Knife
`dbutils` is Databricks-specific. No equivalent in vanilla Spark.

```python
dbutils.fs.ls("/")                    # filesystem operations
dbutils.secrets.get("scope", "key")  # fetch secrets (like AWS SSM)
dbutils.widgets.get("run_date")       # notebook parameters (like Airflow {{ ds }})
dbutils.notebook.run("path", 60)      # call another notebook (like TriggerDagRunOperator)
```

### Auto Loader
Auto Loader (`cloudFiles` format) detects new files in S3/DBFS and processes them exactly once.
It replaces the Kafka → Spark Structured Streaming pattern for file-based sources.

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")
    .load("/mnt/landing/users/"))

df.writeStream.format("delta").option("checkpointLocation", "/checkpoint/bronze").start(...)
```

### Databricks Workflows
Workflows are Databricks' built-in orchestrator. You define jobs via the UI or the REST API.
No YAML DAG files, no Airflow server to maintain.

```json
{
  "name": "daily_users_pipeline",
  "schedule": {"quartz_cron_expression": "0 0 6 * * ?"},
  "tasks": [
    {"task_key": "bronze", "notebook_task": {"notebook_path": "/Users/.../01_batch_pipeline"}},
    {"task_key": "silver", "depends_on": [{"task_key": "bronze"}], ...}
  ]
}
```

### Unity Catalog
Unity Catalog is the governance layer. Tables live at `catalog.schema.table` (3-level namespace).

```sql
USE CATALOG my_company;
USE SCHEMA bronze;
SELECT * FROM users;                  -- full path: my_company.bronze.users

GRANT SELECT ON TABLE silver.users TO `analyst@company.com`;
```

---

## Databricks Cheat Sheet

### Spark / Delta APIs

```python
# Display results with rich UI (replaces .show())
display(df)

# Create a Delta table
spark.sql("CREATE TABLE IF NOT EXISTS bronze.users USING DELTA LOCATION '/mnt/bronze/users'")

# MERGE (upsert) — the idempotent load pattern
spark.sql("""
  MERGE INTO silver.users AS target
  USING staging AS source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# Time travel
spark.read.format("delta").option("versionAsOf", 2).table("bronze.users")

# Table history
spark.sql("DESCRIBE HISTORY bronze.users").display()

# Optimize (compaction + Z-ordering)
spark.sql("OPTIMIZE silver.users ZORDER BY (company_name, department)")

# Vacuum (remove old files, default 7-day retention)
spark.sql("VACUUM silver.users RETAIN 168 HOURS")
```

### dbutils Quick Reference

```python
# Filesystem
dbutils.fs.ls("/FileStore/")
dbutils.fs.mkdirs("/FileStore/mydir")
dbutils.fs.cp("src", "dst", recurse=True)
dbutils.fs.mv("src", "dst")
dbutils.fs.rm("path", recurse=True)
dbutils.fs.head("/FileStore/file.json")   # preview first bytes

# Secrets
secret = dbutils.secrets.get(scope="my-scope", key="api-key")

# Widgets (notebook parameters)
dbutils.widgets.text("run_date", "2024-01-01", "Run Date")
run_date = dbutils.widgets.get("run_date")
dbutils.widgets.removeAll()

# Notebook chaining
result = dbutils.notebook.run("/Users/me/other_notebook", timeout_seconds=300,
                               arguments={"run_date": "2024-01-01"})

# Cluster info
spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
```

### Magic Commands (in notebook cells)

```
%sql   SELECT * FROM bronze.users LIMIT 10;
%sh    ls /dbfs/FileStore/
%fs    ls /FileStore/
%md    ## This is markdown
%run   ./other_notebook          -- run another notebook inline (shares session)
%pip   install pandas==2.0.0
```

---

## Project Files

```
databricks-learning/
├── README.md                          ← this file
├── notebooks/
│   ├── 01_basics/
│   │   ├── 01_delta_lake_intro.py     ← Delta Lake: ACID, MERGE, time travel
│   │   ├── 02_spark_on_databricks.py  ← DBR, display(), dbutils.fs
│   │   └── 03_databricks_utilities.py ← dbutils deep dive
│   ├── 02_etl/
│   │   ├── 01_batch_pipeline.py       ← Full Bronze→Silver→Gold medallion
│   │   └── 02_autoloader_streaming.py ← Auto Loader (file streaming)
│   ├── 03_workflows/
│   │   └── 01_jobs_and_tasks.py       ← Workflows vs Airflow
│   └── 04_advanced/
│       ├── 01_delta_optimization.py   ← OPTIMIZE, ZORDER, VACUUM, Photon
│       └── 02_unity_catalog.py        ← Governance and access control
└── data/
    └── users_sample.json              ← 5 sample users (DummyJSON format)
```

---

## Tips for Running Notebooks

- **Cluster startup**: Always wait for the cluster to be `Running` before attaching a notebook.
- **Session state**: If your cluster restarts (2h timeout), re-run all cells from the top — Spark
  session variables are lost.
- **Persistent storage**: Use `/FileStore/` for files you want to keep between restarts.
  Delta tables written to `/FileStore/` also persist.
- **Widgets**: Widget UI appears at the top of the notebook after the first cell that calls
  `dbutils.widgets.*`.
- **%sql magic**: Great for quick exploration. The results render as an interactive table/chart —
  much better than `.show()`.
- **Community Edition (2025)**: Only Serverless SQL Warehouse is available — no interactive compute.
  Use a free trial account to run PySpark notebooks. See the Setup section above.
