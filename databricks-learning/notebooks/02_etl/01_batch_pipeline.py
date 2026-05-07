# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Batch Pipeline: Bronze → Silver → Gold
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC This is the Databricks version of your production pipeline. You've already built this pattern
# MAGIC with Airflow + Spark + MinIO. Here we run the same medallion pipeline entirely inside a
# MAGIC Databricks notebook — no separate Spark cluster, no Airflow server, no MinIO.
# MAGIC
# MAGIC **Your production stack → Databricks equivalent:**
# MAGIC | Production | Databricks |
# MAGIC |---|---|
# MAGIC | Airflow DAG (`daily_batch_pipeline.py`) | This notebook (or a Workflow — see `03_workflows/`) |
# MAGIC | Spark job (`transform_daily.py`) | PySpark cells in this notebook |
# MAGIC | MinIO Bronze bucket | Delta table at `/FileStore/.../bronze/users` |
# MAGIC | MinIO Silver bucket | Delta table at `/FileStore/.../silver/users` |
# MAGIC | MinIO Gold bucket | Delta table at `/FileStore/.../gold/user_stats` |
# MAGIC | HTTP → Parquet → MinIO | HTTP → Delta table |
# MAGIC
# MAGIC **Same data source**: DummyJSON users API (https://dummyjson.com/users)
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. Setup: paths, widgets, parameters
# MAGIC 2. Bronze: fetch raw JSON from API, write to Delta as-is
# MAGIC 3. Silver: cleanse and flatten, MERGE to handle re-runs idempotently
# MAGIC 4. Gold: aggregate by company + department, overwrite partition
# MAGIC 5. Data quality checks
# MAGIC 6. Comparison with your production setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)
from delta.tables import DeltaTable
from datetime import datetime, date

# Widget: run_date parameter — equivalent to Airflow's {{ ds }}
# When called from a Workflow, this is set automatically.
# In the notebook UI, you can change it and re-run.
dbutils.widgets.removeAll()
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("api_limit", "10", "Number of users to fetch")

run_date  = dbutils.widgets.get("run_date")
api_limit = int(dbutils.widgets.get("api_limit"))

print(f"Pipeline parameters:")
print(f"  run_date  = {run_date}")
print(f"  api_limit = {api_limit}")

# COMMAND ----------

# Base paths for each medallion layer
# In production this would be /mnt/datalake/bronze etc. (mounted S3/ADLS)
# In Community Edition we use /FileStore/ which persists between restarts
BASE = "/FileStore/databricks-learning/medallion"
BRONZE_PATH = f"{BASE}/bronze/users"
SILVER_PATH = f"{BASE}/silver/users"
GOLD_PATH   = f"{BASE}/gold/user_stats"

# Create base directory
dbutils.fs.mkdirs(BASE)
print(f"Medallion base: {BASE}")
print(f"  Bronze: {BRONZE_PATH}")
print(f"  Silver: {SILVER_PATH}")
print(f"  Gold:   {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Layer — Ingest Raw Data
# MAGIC
# MAGIC **Bronze rule**: Store data exactly as received from the source. No transformations.
# MAGIC Add only metadata columns: `_ingested_at`, `_run_date`, `_source`.
# MAGIC
# MAGIC This is the same as your Airflow `fetch_users` task — but instead of writing JSON to MinIO,
# MAGIC we write a Delta table. Delta gives us ACID + time travel for free.

# COMMAND ----------

# Fetch raw data from DummyJSON API — same source as your production pipeline
print(f"Fetching {api_limit} users from DummyJSON API...")

url = f"https://dummyjson.com/users?limit={api_limit}"
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    users_raw = raw_data.get("users", [])
    print(f"Fetched {len(users_raw)} users from API")
except requests.RequestException as e:
    print(f"API call failed ({e}), using sample data instead")
    # Fallback to sample data — useful when no internet access
    users_raw = [
        {"id": 1, "firstName": "Emily",   "lastName": "Johnson",  "email": "emily.johnson@example.com",
         "age": 28, "gender": "female", "phone": "+1-555-0101",
         "address": {"address": "123 Main St", "city": "New York",      "state": "NY", "postalCode": "10001", "country": "United States"},
         "company": {"name": "Acme Corp",    "department": "Engineering", "title": "Software Engineer"}},
        {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael.williams@example.com",
         "age": 35, "gender": "male",   "phone": "+1-555-0102",
         "address": {"address": "456 Oak Ave", "city": "San Francisco", "state": "CA", "postalCode": "94102", "country": "United States"},
         "company": {"name": "TechStart Inc","department": "Product",     "title": "Product Manager"}},
        {"id": 3, "firstName": "Sophia",  "lastName": "Martinez", "email": "sophia.martinez@example.com",
         "age": 31, "gender": "female", "phone": "+1-555-0103",
         "address": {"address": "789 Pine Rd", "city": "Austin",        "state": "TX", "postalCode": "73301", "country": "United States"},
         "company": {"name": "Acme Corp",    "department": "Data",        "title": "Data Engineer"}},
        {"id": 4, "firstName": "James",   "lastName": "Brown",    "email": "james.brown@example.com",
         "age": 42, "gender": "male",   "phone": "+1-555-0104",
         "address": {"address": "321 Elm St",  "city": "Chicago",       "state": "IL", "postalCode": "60601", "country": "United States"},
         "company": {"name": "DataFlow LLC",  "department": "Engineering", "title": "Senior Data Engineer"}},
        {"id": 5, "firstName": "Olivia",  "lastName": "Davis",    "email": "olivia.davis@example.com",
         "age": 26, "gender": "female", "phone": "+1-555-0105",
         "address": {"address": "654 Maple Dr","city": "Seattle",       "state": "WA", "postalCode": "98101", "country": "United States"},
         "company": {"name": "TechStart Inc","department": "Data",        "title": "Analytics Engineer"}},
    ]
    print(f"Using {len(users_raw)} sample users")

# COMMAND ----------

# Convert raw JSON list to a Spark DataFrame
# Keep the nested structure — Bronze stores raw data with minimal schema enforcement
df_raw = spark.createDataFrame(users_raw)

# Add Bronze metadata columns — these are your audit/lineage fields
df_bronze = df_raw.withColumns({
    "_ingested_at": F.current_timestamp(),
    "_run_date":    F.lit(run_date),
    "_source":      F.lit("dummyjson_api"),
})

print("Bronze schema (raw + metadata):")
df_bronze.printSchema()

# COMMAND ----------

display(df_bronze.select("id", "firstName", "lastName", "age", "company", "address", "_run_date", "_ingested_at"))

# COMMAND ----------

# Write to Bronze Delta table — OVERWRITE by run_date partition
# This makes the Bronze layer idempotent: re-running for the same date overwrites that day's data
# Same concept as your MinIO write with partitionBy("load_date")
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_run_date = '{run_date}'")  # only overwrite this partition
    .save(BRONZE_PATH))

# Verify
bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
print(f"Bronze table: {bronze_count} rows at {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer — Cleanse and Flatten
# MAGIC
# MAGIC **Silver rule**: Cleanse, standardize, and flatten. Remove nulls. Enforce schema.
# MAGIC Silver is the "source of truth" that all business logic reads from.
# MAGIC
# MAGIC **Transformations (same as your `transform_daily.py`):**
# MAGIC - `firstName + lastName` → `full_name`
# MAGIC - Flatten `address` struct → individual columns
# MAGIC - Flatten `company` struct → individual columns
# MAGIC - Standardize types (age as integer, email lowercase)
# MAGIC - MERGE instead of overwrite → handles CDC/updates idempotently

# COMMAND ----------

# Read from Bronze — always read from the previous layer, not the source
df_bronze_read = spark.read.format("delta").load(BRONZE_PATH).filter(F.col("_run_date") == run_date)

print(f"Reading {df_bronze_read.count()} rows from Bronze for run_date={run_date}")

# COMMAND ----------

# Silver transformations — same logic as your transform_daily.py
df_silver = (df_bronze_read
    # Full name (same as your production firstName + " " + lastName)
    .withColumn("full_name", F.concat_ws(" ", F.col("firstName"), F.col("lastName")))

    # Flatten address struct
    .withColumn("street",      F.col("address.address"))
    .withColumn("city",        F.col("address.city"))
    .withColumn("state",       F.col("address.state"))
    .withColumn("postal_code", F.col("address.postalCode"))
    .withColumn("country",     F.col("address.country"))

    # Flatten company struct
    .withColumn("company_name", F.col("company.name"))
    .withColumn("department",   F.col("company.department"))
    .withColumn("job_title",    F.col("company.title"))

    # Standardize
    .withColumn("email", F.lower(F.col("email")))
    .withColumn("age",   F.col("age").cast("int"))

    # Add Silver metadata
    .withColumn("_silver_updated_at", F.current_timestamp())
    .withColumn("_run_date", F.lit(run_date))

    # Select only the columns we want in Silver — drop raw nested structs
    .select(
        "id", "full_name", "email", "age", "gender", "phone",
        "street", "city", "state", "postal_code", "country",
        "company_name", "department", "job_title",
        "_silver_updated_at", "_run_date"
    )
    # Drop records with null IDs — data quality gate
    .filter(F.col("id").isNotNull())
    # Drop duplicates by ID (keep latest)
    .dropDuplicates(["id"])
)

print("Silver schema (flattened):")
df_silver.printSchema()

# COMMAND ----------

display(df_silver.select("id", "full_name", "email", "age", "company_name", "department", "city"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE into Silver — Idempotent Upsert
# MAGIC
# MAGIC Unlike Bronze (where we overwrite by date), Silver uses MERGE so we can:
# MAGIC 1. Update existing users when their data changes
# MAGIC 2. Insert new users
# MAGIC 3. Run the pipeline multiple times safely (idempotent)
# MAGIC
# MAGIC This is better than your current production pattern of overwriting an entire partition
# MAGIC because it handles updates to individual records, not just date-partitioned rewrites.

# COMMAND ----------

# Check if Silver table exists — first run needs a CREATE, subsequent runs use MERGE
from pyspark.sql.utils import AnalysisException

try:
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    silver_exists = True
    print(f"Silver table exists — will MERGE {df_silver.count()} records")
except AnalysisException:
    silver_exists = False
    print("Silver table does not exist — will CREATE on first write")

# COMMAND ----------

if not silver_exists:
    # First run: create the table
    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH))
    print(f"Created Silver table with {df_silver.count()} rows")
else:
    # Subsequent runs: MERGE (upsert)
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (silver_table.alias("target")
        .merge(
            df_silver.alias("source"),
            condition="target.id = source.id"
        )
        .whenMatchedUpdateAll()    # update all columns if ID matches
        .whenNotMatchedInsertAll() # insert if ID doesn't exist
        .execute()
    )
    print(f"MERGE complete — Silver table updated")

# Verify Silver
silver_count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"Silver table: {silver_count} rows")

# COMMAND ----------

# Show Silver history — the DESCRIBE HISTORY acts as your pipeline audit log
# This replaces the Airflow task logs for data lineage
spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer — Aggregate by Company and Department
# MAGIC
# MAGIC **Gold rule**: Business-level aggregates. Optimized for reporting and dashboards.
# MAGIC One row per business entity (company + department), not per user.
# MAGIC
# MAGIC This is the same as your Gold layer in production — just in Delta instead of Parquet.

# COMMAND ----------

# Read from Silver
df_silver_read = spark.read.format("delta").load(SILVER_PATH)

# Aggregate: headcount and avg age per company + department
# Same logic as your production aggregate step
df_gold = (df_silver_read
    .groupBy("company_name", "department")
    .agg(
        F.count("id").alias("headcount"),
        F.round(F.avg("age"), 1).alias("avg_age"),
        F.min("age").alias("youngest_age"),
        F.max("age").alias("oldest_age"),
        F.collect_list("full_name").alias("members"),
        F.max("_run_date").alias("last_updated_run_date"),
    )
    .withColumn("_gold_computed_at", F.current_timestamp())
    .orderBy("company_name", "department")
)

print("Gold layer (aggregated by company + department):")
display(df_gold.select("company_name", "department", "headcount", "avg_age", "members"))

# COMMAND ----------

# Write Gold — overwrite completely each run (aggregates are always recomputed from Silver)
# Unlike Silver (MERGE), Gold is always a full recompute — same as your production pattern
(df_gold.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH))

gold_count = spark.read.format("delta").load(GOLD_PATH).count()
print(f"Gold table: {gold_count} rows (one per company+department combination)")

# COMMAND ----------

display(spark.read.format("delta").load(GOLD_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Checks
# MAGIC
# MAGIC Add checks after each layer write. In production you'd raise an exception to fail the job.
# MAGIC Databricks also has **Delta Live Tables expectations** for declarative DQ rules,
# MAGIC but manual checks work fine for learning.

# COMMAND ----------

def run_quality_checks(df, layer_name, checks):
    """
    Run a list of data quality checks on a DataFrame.
    Each check is a (description, boolean_expression) tuple.
    Prints PASS/FAIL for each check.
    """
    print(f"\nData Quality: {layer_name}")
    print("-" * 40)
    all_passed = True
    for description, check_expr in checks:
        result = df.filter(check_expr).count()
        total  = df.count()
        if result == 0:
            print(f"  PASS: {description}")
        else:
            print(f"  FAIL: {description} — {result}/{total} rows failed")
            all_passed = False
    return all_passed

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(SILVER_PATH)
df_gold_check   = spark.read.format("delta").load(GOLD_PATH)

silver_ok = run_quality_checks(df_silver_check, "Silver Users", [
    ("No null IDs",           F.col("id").isNull()),
    ("No null emails",        F.col("email").isNull()),
    ("Age between 18 and 99", ~F.col("age").between(18, 99)),
    ("Email contains @",      ~F.col("email").contains("@")),
    ("No null company_name",  F.col("company_name").isNull()),
])

gold_ok = run_quality_checks(df_gold_check, "Gold User Stats", [
    ("No null company_name",  F.col("company_name").isNull()),
    ("Headcount > 0",         F.col("headcount") <= 0),
    ("avg_age is reasonable", ~F.col("avg_age").between(18, 99)),
])

print(f"\nSilver DQ: {'PASSED' if silver_ok else 'FAILED'}")
print(f"Gold DQ:   {'PASSED' if gold_ok else 'FAILED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline Summary
# MAGIC
# MAGIC Let's confirm the full lineage from API to Gold:

# COMMAND ----------

print("=" * 60)
print(f"PIPELINE SUMMARY — run_date={run_date}")
print("=" * 60)

for layer, path in [("Bronze", BRONZE_PATH), ("Silver", SILVER_PATH), ("Gold", GOLD_PATH)]:
    df = spark.read.format("delta").load(path)
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").orderBy("version").tail(1)[0]
    print(f"\n  {layer}:")
    print(f"    Rows:      {df.count()}")
    print(f"    Path:      {path}")
    print(f"    Version:   {history['version']}")
    print(f"    Last write: {history['timestamp']}")
    print(f"    Operation: {history['operation']}")

print("\n" + "=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Production Pipeline vs Databricks
# MAGIC
# MAGIC | Step | Production (Airflow + Spark + MinIO) | Databricks |
# MAGIC |---|---|---|
# MAGIC | **Schedule** | Airflow DAG with cron | Databricks Workflow (see `03_workflows/`) |
# MAGIC | **Parameters** | `{{ ds }}` in Airflow template | `dbutils.widgets.get("run_date")` |
# MAGIC | **Extract** | `requests` in Airflow PythonOperator | `requests` in notebook cell (same) |
# MAGIC | **Bronze write** | `df.write.parquet(minio_path)` | `df.write.format("delta").save(dbfs_path)` |
# MAGIC | **Silver transform** | `spark-submit transform_daily.py` | PySpark cells in notebook |
# MAGIC | **Idempotent load** | Overwrite partition | Delta MERGE (more precise) |
# MAGIC | **Gold aggregate** | PySpark + overwrite Parquet | PySpark + overwrite Delta |
# MAGIC | **Audit trail** | Airflow task logs | `DESCRIBE HISTORY` on Delta table |
# MAGIC | **Time travel** | Not possible with Parquet | Free with Delta |
# MAGIC | **Schema enforcement** | Manual checks | Delta schema enforcement |
# MAGIC
# MAGIC **The code is nearly identical** — the main changes are:
# MAGIC 1. `.format("delta")` instead of `.parquet()`
# MAGIC 2. `dbutils.widgets.get()` instead of Airflow template vars
# MAGIC 3. MERGE instead of partition overwrite for Silver
# MAGIC 4. Everything runs in one place, no separate cluster/scheduler to manage
# MAGIC
# MAGIC **Next**: `02_autoloader_streaming.py` — Auto Loader for file-based streaming (replaces Kafka for file sources)

# COMMAND ----------

# Cleanup widgets
dbutils.widgets.removeAll()
