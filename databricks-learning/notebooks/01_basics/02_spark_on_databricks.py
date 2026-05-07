# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Spark on Databricks
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC You already know PySpark. Running it on Databricks is mostly the same, but with several
# MAGIC important differences that make your life easier — and a few Databricks-specific things
# MAGIC that have no equivalent in vanilla Spark.
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. Databricks Runtime vs vanilla Spark — what's different under the hood
# MAGIC 2. `display()` — the interactive table/chart viewer (replaces `.show()`)
# MAGIC 3. `dbutils.fs` — file system operations (replaces HDFS shell / S3 CLI)
# MAGIC 4. `%sql` magic — run SQL inline without `spark.sql()`
# MAGIC 5. Reading from DBFS vs S3
# MAGIC 6. Auto-optimize and auto-compact Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Databricks Runtime (DBR) vs Vanilla Spark
# MAGIC
# MAGIC When you start a cluster in Databricks, you choose a **Databricks Runtime** version
# MAGIC (e.g., DBR 17.x LTS). This is NOT vanilla Apache Spark. It's a Databricks-optimized
# MAGIC distribution that includes:
# MAGIC
# MAGIC | Vanilla Spark (your Docker setup) | Databricks Runtime 17 |
# MAGIC |---|---|
# MAGIC | Apache Spark 4.0.2 | Apache Spark 4.x + Databricks patches |
# MAGIC | Scala 2.13 | Scala 2.13 |
# MAGIC | Python 3.12 | Python 3.12 |
# MAGIC | You install Delta Lake manually | Delta Lake 4.x built in, always compatible |
# MAGIC | You manage Python deps | Pre-installed: pandas, numpy, scikit-learn, MLflow, etc. |
# MAGIC | Standard Volcano/Sort execution engine | **Photon** — vectorized C++ engine (10x faster on some queries) |
# MAGIC | No `display()`, no `dbutils` | `display()` and `dbutils` always available |
# MAGIC | `spark` not pre-initialized | `spark` and `sc` available automatically (no `.builder.getOrCreate()` needed) |
# MAGIC
# MAGIC **Practical implication**: In Databricks notebooks, you don't need to create a SparkSession.
# MAGIC `spark` is already there, globally available, in every cell.

# COMMAND ----------

# In a Databricks notebook, spark is pre-initialized — no need for SparkSession.builder
# This line works fine, but it's not necessary:
# spark = SparkSession.builder.getOrCreate()  # returns the existing session

# Check the runtime version
print(f"Spark version: {spark.version}")
print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'N/A')}")

# Check what node you're on
import socket
print(f"Driver hostname: {socket.gethostname()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. display() — The Interactive Table Viewer
# MAGIC
# MAGIC `display()` is a Databricks-only function. It renders DataFrames as an interactive HTML table
# MAGIC with sorting, filtering, pagination, and one-click chart creation.
# MAGIC
# MAGIC You already use `.show()` in your Spark jobs — `display()` is a strict upgrade.
# MAGIC Use `.show()` only when you want plain text output (e.g., in scheduled jobs writing to logs).

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, lit, when, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Recreate the sample dataset
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

# display() renders an interactive table — click column headers to sort, use the + button for charts
display(df)

# COMMAND ----------

# display() also works on:
# - SQL query results
# - Pandas DataFrames
# - Matplotlib figures
# - Lists of dicts

# Example: aggregate and display
agg_df = (df.groupBy("company_name", "department")
    .agg(count("id").alias("headcount"), avg("age").alias("avg_age"))
    .orderBy("company_name", "department"))

# Try clicking the "+" icon below → Bar Chart → Keys: company_name, Values: headcount
display(agg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dbutils.fs — Filesystem Operations
# MAGIC
# MAGIC `dbutils.fs` replaces the HDFS shell commands and S3 CLI you might use outside Databricks.
# MAGIC All paths use the `dbfs:/` scheme or short `/` notation (they're the same).
# MAGIC
# MAGIC | What you'd do outside Databricks | `dbutils.fs` equivalent |
# MAGIC |---|---|
# MAGIC | `aws s3 ls s3://bucket/path/` | `dbutils.fs.ls("/mnt/bucket/path/")` |
# MAGIC | `aws s3 cp src s3://dst` | `dbutils.fs.cp("src", "dst")` |
# MAGIC | `aws s3 mv src s3://dst` | `dbutils.fs.mv("src", "dst")` |
# MAGIC | `aws s3 rm -r s3://path/` | `dbutils.fs.rm("path", recurse=True)` |
# MAGIC | `head file.json` | `dbutils.fs.head("path/file.json")` |

# COMMAND ----------

# Create a directory for our experiments
WORK_DIR = "/FileStore/databricks-learning/spark-basics"
dbutils.fs.mkdirs(WORK_DIR)
print(f"Created: {WORK_DIR}")

# Write a small JSON file to DBFS
df.toPandas().to_json("/dbfs" + WORK_DIR + "/users.json", orient="records", indent=2)
# Note: /dbfs + path is the local filesystem path on the driver that maps to DBFS
# dbutils.fs.* uses dbfs:/ paths; Python's open() uses /dbfs/ paths

# COMMAND ----------

# List files
print("Files in work dir:")
for f in dbutils.fs.ls(WORK_DIR):
    size_kb = f.size / 1024
    print(f"  {f.name:<30} {size_kb:.1f} KB")

# COMMAND ----------

# Preview file content (first 500 bytes) — useful for debugging
print("File preview:")
print(dbutils.fs.head(f"{WORK_DIR}/users.json", 500))

# COMMAND ----------

# Copy a file
dbutils.fs.cp(f"{WORK_DIR}/users.json", f"{WORK_DIR}/users_backup.json")

# List again to confirm
print("After copy:")
for f in dbutils.fs.ls(WORK_DIR):
    print(f"  {f.name}")

# COMMAND ----------

# Remove a file
dbutils.fs.rm(f"{WORK_DIR}/users_backup.json")
print("After rm:")
for f in dbutils.fs.ls(WORK_DIR):
    print(f"  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading files from DBFS
# MAGIC
# MAGIC Reading files in Databricks uses the same Spark APIs you know, just with DBFS paths:

# COMMAND ----------

# Read the JSON file we just created using standard Spark read
df_from_dbfs = spark.read.option("multiLine", "true").json(f"{WORK_DIR}/users.json")
display(df_from_dbfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading from S3 (real cloud setup)
# MAGIC
# MAGIC In a production Databricks workspace, S3 is mounted to DBFS using mount points:
# MAGIC
# MAGIC ```python
# MAGIC # Mount S3 bucket (done once, persists across clusters in the same workspace)
# MAGIC dbutils.fs.mount(
# MAGIC     source="s3a://my-data-lake-bucket/",
# MAGIC     mount_point="/mnt/datalake",
# MAGIC     extra_configs={
# MAGIC         "fs.s3a.access.key": dbutils.secrets.get("aws", "access-key"),
# MAGIC         "fs.s3a.secret.key": dbutils.secrets.get("aws", "secret-key"),
# MAGIC     }
# MAGIC )
# MAGIC
# MAGIC # After mounting, read just like a local path:
# MAGIC df = spark.read.format("delta").load("/mnt/datalake/bronze/users")
# MAGIC
# MAGIC # Or use the s3a:// scheme directly (no mount needed):
# MAGIC df = spark.read.format("delta").load("s3a://my-data-lake-bucket/bronze/users")
# MAGIC ```
# MAGIC
# MAGIC This is equivalent to MinIO in your Docker setup — same S3 APIs, different endpoint URL.
# MAGIC With your MinIO setup you'd do the same but set `fs.s3a.endpoint` to your MinIO URL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. %sql Magic — Inline SQL
# MAGIC
# MAGIC You can switch to SQL in any cell with `%sql`. The result renders as an interactive table,
# MAGIC same as `display()`. This is much faster than writing `spark.sql("...")` for exploration.

# COMMAND ----------

# First, register the DataFrame as a temp view so we can query it with %sql
df.createOrReplaceTempView("users")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This cell runs as SQL — no Python needed
# MAGIC -- Great for quick exploration, ad-hoc queries, and showing results to non-engineers
# MAGIC SELECT
# MAGIC   company_name,
# MAGIC   department,
# MAGIC   COUNT(*) AS headcount,
# MAGIC   ROUND(AVG(age), 1) AS avg_age,
# MAGIC   MIN(age) AS youngest,
# MAGIC   MAX(age) AS oldest
# MAGIC FROM users
# MAGIC GROUP BY company_name, department
# MAGIC ORDER BY company_name, department

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can also query Delta tables directly with %sql
# MAGIC -- In production: SELECT * FROM catalog.schema.table
# MAGIC -- In Community Edition with path-based tables:
# MAGIC SELECT firstName, lastName, company_name, department
# MAGIC FROM users
# MAGIC WHERE age < 35
# MAGIC ORDER BY age

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. %fs Magic — Filesystem Shorthand
# MAGIC
# MAGIC `%fs` is a shorthand for `dbutils.fs` commands:

# COMMAND ----------

# MAGIC %fs ls /FileStore/databricks-learning/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Auto-Optimize and Auto-Compact for Delta Tables
# MAGIC
# MAGIC When you stream or do many small writes to a Delta table, you end up with many tiny
# MAGIC Parquet files — the "small files problem" that hurts query performance.
# MAGIC
# MAGIC Databricks solves this with two automatic features:
# MAGIC
# MAGIC **Auto Optimize** (Optimized Writes): Databricks automatically coalesces small files
# MAGIC during the write itself. Enabled by default in DBR 8.2+, on by default in DBR 17.
# MAGIC
# MAGIC **Auto Compact**: After each write, Databricks automatically runs a background
# MAGIC compaction if there are too many small files.
# MAGIC
# MAGIC You can enable these at the table level or the session level:

# COMMAND ----------

# Enable auto-optimize for this session (affects all Delta writes in this notebook)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Or enable for a specific table using table properties:
# spark.sql("""
#     ALTER TABLE bronze.users
#     SET TBLPROPERTIES (
#         'delta.autoOptimize.optimizeWrite' = 'true',
#         'delta.autoOptimize.autoCompact' = 'true'
#     )
# """)

print("Auto-optimize and auto-compact enabled for this session.")
print("Databricks will automatically manage file sizes during writes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate: write many small batches, see how auto-compact helps

# COMMAND ----------

import random

SMALL_WRITES_PATH = f"/FileStore/databricks-learning/small-writes-demo"

# Write 10 small batches — in Parquet this would create 10+ tiny files
for i in range(10):
    batch_data = [(i * 10 + j, f"user_{i}_{j}", f"user{i}{j}@test.com") for j in range(5)]
    batch_schema = "id INT, name STRING, email STRING"
    batch_df = spark.createDataFrame(batch_data, schema=batch_schema)

    (batch_df.write
        .format("delta")
        .mode("append")
        .save(SMALL_WRITES_PATH))

# Count the Parquet files — auto-compact may have already merged some
parquet_files = [f for f in dbutils.fs.ls(SMALL_WRITES_PATH) if f.name.endswith(".parquet")]
print(f"Parquet files after 10 small writes: {len(parquet_files)}")
print("Without auto-compact this would be 10+ files; Databricks merges them automatically.")

# COMMAND ----------

# Verify data integrity — all 50 rows should be there
df_small = spark.read.format("delta").load(SMALL_WRITES_PATH)
print(f"Total rows: {df_small.count()}")
display(df_small.orderBy("id").limit(10))

# COMMAND ----------

# Cleanup
dbutils.fs.rm(SMALL_WRITES_PATH, recurse=True)
print("Cleaned up small writes demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Key Differences: Vanilla Spark vs Databricks
# MAGIC
# MAGIC | Feature | Vanilla Spark | Databricks |
# MAGIC |---|---|---|
# MAGIC | SparkSession | `SparkSession.builder.getOrCreate()` | `spark` pre-initialized |
# MAGIC | Show DataFrame | `.show(n)` → plain text | `display(df)` → interactive table |
# MAGIC | File operations | `os`, `boto3`, HDFS CLI | `dbutils.fs.*` |
# MAGIC | SQL in notebook | `spark.sql("...")` | `%sql ...` magic cell |
# MAGIC | Inline filesystem | `os.system("ls ...")` | `%fs ls ...` magic cell |
# MAGIC | Small files | Run OPTIMIZE manually | Auto-compact runs automatically |
# MAGIC | Delta Lake | Install separately, configure | Built in, always compatible |
# MAGIC | Python deps | `pip install` in Dockerfile | Pre-installed, or `%pip install` |
# MAGIC
# MAGIC **Next**: `03_databricks_utilities.py` — Deep dive into dbutils: secrets, widgets, notebook chaining
