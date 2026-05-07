# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Delta Lake Introduction
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC Delta Lake is the foundation of everything in Databricks. If you've been writing Parquet files
# MAGIC with Spark, Delta is what you wish Parquet was — it adds ACID transactions, time travel,
# MAGIC schema enforcement, and efficient MERGE/UPSERT on top of the same columnar file format.
# MAGIC
# MAGIC **What you already know → What's new:**
# MAGIC - You write `df.write.parquet(...)` → Delta is `df.write.format("delta")...` — same API
# MAGIC - You handle idempotency manually in your `transform_daily.py` → Delta MERGE does this natively
# MAGIC - You've never been able to "undo" a bad write → Delta time travel lets you query any past version
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. Parquet vs Delta — what's actually different on disk
# MAGIC 2. Create a Delta table from a DataFrame
# MAGIC 3. MERGE / UPSERT — idempotent loads without duplicates
# MAGIC 4. Time travel — read previous versions
# MAGIC 5. DESCRIBE HISTORY — audit log built in

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parquet vs Delta Lake — What's Actually Different
# MAGIC
# MAGIC Parquet stores data in columnar files. Delta Lake stores data in **the same Parquet files**,
# MAGIC but adds a `_delta_log/` directory next to them. That directory is a transaction log —
# MAGIC every write (insert, update, delete, merge) appends a JSON entry describing what changed.
# MAGIC
# MAGIC ```
# MAGIC /my-table/
# MAGIC   _delta_log/
# MAGIC     00000000000000000000.json   ← "added file part-001.parquet"
# MAGIC     00000000000000000001.json   ← "added file part-002.parquet, removed part-001.parquet"
# MAGIC   part-001.parquet             ← old (logically deleted, still on disk until VACUUM)
# MAGIC   part-002.parquet             ← current
# MAGIC ```
# MAGIC
# MAGIC This gives you:
# MAGIC - **ACID transactions**: two writers don't corrupt each other
# MAGIC - **Time travel**: the log knows which files were "current" at version N
# MAGIC - **Schema enforcement**: rejects writes that don't match the table schema
# MAGIC - **Efficient MERGE**: updates only the affected Parquet files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a Delta Table from a DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit
import json

spark = SparkSession.builder.getOrCreate()

# Sample users data — same structure as DummyJSON API your production pipeline uses
users_data = [
    (1, "Emily",   "Johnson",  "emily.johnson@example.com",   28, "New York",     "NY", "Acme Corp",    "Engineering"),
    (2, "Michael", "Williams", "michael.williams@example.com", 35, "San Francisco","CA", "TechStart Inc","Product"),
    (3, "Sophia",  "Martinez", "sophia.martinez@example.com",  31, "Austin",       "TX", "Acme Corp",    "Data"),
    (4, "James",   "Brown",    "james.brown@example.com",      42, "Chicago",      "IL", "DataFlow LLC", "Engineering"),
    (5, "Olivia",  "Davis",    "olivia.davis@example.com",     26, "Seattle",      "WA", "TechStart Inc","Data"),
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

# Display is the Databricks replacement for .show()
# It renders an interactive table with sorting, filtering, and charting
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write as a Delta table
# MAGIC
# MAGIC In your production pipeline you write Parquet:
# MAGIC ```python
# MAGIC df.write.mode("overwrite").parquet("/mnt/silver/users")
# MAGIC ```
# MAGIC
# MAGIC With Delta, change the format — everything else is the same:

# COMMAND ----------

# Define a base path for this notebook's Delta tables
# In Community Edition, /FileStore/ persists between cluster restarts
BASE_PATH = "/FileStore/databricks-learning/delta-intro"

# Write as Delta table — same API as Parquet, just format("delta")
(df.write
   .format("delta")
   .mode("overwrite")
   .save(f"{BASE_PATH}/users"))

print(f"Delta table written to: {BASE_PATH}/users")

# Check what's actually on disk — notice the _delta_log directory
dbutils.fs.ls(f"{BASE_PATH}/users")

# COMMAND ----------

# Let's also look inside the _delta_log to demystify it
# Each JSON file is one transaction (one commit)
files_in_log = dbutils.fs.ls(f"{BASE_PATH}/users/_delta_log")
print("Transaction log files:")
for f in files_in_log:
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# Read the first transaction log entry to see what it contains
# This is what makes time travel and ACID possible
first_log = dbutils.fs.head(f"{BASE_PATH}/users/_delta_log/00000000000000000000.json", 2000)
print("First transaction log entry (truncated):")
print(first_log[:1500])

# COMMAND ----------

# Read the Delta table back — same as reading Parquet
df_read = spark.read.format("delta").load(f"{BASE_PATH}/users")
display(df_read)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MERGE / UPSERT — The Idempotent Load Pattern
# MAGIC
# MAGIC In your production pipeline (`transform_daily.py`), you handle idempotency by:
# MAGIC - Overwriting a partition (`mode("overwrite").partitionBy("load_date")`)
# MAGIC - Or reading existing data, deduplicating, then writing back
# MAGIC
# MAGIC Delta MERGE is cleaner — it's SQL's MERGE statement with Spark semantics:
# MAGIC - If a record with the same ID exists → UPDATE it
# MAGIC - If it doesn't exist → INSERT it
# MAGIC - Optionally: if it's in the target but not the source → DELETE it
# MAGIC
# MAGIC This is the same pattern as `INSERT ... ON CONFLICT DO UPDATE` in PostgreSQL.

# COMMAND ----------

# Simulate a daily load: 2 updated users + 1 new user
new_data = [
    # id=1: Emily got promoted (title changed)
    (1, "Emily",   "Johnson",  "emily.johnson@example.com",   28, "New York",     "NY", "Acme Corp",    "Platform Engineering"),
    # id=3: Sophia moved cities
    (3, "Sophia",  "Martinez", "sophia.martinez@example.com",  32, "Denver",       "CO", "Acme Corp",    "Data"),
    # id=6: new user
    (6, "Liam",    "Wilson",   "liam.wilson@example.com",     29, "Boston",       "MA", "DataFlow LLC", "Analytics"),
]

df_new = spark.createDataFrame(new_data, schema)

# Register as a temporary view so we can reference it in SQL MERGE
df_new.createOrReplaceTempView("users_staging")

# Register the Delta table path as a temp view too
spark.read.format("delta").load(f"{BASE_PATH}/users").createOrReplaceTempView("users_delta_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The MERGE statement
# MAGIC
# MAGIC You can use either the SQL API or the Python DeltaTable API.
# MAGIC SQL is more readable — use it when you can.

# COMMAND ----------

# Using spark.sql() — note we need to load the Delta table, not a view, as the target
# In a real Databricks setup you'd reference a registered table like `bronze.users`
# Here we patch around Community Edition limitations by re-registering

from delta.tables import DeltaTable

# Load the Delta table using the DeltaTable API (gives us merge, update, delete methods)
delta_table = DeltaTable.forPath(spark, f"{BASE_PATH}/users")

# MERGE: update existing rows, insert new rows
(delta_table.alias("target")
    .merge(
        df_new.alias("source"),
        condition="target.id = source.id"   # join key
    )
    .whenMatchedUpdateAll()      # if ID matches → overwrite all columns
    .whenNotMatchedInsertAll()   # if ID is new → insert
    .execute()
)

print("MERGE complete. Reading result:")

# COMMAND ----------

# Verify: Sophia's city should be Denver, Emily's department should be Platform Engineering
# and Liam (id=6) should now exist
df_after = spark.read.format("delta").load(f"{BASE_PATH}/users")
display(df_after.orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What happened under the hood:**
# MAGIC - Delta identified which Parquet files contained rows with id=1 and id=3
# MAGIC - It rewrote those files with the updated values
# MAGIC - It added a new file for id=6
# MAGIC - It wrote a new entry to `_delta_log/` recording exactly what changed
# MAGIC - The old Parquet files still exist on disk (needed for time travel) until you VACUUM

# COMMAND ----------

# Check the transaction log again — there's now a second entry for the MERGE
files_in_log = dbutils.fs.ls(f"{BASE_PATH}/users/_delta_log")
print("Transaction log files after MERGE:")
for f in files_in_log:
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time Travel — Read Previous Versions
# MAGIC
# MAGIC Because every version of the data is in the transaction log, you can read the table
# MAGIC as it was at any point in time. This is invaluable for:
# MAGIC - Auditing ("what did the data look like when we ran the report on Tuesday?")
# MAGIC - Recovering from a bad write ("I accidentally deleted users, restore from version 2")
# MAGIC - Reproducible ML training ("train on the same snapshot that was used 3 months ago")

# COMMAND ----------

# Read version 0 — the original 5 users, before the MERGE
df_v0 = (spark.read
    .format("delta")
    .option("versionAsOf", 0)    # version number
    .load(f"{BASE_PATH}/users"))

print("Version 0 (original — 5 users, before MERGE):")
display(df_v0.orderBy("id"))

# COMMAND ----------

# Read version 1 — after the MERGE (6 users, updated rows)
df_v1 = (spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(f"{BASE_PATH}/users"))

print("Version 1 (after MERGE — 6 users):")
display(df_v1.orderBy("id"))

# COMMAND ----------

# You can also use timestamp-based time travel
# This is useful when you don't know the version number
# Uncomment and replace with an actual timestamp from DESCRIBE HISTORY
# df_ts = (spark.read
#     .format("delta")
#     .option("timestampAsOf", "2024-01-15 10:00:00")
#     .load(f"{BASE_PATH}/users"))

# Or using the @v1 syntax in SQL:
# spark.sql(f"SELECT * FROM delta.`{BASE_PATH}/users@v1`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DESCRIBE HISTORY — Built-in Audit Log
# MAGIC
# MAGIC Every Delta table has a full audit history. This tells you:
# MAGIC - Who did what and when
# MAGIC - What operation was performed (WRITE, MERGE, DELETE, OPTIMIZE)
# MAGIC - How many rows were added/removed
# MAGIC - What Spark job triggered it

# COMMAND ----------

# DESCRIBE HISTORY using spark.sql on a Delta path
history_df = spark.sql(f"DESCRIBE HISTORY delta.`{BASE_PATH}/users`")
display(history_df.select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# The operationMetrics column shows counts — very useful for monitoring pipelines
# For a MERGE you'll see: numTargetRowsInserted, numTargetRowsUpdated, numTargetRowsDeleted
history_rows = history_df.collect()
for row in history_rows:
    print(f"Version {row['version']}: {row['operation']}")
    if row['operationMetrics']:
        for k, v in row['operationMetrics'].items():
            print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema Evolution (Bonus)
# MAGIC
# MAGIC Delta enforces schema by default — writing a DataFrame with extra columns will fail.
# MAGIC Enable schema evolution to automatically add new columns:

# COMMAND ----------

# Simulate a source with a new column: "phone"
data_with_phone = [
    (7, "Noah", "Taylor", "noah.taylor@example.com", 33, "Miami", "FL", "Acme Corp", "Sales", "+1-555-0107"),
]
schema_with_phone = schema.add("phone", StringType(), True)
df_with_phone = spark.createDataFrame(data_with_phone, schema_with_phone)

# This would FAIL without mergeSchema:
# df_with_phone.write.format("delta").mode("append").save(f"{BASE_PATH}/users")

# This WORKS — Delta adds the new column, existing rows get NULL for phone
(df_with_phone.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")   # allow new columns
    .save(f"{BASE_PATH}/users"))

print("Appended with schema evolution. New schema:")
df_evolved = spark.read.format("delta").load(f"{BASE_PATH}/users")
df_evolved.printSchema()

# COMMAND ----------

display(df_evolved.orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — What to Remember
# MAGIC
# MAGIC | Concept | Key API | When to use |
# MAGIC |---|---|---|
# MAGIC | Write Delta | `.format("delta")` | Always — never write bare Parquet in Databricks |
# MAGIC | Read Delta | `.format("delta").load(...)` | Same as Parquet |
# MAGIC | Upsert | `DeltaTable.merge(...).whenMatched...whenNotMatched...execute()` | Daily loads, CDC |
# MAGIC | Time travel by version | `.option("versionAsOf", N)` | Reproducibility, debugging |
# MAGIC | Time travel by timestamp | `.option("timestampAsOf", "2024-01-01")` | Auditing |
# MAGIC | Audit history | `DESCRIBE HISTORY delta.\`path\`` | Monitoring, compliance |
# MAGIC | Schema evolution | `.option("mergeSchema", "true")` | When upstream adds columns |
# MAGIC
# MAGIC **Next**: `02_spark_on_databricks.py` — Databricks Runtime vs vanilla Spark, `display()`, and `dbutils.fs`
