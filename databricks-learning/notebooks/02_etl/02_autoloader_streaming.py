# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Auto Loader: File-Based Streaming
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC You've built a streaming pipeline with Kafka → Spark Structured Streaming → MinIO.
# MAGIC Auto Loader solves a different but related problem: **what if your source is files on S3/DBFS,
# MAGIC not a Kafka topic?**
# MAGIC
# MAGIC **When Kafka streaming fits:**
# MAGIC - Real-time event streams (clickstream, IoT sensors, transactions)
# MAGIC - Source systems that push events actively
# MAGIC - Sub-second or low-second latency requirements
# MAGIC
# MAGIC **When Auto Loader fits:**
# MAGIC - Files landing in S3/DBFS on a schedule (partner feeds, CSV exports, API dumps)
# MAGIC - You want Spark Structured Streaming semantics (exactly-once, checkpoints) for files
# MAGIC - You're already on Databricks and don't want to manage a Kafka cluster
# MAGIC
# MAGIC **How Auto Loader compares to your Kafka pipeline:**
# MAGIC | Kafka Streaming (your setup) | Auto Loader |
# MAGIC |---|---|
# MAGIC | Source: Kafka topic | Source: S3/DBFS directory |
# MAGIC | Uses `format("kafka")` | Uses `format("cloudFiles")` |
# MAGIC | Checkpoint tracks Kafka offsets | Checkpoint tracks which files are processed |
# MAGIC | Kafka broker manages what's "new" | Auto Loader uses S3 notifications or list-based detection |
# MAGIC | Exactly-once with checkpoints | Exactly-once with checkpoints |
# MAGIC | Real-time (milliseconds-seconds) | Near-real-time (seconds-minutes) |
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. How Auto Loader works internally
# MAGIC 2. Set up a simulated file landing zone
# MAGIC 3. Read new files with `cloudFiles` format
# MAGIC 4. Write to a Bronze Delta table with checkpointing
# MAGIC 5. Add more files — watch Auto Loader pick them up
# MAGIC 6. Schema evolution in Auto Loader
# MAGIC 7. Compare to your `streaming_job.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. How Auto Loader Works
# MAGIC
# MAGIC Auto Loader uses one of two modes to detect new files:
# MAGIC
# MAGIC **Directory listing mode** (default, works everywhere):
# MAGIC - Periodically lists the source directory
# MAGIC - Compares to the checkpoint (list of already-processed files)
# MAGIC - Processes only new files
# MAGIC - Good for: any S3/DBFS path, low-volume
# MAGIC
# MAGIC **File notification mode** (production recommendation):
# MAGIC - Sets up S3 Event Notifications → SQS queue automatically
# MAGIC - New files trigger processing immediately via queue message
# MAGIC - Good for: high-volume, latency-sensitive, large directories
# MAGIC
# MAGIC Both modes guarantee **exactly-once processing** using a checkpoint directory —
# MAGIC same concept as Spark Structured Streaming with Kafka.
# MAGIC
# MAGIC ```
# MAGIC S3 Landing Zone              Auto Loader              Delta Table
# MAGIC /landing/users/          →   cloudFiles format    →   /bronze/users/
# MAGIC   2024-01-01.json              detects new files
# MAGIC   2024-01-02.json              reads exactly once
# MAGIC   2024-01-03.json    ←new      checkpoints progress
# MAGIC ```

# COMMAND ----------

import json
import time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Paths
LANDING_ZONE    = "/FileStore/databricks-learning/autoloader/landing"
BRONZE_PATH     = "/FileStore/databricks-learning/autoloader/bronze"
CHECKPOINT_PATH = "/FileStore/databricks-learning/autoloader/checkpoint"
SCHEMA_PATH     = "/FileStore/databricks-learning/autoloader/schema"

# Clean up from any previous runs
for path in [LANDING_ZONE, BRONZE_PATH, CHECKPOINT_PATH, SCHEMA_PATH]:
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass

dbutils.fs.mkdirs(LANDING_ZONE)
print("Paths initialized:")
print(f"  Landing zone:   {LANDING_ZONE}")
print(f"  Bronze output:  {BRONZE_PATH}")
print(f"  Checkpoint:     {CHECKPOINT_PATH}")
print(f"  Schema:         {SCHEMA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate a File Landing Zone
# MAGIC
# MAGIC In production, a partner/upstream system drops JSON files into S3.
# MAGIC Here we simulate that by writing files to DBFS.

# COMMAND ----------

def write_landing_file(batch_id, users):
    """Write a batch of users as a JSON file to the landing zone."""
    content = json.dumps(users)
    # Write via Python's /dbfs path (driver-local filesystem view)
    local_path = f"/dbfs{LANDING_ZONE}/batch_{batch_id:03d}.json"
    with open(local_path, "w") as f:
        f.write(content)
    print(f"  Written: batch_{batch_id:03d}.json ({len(users)} users)")

# Initial batch — 3 users (simulates Day 1 data drop)
batch_1_users = [
    {"id": 1, "firstName": "Emily",   "lastName": "Johnson",  "email": "emily@example.com",   "age": 28, "company": "Acme Corp",    "department": "Engineering"},
    {"id": 2, "firstName": "Michael", "lastName": "Williams", "email": "michael@example.com", "age": 35, "company": "TechStart Inc","department": "Product"},
    {"id": 3, "firstName": "Sophia",  "lastName": "Martinez", "email": "sophia@example.com",  "age": 31, "company": "Acme Corp",    "department": "Data"},
]

print("Landing Zone — Initial files:")
write_landing_file(1, batch_1_users)

# Verify
print("\nFiles in landing zone:")
for f in dbutils.fs.ls(LANDING_ZONE):
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read with Auto Loader (cloudFiles format)
# MAGIC
# MAGIC The key difference from your Kafka streaming:
# MAGIC
# MAGIC **Your Kafka streaming (`streaming_job.py`):**
# MAGIC ```python
# MAGIC df = (spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "localhost:9092")
# MAGIC     .option("subscribe", "users-topic")
# MAGIC     .load())
# MAGIC ```
# MAGIC
# MAGIC **Auto Loader (cloudFiles):**
# MAGIC ```python
# MAGIC df = (spark.readStream
# MAGIC     .format("cloudFiles")          # instead of "kafka"
# MAGIC     .option("cloudFiles.format", "json")   # file format inside the directory
# MAGIC     .option("cloudFiles.schemaLocation", schema_path)  # infer & store schema
# MAGIC     .load(landing_zone_path))      # directory, not a topic name
# MAGIC ```
# MAGIC
# MAGIC Both use Spark Structured Streaming — same `writeStream`, same `checkpointLocation`,
# MAGIC same trigger modes, same exactly-once guarantee.

# COMMAND ----------

# Define the streaming read with Auto Loader
# cloudFiles.format tells Auto Loader the format of the files in the directory
# cloudFiles.schemaLocation stores the inferred schema (persists across restarts)
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    # Directory listing mode (default) — good for Community Edition and non-S3 paths
    # For S3 in production, add: .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.inferColumnTypes", "true")  # infer int/string/etc.
    .load(LANDING_ZONE)
)

print("Auto Loader stream defined:")
print(f"  Reading from: {LANDING_ZONE}")
print(f"  Schema stored at: {SCHEMA_PATH}")
print(f"  Stream type: {type(df_stream)}")
print(f"  isStreaming: {df_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Bronze Delta with Checkpointing
# MAGIC
# MAGIC Same pattern as your Kafka streaming write — `writeStream` with `checkpointLocation`.
# MAGIC The checkpoint is what guarantees exactly-once: it records which files (or Kafka offsets)
# MAGIC have been committed to the output.

# COMMAND ----------

# Add metadata columns (same pattern as batch Bronze)
df_stream_with_meta = (df_stream
    .withColumn("_source_file",   F.input_file_name())  # which file this row came from
    .withColumn("_ingested_at",   F.current_timestamp())
    .withColumn("_autoloader_run", F.lit("streaming"))
)

# Write stream to Delta table
# trigger(availableNow=True) is like a "batch" trigger — processes all available data,
# then stops. Great for scheduled runs. (Same as .trigger(once=True) in older Spark versions.)
# For true continuous streaming, use .trigger(processingTime="30 seconds") instead.
streaming_query = (df_stream_with_meta.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)   # process all available files then stop
    .start(BRONZE_PATH)
)

# Wait for the trigger to finish (since we used availableNow=True)
streaming_query.awaitTermination()
print("First streaming batch complete!")

# COMMAND ----------

# Verify what was written
df_bronze = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze table has {df_bronze.count()} rows")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add More Files — Watch Auto Loader Process Only New Ones
# MAGIC
# MAGIC This is the key feature: Auto Loader tracks which files it has already processed
# MAGIC (in the checkpoint). When you run the streaming query again, it processes only the
# MAGIC new files — not the ones it already read.
# MAGIC
# MAGIC **This is equivalent to Kafka's offset tracking**: Kafka remembers your consumer offset
# MAGIC so you don't re-read messages you already processed. Auto Loader does the same for files.

# COMMAND ----------

# Simulate Day 2: new batch of users arrives in the landing zone
batch_2_users = [
    {"id": 4, "firstName": "James",  "lastName": "Brown", "email": "james@example.com",  "age": 42, "company": "DataFlow LLC",  "department": "Engineering"},
    {"id": 5, "firstName": "Olivia", "lastName": "Davis", "email": "olivia@example.com", "age": 26, "company": "TechStart Inc","department": "Data"},
]

print("Day 2: New files landing in S3/DBFS...")
write_landing_file(2, batch_2_users)

# Check landing zone — now has 2 files
print("\nLanding zone now has:")
for f in dbutils.fs.ls(LANDING_ZONE):
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# Run Auto Loader again — it will ONLY process batch_002.json
# batch_001.json is already in the checkpoint, so it's skipped automatically
streaming_query_2 = (df_stream_with_meta.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)  # SAME checkpoint — this is what prevents duplicates
    .trigger(availableNow=True)
    .start(BRONZE_PATH)
)

streaming_query_2.awaitTermination()
print("Second streaming batch complete!")

# COMMAND ----------

# Verify: should have 5 rows total (3 from Day 1 + 2 from Day 2)
# NOT 8 rows (which would be the case if we re-processed batch_001.json)
df_bronze_2 = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze table now has: {df_bronze_2.count()} rows (should be 5, not 8)")
display(df_bronze_2.orderBy("id"))

# COMMAND ----------

# The _source_file column shows exactly which file each row came from
# Invaluable for debugging and data lineage
print("Source file tracking:")
display(df_bronze_2.select("id", "firstName", "_source_file", "_ingested_at").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema Evolution in Auto Loader
# MAGIC
# MAGIC One of the most useful Auto Loader features: it handles new columns automatically.
# MAGIC When a source file has a column that wasn't in previous files, Auto Loader can:
# MAGIC - **Fail** (default): raise an error so you know the schema changed
# MAGIC - **Rescue**: put unknown columns into a special `_rescued_data` JSON column
# MAGIC - **Evolve**: add new columns to the target Delta table automatically

# COMMAND ----------

# Simulate a schema change: Day 3 files have a new "phone" column
batch_3_users = [
    {"id": 6, "firstName": "Liam", "lastName": "Wilson", "email": "liam@example.com",
     "age": 29, "company": "DataFlow LLC", "department": "Analytics", "phone": "+1-555-0106"},
]

print("Day 3: New file with extra 'phone' column...")
write_landing_file(3, batch_3_users)

# COMMAND ----------

# Use schema evolution: cloudFiles.schemaEvolutionMode = "addNewColumns"
# This tells Auto Loader to automatically add new columns to the Delta table
df_stream_evolving = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # handle new columns
    .load(LANDING_ZONE)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

# Need a new checkpoint for the evolved stream
CHECKPOINT_EVOLVED = "/FileStore/databricks-learning/autoloader/checkpoint_evolved"
try:
    dbutils.fs.rm(CHECKPOINT_EVOLVED, recurse=True)
except Exception:
    pass

streaming_query_3 = (df_stream_evolving.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_EVOLVED)
    .option("mergeSchema", "true")  # allow schema evolution in Delta target
    .trigger(availableNow=True)
    .start(BRONZE_PATH + "_evolved")  # separate path for this demo
)

streaming_query_3.awaitTermination()
print("Schema-evolving batch complete!")

# COMMAND ----------

df_evolved = spark.read.format("delta").load(BRONZE_PATH + "_evolved")
print(f"Evolved table: {df_evolved.count()} rows")
print("Schema after evolution (phone column should appear):")
df_evolved.printSchema()
display(df_evolved.select("id", "firstName", "company", "phone", "_source_file").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comparison: Kafka Streaming vs Auto Loader
# MAGIC
# MAGIC Here's the side-by-side of your `streaming_job.py` vs Auto Loader:
# MAGIC
# MAGIC **Your Kafka streaming pipeline:**
# MAGIC ```python
# MAGIC # Source: Kafka
# MAGIC df = (spark.readStream
# MAGIC     .format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", "kafka:9092")
# MAGIC     .option("subscribe", "users")
# MAGIC     .option("startingOffsets", "latest")
# MAGIC     .load()
# MAGIC     .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
# MAGIC     .select("data.*"))
# MAGIC
# MAGIC # Sink: MinIO/S3
# MAGIC query = (df.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", "s3a://bucket/checkpoints/users")
# MAGIC     .outputMode("append")
# MAGIC     .start("s3a://bucket/bronze/users"))
# MAGIC ```
# MAGIC
# MAGIC **Auto Loader pipeline:**
# MAGIC ```python
# MAGIC # Source: S3 directory (no Kafka broker needed)
# MAGIC df = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", "s3a://bucket/schema/users")
# MAGIC     .load("s3a://bucket/landing/users/"))
# MAGIC
# MAGIC # Sink: Delta table (same as Kafka version)
# MAGIC query = (df.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", "s3a://bucket/checkpoints/users")
# MAGIC     .outputMode("append")
# MAGIC     .start("s3a://bucket/bronze/users"))
# MAGIC ```
# MAGIC
# MAGIC **The write side is identical.** Only the read source changes.
# MAGIC
# MAGIC ### When to use which
# MAGIC
# MAGIC | Scenario | Use |
# MAGIC |---|---|
# MAGIC | IoT sensors pushing events | Kafka |
# MAGIC | Partner drops CSV/JSON files every hour | Auto Loader |
# MAGIC | Application event tracking (clickstream) | Kafka |
# MAGIC | ETL from legacy system that exports files | Auto Loader |
# MAGIC | You need sub-second latency | Kafka |
# MAGIC | You're already paying for Databricks, want simplicity | Auto Loader |
# MAGIC | Files from S3, no Kafka available | Auto Loader |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Production Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continuous streaming (instead of triggered batches)
# MAGIC
# MAGIC For true real-time processing, change the trigger:
# MAGIC
# MAGIC ```python
# MAGIC # Process files as they arrive, continuously
# MAGIC query = (df_stream.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", checkpoint_path)
# MAGIC     .trigger(processingTime="1 minute")   # check for new files every minute
# MAGIC     # OR:
# MAGIC     # .trigger(continuous="30 seconds")   # ultra-low latency (experimental)
# MAGIC     .start(bronze_path))
# MAGIC
# MAGIC # Keep the job running in a Databricks Workflow:
# MAGIC # query.awaitTermination()  # block until stopped or error
# MAGIC ```
# MAGIC
# MAGIC ### Using file notifications (S3 → SQS → Auto Loader)
# MAGIC
# MAGIC ```python
# MAGIC # For high-volume S3 in production — Databricks sets up the SQS queue automatically
# MAGIC df_stream = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", schema_path)
# MAGIC     .option("cloudFiles.useNotifications", "true")  # use S3 events instead of listing
# MAGIC     .option("cloudFiles.region", "us-east-1")
# MAGIC     .load("s3a://my-bucket/landing/users/"))
# MAGIC ```
# MAGIC
# MAGIC ### File path as partition metadata
# MAGIC
# MAGIC ```python
# MAGIC # Extract date from file path: /landing/users/2024/01/15/batch.json
# MAGIC df_with_date = (df_stream
# MAGIC     .withColumn("_file_path",  F.input_file_name())
# MAGIC     .withColumn("_file_date",
# MAGIC         F.regexp_extract(F.input_file_name(), r"(\d{4}/\d{2}/\d{2})", 1)
# MAGIC     ))
# MAGIC ```

# COMMAND ----------

# Cleanup
for path in [LANDING_ZONE, BRONZE_PATH, BRONZE_PATH + "_evolved",
             CHECKPOINT_PATH, CHECKPOINT_EVOLVED, SCHEMA_PATH]:
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass
print("Cleaned up Auto Loader demo files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Details |
# MAGIC |---|---|
# MAGIC | **Format** | `format("cloudFiles")` with `.option("cloudFiles.format", "json/csv/parquet")` |
# MAGIC | **Schema inference** | Automatic, stored at `cloudFiles.schemaLocation` |
# MAGIC | **Exactly-once** | `checkpointLocation` tracks processed files (same as Kafka) |
# MAGIC | **New files only** | Checkpoint prevents reprocessing — just like Kafka offsets |
# MAGIC | **Trigger modes** | `availableNow=True` (batch), `processingTime="N seconds"` (micro-batch) |
# MAGIC | **Schema evolution** | `cloudFiles.schemaEvolutionMode = "addNewColumns"` + `mergeSchema=true` |
# MAGIC | **File metadata** | `input_file_name()`, `_metadata.file_path`, `_metadata.file_modification_time` |
# MAGIC | **Notification mode** | `cloudFiles.useNotifications=true` for high-volume S3 |
# MAGIC
# MAGIC **Next**: `03_workflows/01_jobs_and_tasks.py` — Databricks Workflows as a replacement for Airflow
