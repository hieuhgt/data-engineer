# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Delta Lake Optimization
# MAGIC
# MAGIC ## What this notebook teaches
# MAGIC You've tuned Spark performance with `spark.sql.shuffle.partitions`, `repartition()`,
# MAGIC `coalesce()`, and partition pruning. Delta Lake has its own performance toolbox that
# MAGIC works at a higher level — you don't think about Spark shuffle, you think about
# MAGIC **file layout** and **data skipping**.
# MAGIC
# MAGIC **Your Spark tuning → Delta tuning:**
# MAGIC | Spark tuning | Delta equivalent | What it does |
# MAGIC |---|---|---|
# MAGIC | `coalesce()` before write | `OPTIMIZE` | Compacts many small files into fewer large files |
# MAGIC | `partitionBy("date")` | `ZORDER BY` or Liquid clustering | Groups related data so filters skip files |
# MAGIC | Manual partition management | `VACUUM` | Removes old Parquet files no longer needed |
# MAGIC | `spark.sql.shuffle.partitions=200` | Photon engine | Vectorized execution, automatic parallelism |
# MAGIC
# MAGIC **Topics:**
# MAGIC 1. OPTIMIZE — compaction (fix the small files problem)
# MAGIC 2. ZORDER BY — data skipping (co-locate frequently filtered columns)
# MAGIC 3. VACUUM — remove old versions (free up storage)
# MAGIC 4. Liquid Clustering — better than ZORDER (Databricks-specific)
# MAGIC 5. Photon engine — vectorized execution, no code changes needed
# MAGIC 6. Statistics and data skipping — how Delta knows which files to skip

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable
import random
import string

# Base path for this notebook's tables
BASE = "/FileStore/databricks-learning/optimization"
dbutils.fs.mkdirs(BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The Small Files Problem
# MAGIC
# MAGIC When you do many small appends (streaming, hourly batch jobs, frequent API polling),
# MAGIC you end up with hundreds or thousands of tiny Parquet files in a table.
# MAGIC Queries slow down because:
# MAGIC 1. Each file requires a separate S3/DBFS read request (network overhead)
# MAGIC 2. The Spark driver has to plan for hundreds of tasks
# MAGIC 3. Statistics (min/max per column) are spread across many files, making data skipping less effective
# MAGIC
# MAGIC **OPTIMIZE** compacts those tiny files into larger ones (target: 1GB each).

# COMMAND ----------

# Simulate the small files problem: write 20 tiny batches
# This is what happens with streaming writes or frequent incremental loads

FRAGMENTED_TABLE = f"{BASE}/fragmented_users"

print("Creating fragmented table (20 small writes)...")
companies = ["Acme Corp", "TechStart Inc", "DataFlow LLC", "CloudSystems Co", "Analytics Inc"]
departments = ["Engineering", "Data", "Product", "Analytics", "Sales", "Finance"]

for batch_num in range(20):
    batch_data = []
    for j in range(5):  # 5 rows per batch = 100 rows total
        user_id = batch_num * 5 + j + 1
        batch_data.append((
            user_id,
            f"User_{user_id}",
            f"user{user_id}@example.com",
            random.randint(22, 60),
            random.choice(companies),
            random.choice(departments),
            round(random.uniform(50000, 200000), 2),
        ))

    batch_df = spark.createDataFrame(
        batch_data,
        "id INT, name STRING, email STRING, age INT, company STRING, department STRING, salary DOUBLE"
    )

    (batch_df.write
        .format("delta")
        .mode("append")
        .save(FRAGMENTED_TABLE))

# Count Parquet files
all_files = dbutils.fs.ls(FRAGMENTED_TABLE)
parquet_files = [f for f in all_files if f.name.endswith(".parquet")]
print(f"\nFiles after 20 small writes: {len(parquet_files)} Parquet files")
print(f"Total rows: {spark.read.format('delta').load(FRAGMENTED_TABLE).count()}")
print("\nThis is the small files problem — each write created separate tiny files.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE — File Compaction
# MAGIC
# MAGIC `OPTIMIZE` rewrites multiple small Parquet files into fewer large files.
# MAGIC Target file size is ~1GB per file (configurable).
# MAGIC
# MAGIC **When to run it:**
# MAGIC - After streaming writes (Auto Loader, Structured Streaming)
# MAGIC - After many small batch appends
# MAGIC - As part of a nightly maintenance job
# MAGIC
# MAGIC **Impact**: 2-10x query speedup for tables with many small files.
# MAGIC No data changes — purely a file layout optimization.

# COMMAND ----------

# Count files before OPTIMIZE
before_files = [f for f in dbutils.fs.ls(FRAGMENTED_TABLE) if f.name.endswith(".parquet")]
print(f"Before OPTIMIZE: {len(before_files)} Parquet files")

# Run OPTIMIZE — this compacts files, no data change
result = spark.sql(f"OPTIMIZE delta.`{FRAGMENTED_TABLE}`")
display(result)

# Count files after OPTIMIZE
after_files = [f for f in dbutils.fs.ls(FRAGMENTED_TABLE) if f.name.endswith(".parquet")]
print(f"\nAfter OPTIMIZE: {len(after_files)} Parquet files")
print("(Old files still exist on disk until VACUUM — they're needed for time travel)")
print(f"Total rows unchanged: {spark.read.format('delta').load(FRAGMENTED_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ZORDER BY — Data Skipping
# MAGIC
# MAGIC `ZORDER BY` is like `partitionBy()` but more flexible and doesn't create subdirectories.
# MAGIC It physically co-locates rows with the same values in the ZORDER columns within each file.
# MAGIC
# MAGIC **How data skipping works:**
# MAGIC 1. Delta stores min/max statistics for each column in each Parquet file
# MAGIC 2. When you query `WHERE company = 'Acme Corp'`, Delta checks which files have
# MAGIC    min <= "Acme Corp" <= max
# MAGIC 3. Files where this is false are **skipped** entirely — never read from storage
# MAGIC
# MAGIC **ZORDER BY** improves step 2 by making the min-max ranges tighter — more files can be skipped.
# MAGIC
# MAGIC **Rule of thumb**: ZORDER BY your most common filter columns (high-cardinality columns
# MAGIC you filter on often, like `user_id`, `event_date`, `company`).
# MAGIC Don't ZORDER BY partition columns (they already skip files).

# COMMAND ----------

# Create a larger table to demonstrate ZORDER effectiveness
LARGE_TABLE = f"{BASE}/large_users"

# Generate 10,000 users
print("Generating 10,000 users for ZORDER demo...")
all_data = []
for i in range(10_000):
    all_data.append((
        i + 1,
        f"User_{i+1}",
        f"user{i+1}@example.com",
        random.randint(22, 60),
        random.choice(companies),
        random.choice(departments),
        round(random.uniform(50000, 200000), 2),
    ))

large_df = spark.createDataFrame(
    all_data,
    "id INT, name STRING, email STRING, age INT, company STRING, department STRING, salary DOUBLE"
)

(large_df.write
    .format("delta")
    .mode("overwrite")
    .save(LARGE_TABLE))

print(f"Written {large_df.count()} rows to {LARGE_TABLE}")

# COMMAND ----------

# Before ZORDER: query that filters on company
# Delta will scan all files (or most) because company values are randomly distributed
spark.conf.set("spark.databricks.delta.stats.collect", "true")

print("Query before ZORDER BY company:")
result_before = (spark.read.format("delta").load(LARGE_TABLE)
    .filter(F.col("company") == "Acme Corp")
    .agg(F.count("*").alias("count"), F.avg("salary").alias("avg_salary")))

display(result_before)

# COMMAND ----------

# OPTIMIZE with ZORDER BY — compact AND co-locate by company + department
# This is the typical "nightly optimize" job in production
print("Running OPTIMIZE ZORDER BY company, department...")
result = spark.sql(f"""
    OPTIMIZE delta.`{LARGE_TABLE}`
    ZORDER BY (company, department)
""")
display(result)
# The result shows: numFilesRemoved, numFilesAdded, numBytesRemoved, numBytesAdded

# COMMAND ----------

# After ZORDER: same query but now Delta can skip most files
# Files are sorted so all "Acme Corp" rows are in fewer files with tight min/max ranges
print("Query after ZORDER BY company (should scan fewer files):")
result_after = (spark.read.format("delta").load(LARGE_TABLE)
    .filter(F.col("company") == "Acme Corp")
    .agg(F.count("*").alias("count"), F.avg("salary").alias("avg_salary")))

display(result_after)

print("\nTo see how many files were skipped, check the Spark UI:")
print("  Cluster UI → SQL tab → your query → number of files scanned vs total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. VACUUM — Remove Old Files
# MAGIC
# MAGIC After OPTIMIZE, the old small files are still on disk — Delta keeps them for time travel.
# MAGIC `VACUUM` deletes files that are:
# MAGIC 1. No longer referenced by any table version
# MAGIC 2. Older than the retention period (default: 7 days / 168 hours)
# MAGIC
# MAGIC **Warning**: After VACUUM, you can no longer time-travel to versions that used those files.
# MAGIC Think carefully before reducing the retention period below 7 days.

# COMMAND ----------

# Check current history before VACUUM
print("Table history before VACUUM:")
spark.sql(f"DESCRIBE HISTORY delta.`{FRAGMENTED_TABLE}`").select(
    "version", "timestamp", "operation"
).show(truncate=False)

# COMMAND ----------

# Dry run — see what VACUUM would delete without actually deleting
print("VACUUM dry run (what would be deleted):")
vacuum_preview = spark.sql(f"VACUUM delta.`{FRAGMENTED_TABLE}` RETAIN 0 HOURS DRY RUN")
display(vacuum_preview)
print(f"Files that would be removed: {vacuum_preview.count()}")

# COMMAND ----------

# For this demo, we'll vacuum with 0 hour retention to clean up
# In production, NEVER go below 7 days (or your concurrent readers may fail)
# We need to disable the safety check for this demo (Community Edition only)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"VACUUM delta.`{FRAGMENTED_TABLE}` RETAIN 0 HOURS")
print("VACUUM complete")

# Count files after VACUUM — old small files should be gone
after_vacuum = [f for f in dbutils.fs.ls(FRAGMENTED_TABLE) if f.name.endswith(".parquet")]
print(f"Parquet files after VACUUM: {len(after_vacuum)}")
print("(Only the compacted files remain — old small files deleted)")

# Re-enable the safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM in production
# MAGIC
# MAGIC ```python
# MAGIC # Standard production VACUUM — 7-day retention (default)
# MAGIC spark.sql("VACUUM catalog.schema.users")
# MAGIC
# MAGIC # Explicit retention period
# MAGIC spark.sql("VACUUM catalog.schema.users RETAIN 168 HOURS")  # 7 days
# MAGIC
# MAGIC # Shorter retention (risky — only do this if you're sure)
# MAGIC spark.sql("VACUUM catalog.schema.users RETAIN 24 HOURS")   # 1 day
# MAGIC
# MAGIC # Check what VACUUM would delete before running
# MAGIC spark.sql("VACUUM catalog.schema.users RETAIN 168 HOURS DRY RUN").show()
# MAGIC ```
# MAGIC
# MAGIC **OPTIMIZE + VACUUM together = the standard nightly maintenance job:**
# MAGIC ```python
# MAGIC spark.sql("OPTIMIZE catalog.silver.users ZORDER BY (company, department)")
# MAGIC spark.sql("VACUUM catalog.silver.users RETAIN 168 HOURS")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering — Better Than ZORDER (Databricks 13.3+)
# MAGIC
# MAGIC `ZORDER BY` has limitations:
# MAGIC - You have to re-OPTIMIZE the whole table to change the ZORDER columns
# MAGIC - It doesn't work well with streaming writes (files are added unordered)
# MAGIC - Adding data doesn't maintain the ZORDER — you need to re-run OPTIMIZE
# MAGIC
# MAGIC **Liquid Clustering** is a newer Databricks feature that:
# MAGIC - Incrementally clusters new data as it's written (no need to re-OPTIMIZE everything)
# MAGIC - You can change the clustering columns without rewriting the table
# MAGIC - Better performance than ZORDER for high-write-frequency tables
# MAGIC - Integrated with Auto Optimize — runs automatically in the background

# COMMAND ----------

# Create a table WITH Liquid Clustering from the start
# CLUSTER BY replaces ZORDER BY in the OPTIMIZE step
LIQUID_TABLE = f"{BASE}/liquid_users"

# Drop if exists
try:
    spark.sql(f"DROP TABLE IF EXISTS liquid_users_demo")
    dbutils.fs.rm(LIQUID_TABLE, recurse=True)
except Exception:
    pass

# Create with CLUSTER BY — this defines the clustering columns upfront
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{LIQUID_TABLE}`
    (id INT, name STRING, email STRING, age INT, company STRING, department STRING, salary DOUBLE)
    USING DELTA
    CLUSTER BY (company, department)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
""")

print("Created Liquid Clustered table")
print("CLUSTER BY (company, department) defined at CREATE TABLE time")

# COMMAND ----------

# Insert data — Liquid Clustering automatically clusters as data flows in
# No manual OPTIMIZE ZORDER needed
from pyspark.sql import Row

sample_users = spark.createDataFrame([
    (1, "Emily",   "emily@example.com",   28, "Acme Corp",     "Engineering", 95000.0),
    (2, "Michael", "michael@example.com", 35, "TechStart Inc", "Product",     120000.0),
    (3, "Sophia",  "sophia@example.com",  31, "Acme Corp",     "Data",        110000.0),
    (4, "James",   "james@example.com",   42, "DataFlow LLC",  "Engineering", 140000.0),
    (5, "Olivia",  "olivia@example.com",  26, "TechStart Inc", "Data",        85000.0),
], "id INT, name STRING, email STRING, age INT, company STRING, department STRING, salary DOUBLE")

(sample_users.write
    .format("delta")
    .mode("append")
    .save(LIQUID_TABLE))

print(f"Inserted {sample_users.count()} rows into Liquid Clustered table")

# COMMAND ----------

# For Liquid Clustering, OPTIMIZE without ZORDER triggers incremental clustering
# It's faster than full ZORDER because it only clusters new/unclustered data
print("Running OPTIMIZE (triggers Liquid Clustering):")
result = spark.sql(f"OPTIMIZE delta.`{LIQUID_TABLE}`")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ZORDER vs Liquid Clustering — when to use which
# MAGIC
# MAGIC | | ZORDER BY | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | Databricks version | Any DBR | DBR 13.3+ (default on DBR 17) |
# MAGIC | Cluster columns defined | At OPTIMIZE time | At CREATE TABLE time |
# MAGIC | Change cluster columns | Re-OPTIMIZE entire table | `ALTER TABLE ... CLUSTER BY (new_cols)` |
# MAGIC | Works with streaming | Needs periodic full re-OPTIMIZE | Incremental, works great |
# MAGIC | Compaction | Yes | Yes |
# MAGIC | High write frequency | Poor (needs frequent OPTIMIZE) | Excellent |
# MAGIC | Recommendation | Legacy / DBR < 13.3 | **Use this for new tables** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Photon Engine
# MAGIC
# MAGIC Photon is Databricks' vectorized execution engine written in C++.
# MAGIC It replaces Spark's Volcano/Sort-Merge execution model for a significant subset of operations.
# MAGIC
# MAGIC **Key points:**
# MAGIC - **No code changes needed** — your existing PySpark code runs on Photon automatically
# MAGIC - Enable by selecting a "Photon" runtime or instance type in cluster config
# MAGIC - 2-4x faster for SQL aggregations, joins, and scans on Delta tables
# MAGIC - Particularly effective for: GROUP BY, JOIN, ORDER BY, window functions
# MAGIC - Not available in Community Edition (requires a Photon-enabled cluster)
# MAGIC
# MAGIC **Photon vs standard Spark execution:**
# MAGIC ```
# MAGIC Standard Spark (JVM-based):
# MAGIC   Row-at-a-time processing in Java Virtual Machine
# MAGIC   Each operator processes one row, passes to next
# MAGIC
# MAGIC Photon (C++-based):
# MAGIC   Column-at-a-time (vectorized) processing in C++
# MAGIC   Process entire columns (arrays) at once with SIMD CPU instructions
# MAGIC   Bypasses JVM overhead entirely
# MAGIC ```

# COMMAND ----------

# Check if Photon is enabled (won't be in Community Edition)
photon_enabled = spark.conf.get("spark.databricks.photon.enabled", "false")
print(f"Photon enabled: {photon_enabled}")

if photon_enabled == "false":
    print("\nPhoton is not available in Community Edition.")
    print("In a Photon-enabled cluster, this same code runs 2-4x faster automatically.")
    print("No code changes required — just pick a Photon-enabled cluster type.")
else:
    print("\nPhoton is active — aggregations and scans will use vectorized C++ execution.")

# COMMAND ----------

# Photon automatically accelerates these operations:
photon_accelerated = [
    "GROUP BY aggregations (COUNT, SUM, AVG, MIN, MAX)",
    "JOIN operations (sort-merge join, broadcast join)",
    "ORDER BY / SORT",
    "Window functions (ROW_NUMBER, RANK, LEAD, LAG)",
    "Delta Lake reads and writes",
    "String operations (LIKE, TRIM, CONCAT)",
    "Numeric operations",
]

print("Operations accelerated by Photon (no code changes needed):")
for op in photon_accelerated:
    print(f"  + {op}")

# Not accelerated by Photon (falls back to JVM Spark):
not_accelerated = [
    "Python UDFs (use SQL functions or Pandas UDFs instead)",
    "Scala UDFs",
    "Complex nested data types in some cases",
    "Streaming with stateful operations",
]

print("\nNot accelerated by Photon (use alternatives where possible):")
for op in not_accelerated:
    print(f"  - {op}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Delta Statistics and Data Skipping (How It All Works)

# COMMAND ----------

# Delta stores per-file statistics (min, max, nullCount) for the first 32 columns
# These are used for data skipping — files outside the filter range are never read

# View the statistics Delta has collected for our table
print("File-level statistics stored in Delta transaction log:")
detail = spark.sql(f"DESCRIBE DETAIL delta.`{LARGE_TABLE}`")
display(detail.select("numFiles", "sizeInBytes", "numRows", "properties"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to verify data skipping is working
# MAGIC
# MAGIC After OPTIMIZE ZORDER BY, check the Spark SQL tab in the cluster UI:
# MAGIC
# MAGIC 1. Run a filtered query
# MAGIC 2. In the notebook, click the "Spark Jobs" link that appears
# MAGIC 3. In the Spark UI, go to SQL tab
# MAGIC 4. Find your query, click the "Details" link
# MAGIC 5. Look for: **"number of files scanned"** vs **"number of files total"**
# MAGIC
# MAGIC If ZORDER is working, you'll see significantly fewer files scanned than the total.
# MAGIC
# MAGIC You can also check this programmatically:
# MAGIC
# MAGIC ```python
# MAGIC # Enable verbose stats output
# MAGIC spark.conf.set("spark.databricks.delta.stats.explain", "true")
# MAGIC
# MAGIC # Run a query — the execution plan will show data skipping stats
# MAGIC df = spark.read.format("delta").load(path).filter(F.col("company") == "Acme Corp")
# MAGIC df.explain(True)  # look for "SkippedFiles" in the plan
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Delta Optimization Cheat Sheet
# MAGIC
# MAGIC ### When to run what
# MAGIC
# MAGIC | Problem | Solution | When |
# MAGIC |---|---|---|
# MAGIC | Many small files (from streaming/incremental) | `OPTIMIZE delta.\`path\`` | Nightly, or after each streaming batch |
# MAGIC | Slow queries filtering on a column | `OPTIMIZE delta.\`path\` ZORDER BY (col)` | After data load, nightly |
# MAGIC | Disk space growing too large | `VACUUM delta.\`path\` RETAIN 168 HOURS` | Weekly |
# MAGIC | New table with frequent writes | Use `CLUSTER BY` at CREATE TABLE time | Once at table creation |
# MAGIC | Slow aggregations / joins | Enable Photon cluster | Cluster configuration |
# MAGIC
# MAGIC ### The standard nightly maintenance job
# MAGIC ```python
# MAGIC tables_to_optimize = [
# MAGIC     ("catalog.silver.users",      ["company_name", "department"]),
# MAGIC     ("catalog.silver.events",     ["event_date", "user_id"]),
# MAGIC     ("catalog.gold.user_stats",   ["company_name"]),
# MAGIC ]
# MAGIC
# MAGIC for table, zorder_cols in tables_to_optimize:
# MAGIC     cols = ", ".join(zorder_cols)
# MAGIC     spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
# MAGIC     spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
# MAGIC     print(f"Optimized and vacuumed: {table}")
# MAGIC ```
# MAGIC
# MAGIC **Next**: `04_advanced/02_unity_catalog.py` — Governance, access control, and data lineage

# COMMAND ----------

# Cleanup
for path in [BASE]:
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass
print("Cleaned up optimization demo tables")
