# Apache Spark Complete Guide: Junior to Senior Level

## Table of Contents
1. [LEVEL 1: JUNIOR](#level-1-junior)
2. [LEVEL 2: INTERMEDIATE](#level-2-intermediate)
3. [LEVEL 3: ADVANCED](#level-3-advanced)
4. [LEVEL 4: SENIOR](#level-4-senior)

---

# LEVEL 1: JUNIOR
## Fundamentals (Weeks 1-2)

### What is Apache Spark?

**Definition:** Apache Spark is a unified computing engine for large-scale data processing.

**Why Spark?**
```
Problem: Processing 1TB of data on single machine = SLOW (days)
Solution: Spark distributes work across cluster (hours)

1TB of data
├─ Partition 0: 250GB → Worker 1
├─ Partition 1: 250GB → Worker 2
├─ Partition 2: 250GB → Worker 3
└─ Partition 3: 250GB → Worker 4

Result: 4 workers process in parallel = ~4x faster (ideally)
```

### Spark Architecture

```
Driver Node (Master)
├─ Spark Context / Spark Session
└─ Task Scheduler

        ↓ (sends tasks)

Executor Nodes (Workers) - Multiple instances
├─ Executor 1 (4 cores, 4GB memory)
│  └─ Task 1, Task 2, Task 3, Task 4
├─ Executor 2 (4 cores, 4GB memory)
│  └─ Task 5, Task 6, Task 7, Task 8
└─ Executor 3 (4 cores, 4GB memory)
   └─ Task 9, Task 10, Task 11, Task 12

Cluster Manager (YARN, Mesos, Kubernetes, or Standalone)
└─ Allocates resources to executors
```

**Key Terms:**
- **Driver:** Orchestrates jobs, runs main program
- **Executor:** Runs tasks in parallel on worker nodes
- **Task:** Unit of work (e.g., process 1 partition)
- **Partition:** Chunk of data distributed across cluster
- **Cluster Manager:** Allocates resources (YARN, K8s, etc.)

### RDD (Resilient Distributed Dataset)

RDD is Spark's low-level abstraction:

```python
# Create RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Transformation (lazy)
rdd_squared = rdd.map(lambda x: x * x)

# Action (eager)
result = rdd_squared.collect()  # [1, 4, 9, 16, 25]
```

**RDD vs DataFrame:**
- RDD: Low-level, untyped, less optimized
- DataFrame: High-level, typed, optimized by Catalyst
- **Recommendation:** Use DataFrame 99% of the time

### DataFrame (The Fundamental Data Structure)

**What is a DataFrame?**
```
Think of it like a SQL table distributed across multiple machines

User DataFrame:
┌────┬──────┬───────┐
│ id │ name │ email │
├────┬──────┬───────┤
│ 1  │ Alice│ a@... │
│ 2  │ Bob  │ b@... │
│ 3  │ Carol│ c@... │
└────┴──────┴───────┘

Distributed across cluster:
Worker 1: Rows 1-2 (Alice, Bob)
Worker 2: Rows 3-4 (Carol, David)
Worker 3: Rows 5-6 (Eve, Frank)
```

### Creating DataFrames

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# From Python list
data = [("Alice", 25), ("Bob", 30), ("Carol", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# From CSV
df = spark.read.csv("users.csv", header=True, inferSchema=True)

# From JSON
df = spark.read.json("events.json")

# From Parquet
df = spark.read.parquet("data.parquet")

# From SQL
df = spark.sql("SELECT * FROM my_table")
```

### Basic Operations

```python
# Show data
df.show()  # Print first 20 rows
df.show(5)  # Print first 5 rows

# Schema
df.printSchema()  # Column names and types
df.columns  # List column names
df.dtypes  # Column types

# Info
df.count()  # Total rows
df.describe().show()  # Statistics (count, mean, stddev, min, max)

# Select columns
df.select("name", "age")
df.select(df.name, df.age)

# Filter rows
df.filter(df.age > 25)
df.filter(df.name == "Alice")
df.filter((df.age > 25) & (df.name == "Alice"))  # AND
df.filter((df.age > 25) | (df.name == "Alice"))  # OR

# Add column
from pyspark.sql.functions import col, lit
df.withColumn("age_next_year", col("age") + 1)
df.withColumn("country", lit("USA"))  # Constant value

# Remove column
df.drop("email")

# Rename column
df.withColumnRenamed("name", "user_name")

# Sort
df.orderBy("age")  # Ascending
df.orderBy(col("age").desc())  # Descending

# Distinct
df.distinct()  # Remove duplicates

# Limit
df.limit(5)  # First 5 rows
```

### Aggregations

```python
# Count
df.count()  # Total rows

# Group by
df.groupBy("city").count()  # Count per city

# Multiple aggregations
from pyspark.sql.functions import sum, avg, min, max, count

df.groupBy("city") \
    .agg(
        count("*").alias("total_users"),
        avg("age").alias("avg_age"),
        sum("salary").alias("total_salary")
    )

# With filter
df.groupBy("city") \
    .agg(count("*").alias("count")) \
    .filter(col("count") > 10)  # Cities with > 10 users
```

### Joins

```python
# Inner join (only matching rows)
users.join(orders, on="user_id", how="inner")

# Left join (all from left, matching from right)
users.join(orders, on="user_id", how="left")

# Right join (all from right, matching from left)
users.join(orders, on="user_id", how="right")

# Full outer join (all rows from both)
users.join(orders, on="user_id", how="full")

# Join with condition
users.join(orders, users.id == orders.user_id)

# Multiple join conditions
users.join(
    orders,
    (users.id == orders.user_id) & (users.city == orders.city)
)
```

### SQL Queries

```python
# Register DataFrame as table
df.createOrReplaceTempView("users")  # Temporary
df.createOrReplaceGlobalTempView("users")  # Global

# Query with SQL
result = spark.sql("SELECT * FROM users WHERE age > 25")

# Complex query
spark.sql("""
    SELECT
        city,
        COUNT(*) as total_users,
        AVG(age) as avg_age
    FROM users
    GROUP BY city
    HAVING COUNT(*) > 10
    ORDER BY avg_age DESC
""")
```

### Writing Data

```python
# Parquet (best for Spark)
df.write.parquet("output/")
df.write.mode("overwrite").parquet("output/")

# CSV
df.write.csv("output/", header=True)

# JSON
df.write.json("output/")

# Parquet modes
df.write.mode("overwrite").parquet(path)  # Replace
df.write.mode("append").parquet(path)     # Add to existing
df.write.mode("ignore").parquet(path)     # Skip if exists
df.write.mode("error").parquet(path)      # Raise error if exists
```

### Data Types

```python
from pyspark.sql.types import *

# Common types
IntegerType()
LongType()
FloatType()
DoubleType()
StringType()
BooleanType()
DateType()
TimestampType()
DecimalType(10, 2)  # 10 total digits, 2 decimal places

# Define schema explicitly
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("salary", DoubleType())
])

df = spark.read.schema(schema).csv("data.csv")
```

### Common Functions

```python
from pyspark.sql.functions import *

# String functions
upper(col("name"))  # "alice" → "ALICE"
lower(col("name"))  # "ALICE" → "alice"
length(col("name"))  # String length
trim(col("name"))  # Remove whitespace
concat(col("first"), lit(" "), col("last"))  # Concatenate

# Math functions
abs(col("salary"))  # Absolute value
round(col("salary"), 2)  # Round to 2 decimals
ceil(col("price"))  # Round up
floor(col("price"))  # Round down

# Null handling
coalesce(col("email"), lit("unknown"))  # First non-null
when(col("age").isNull(), 0).otherwise(col("age"))  # Conditional

# Date functions
current_date()  # Today
to_date(col("date_str"), "yyyy-MM-dd")  # Parse string to date
year(col("date"))  # Extract year
month(col("date"))  # Extract month
```

### Performance Basics: Lazy Evaluation

```python
# Transformations are LAZY (not executed)
df = spark.read.csv("file.csv")  # Not executed
filtered = df.filter(df.age > 25)  # Not executed
selected = filtered.select("name")  # Not executed

# Action TRIGGERS execution
selected.show()  # NOW everything executes

# Execution plan built:
# 1. Read CSV
# 2. Filter age > 25
# 3. Select name
# 4. Show results
```

### Example: Word Count (Classic)

```python
from pyspark.sql.functions import col, explode, split, lower, count

# Read text file
text = spark.read.text("words.txt")

# Split into words, count
word_count = text \
    .select(explode(split(lower(col("value")), " ")).alias("word")) \
    .filter(col("word") != "") \
    .groupBy("word") \
    .count() \
    .orderBy(col("count").desc())

word_count.show()
```

---

# LEVEL 2: INTERMEDIATE
## Advanced Operations (Weeks 3-5)

### Complex Joins

```python
# Problem: Join user info with order info
# Challenge: Ensure correctness and efficiency

users = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Carol")],
    ["user_id", "name"]
)

orders = spark.createDataFrame(
    [(1, 100, "2024-01-01"), (1, 200, "2024-01-02"), (2, 150, "2024-01-03"), (4, 300, "2024-01-04")],
    ["user_id", "amount", "date"]
)

# Inner join (only users with orders)
result = users.join(orders, on="user_id", how="inner")
# Result: Alice (1, 100), Alice (1, 200), Bob (2, 150)

# Left join (all users, whether or not they have orders)
result = users.join(orders, on="user_id", how="left")
# Result: Alice (1, 100), Alice (1, 200), Bob (2, 150), Carol (no order)

# Left anti-join (users WITHOUT orders)
result = users.join(orders, on="user_id", how="left_anti")
# Result: Carol

# Problem with joins: Skew
# If one user has 1M orders and most have 10, optimizer may struggle
# Solution: Skew handling (advanced, Level 3)
```

### Window Functions

Window functions operate within groups of rows:

```python
from pyspark.sql.functions import *
from pyspark.sql import Window

# Setup data
sales = spark.createDataFrame([
    ("Alice", "2024-01-01", 100),
    ("Alice", "2024-01-02", 150),
    ("Alice", "2024-01-03", 200),
    ("Bob", "2024-01-01", 50),
    ("Bob", "2024-01-02", 75),
], ["name", "date", "amount"])

# Row number (rank within each person)
window = Window.partitionBy("name").orderBy("date")
sales.withColumn("row_num", row_number().over(window)).show()
# Alice: 1, 2, 3 (in date order)
# Bob: 1, 2 (in date order)

# Rank with ties
sales.withColumn("rank", rank().over(window)).show()

# Lead/lag (previous/next row)
sales.withColumn("prev_amount", lag("amount").over(window)).show()
sales.withColumn("next_amount", lead("amount").over(window)).show()

# Running total
sales.withColumn(
    "cumulative",
    sum("amount").over(window)
).show()
# Alice: 100, 250, 450 (running sum)

# Top N per group
window_rank = Window.partitionBy("name").orderBy(col("amount").desc())
top_2 = sales \
    .withColumn("rank", row_number().over(window_rank)) \
    .filter(col("rank") <= 2)
# Result: Top 2 sales per person
```

### Handling Duplicates

```python
# Drop exact duplicates
df.dropDuplicates()

# Drop duplicates by specific columns
df.dropDuplicates(["user_id", "email"])
# Keeps first occurrence, removes others with same user_id, email

# Alternative: Keep latest by timestamp
from pyspark.sql.functions import row_number, desc
window = Window.partitionBy("user_id").orderBy(desc("timestamp"))
df.withColumn("row_num", row_number().over(window)) \
  .filter(col("row_num") == 1) \
  .drop("row_num")
```

### Handling Missing Data

```python
from pyspark.sql.functions import *

# Find nulls
df.filter(col("email").isNull()).show()  # Rows where email is null
df.select(sum(isnull(col("email")))).show()  # Count of nulls

# Drop nulls
df.dropna()  # Drop rows with any null
df.dropna(subset=["email"])  # Drop if email is null
df.dropna(how="all")  # Only drop if ALL columns are null

# Fill nulls
df.fillna(0)  # Replace all nulls with 0
df.fillna({"email": "unknown", "phone": "N/A"})  # Different values per column
df.fillna(df.agg(avg("salary")).collect()[0][0], subset=["salary"])  # Fill with average
```

### Transformations with UDF (User Defined Functions)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF (slower, but flexible)
def clean_email(email):
    if email is None:
        return "unknown"
    return email.lower().strip()

clean_email_udf = udf(clean_email, StringType())
df.withColumn("clean_email", clean_email_udf(col("email"))).show()

# Vectorized UDF (faster, uses Pandas)
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def clean_email_pandas(s: pd.Series) -> pd.Series:
    return s.fillna("unknown").str.lower().str.strip()

df.withColumn("clean_email", clean_email_pandas(col("email"))).show()
```

### Query Execution Plans

```python
# View the execution plan
df.explain()  # Logical plan only

df.explain(extended=True)  # Logical + physical plans

# Output explains:
# - Which operations are performed
# - Join strategy (broadcast, hash join, etc.)
# - Shuffle operations (expensive)
# - Filters pushed down (optimized)

# Example output:
# == Physical Plan ==
# *(1) Filter (age#0 > 25)
# +- *(1) Scan csv [name#1, age#0]
```

### Performance Analysis: Shuffle Operations

```python
# Shuffle = expensive data movement between workers

# Example 1: No shuffle (good)
df.select("name", "age")  # Just reads existing partitions

# Example 2: Small shuffle (acceptable)
df.filter(df.age > 25)  # Filter is local to each partition

# Example 3: Large shuffle (bad)
df.groupBy("city").count()  # Moves all data with same city to one worker
# SOLUTION: Pre-filter if possible
df.filter(df.active == True).groupBy("city").count()

# Example 4: Join shuffle (medium)
users.join(orders, on="user_id")  # Shuffles to match user_ids
# SOLUTION: If one table is small, broadcast it
from pyspark.sql.functions import broadcast
users.join(broadcast(small_table), on="user_id")  # No shuffle!
```

### Caching Basics

```python
# Cache in memory for reuse
df = spark.read.parquet("large_file.parquet")
df.cache()  # Store in memory after first use

# Use df multiple times
df.filter(col("age") > 25).count()
df.filter(col("age") < 18).count()
# Second operation faster because df is cached

# Remove from cache
df.unpersist()

# Different cache levels
df.cache()  # MEMORY_AND_DISK
df.persist(pyspark.StorageLevel.MEMORY_ONLY)  # Memory only (fast, risky if not enough)
df.persist(pyspark.StorageLevel.DISK_ONLY)  # Disk only (slow)
```

---

# LEVEL 3: ADVANCED
## Production Systems (Weeks 6-12)

### Partitioning Strategy

```python
# Problem: 100GB file, reading all 100GB every time is slow

# Solution 1: Partition by date
df.write.partitionBy("date").mode("append").parquet("data/")

# Result structure:
# data/
# ├─ date=2024-01-01/part-0000.parquet
# ├─ date=2024-01-01/part-0001.parquet
# ├─ date=2024-01-02/part-0000.parquet
# └─ ...

# Reading only specific date (partition pruning)
df = spark.read.parquet("data/")
filtered = df.filter(col("date") == "2024-01-01")  # Only reads that partition

# Solution 2: Partition by multiple columns
df.write.partitionBy("date", "region").parquet("data/")
# data/date=2024-01-01/region=US/
# data/date=2024-01-01/region=EU/

# Solution 3: Bucketing (for joins)
df.write.bucketBy(10, "user_id").mode("overwrite").parquet("data/")
# Distributes by hash(user_id), helps with joins
```

### Optimization Techniques

```python
# 1. Reduce columns early
df = spark.read.parquet("large_file.parquet")
df = df.select("name", "email")  # Drop unnecessary columns early
# Pushes column pruning to read stage

# 2. Filter early
df = df.filter(col("active") == True)  # Filter immediately after read
# Reduces data flowing through pipeline

# 3. Broadcast small tables
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_lookup), on="key")
# small_lookup sent to all workers, no shuffle

# 4. Repartition before expensive operations
df = df.repartition(100)  # Increase partitions for parallelism
df = df.coalesce(1)  # Decrease partitions (when finishing)

# 5. Cache intermediate results
df = spark.read.parquet("data/")
df = df.filter(col("age") > 25)
df.cache()  # Cache here

result1 = df.groupBy("city").count()
result2 = df.filter(col("age") > 50).count()
# Both use cached df, faster than re-reading

# 6. Use columnar formats (Parquet)
spark.read.parquet("file.parquet")  # Columnar, fast
# NOT
spark.read.csv("file.csv")  # Row-based, slower
```

### Streaming with Structured Streaming

```python
# Problem: Data arriving continuously (Kafka stream, IoT sensors)
# Solution: Structured Streaming

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Parse value column (Kafka stores data as bytes)
from pyspark.sql.functions import col, from_json
schema = "name STRING, age INT"

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Aggregation with watermarking (handle late data)
result = parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("name")
    ).count()

# Write to sink (storage)
query = result.writeStream \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()  # Run indefinitely
```

### Error Handling

```python
# Bad data is inevitable in production

# 1. Schema validation
from pyspark.sql.types import *

expected_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
])

df = spark.read.schema(expected_schema).json("data.json")
# Fails loudly if schema doesn't match

# 2. Corrupt record handling
df = spark.read.json("data.json")
df.filter(col("_corrupt_record").isNull()).show()  # Good records
df.filter(col("_corrupt_record").isNotNull()).show()  # Bad records

# 3. Try-except with transformations
from pyspark.sql.functions import col, when, try_catch
# Approximate: use when/otherwise or custom UDF
df.withColumn(
    "parsed_age",
    when(col("age").cast("int").isNotNull(), col("age").cast("int"))
    .otherwise(None)
)

# 4. Dead Letter Queue pattern
def process_with_dlq(df, process_func):
    try:
        return process_func(df), None
    except Exception as e:
        return None, df  # Send to DLQ (save bad data)

good, bad = process_with_dlq(df, my_transformation)
good.write.parquet("output/good/")
bad.write.parquet("output/dlq/")
```

### Integration with Kafka

```python
# Real-world pattern: Kafka → Spark → Data Lake

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("KafkaToDataLake") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
schema = "event_id STRING, user_id INT, amount DOUBLE, timestamp STRING"

events = df.select(
    from_json(col("value").cast("string"), schema).alias("e"),
    col("timestamp").alias("kafka_time")
).select(
    col("e.*"),
    col("kafka_time"),
    col("e.timestamp").cast("timestamp").alias("event_time")
)

# Add processing metadata
processed = events \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("date", to_date(col("event_time")))

# Write to data lake (partitioned by date)
query = processed.writeStream \
    .format("parquet") \
    .partitionBy("date") \
    .option("path", "s3://data-lake/events/") \
    .option("checkpointLocation", "s3://checkpoint/events/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

---

# LEVEL 4: SENIOR
## Enterprise Scale (Weeks 13-20)

### Cluster Tuning

```
Problem: Job takes 1 hour, but cluster is expensive

Solution: Right-size the cluster

Before (Too many executors):
spark.conf.set("spark.executor.instances", 100)
spark.conf.set("spark.executor.cores", 4)
spark.conf.set("spark.executor.memory", "4g")
# Cost: $500/hour, still 1 hour

After (Right-sized):
spark.conf.set("spark.executor.instances", 20)
spark.conf.set("spark.executor.cores", 4)
spark.conf.set("spark.executor.memory", "4g")
# Cost: $100/hour, still 1 hour (parallelism was not bottleneck)
```

### Memory Management

```python
# Memory is divided:
# Total = spark.executor.memory (4g)
# ├─ Heap (Java objects) = 0.6 * 4g = 2.4g
# ├─ Off-heap = 0.4 * 4g = 1.6g
# └─ Reserved = 300mb

# Memory leak example
large_list = []
for i in range(1000000):
    large_list.append(i)  # Accumulates in driver memory!

# Solution: Use Spark operations
rdd = spark.sparkContext.parallelize(range(1000000))
# Distributed, doesn't accumulate in driver

# Out of memory when collecting
df = spark.read.parquet("100gb_file.parquet")
data = df.collect()  # Crash! Can't fit 100GB in driver memory

# Solution: Process distributed
df.groupBy("category").count().show()  # Process in cluster
```

### Catalyst Optimizer

Spark's optimizer automatically optimizes queries:

```python
# Query 1: Inefficient (but Catalyst fixes it)
df = spark.read.parquet("users.parquet")
filtered = df.select("*")  # Select all
filtered2 = filtered.filter(col("age") > 25)  # Filter after select

# Catalyst transforms to:
df = spark.read.parquet("users.parquet")
df.filter(col("age") > 25)  # Filter pushed down before select
# Result: Fewer rows to process

# Query 2: Join optimization
users = spark.read.parquet("users.parquet")  # 1M rows
orders = spark.read.parquet("orders.parquet")  # 100M rows

result = users.join(orders, on="user_id", how="inner")

# Catalyst recognizes users is smaller
# Automatically broadcasts users (no shuffle!)
# Result: Much faster than shipping 100M rows

# You can hint Spark
from pyspark.sql.functions import broadcast
result = users.join(broadcast(orders), on="user_id")
# Explicit hint (use when auto-optimization fails)
```

### Cost Optimization

```python
# Cost formula: hours * (num_instances * cost_per_instance)
# = 1 hour * (10 instances * $1/hour) = $10

# Strategy 1: Use spot instances (cheaper, unreliable)
# 10 on-demand (reliable) + 30 spot (cheap)
# Cost savings: 30% -> 80% per job

# Strategy 2: Right-size instances
# Instead of: 100 x small instances
# Use: 20 x large instances
# Same parallelism, often cheaper

# Strategy 3: Use columnar formats
spark.read.parquet("file.parquet")  # Columnar, efficient
# NOT csv (row-based, reads every column even if not used)

# Strategy 4: Archive old data
# Daily loads of 100GB
# After 1 year: 36.5TB in Spark
# Solution: Archive to cheap storage (S3 Glacier)
# Access pattern: Recent data (hot), old data (cold)

# Strategy 5: Remove duplicates early
df = df.dropDuplicates()  # Smaller data flowing through pipeline

# Total optimization: 70% cost reduction
# 1 hour * 100 instances * $1 = $100
# 1 hour * 20 instances * $1 = $20 (saves $80/job)
```

### Monitoring and Observability

```python
# Spark UI (built-in)
# http://localhost:4040  # While job running

# Metrics
from pyspark.sql.functions import count, sum, avg

df = spark.read.parquet("data.parquet")
metrics = df.agg(
    count("*").alias("total_rows"),
    count(when(col("email").isNull(), 1)).alias("null_emails"),
    avg("age").alias("avg_age"),
    sum("salary").alias("total_salary")
)
metrics.show()

# Custom metrics
from pyspark.sql.functions import when

completeness = df.select(
    (count(when(col("email").isNotNull(), 1)) / count("*")).alias("email_completeness"),
    (count(when(col("phone").isNotNull(), 1)) / count("*")).alias("phone_completeness")
)
completeness.show()

# Logging
import logging
logger = logging.getLogger(__name__)

logger.info(f"Processing {df.count()} rows")
logger.error(f"Failed to process {error_count} records")
```

### Real-World Example: ETL Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder \
    .appName("ETL_Pipeline") \
    .config("spark.sql.shuffle.partitions", 200) \
    .getOrCreate()

# EXTRACT
print(f"[{datetime.now()}] Extracting data...")
raw = spark.read.option("bad StructType", "DROPMALFORMED") \
    .schema(...) \
    .json("s3://bucket/raw/events/")

# TRANSFORM
print(f"[{datetime.now()}] Transforming...")
cleaned = raw \
    .dropDuplicates(["event_id"]) \
    .filter(col("timestamp").isNotNull()) \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .dropna(subset=["user_id"])

validated = cleaned \
    .filter(col("amount") > 0) \
    .filter(col("user_id") > 0)

enriched = validated \
    .join(broadcast(user_dims), on="user_id", how="left") \
    .select(
        "event_id", "user_id", "event_type", "amount",
        "timestamp", "event_date", "user_segment"
    )

# LOAD
print(f"[{datetime.now()}] Loading...")
enriched \
    .write \
    .mode("append") \
    .partitionBy("event_date") \
    .parquet("s3://bucket/silver/events/")

print(f"[{datetime.now()}] Done!")
```

---

## Summary Table: All Levels

| Level | Focus | Key Skills | Time |
|-------|-------|-----------|------|
| L1 Junior | Basics | DataFrames, SQL, basic ops | 2w |
| L2 Intermediate | Efficiency | Joins, windowing, optimization | 3w |
| L3 Advanced | Production | Streaming, error handling, Kafka | 7w |
| L4 Senior | Scale | Tuning, ML, monitoring, cost | 8w |

---

## Quick Interview Prep

**Q: What's faster - RDD or DataFrame?**
A: DataFrame (optimized by Catalyst optimizer)

**Q: How to optimize slow join?**
A: Broadcast small side, partition large side, reduce data beforehand

**Q: How to handle large data that doesn't fit in memory?**
A: Don't collect() it, process distributed using Spark

**Q: Streaming vs batch?**
A: Batch for analytics, streaming for real-time, Structured Streaming bridges

**Q: Biggest performance issue?**
A: Shuffle (moving data between workers). Minimize with broadcast joins, partitioning, filtering early
