# Apache Spark - Complete Deep Dive (PySpark)

## 📚 Welcome to Spark Mastery

This directory contains a **comprehensive journey from Junior to Senior Spark engineer** with:

- 📖 **Complete guide** covering all levels (fundamentals to production optimization)
- 💻 **Runnable code examples** for each topic
- 🎯 **Structured learning path** (week-by-week progression)
- 🏗️ **Production patterns** (real-world scenarios)
- 🚀 **Performance optimization** (tuning, partitioning, caching)
- 🔗 **Integration examples** (Kafka, Airflow, Data Lake)

---

## 🎯 Quick Start

### For Beginners (Junior)
```bash
# 1. Read the guide (start with Level 1)
grep -A 200 "LEVEL 1: JUNIOR" SPARK_COMPLETE_GUIDE.md

# 2. Run the examples
python examples/level1_junior.py

# 3. Try interactive mode
pyspark  # Launch Spark shell
```

### For Learning Path
```bash
# See the complete learning roadmap
cat SPARK_LEARNING_PATH.md

# Follow week-by-week guidance
```

### For Understanding Concepts
```bash
# Deep understanding of all topics
cat SPARK_COMPLETE_GUIDE.md

# All levels with explanations and code
```

---

## 📂 What's Here

```
spark/
├── SPARK_COMPLETE_GUIDE.md     ← Read this first! (Comprehensive)
├── SPARK_LEARNING_PATH.md      ← Your learning roadmap
├── README.md                   ← This file
│
├── examples/
│   ├── level1_junior.py        ← Fundamentals (RDDs, DataFrames, SQL)
│   ├── level2_intermediate.py  ← Advanced (joins, aggregations, optimization)
│   ├── level3_advanced.py      ← Production (streaming, tuning, patterns)
│   └── level4_senior.py        ← Enterprise (clusters, ML, real systems)
│
├── notebooks/
│   ├── 01-dataframe-basics.ipynb
│   ├── 02-sql-queries.ipynb
│   ├── 03-performance-tuning.ipynb
│   └── 04-streaming.ipynb
│
└── jobs/
    ├── ingest_kafka.py         ← Read from Kafka (production)
    ├── transform_data.py       ← ETL transformation (production)
    └── aggregate_metrics.py    ← Analytics aggregation (production)
```

---

## 📖 Learning Levels

### Level 1: Junior (Weeks 1-2)
**Goal:** Understand Spark fundamentals and write working code

**Topics:**
- What is Spark? Architecture & RDDs
- SparkSession and DataFrames
- Loading data (CSV, JSON, Parquet)
- Basic DataFrame operations (select, filter, map)
- SQL queries
- Basic aggregations (count, sum, avg)

**You'll learn:**
- ✅ How to create DataFrames
- ✅ How to perform basic transformations
- ✅ How SQL works in Spark
- ✅ How to write data to storage

---

### Level 2: Intermediate (Weeks 3-5)
**Goal:** Build efficient, optimized pipelines

**Topics:**
- Complex joins (inner, left, right, full)
- Window functions (row_number, lag, lead)
- Complex aggregations (groupBy, pivot)
- Handling missing data
- Performance optimization basics
- Query execution plans

**You'll learn:**
- ✅ How to join large datasets efficiently
- ✅ How to use window functions
- ✅ How to read and understand query plans
- ✅ How to optimize basic queries

---

### Level 3: Advanced (Weeks 6-12)
**Goal:** Implement production-grade systems

**Topics:**
- Partitioning strategies
- Caching and persistence
- Broadcasting for optimization
- Streaming with Structured Streaming
- Complex transformations
- Error handling and data quality
- Integration with Kafka

**You'll learn:**
- ✅ How to partition data for performance
- ✅ How to use caching strategically
- ✅ How to process streaming data
- ✅ How to handle failures gracefully

---

### Level 4: Senior (Weeks 13-20)
**Goal:** Operate enterprise-scale systems

**Topics:**
- Cluster management and tuning
- Memory management
- Catalyst optimizer deep dive
- Cost optimization
- Monitoring and observability
- Multi-job orchestration
- Security and compliance
- ML with Spark

**You'll learn:**
- ✅ How to tune clusters for cost/performance
- ✅ How to optimize for large-scale data
- ✅ How to troubleshoot production issues
- ✅ How to build ML pipelines

---

## 🚀 How to Use This

### Method 1: Structured Learning (Recommended)
```bash
# 1. Read the learning path
cat SPARK_LEARNING_PATH.md

# 2. Follow week-by-week guidance
# 3. Run examples for your current level

python examples/level1_junior.py      # Week 1-2
python examples/level2_intermediate.py # Week 3-5
python examples/level3_advanced.py     # Week 6-12
python examples/level4_senior.py       # Week 13-20
```

### Method 2: Problem-Based Learning
**"I need to..."**
| Need | Example | File |
|------|---------|------|
| Load CSV | level1_junior.py example 1 | level1_junior.py |
| Query with SQL | level1_junior.py example 3 | level1_junior.py |
| Join tables | level2_intermediate.py example 1 | level2_intermediate.py |
| Get top N per group | level2_intermediate.py example 2 | level2_intermediate.py |
| Stream from Kafka | level3_advanced.py example 3 | level3_advanced.py |
| Optimize large job | level4_senior.py example 1 | level4_senior.py |

### Method 3: Reference Learning
**Look up any topic:**
```bash
# Search the complete guide
grep -n "DataFrame" SPARK_COMPLETE_GUIDE.md
grep -n "optimization" SPARK_COMPLETE_GUIDE.md
grep -n "streaming" SPARK_COMPLETE_GUIDE.md
```

---

## 💡 Key Concepts at a Glance

### RDD vs DataFrame vs SQL
```
RDD (Low-level):
- Unstructured, distributed collection
- Low-level transformations (map, filter)
- Less optimized

DataFrame (High-level):
- Structured data (like SQL table)
- Optimized by Catalyst
- Recommended for most use cases

SQL (Highest-level):
- Write SQL queries directly
- Same performance as DataFrame
- More familiar to data analysts
```

### Transformations vs Actions
```
Transformations (Lazy):
- map, filter, groupBy, join
- Don't execute immediately
- Builds execution plan

Actions (Eager):
- show, collect, count, write
- Execute immediately
- Actually compute results
```

### Partitioning
```
Data without partitioning:
[Partition 0: ALL 100M rows]
Result: One executor, slow

Data with partitioning:
[Part 0: 25M] [Part 1: 25M] [Part 2: 25M] [Part 3: 25M]
Result: 4 executors parallel, 4x faster
```

---

## 🔧 Setup & Environment

### Local Development
```bash
# Install PySpark
pip install pyspark

# Start Spark shell
pyspark

# Or use in Python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

### Docker Setup
```bash
# Run Spark in Docker
docker run -it apache/spark:latest /bin/bash

# Or use docker-compose
docker-compose up spark-master spark-worker-1
```

### Configuration
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataPipeline") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

---

## 📊 Learning Outcomes

### After Level 1
- [ ] Understand Spark architecture
- [ ] Create and manipulate DataFrames
- [ ] Write Spark SQL queries
- [ ] Load/save data in multiple formats
- [ ] Run basic transformations

### After Level 2
- [ ] Join multiple datasets
- [ ] Use window functions
- [ ] Optimize basic queries
- [ ] Handle missing/duplicate data
- [ ] Understand query execution plans

### After Level 3
- [ ] Design partitioning strategies
- [ ] Implement caching strategy
- [ ] Process streaming data
- [ ] Integrate with Kafka
- [ ] Build production ETL pipelines

### After Level 4
- [ ] Tune clusters for performance
- [ ] Optimize memory usage
- [ ] Troubleshoot production issues
- [ ] Build ML pipelines
- [ ] Lead infrastructure projects

---

## 🚨 Common Mistakes (Don't Make These!)

```python
# ❌ DON'T: Collect large data to driver
large_df = spark.read.parquet("data.parquet")
data = large_df.collect()  # Crashes if > driver memory!

# ✅ DO: Process in Spark (distributed)
large_df.write.parquet("output/")  # Stays distributed

# ❌ DON'T: Create many small DataFrames
df1 = spark.read.csv("file1.csv")
df2 = spark.read.csv("file2.csv")
df3 = spark.read.csv("file3.csv")
# Result: 3 separate jobs, slow

# ✅ DO: Load all at once
df = spark.read.csv("file*.csv")  # 1 job

# ❌ DON'T: Ignore shuffling
df.groupBy("user_id").count()  # Huge shuffle if many users!

# ✅ DO: Partition before grouping
df.repartition("user_id").groupBy("user_id").count()

# ❌ DON'T: Join without knowing sizes
large_df.join(other_large_df)  # Cartesian product = disaster

# ✅ DO: Use broadcast for small data
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")  # Small data sent to all workers
```

---

## 📞 Quick Reference

### Loading Data
```python
# CSV
df = spark.read.csv("file.csv", header=True)

# JSON
df = spark.read.json("file.json")

# Parquet
df = spark.read.parquet("file.parquet")

# Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .load()
```

### Common Operations
```python
# Select columns
df.select("name", "age")

# Filter rows
df.filter(df.age > 18)

# Add column
from pyspark.sql.functions import col
df.withColumn("age_group", when(col("age") < 18, "minor").otherwise("adult"))

# Group and aggregate
df.groupBy("category").count()

# Join
df1.join(df2, on="user_id", how="inner")

# Window function
from pyspark.sql.functions import row_number, Window
df.withColumn("rank", row_number().over(Window.partitionBy("category").orderBy("price")))
```

### Saving Data
```python
# Write (overwrite)
df.write.mode("overwrite").parquet("output/")

# Append
df.write.mode("append").parquet("output/")

# Streaming
query = df.writeStream \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

---

## 🔗 Integration Examples

### With Kafka (Streaming)
See: `jobs/ingest_kafka.py`
```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Transform
result = df.select(col("value").cast("string"))

# Write to storage
query = result.writeStream \
    .option("checkpointLocation", "/tmp/chk") \
    .parquet("data/")
```

### With Airflow (Scheduling)
See: `../airflow/dags/spark_job.py`
```python
from airflow.operators.spark_submit import SparkSubmitOperator

task = SparkSubmitOperator(
    task_id="spark_job",
    application="jobs/transform_data.py",
    conf={"spark.executor.memory": "4g"}
)
```

### Data Lake (Bronze → Silver → Gold)
```
Bronze Layer (Raw)
  ↓ ingest_kafka.py (Spark Streaming)
Silver Layer (Cleaned)
  ↓ transform_data.py (Spark Batch)
Gold Layer (Analytics)
  ↓ aggregate_metrics.py (Spark SQL)
Ready for BI Tools
```

---

## 🎓 Interview Preparation

### Common Questions
1. **RDD vs DataFrame:** Which and why?
   - Answer: DataFrame (optimized, faster, recommended for most use cases)

2. **How to optimize slow job?**
   - Answer: Partitioning, caching, broadcast join, reduce shuffling

3. **How to handle large joins?**
   - Answer: Broadcast small side, partition large side, use skew handling

4. **Spark vs SQL?**
   - Answer: Same performance, SQL more familiar, Spark more flexible

5. **Streaming vs Batch?**
   - Answer: Batch for analytics, Streaming for real-time, Structured Streaming bridges gap

---

## 🎯 Next Steps

1. **Start with Level 1**: Run `examples/level1_junior.py`
2. **Follow the path**: Use `SPARK_LEARNING_PATH.md` as guide
3. **Build projects**: Mini-ETL, data transformations
4. **Read deep guide**: Reference `SPARK_COMPLETE_GUIDE.md`
5. **Integrate**: Use with Kafka, Airflow, Data Lake

---

## 📚 Additional Resources

- **Spark Official Docs**: https://spark.apache.org/docs/latest/
- **Databricks Learning**: https://academy.databricks.com/
- **This Guide**: SPARK_COMPLETE_GUIDE.md
- **Learning Path**: SPARK_LEARNING_PATH.md
- **Examples**: `examples/` directory

---

**Ready? Start with `examples/level1_junior.py` to see it all work together!** 🚀

**Questions? Check `SPARK_COMPLETE_GUIDE.md` for detailed explanations.**

**Want structure? Follow `SPARK_LEARNING_PATH.md` for week-by-week guidance.**
