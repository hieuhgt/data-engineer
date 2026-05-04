# Spark Learning Path: Week-by-Week Guidance

## Overview
This path takes you from "what is Spark?" to "I can design production systems" in 20 weeks.

---

## LEVEL 1: JUNIOR (Weeks 1-2)

### Week 1: Fundamentals

**Monday-Tuesday: Understand Architecture**
- Read: SPARK_COMPLETE_GUIDE.md sections on Spark architecture
- Concepts: Driver, executor, partitions, Spark Context
- Time: 3 hours
- Goal: Understand the distributed model

**Wednesday-Thursday: DataFrames & Basic Operations**
- Read: DataFrame section in guide
- Do: Create DataFrame from CSV, explore with `show()`, `printSchema()`
- Code: `examples/level1_junior.py` - Examples 1-3
- Time: 4 hours
- Goal: Comfortable creating and exploring data

**Friday: Writing Queries**
- Read: Aggregations & SQL sections
- Do: Group by, count, sum, filter
- Code: `examples/level1_junior.py` - Examples 4-5
- Time: 3 hours
- Goal: Write basic SQL queries

**Weekend: Review**
- Review concepts
- Run all Level 1 examples (1-7)
- Time: 2 hours

### Week 2: Data Processing

**Monday-Tuesday: Joins**
- Learn: Inner, left, right, full joins
- Do: Join practice exercises
- Time: 3 hours

**Wednesday-Thursday: Reading & Writing**
- Learn: Parquet, CSV, JSON formats
- Do: Load from CSV, write to Parquet
- Time: 3 hours

**Friday: End-to-End Project**
- Project: Load → Filter → Transform → Write
- Example: Load users CSV, count by city, save as Parquet
- Time: 4 hours

**Weekend: Consolidate**
- Complete all Level 1 examples
- Build mini-project: word count or data summary
- Time: 2 hours

**Checkpoint:** Can you create DataFrame, filter, aggregate, save to Parquet?

---

## LEVEL 2: INTERMEDIATE (Weeks 3-5)

### Week 3: Complex Operations

**Monday: Window Functions**
- Learn: PARTITION BY, ORDER BY, ROW_NUMBER, LAG, LEAD
- Do: Rank products by category, running totals
- Time: 4 hours
- Goal: Comfortable with window functions

**Tuesday-Wednesday: Complex Joins**
- Learn: Skew handling, broadcast joins
- Do: Practice with sample data
- Time: 3 hours

**Thursday: Handling Bad Data**
- Learn: Nulls, duplicates, type conversion
- Do: Clean messy dataset
- Time: 3 hours

**Friday: Execution Plans**
- Learn: EXPLAIN, Catalyst optimizer
- Do: Read query plans, identify improvements
- Time: 3 hours

### Week 4: Performance

**Monday-Tuesday: Query Optimization**
- Learn: Column pruning, filter pushdown, join strategies
- Do: Optimize 5 slow queries
- Time: 4 hours

**Wednesday-Thursday: Caching**
- Learn: When to cache, memory considerations
- Do: Cache vs no-cache performance test
- Time: 3 hours

**Friday: Practical Tuning**
- Learn: Partitioning, repartition, coalesce
- Do: Partition exercise
- Time: 3 hours

### Week 5: Integration Project

**Monday-Friday: Build Real Pipeline**
- Project: ETL pipeline
  1. Load from multiple CSVs
  2. Join and deduplicate
  3. Add computed columns
  4. Save partitioned Parquet
- Time: 20 hours
- Goal: Build something real

**Checkpoint:** Can you optimize slow query, understand execution plans, build integrated pipeline?

---

## LEVEL 3: ADVANCED (Weeks 6-12)

### Week 6: Streaming Basics

**Monday-Wednesday: Structured Streaming Intro**
- Learn: ReadStream, WriteStream, micro-batches
- Do: Simple streaming example (files → storage)
- Time: 6 hours

**Thursday-Friday: Kafka Integration**
- Learn: Subscribe, parse, aggregate
- Do: Read from Kafka topic, transform, write
- Time: 4 hours

### Week 7: Advanced Streaming

**Monday-Wednesday: Watermarking & State**
- Learn: Handle late data, stateful operations
- Do: Aggregation with watermarking
- Time: 6 hours

**Thursday-Friday: Error Handling**
- Learn: Bad records, corruption, DLQ
- Do: Build resilient pipeline
- Time: 4 hours

### Weeks 8-10: Production Patterns

**Focus: Build reliable systems**
- Week 8: Idempotency, exactly-once semantics
- Week 9: Monitoring, metrics, alerting
- Week 10: Multi-job coordination, dependencies

### Weeks 11-12: Capstone Project

**Build:** Production-grade pipeline
- Ingest from Kafka
- Transform and validate
- Load to data lake (partitioned)
- Include monitoring
- Handle failures gracefully
- Time: 40 hours

**Checkpoint:** Can you build streaming pipeline from Kafka, handle errors, monitor health?

---

## LEVEL 4: SENIOR (Weeks 13-20)

### Week 13: Cluster Tuning

**Monday-Tuesday: Memory Management**
- Learn: Heap, off-heap, memory pressure
- Do: Tune executor memory for different workloads
- Time: 4 hours

**Wednesday-Thursday: Executor Configuration**
- Learn: Cores, instances, resource allocation
- Do: Right-size cluster for cost/performance
- Time: 4 hours

**Friday: Monitoring**
- Learn: Spark UI, metrics, profiling
- Do: Monitor real job, identify bottlenecks
- Time: 2 hours

### Week 14: Advanced Optimization

**Monday-Wednesday: Catalyst Deep Dive**
- Learn: Logical vs physical plans, cost-based optimization
- Do: Understand query plan optimization
- Time: 6 hours

**Thursday-Friday: Cost Optimization**
- Learn: Spot instances, instance selection, storage
- Do: Calculate and optimize monthly spend
- Time: 4 hours

### Week 15: ML Integration

**Monday-Friday: Spark MLlib**
- Learn: Feature engineering, ML pipelines
- Do: Build simple ML model
- Time: 20 hours

### Week 16: Multi-Job Systems

**Focus: Orchestration and dependencies**
- Learn: Job dependencies, retry logic
- Do: Integrate with Airflow (next topic)
- Time: 20 hours

### Weeks 17-20: Enterprise Project

**Build:** Complete data platform
- Multiple Kafka sources
- Complex transformations
- Streaming + batch
- ML model serving
- Monitoring and alerting
- Time: 80 hours

**Checkpoint:** Can you design and operate production Spark system at scale?

---

## Recommended Practice Path

### If you have 1 week:
- Days 1-2: SPARK_COMPLETE_GUIDE.md LEVEL 1
- Days 3-4: examples/level1_junior.py + examples/level2_intermediate.py
- Days 5-7: Build small ETL project

### If you have 3 weeks:
- Week 1: LEVEL 1 (complete)
- Week 2: LEVEL 2 (complete)
- Week 3: Build medium project + practice queries

### If you have 2 months:
- Weeks 1-2: LEVEL 1 + early LEVEL 2
- Weeks 3-4: LEVEL 2 + optimization
- Weeks 5-6: LEVEL 3 + streaming
- Weeks 7-8: Capstone project

### For Interview Prep (3 days):
- Day 1: SPARK_COMPLETE_GUIDE.md (skim L1 + L2, focus on concepts)
- Day 2: Practice optimization exercises + streaming basics
- Day 3: Review quick reference, practice interview questions

---

## Key Milestones

### After Week 2 (JUNIOR)
- [ ] Create DataFrame from various sources
- [ ] Filter, select, group, aggregate
- [ ] Write Spark SQL queries
- [ ] Understand basic optimization

### After Week 5 (INTERMEDIATE)
- [ ] Optimize slow queries (using EXPLAIN)
- [ ] Use window functions effectively
- [ ] Handle duplicates and nulls
- [ ] Cache for performance
- [ ] Build integrated ETL pipeline

### After Week 12 (ADVANCED)
- [ ] Process streaming data with Structured Streaming
- [ ] Integrate Kafka into Spark job
- [ ] Implement error handling and retries
- [ ] Build production-grade pipeline
- [ ] Monitor job health

### After Week 20 (SENIOR)
- [ ] Tune clusters for cost/performance
- [ ] Optimize memory usage
- [ ] Understand Catalyst optimizer
- [ ] Build ML pipelines
- [ ] Design enterprise systems
- [ ] Troubleshoot production issues

---

## Interview Preparation Schedule

### 1-Day Intensive (Before Interview Tomorrow)
```
Morning (3 hours):
- Read SPARK_COMPLETE_GUIDE.md L1 (DataFrame, operations)
- Review L2 (joins, optimization)

Afternoon (2 hours):
- Do 5 practice optimization exercises
- Review common mistakes section

Evening (2 hours):
- Practice interview questions
- Get good sleep (important!)
```

### 3-Day Preparation (Recommended)
```
Day 1 (6 hours):
- LEVEL 1 (fundamentals)
- examples/level1_junior.py

Day 2 (6 hours):
- LEVEL 2 (optimization)
- examples/level2_intermediate.py
- Practice query optimization

Day 3 (4 hours):
- Quick review of all levels
- Interview questions practice
- Sleep well before interview
```

---

## Common Questions & Answers

**Q: Do I need to install Spark?**
A: Yes, or use PySpark locally. For interview: understand concepts, can write pseudocode

**Q: Should I use Scala or Python?**
A: Python (PySpark) for data engineering, Scala for Spark internals

**Q: How much SQL do I need to know?**
A: Joins, groupBy, window functions, subqueries (Level 2-3 SQL required)

**Q: Should I learn Spark before Airflow?**
A: Yes, understand Spark first, then learn Airflow for orchestration

**Q: What's the most important topic?**
A: Optimization (tuning, partitioning, caching) - this is what makes you valuable

---

## Next: Airflow

After completing LEVEL 2-3 of Spark, move to:
- `../airflow/AIRFLOW_COMPLETE_GUIDE.md`
- Learn how to schedule and orchestrate Spark jobs
