# Apache Kafka - Complete Deep Dive

## 📚 Welcome to Kafka Mastery

This directory contains a **comprehensive journey from Junior to Senior Kafka engineer** with:

- 📖 **Complete guide** covering all levels
- 💻 **Runnable code examples** for each topic
- 🎯 **Structured learning path**
- 🏗️ **Production patterns**
- 🚀 **Real-world scenarios**

---

## 🎯 Quick Start

### For Beginners (Junior)

```bash
# 1. Read the guide
cat KAFKA_COMPLETE_GUIDE.md | grep -A 100 "LEVEL 1"

# 2. Run the examples
python examples/level1_junior.py

# 3. Choose example (try example 7 for end-to-end)
# 4. Explore and modify
```

### For Learning Path

```bash
# See the complete learning roadmap
cat KAFKA_LEARNING_PATH.md

# This tells you exactly what to learn and when
```

### For Understanding Concepts

```bash
# Deep understanding of all topics
cat KAFKA_COMPLETE_GUIDE.md

# All 4 levels with explanations and code
```

---

## 📂 What's Here

```
kafka/
├── KAFKA_COMPLETE_GUIDE.md     ← Read this first! (Comprehensive)
├── KAFKA_LEARNING_PATH.md      ← Your learning roadmap
├── README.md                   ← This file
│
├── examples/
│   ├── level1_junior.py        ← Fundamentals (7 examples)
│   ├── level2_intermediate.py  ← Advanced patterns (8 examples)
│   └── level3_advanced.py      ← Production patterns (7 examples)
│
├── producers/
│   └── event_producer.py       ← Real producer (used in pipeline)
│
├── consumers/
│   └── event_consumer.py       ← Real consumer (used in pipeline)
│
└── schemas/
    └── events.avsc             ← Avro schema example
```

---

## 📖 Learning Levels

### Level 1: Junior (Weeks 1-2)
**Goal**: Understand Kafka basics and write working code

**Topics**:
- What is Kafka?
- Topics, partitions, brokers, offsets
- Producers and consumers
- Consumer groups
- Basic error handling

**Examples**: `level1_junior.py` (7 examples)
- Basic producer
- Basic consumer
- Consumer groups
- Callbacks
- Offset tracking
- Error handling
- End-to-end pipeline

**You'll learn**:
- ✅ How to write producer code
- ✅ How to write consumer code
- ✅ How consumer groups work
- ✅ How to handle basic errors

---

### Level 2: Intermediate (Weeks 3-5)
**Goal**: Build reliable, monitored systems

**Topics**:
- Consumer group coordination
- Manual vs auto offset commits
- Consumer lag monitoring
- Replication & fault tolerance
- Error handling & Dead Letter Queue
- Performance tuning basics

**Examples**: `level2_intermediate.py` (8 examples)
- Consumer groups with load balancing
- Manual offset management
- Consumer lag monitoring
- Seeking to offsets
- Producer retries
- Dead letter queue
- Batch processing
- Metrics monitoring

**You'll learn**:
- ✅ How to manage offsets reliably
- ✅ How to monitor consumer health
- ✅ How to implement DLQ
- ✅ How to optimize throughput
- ✅ How to handle producer failures

---

### Level 3: Advanced (Weeks 6-12)
**Goal**: Implement production patterns

**Topics**:
- Idempotent producers (exactly-once)
- Transactions
- Exactly-once consumer processing
- Stream processing & windowing
- Complex failure scenarios
- Multi-partition coordination
- Performance analysis

**Examples**: `level3_advanced.py` (7 examples)
- Idempotent producer
- Transactions (atomic writes)
- Exactly-once consumer
- Stream processing with windows
- DLQ with exponential backoff
- Multi-partition tracking
- Performance analysis

**You'll learn**:
- ✅ How to guarantee exactly-once delivery
- ✅ How to use transactions
- ✅ How to build stream processing
- ✅ How to handle complex failures
- ✅ How to measure performance

---

### Level 4: Senior (Weeks 13-20)
**Goal**: Operate production systems at scale

**Topics**:
- Multi-datacenter replication
- Security (ACLs, SSL/TLS, SASL)
- High-availability clusters
- Scaling & optimization
- Operational excellence
- Disaster recovery
- Advanced patterns

**Resources**:
- Read: Level 4 in `KAFKA_COMPLETE_GUIDE.md`
- Deploy: `kubernetes/` configurations
- Reference: `docs/DEPLOYMENT_GUIDE.md`

**You'll learn**:
- ✅ How to design HA clusters
- ✅ How to implement security
- ✅ How to operate at scale
- ✅ How to handle disasters
- ✅ How to troubleshoot issues

---

## 🚀 How to Use This

### Method 1: Structured Learning (Recommended)

```bash
# 1. Read the learning path
cat KAFKA_LEARNING_PATH.md

# 2. Follow week-by-week guidance
# 3. Run examples for your current level
# 4. Build mini-projects

python examples/level1_junior.py      # Week 1-2
python examples/level2_intermediate.py # Week 3-5
python examples/level3_advanced.py     # Week 6-12
```

### Method 2: Problem-Based Learning

**"I need to..."**
| Need | Example | File |
|------|---------|------|
| Send data | level1_junior.py example 1 | `producers/event_producer.py` |
| Receive data | level1_junior.py example 2 | `consumers/event_consumer.py` |
| Load balance | level2_intermediate.py example 1 | level2_intermediate.py |
| Monitor lag | level2_intermediate.py example 3 | level2_intermediate.py |
| Guarantee delivery | level3_advanced.py example 1 | level3_advanced.py |
| Process transactionally | level3_advanced.py example 2 | level3_advanced.py |

### Method 3: Reference Learning

**Look up any topic**:
```bash
# Search the complete guide
grep -n "idempotent" KAFKA_COMPLETE_GUIDE.md
grep -n "consumer group" KAFKA_COMPLETE_GUIDE.md
grep -n "exactly-once" KAFKA_COMPLETE_GUIDE.md
```

---

## 💡 Key Concepts at a Glance

### Topics & Partitions
```
Topic: "orders"
├── Partition 0: [msg1, msg2, msg3]
├── Partition 1: [msg4, msg5]
└── Partition 2: [msg6, msg7, msg8]

Benefits:
- Parallelism (multiple consumers)
- Scalability (spread load)
- Ordering (within partition)
```

### Consumer Groups
```
Topic: "orders"

Group: "payment_processors"
├── Consumer 1 → Partition 0
├── Consumer 2 → Partition 1
└── Consumer 3 → Partition 2

Benefits:
- Load balancing
- Failover (if consumer dies, partition reassigned)
- Scaling (add more consumers)
```

### Offsets & Commits
```
Partition: [m0, m1, m2, m3, m4, m5]
           0   1   2   3   4   5

Consumer offset: 2
Meaning: Already processed m0 and m1

Commit: Save offset to Kafka
Next time: Start from offset 2+1=3
```

### Exactly-Once Semantics
```
Without: Retry → Duplicate messages
With: Producer deduplicates → No duplicates
Pattern: idempotent producer + manual commits
```

---

## 🔧 Running Examples

### Prerequisites

```bash
# Kafka running
docker-compose -f docker-compose.full.yml up -d

# Wait for startup
sleep 30

# Verify Kafka is ready
docker-compose -f docker-compose.full.yml logs kafka
```

### Run an Example

```bash
# Terminal 1: Start Kafka
cd /path/to/data-engineer
docker-compose -f docker-compose.full.yml up -d

# Terminal 2: Run examples
python kafka/examples/level1_junior.py

# Choose example (1-7)
# Example 1: Basic producer
# Example 2: Basic consumer
# etc.
```

### Create Sample Data

```bash
# Generate test data
python scripts/generate_sample_data.py

# Produce events
python kafka/producers/event_producer.py
```

### Monitor Kafka

```bash
# List topics
docker-compose -f docker-compose.full.yml exec kafka \
  kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
docker-compose -f docker-compose.full.yml exec kafka \
  kafka-topics.sh --describe --bootstrap-server localhost:9092 \
  --topic my_topic

# Monitor consumer group
docker-compose -f docker-compose.full.yml exec kafka \
  kafka-consumer-groups.sh --describe \
  --bootstrap-server localhost:9092 \
  --group my_group
```

---

## 📊 What You'll Build

### Week 2 (Junior): Simple Pipeline
```
Producer → Kafka → Consumer
(Events)   (Queue)  (Process)
```

### Week 5 (Intermediate): Reliable System
```
Producer
  ├─ Retries
  └─ Acks=all
     ↓
  Kafka Topic
     ↓
Consumer
  ├─ Manual commits
  ├─ Error handling
  └─ DLQ for failures
```

### Week 12 (Advanced): Production System
```
Idempotent Producer
  ├─ No duplicates
  └─ Transactions
     ↓
  Kafka Cluster (3 brokers)
     ↓
Exactly-Once Consumer
  ├─ Exactly-once processing
  ├─ Exponential backoff
  └─ DLQ with retry
     ↓
  Analytics / Data Lake
```

### Week 20 (Senior): Enterprise System
```
Multi-DC Kafka Cluster
  ├─ Security (SASL + SSL + ACLs)
  ├─ Replication across datacenters
  ├─ HA coordination
  └─ Monitoring + Alerting
     ↓
Event Sourcing Pipeline
  ├─ Exactly-once delivery
  ├─ Transactions
  ├─ Stream processing
  └─ Disaster recovery
```

---

## 🎓 Learning Outcomes

### After Level 1
- [ ] Understand Kafka architecture
- [ ] Write basic producer/consumer
- [ ] Know how consumer groups work
- [ ] Can explain offsets and commits

### After Level 2
- [ ] Implement reliable producer with retries
- [ ] Manage offsets manually
- [ ] Monitor consumer lag
- [ ] Build dead letter queue
- [ ] Optimize throughput with batching

### After Level 3
- [ ] Implement exactly-once semantics
- [ ] Use Kafka transactions
- [ ] Build stream processing apps
- [ ] Handle complex failures
- [ ] Analyze performance

### After Level 4
- [ ] Design HA Kafka clusters
- [ ] Implement security policies
- [ ] Operate production systems
- [ ] Troubleshoot complex issues
- [ ] Lead infrastructure projects

---

## 🚨 Common Mistakes (Don't Make These!)

```python
# ❌ DON'T: Auto-commit with error
consumer = KafkaConsumer(..., enable_auto_commit=True)
for msg in consumer:
    process(msg)  # If error, still committed!

# ✅ DO: Manual commit after processing
consumer = KafkaConsumer(..., enable_auto_commit=False)
for msg in consumer:
    try:
        process(msg)
        consumer.commit()  # Only after success
    except:
        pass  # Don't commit = retry

# ❌ DON'T: acks=0 for important data
producer = KafkaProducer(..., acks=0)  # No confirmation!

# ✅ DO: acks='all' for critical data
producer = KafkaProducer(..., acks='all')  # Wait for all replicas

# ❌ DON'T: Reuse message IDs (breaks deduplication)
msg = {"id": 123, "data": "..."}
producer.send(..., value=msg)
producer.send(..., value=msg)  # Duplicate!

# ✅ DO: Use idempotent producer
producer = KafkaProducer(..., enable_idempotence=True)
# Kafka deduplicates internally
```

---

## 📞 Quick Reference

### When to Use Consumer Groups
- Scaling: Add more consumers to process faster
- Failover: If consumer dies, partition reassigned
- Load balancing: Distribute work across consumers

### When to Use DLQ
- Poison messages: Can't be processed
- Temporary failures: Retry later
- Invalid data: Log for manual inspection

### When to Use Transactions
- Atomic writes: All-or-nothing across topics
- Event sourcing: Multiple related events
- CQRS: Synchronized read/write models

### When to Manual Commit
- Complex processing: Ensure success before committing
- Batch processing: Commit after batch done
- Exactly-once: Sync offset with DB writes

---

## 🔗 Integration with Data Pipeline

This Kafka guide integrates with your main data pipeline:

```
Data Engineer Project
│
├── Kafka (THIS GUIDE)
│   ├── Produces events
│   ├── Consumes for processing
│   └── Integrates with Spark
│
├── Spark Jobs
│   ├── Read from Kafka
│   ├── Process data
│   └── Write to data lake
│
├── Airflow Orchestration
│   ├── Monitor Kafka lag
│   ├── Trigger on data arrival
│   └── Manage pipeline
│
└── Data Lake (Bronze/Silver/Gold)
    ├── Bronze: Raw Kafka events
    ├── Silver: Transformed
    └── Gold: Ready for analytics
```

See `../ kafka/producers/event_producer.py` for real producer used in pipeline.
See `../spark/jobs/ingest_kafka.py` for consuming Kafka into Spark.

---

## 🎯 Next Steps

1. **Start with Level 1**: Run `level1_junior.py` examples
2. **Follow the path**: Use `KAFKA_LEARNING_PATH.md` as guide
3. **Build projects**: After each level, build something
4. **Read deep guide**: Reference `KAFKA_COMPLETE_GUIDE.md`
5. **Integrate**: Use with your data pipeline

---

## 📚 Additional Resources

- **Apache Kafka Official Docs**: https://kafka.apache.org/documentation/
- **Confluent Blog**: https://www.confluent.io/blog/
- **This Guide**: KAFKA_COMPLETE_GUIDE.md
- **Learning Path**: KAFKA_LEARNING_PATH.md
- **Examples**: `examples/` directory

---

**Ready? Start with `level1_junior.py` example 7 (end-to-end) to see it all work together!** 🚀

**Questions? Check `KAFKA_COMPLETE_GUIDE.md` for detailed explanations.**

**Want structure? Follow `KAFKA_LEARNING_PATH.md` for week-by-week guidance.**
