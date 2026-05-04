# Apache Kafka: Complete Guide (Junior → Senior)

## Table of Contents

1. [Level 1: Fundamentals (Junior)](#level-1-fundamentals-junior)
2. [Level 2: Intermediate](#level-2-intermediate)
3. [Level 3: Advanced](#level-3-advanced)
4. [Level 4: Production Mastery (Senior)](#level-4-production-mastery-senior)
5. [Real-World Scenarios](#real-world-scenarios)
6. [Troubleshooting Guide](#troubleshooting-guide)

---

## LEVEL 1: Fundamentals (Junior)

### 1.1 What is Kafka?

**Simple Definition**: Kafka is a **distributed message queue** (event streaming platform) that allows applications to:
- **Produce** (send) data/events
- **Consume** (receive) data/events
- **Store** data durably for replay

**Real-World Analogy**:
```
Think of Kafka like a newspaper:
- Writers (Producers) write articles
- Newspapers store them on shelves (Topics)
- Readers (Consumers) pick up papers to read
- Readers can go back to read old papers (Replay)
```

### 1.2 Core Concepts

#### Concept 1: Topics
A **topic** is a named channel where data flows.

```
Topic: "user_events"
├── Partition 0: [event1, event2, event3]
├── Partition 1: [event4, event5, event6]
└── Partition 2: [event7, event8, event9]
```

Think: "events table in a database"

```python
# When you produce to a topic
topic = "user_events"  # Must exist or be auto-created

# Data goes into partitions automatically
producer.send("user_events", value={"user_id": 123, "action": "click"})
```

#### Concept 2: Partitions
A **partition** is a log of messages within a topic.

```
Why partitions?
1. Parallelism - multiple consumers can read different partitions
2. Scalability - spread load across machines
3. Ordering - messages in a partition are ordered

Partition = Ordered log
[msg1] → [msg2] → [msg3] → [msg4] → [msg5]
 ↑
offset 0, 1, 2, 3, 4 (position of each message)
```

#### Concept 3: Broker
A **broker** is a Kafka server that stores data.

```
Kafka Cluster
├── Broker 1 (host1:9092)
├── Broker 2 (host2:9092)
└── Broker 3 (host3:9092)
```

#### Concept 4: Offsets
An **offset** is the position of a message in a partition.

```
Partition 0:
Position:  0    1    2    3    4
Message: [msg1][msg2][msg3][msg4][msg5]
         ↑                        ↑
      offset 0              offset 4 (current)

Consumer tracking:
- Consumer A is at offset 2 (read msg1, msg2)
- Consumer B is at offset 4 (read all messages)
```

### 1.3 Producer Basics

**Producer** = Application that sends data to Kafka

```python
from kafka import KafkaProducer
import json

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send single message (async)
producer.send('user_events', value={'user_id': 123, 'action': 'click'})

# Wait for all messages to send
producer.flush()

# Close producer
producer.close()
```

**Key Points**:
- **Async by default** - send() returns immediately
- **Topics created automatically** if `auto.create.topics.enable=true`
- **Partitioning** - messages distributed across partitions automatically

### 1.4 Consumer Basics

**Consumer** = Application that reads data from Kafka

```python
from kafka import KafkaConsumer
import json

# Create consumer
consumer = KafkaConsumer(
    'user_events',  # Topic to consume from
    bootstrap_servers=['localhost:9092'],
    group_id='my_app',  # Consumer group (more on this later)
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'  # Start from beginning if no offset
)

# Read messages
for message in consumer:
    print(f"Received: {message.value}")
    # Message automatically committed (default behavior)

# Close consumer
consumer.close()
```

### 1.5 Producer-Consumer Workflow

```
PRODUCER SIDE:
──────────────
Event occurs in App A
    ↓
producer.send("events", value={...})
    ↓
Message queued in Kafka
    ↓
Message stored in partition
    ↓
Producer gets confirmation (optional)

KAFKA STORAGE:
──────────────
Topic: "events"
├── Partition 0: [event1, event2, event3] ← Messages stored
├── Partition 1: [event4, event5]
└── Partition 2: [event6, event7, event8]

CONSUMER SIDE:
──────────────
App B creates consumer
    ↓
consumer.poll() → fetch messages
    ↓
Process message
    ↓
Commit offset (mark as processed)
    ↓
Continue to next message
```

### 1.6 Simple End-to-End Example

**Producer** (producer_basic.py):
```python
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send 10 events
for i in range(10):
    event = {'id': i, 'message': f'Event {i}', 'timestamp': time.time()}
    producer.send('my_topic', value=event)
    print(f"Sent: {event}")

producer.flush()
producer.close()
```

**Consumer** (consumer_basic.py):
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='my_group'
)

print("Waiting for messages...")
for message in consumer:
    print(f"Received: {message.value}")
```

**Run It**:
```bash
# Terminal 1: Start consumer (waits for messages)
python consumer_basic.py

# Terminal 2: Run producer (sends messages)
python producer_basic.py

# Terminal 1 output:
# Received: {'id': 0, 'message': 'Event 0', ...}
# Received: {'id': 1, 'message': 'Event 1', ...}
# ... (all 10 messages)
```

---

## LEVEL 2: Intermediate

### 2.1 Consumer Groups

**Consumer Group** = Named group of consumers reading same topic

```
Topic: "orders"

Without Consumer Groups (all consumers read all messages):
┌─────────────────┐
│  Consumer A     │ ← Gets messages 1,2,3,4,5
└─────────────────┘
┌─────────────────┐
│  Consumer B     │ ← Gets messages 1,2,3,4,5 (DUPLICATE!)
└─────────────────┘

With Consumer Groups (messages distributed):
┌──────────────────────────────────┐
│   group_id = "payment_service"   │
├──────────────┬──────────────────┤
│ Consumer A   │  Consumer B      │
│ reads part 0 │  reads part 1    │
├──────────────┼──────────────────┤
│ offset: 100  │  offset: 95      │
└──────────────┴──────────────────┘
```

**Why Consumer Groups?**
1. **Load balancing** - divide work among consumers
2. **Scaling** - add more consumers to process faster
3. **Fault tolerance** - if one consumer dies, others continue

```python
# Consumer Group Example
from kafka import KafkaConsumer

# All consumers with same group_id = one group
consumer1 = KafkaConsumer(
    'orders',
    group_id='payment_processors',  # Same group
    bootstrap_servers=['localhost:9092']
)

consumer2 = KafkaConsumer(
    'orders',
    group_id='payment_processors',  # Same group = LOAD BALANCED
    bootstrap_servers=['localhost:9092']
)

# Messages split between consumers:
# Message 0 → Consumer 1
# Message 1 → Consumer 2
# Message 2 → Consumer 1
# ... (alternating by partition assignment)
```

### 2.2 Offset Management

**Offset** = Consumer's position in partition

```python
from kafka import KafkaConsumer, OffsetAndTimestamp

consumer = KafkaConsumer(
    'orders',
    group_id='my_group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',     # What offset to start at
    enable_auto_commit=True,            # Auto-commit offset
    auto_commit_interval_ms=5000        # Commit every 5 seconds
)

# Manual offset management
for message in consumer:
    try:
        process(message)
        # Only commit after successful processing
        consumer.commit()  # Commit current offset
    except Exception as e:
        # Failed - don't commit, will retry from last offset
        logger.error(f"Failed: {e}")
```

**Offset Reset Strategies**:
```python
# 'earliest' = Start from beginning of topic
consumer = KafkaConsumer(auto_offset_reset='earliest')
# Offset: 0

# 'latest' = Start from end of topic
consumer = KafkaConsumer(auto_offset_reset='latest')
# Offset: current_end

# Seeking to specific offset
consumer.seek(TopicPartition('orders', partition=0), offset=100)
# Start reading from offset 100

# Seek to timestamp
timestamp = 1677000000000  # Unix timestamp in milliseconds
consumer.seek(TopicPartition('orders', partition=0), timestamp=timestamp)
```

### 2.3 Replication & Fault Tolerance

**Replication** = Data copied across multiple brokers

```
Topic "orders" with replication_factor=3:

Broker 1        Broker 2        Broker 3
─────────       ─────────       ─────────
Part 0 (L)      Part 0 (R)
Part 1          Part 1 (L)      Part 1 (R)
Part 2 (R)                      Part 2 (L)

L = Leader (handles reads/writes)
R = Replica (backup copy)

If Broker 1 dies:
- Consumers switch to replicas in Broker 2/3
- No data loss!
```

```python
# Topic creation with replication
# Via CLI:
kafka-topics.sh --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server localhost:9092
```

### 2.4 Schema Management (Avro)

**Schema** = Structure/format of data

```python
# Without Schema (JSON) - error-prone
data = {"user_id": 123, "action": "click"}  # Is user_id int or string?

# With Schema (Avro) - enforced
from io import BytesIO
import avro.schema
import avro.io

# Define schema
SCHEMA = avro.schema.parse("""
{
  "type": "record",
  "name": "Event",
  "fields": [
    {"name": "user_id", "type": "int"},
    {"name": "action", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
""")

# Serialize with schema
event = {"user_id": 123, "action": "click", "timestamp": 1234567890}
bytes_writer = BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
writer = avro.io.DatumWriter(SCHEMA)
writer.write(event, encoder)
data_bytes = bytes_writer.getvalue()

# Send with schema
producer.send('events', value=data_bytes)
```

**Why Avro?**
1. **Type safety** - catch errors early
2. **Backward compatible** - schema evolution
3. **Smaller size** - binary format vs JSON
4. **Documentation** - self-describing

### 2.5 Error Handling & Retries

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    retries=3,                    # Retry failed sends
    retry_backoff_ms=100,         # Wait 100ms between retries
    acks='all',                   # Wait for all replicas
    max_in_flight_requests_per_connection=1  # Preserve order
)

# Send with error callback
def on_send_success(record_metadata):
    print(f"✓ Sent to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")

def on_send_error(exc):
    print(f"✗ Failed to send: {exc}")

future = producer.send('events', value={'id': 1})
future.add_callback(on_send_success)
future.add_errback(on_send_error)

# Wait for all messages
producer.flush()
```

### 2.6 Consumer Error Handling

```python
from kafka import KafkaConsumer
from kafka.errors import ConsumerTimeout

consumer = KafkaConsumer(
    'events',
    bootstrap_servers=['localhost:9092'],
    group_id='my_app',
    consumer_timeout_ms=10000  # Timeout if no message for 10s
)

try:
    for message in consumer:
        try:
            # Process message
            result = process(message.value)
            # Commit only after success
            consumer.commit()
        except ValueError as e:
            # Data error - log and skip
            logger.error(f"Invalid data: {e}")
            consumer.commit()  # Still commit to move forward
        except Exception as e:
            # Unexpected error - don't commit, retry
            logger.error(f"Unexpected error: {e}")
            consumer.position(message.topic_partition) - 1

except ConsumerTimeout:
    print("No messages for 10 seconds")
finally:
    consumer.close()
```

### 2.7 Performance Tuning (Intermediate)

```python
from kafka import KafkaProducer, KafkaConsumer

# PRODUCER OPTIMIZATION
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    batch_size=16384,              # Batch 16KB of messages
    linger_ms=10,                  # Wait up to 10ms for batch
    compression_type='snappy',     # Compress batches
    acks=1,                        # Leader ack only (faster)
    buffer_memory=67108864         # 64MB buffer
)

# CONSUMER OPTIMIZATION
consumer = KafkaConsumer(
    'events',
    bootstrap_servers=['localhost:9092'],
    max_poll_records=500,          # Fetch 500 at a time
    fetch_min_bytes=1024,          # Wait for at least 1KB
    fetch_max_wait_ms=500,         # Or 500ms
    connections_max_idle_ms=540000 # Close idle connections
)

# Send many messages efficiently
for i in range(1000):
    producer.send('events', value={'id': i})

producer.flush()  # Wait for all to send

# Consume efficiently
for message in consumer:
    process(message)
```

---

## LEVEL 3: Advanced

### 3.1 Exactly-Once Semantics (Idempotence)

**Problem**: Messages might be delivered multiple times

```
Producer sends message
    ↓
Network timeout
    ↓
Producer doesn't know if it arrived
    ↓
Producer retries
    ↓
Result: Duplicate messages!
```

**Solution 1: Idempotent Producer**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    enable_idempotence=True,           # Exactly once!
    acks='all',
    retries=2147483647,                # Max retries
    max_in_flight_requests_per_connection=5
)

# Even if we retry, Kafka deduplicates internally
producer.send('orders', value={'order_id': 123, 'amount': 100})
producer.send('orders', value={'order_id': 123, 'amount': 100})  # Retried
# Result: Only one message in Kafka!
```

**How It Works**:
```
Producer sequence:
- Producer ID: 12345
- Sequence number: 0, 1, 2, 3, ...

Kafka broker checks:
- Producer 12345, seq 0 → Accepted
- Producer 12345, seq 0 → Duplicate! Ignored
- Producer 12345, seq 1 → Accepted
```

**Solution 2: Transactions**

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    transactional_id='payment-processor-1',  # Unique ID
    acks='all'
)

# Use transaction
producer.begin_transaction()
try:
    # All-or-nothing behavior
    producer.send('orders', value={'order_id': 1})
    producer.send('payments', value={'order_id': 1, 'amount': 100})
    producer.commit_transaction()  # Both sent or both fail
except Exception as e:
    producer.abort_transaction()  # Neither sent
    raise
```

### 3.2 Stream Processing with Kafka Streams

**Kafka Streams** = Process data in real-time AS it flows through Kafka

```python
from kafka.streams import KafkaStreams, StreamsBuilder
from kafka.common import TopicPartition
import json

# Create stream topology
builder = StreamsBuilder()

# Read from 'events' topic
events = builder.stream('events')

# Transform: Extract user_id and action
user_actions = events.map(
    lambda k, v: (
        json.loads(v)['user_id'],
        json.loads(v)['action']
    )
)

# Filter: Only 'purchase' actions
purchases = user_actions.filter(
    lambda k, v: v == 'purchase'
)

# Send to new topic
purchases.to('purchase_events')

# Count by user (tumbling window)
user_purchase_counts = events \
    .map(lambda k, v: (
        json.loads(v)['user_id'],
        1
    )) \
    .window(TimeWindows.of(60000)) \  # 60 second window
    .reduce(lambda v1, v2: v1 + v2)

user_purchase_counts.to_stream().to('purchase_counts')

# Build and start
topology = builder.build()
streams = KafkaStreams(topology, config={
    'bootstrap.servers': 'localhost:9092',
    'application.id': 'purchase_counter'
})
streams.start()
```

### 3.3 Windowing & Stateful Operations

**Windowing** = Group messages by time

```
Time Window Example:
────────────────────────────────

10:00 - 10:10 Window:
[msg1, msg2, msg3] → Sum = 30
[msg4, msg5] → Sum = 20
[msg6] → Sum = 10

Types of windows:
1. Tumbling: Non-overlapping fixed size
2. Hopping: Overlapping fixed size
3. Session: Gap-based grouping
```

```python
from kafka.streams import TimeWindows, SessionWindows, HoppingWindows

# Tumbling Window (non-overlapping)
tumbling = events \
    .map(lambda k, v: (json.loads(v)['user_id'], 1)) \
    .window(TimeWindows.of(60000))  # 1 minute windows
    .reduce(lambda v1, v2: v1 + v2)

# Hopping Window (overlapping)
hopping = events \
    .map(lambda k, v: (json.loads(v)['user_id'], 1)) \
    .window(HoppingWindows.of(60000).with_grace(10000))  # 1min hop, 1min window
    .reduce(lambda v1, v2: v1 + v2)

# Session Window (gap-based)
session = events \
    .map(lambda k, v: (json.loads(v)['user_id'], 1)) \
    .window(SessionWindows.with_inactivity_gap(300000))  # 5 min gap
    .reduce(lambda v1, v2: v1 + v2)
```

### 3.4 Consumer Lag & Monitoring

**Consumer Lag** = How far behind the consumer is

```
Topic: orders
Partition 0: [msg0] → [msg1] → [msg2] → [msg3] → [msg4]
             offset 0   1      2      3      4 (total)
                                             ↑
                                        Latest offset: 4

Consumer Group: payment_processor
Consumer 1: Currently at offset 2

Lag = Latest offset - Consumer offset
    = 4 - 2
    = 2 messages behind
```

```python
from kafka import KafkaConsumer, TopicPartition
import time

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='payment_processor'
)

# Get lag
def get_consumer_lag():
    lag_dict = {}

    for partition in consumer.partitions_for_topic('orders'):
        tp = TopicPartition('orders', partition)

        # Get consumer position (where we are)
        consumer.assign([tp])
        consumer_pos = consumer.position(tp)

        # Get latest offset
        consumer.seek_to_end(tp)
        latest = consumer.position(tp)

        # Calculate lag
        lag = latest - consumer_pos
        lag_dict[partition] = lag

    return lag_dict

# Monitor continuously
while True:
    lag = get_consumer_lag()
    print(f"Consumer lag: {lag}")

    if any(v > 1000 for v in lag.values()):
        print("⚠ High lag detected!")

    time.sleep(10)
```

### 3.5 Dead Letter Queue Pattern

**Problem**: Some messages can't be processed

```python
from kafka import KafkaProducer, KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['localhost:9092'],
    group_id='processor'
)

dlq_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    try:
        order = json.loads(message.value)
        process_order(order)
        consumer.commit()

    except ValueError as e:
        # Data format error - send to DLQ
        dlq_producer.send('orders_dlq', value={
            'error': str(e),
            'original_message': message.value.decode('utf-8'),
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp
        })
        consumer.commit()  # Move forward anyway

    except Exception as e:
        # Unexpected error - don't commit, will retry
        logger.error(f"Unexpected error: {e}")
```

---

## LEVEL 4: Production Mastery (Senior)

### 4.1 Multi-Datacenter Replication

**MirrorMaker** = Replicate data across datacenters

```
DC1 (Primary)              DC2 (Backup)
──────────────             ────────────
Kafka Cluster     →→→→→→→  Kafka Cluster
  Topic: orders    Mirror   Topic: orders
  Partition 0      Maker    Partition 0
  [msg1, msg2]     →→→→→→→  [msg1, msg2]

Benefits:
- Disaster recovery
- Geographic distribution
- Compliance (data locality)
```

```bash
# MirrorMaker configuration
# mirror-maker.properties

clusters = primary, secondary
primary.bootstrap.servers = dc1.internal:9092
secondary.bootstrap.servers = dc2.internal:9092

# Topics to replicate
topics = orders, payments, events

# Consumer group for tracking
primary->secondary.group.id = mm-cluster
primary->secondary.enabled = true
```

```bash
# Run MirrorMaker
connect-mirror-maker.sh mirror-maker.properties
```

### 4.2 Security (ACLs, SSL/TLS, SASL)

**ACL** = Access Control List - who can do what

```bash
# Create topic with ACL
kafka-topics.sh --create --topic sensitive_data \
  --bootstrap-server localhost:9092

# Grant permissions
kafka-acls.sh --create \
  --allow-principal User:app_name \
  --operation Read,Write \
  --topic sensitive_data \
  --bootstrap-server localhost:9092

# Deny permissions
kafka-acls.sh --create \
  --deny-principal User:untrusted_app \
  --operation Write \
  --topic sensitive_data \
  --bootstrap-server localhost:9092

# List ACLs
kafka-acls.sh --list --bootstrap-server localhost:9092
```

```python
# Producer with SASL authentication
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
    bootstrap_servers=['kafka.prod.internal:9093'],
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="data_pipeline",
    sasl_plain_password="secure_password_here",
    ssl_cafile="/path/to/ca-cert.pem",
    ssl_certfile="/path/to/client-cert.pem",
    ssl_keyfile="/path/to/client-key.pem"
)
```

### 4.3 High-Availability Cluster Setup

```yaml
# docker-compose for HA Kafka cluster
version: '3.8'

services:
  zookeeper-1:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  zookeeper-2:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  zookeeper-3:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888

  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://kafka-1:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2

  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29092,PLAINTEXT_HOST://kafka-2:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2

  kafka-3:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29092,PLAINTEXT_HOST://kafka-3:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
```

### 4.4 Performance at Scale (Tuning)

```python
from kafka import KafkaProducer, KafkaConsumer

# PRODUCER FOR MILLIONS OF MESSAGES/SECOND
producer = KafkaProducer(
    bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],

    # Throughput optimization
    batch_size=32768,              # 32KB batches
    linger_ms=10,                  # Wait 10ms to batch
    compression_type='snappy',     # Compress (CPU vs Network trade-off)

    # Reliability
    acks='all',                    # Wait for all replicas
    retries=3,
    max_in_flight_requests_per_connection=5,  # Parallel requests

    # Memory
    buffer_memory=134217728,       # 128MB buffer

    # Network
    request_timeout_ms=30000,
    connections_max_idle_ms=540000,
)

# CONSUMER FOR MILLIONS OF MESSAGES/SECOND
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
    group_id='processor',

    # Throughput
    max_poll_records=1000,         # Fetch 1000 per poll
    fetch_min_bytes=10485760,      # Wait for 10MB
    fetch_max_wait_ms=1000,        # Or 1 second

    # Reliability
    enable_auto_commit=False,       # Manual commits
    session_timeout_ms=30000,

    # Concurrency
    connections_max_idle_ms=540000,
    receive_buffer_size=65536,     # 64KB
)

# Processing loop optimized for speed
batch = []
for message in consumer:
    batch.append(message)

    if len(batch) >= 1000:
        # Process batch of 1000
        process_batch(batch)
        consumer.commit()
        batch = []
```

### 4.5 Operational Excellence

#### Monitoring Metrics

```python
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType
import time

admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

# Monitor topic health
def monitor_topics():
    """Monitor broker and topic health"""

    while True:
        try:
            # Get cluster metadata
            metadata = admin.describe_cluster()

            print(f"Cluster ID: {metadata[0]}")
            print(f"Controller: {metadata[1]}")
            print(f"Brokers: {metadata[2]}")

            # Get broker metrics
            for broker_id in metadata[2]:
                broker = admin.describe_brokers([broker_id])
                # Assess broker health

            # Get topic metrics
            topics = admin.list_topics()
            for topic in topics.keys():
                partitions = admin.describe_topics([topic])
                # Check partition leadership, replicas

        except Exception as e:
            print(f"Error monitoring: {e}")

        time.sleep(60)  # Check every minute
```

#### Alerting Strategy

```python
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram
import time

# Define metrics
msg_counter = Counter('kafka_messages_total', 'Total messages processed')
lag_gauge = Gauge('kafka_consumer_lag', 'Current consumer lag')
processing_time = Histogram('kafka_processing_seconds', 'Processing time')

def process_with_monitoring():
    consumer = KafkaConsumer('events', group_id='processor')

    for message in consumer:
        start = time.time()

        try:
            process(message)
            msg_counter.inc()

        except Exception as e:
            print(f"Alert: Processing failed: {e}")
            # Send alert to Slack/PagerDuty

        finally:
            elapsed = time.time() - start
            processing_time.observe(elapsed)

            # Alert if lag is high
            lag = get_lag()
            lag_gauge.set(lag)

            if lag > 10000:
                print("Alert: High consumer lag detected!")
                # escalate
```

### 4.6 Disaster Recovery Plan

```
DISASTER SCENARIOS & RESPONSES
──────────────────────────────

1. Broker Failure
   ├── Detection: Broker heartbeat timeout (10-30s)
   ├── Response: Leader election, replica promotion
   └── Recovery: Replace broker, rejoin cluster

2. Partition Unbalance
   ├── Cause: Broker failure, new brokers added
   ├── Detection: Uneven leader distribution
   └── Recovery: Run partition reassignment

3. High Consumer Lag
   ├── Cause: Slow consumer, producer burst
   ├── Detection: Lag monitoring threshold exceeded
   └── Recovery: Scale consumer, adjust batching

4. Complete Cluster Failure
   ├── Cause: Multiple broker failures
   ├── Detection: All brokers down
   └── Recovery: Restore from backup, rebuild cluster
```

```bash
# Recovery commands

# 1. Check broker status
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 2. Check topic partition distribution
kafka-topics.sh --describe --bootstrap-server localhost:9092

# 3. Reassign partitions
kafka-reassign-partitions.sh \
  --bootstrap-server localhost:9092 \
  --topics-to-move-json-file topics.json \
  --broker-list 1,2,3 \
  --generate

# 4. Check replica lag
kafka-replica-verification.sh \
  --bootstrap-server localhost:9092 \
  --broker-list 1,2,3

# 5. Reset consumer offset to latest
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my_group \
  --reset-offsets \
  --to-latest \
  --execute
```

### 4.7 Advanced Patterns

#### Pattern 1: Event Sourcing
```python
"""
Store all changes as events
├── User created
├── Name updated
├── Email updated
├── Deleted
Replay events to reconstruct state at any point in time
"""

# Event stream
topic = "user_events"

# Events sent
{"id": 1, "type": "created", "name": "Alice", "timestamp": 100}
{"id": 1, "type": "name_updated", "name": "Alice Smith", "timestamp": 200}
{"id": 1, "type": "email_updated", "email": "alice@example.com", "timestamp": 300}

# Reconstruct state at timestamp 150:
# User 1: Alice (only created event applies)

# Reconstruct state at timestamp 250:
# User 1: Alice Smith, email: none (created + name_updated)

# Reconstruct state at current:
# User 1: Alice Smith, email: alice@example.com
```

#### Pattern 2: CQRS (Command Query Responsibility Segregation)

```python
"""
Separate read and write models

Commands (Write):
user_commands → process → user_events → event store

Queries (Read):
user_events → build → read_models (views)
            → database (optimized for reads)
"""

# Command side (write)
def handle_create_user_command(cmd):
    user = create_user(cmd.name, cmd.email)
    emit_event('user_created', {'id': user.id, 'name': cmd.name})

# Query side (read)
def build_user_view():
    """Build optimized read model from events"""
    users = {}
    for event in get_all_events():
        if event.type == 'user_created':
            users[event.user_id] = {'id': event.user_id, 'name': event.name}
        elif event.type == 'user_updated':
            users[event.user_id].update(event.changes)
    return users

# Fast reads from view
user = read_model['user_1']  # No event replay needed
```

#### Pattern 3: Exactly-Once Processing with Offset Commit

```python
from kafka import KafkaConsumer
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:pass@localhost/db')

consumer = KafkaConsumer(
    'orders',
    group_id='payment_processor',
    enable_auto_commit=False  # Manual commits
)

for message in consumer:
    # 1. Read message
    order = json.loads(message.value)

    # 2. Process and save atomically
    with engine.connect() as conn:
        # Check idempotency key
        existing = conn.execute(
            "SELECT id FROM processed_orders WHERE idempotency_key = %s",
            (order['idempotency_key'],)
        ).first()

        if existing:
            # Already processed
            pass
        else:
            # Process payment
            payment_id = process_payment(order)

            # Save atomically with offset
            conn.execute(
                "INSERT INTO processed_orders (idempotency_key, payment_id, offset, partition) "
                "VALUES (%s, %s, %s, %s)",
                (order['idempotency_key'], payment_id,
                 message.offset, message.partition)
            )

        # 3. Commit offset ONLY after successful processing
        consumer.commit()
```

---

## Real-World Scenarios

### Scenario 1: E-commerce Order Pipeline

```
Customer places order
    ↓
order_placed event → Kafka topic: "orders"
    ↓
├── [Inventory Service] ← Updates stock
├── [Payment Service] ← Processes payment
├── [Shipping Service] ← Prepares shipment
└── [Analytics] ← Records conversion
```

```python
# Order event schema
ORDER_SCHEMA = {
    "order_id": "UUID",
    "customer_id": "int",
    "items": [
        {"product_id": "int", "quantity": "int", "price": "float"}
    ],
    "total": "float",
    "timestamp": "long"
}

# Producer: E-commerce service
producer.send('orders', value=order_data)

# Consumer 1: Inventory service
def process_inventory(order):
    for item in order['items']:
        reduce_stock(item['product_id'], item['quantity'])

# Consumer 2: Payment service
def process_payment(order):
    payment_id = charge_card(order['customer_id'], order['total'])
    producer.send('payments', value={'order_id': order['order_id'], 'payment_id': payment_id})

# Consumer 3: Shipping service (depends on payment)
def process_shipping(payment):
    order = get_order(payment['order_id'])
    create_shipment(order)
    producer.send('shipments', value={'order_id': order['order_id'], 'status': 'ready'})
```

### Scenario 2: Real-time Analytics Pipeline

```
User events (clicks, views, purchases)
    ↓
kafka topic: "user_events"
    ↓
Spark Streaming
    ├── Count events by type
    ├── Calculate moving averages
    ├── Detect anomalies
    └── Update dashboard in real-time
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count

spark = SparkSession.builder.appName("analytics").getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

# Parse JSON
events = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Real-time aggregations (5-minute windows)
event_counts = events \
    .groupBy(window("timestamp", "5 minutes"), "event_type") \
    .agg(count("*").alias("count")) \
    .orderBy("window")

# Write to console (or your data sink)
query = event_counts \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
```

### Scenario 3: Multi-datacenter Replication

```
DC1 (US-EAST):
Order Topic
[msg1, msg2, msg3]
    ↓ MirrorMaker
    ↓ Replication
DC2 (EU):
Order Topic
[msg1, msg2, msg3]

Failover:
If DC1 fails → Switch consumers to DC2
```

---

## Troubleshooting Guide

### Issue 1: Consumer Lag Growing

**Symptoms**: `get_consumer_lag()` increasing over time

**Root Causes**:
1. Consumer processing too slow
2. Sudden producer spike
3. GC pauses in consumer JVM

**Diagnosis**:
```bash
# Check lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my_group \
  --describe

# Check producer rate
kafka-run-class.sh kafka.tools.ProducerPerformance \
  --topic test \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092
```

**Solutions**:
```python
# 1. Increase consumer instances
for i in range(4):  # 4 consumers instead of 1
    consumer = KafkaConsumer(
        'orders',
        group_id='processor',  # Same group = load balanced
        bootstrap_servers=['localhost:9092']
    )

# 2. Optimize processing speed
for message in consumer:
    # Batch process instead of one-by-one
    batch.append(message)
    if len(batch) >= 100:
        fast_batch_process(batch)
        batch = []

# 3. Increase consumer batch size
consumer = KafkaConsumer(
    max_poll_records=1000,  # Fetch 1000 instead of 500
    fetch_min_bytes=10485760  # Wait for 10MB
)

# 4. Adjust JVM heap
# kafka/consumer.sh --heap 2G
```

### Issue 2: Message Loss

**Symptoms**: Some messages not appearing in consumers

**Root Causes**:
1. `acks=1` → loses messages if broker fails
2. Auto-commit with error → skips messages
3. Network partitions

**Solution**:
```python
# Use acks='all' + idempotent producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks='all',                      # Wait for all replicas
    enable_idempotence=True,         # Deduplicate
    retries=2147483647              # Infinite retries
)

# Consumer: manual commit
consumer = KafkaConsumer(
    'orders',
    enable_auto_commit=False         # Don't auto-commit
)

for message in consumer:
    try:
        process(message)
        consumer.commit()  # Commit only after success
    except:
        pass  # Don't commit = will retry
```

### Issue 3: High Latency

**Symptoms**: Messages taking long to appear in consumers

**Root Causes**:
1. Small batches
2. High `linger_ms`
3. Network latency
4. Slow consumer

**Solution**:
```python
# Producer: balance latency vs throughput
producer = KafkaProducer(
    batch_size=16384,    # Small batch = low latency
    linger_ms=0,         # Don't wait = send immediately
    compression_type='snappy'
)

# Consumer: process faster
consumer = KafkaConsumer(
    max_poll_records=100,     # Small batches = fast processing
    fetch_min_bytes=1,        # Don't wait for large batch
    fetch_max_wait_ms=100     # Max wait 100ms
)
```

### Issue 4: Disk Space Growing

**Symptoms**: Kafka broker disk full

**Root Causes**:
1. High retention policy
2. Slow consumers (can't delete old messages)
3. High producer volume

**Solution**:
```bash
# Check retention settings
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe

# Reduce retention to 7 days
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=604800000  # 7 days

# Or by size
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.bytes=107374182400  # 100GB
```

### Issue 5: Producer Timeout

**Symptoms**: `KafkaTimeoutError` on send

**Root Causes**:
1. Broker not responding
2. Network issue
3. Producer buffer full

**Solution**:
```python
# Increase timeout
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    request_timeout_ms=60000,     # 60 seconds
    connections_max_idle_ms=540000
)

# Check broker health
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Monitor network
netstat -an | grep ESTABLISHED | grep 9092
```

---

## Kafka Best Practices Summary

### ✅ DO
- ✅ Use `acks='all'` for critical data
- ✅ Commit offsets AFTER successful processing
- ✅ Monitor consumer lag
- ✅ Set meaningful retention policies
- ✅ Use consumer groups for load balancing
- ✅ Implement idempotent processing
- ✅ Use schemas (Avro/Protobuf)
- ✅ Set up monitoring & alerting
- ✅ Test failover scenarios
- ✅ Document topic purposes and SLAs

### ❌ DON'T
- ❌ Use `acks=0` unless you don't care about data loss
- ❌ Auto-commit with risky processing
- ❌ Ignore consumer lag
- ❌ Set infinite retention
- ❌ Reuse message IDs (kills idempotency)
- ❌ Process in main consumer loop without batching
- ❌ Ignore schema changes
- ❌ Run without monitoring
- ❌ Skip testing disaster scenarios
- ❌ Assume Kafka will solve all your problems

---

**Now you're ready to design, build, and operate production Kafka systems at any scale!** 🚀
