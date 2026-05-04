# Python Data Engineering Best Practices

## 1. Configuration Management
✅ **DO:**
- Use config objects or dataclasses for centralized configuration
- Load configs from environment variables for different environments
- Use `.env` files for local development (don't commit)

❌ **DON'T:**
- Hardcode values in scripts
- Mix configs across files
- Store secrets in code

## 2. Logging
✅ **DO:**
- Use `loguru` or `logging` module consistently
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)
- Include context in log messages (function, line number)
- Rotate logs to prevent disk overflow

❌ **DON'T:**
- Use `print()` in production code
- Log sensitive data (passwords, API keys)
- Ignore exceptions silently

```python
# Good
logger.info(f"Processing {file_count} files from {source_path}")

# Bad
print("Processing files")
```

## 3. Spark Best Practices

### Partitioning & Performance
- Set appropriate `spark.sql.shuffle.partitions` (default 200 is often too high for small clusters)
- Enable adaptive query execution for better performance
- Coalesce partitions before writing small datasets

```python
# Good - adaptive execution
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Good - write efficiently
df.coalesce(1).write.parquet("path")  # For small outputs

# Bad
df.write.parquet("path")  # Creates many small files
```

### Data Formats
- **Parquet**: Default for data lakes (columnar, compressed, schema preserved)
- **CSV**: For human-readable inputs/outputs only
- **Avro**: For schema evolution in streaming
- Avoid: Uncompressed text formats for large data

### Transformations
- Use PySpark SQL when possible (better optimizations)
- Avoid `rdd.map()` - use DataFrame API
- Cache/persist only intermediate results you'll reuse multiple times
- Avoid wide transformations without repartitioning

```python
# Good
df.select(col("user_id")).distinct()

# Bad
df.rdd.map(lambda x: x[0]).distinct()
```

### Schema Management
- Define schemas explicitly (don't rely on inference in production)
- Validate schemas at input boundaries

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("age", IntegerType(), True),
])

df = spark.read.schema(schema).csv("data.csv")
```

## 4. Error Handling

✅ **DO:**
- Catch specific exceptions
- Log errors before re-raising
- Use try-except at system boundaries

❌ **DON'T:**
- Catch bare `Exception`
- Silently ignore errors
- Use try-except for control flow

```python
# Good
from kafka.errors import KafkaError

try:
    producer.send(topic, value=event).get(timeout=10)
except KafkaError as e:
    logger.error(f"Failed to send event: {e}")
    raise

# Bad
try:
    producer.send(topic, value=event)
except:
    pass
```

## 5. Kafka Best Practices

### Producers
- Set `acks="all"` for critical data
- Use key-based partitioning for ordering
- Batch messages when possible
- Handle failures gracefully

```python
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    acks="all",
    retries=3,
    max_in_flight_requests_per_connection=1,
)
```

### Consumers
- Use consumer groups for parallel processing
- Set appropriate `session_timeout_ms`
- Enable auto-commit only for non-critical data
- Process messages idempotently

```python
consumer = KafkaConsumer(
    group_id='my-group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    session_timeout_ms=30000,
)
```

## 6. Docker Best Practices

✅ **DO:**
- Use slim/alpine base images
- Multi-stage builds for smaller images
- Pin versions in requirements.txt
- Use .dockerignore to exclude unnecessary files
- Add health checks

❌ **DON'T:**
- Use `latest` tags
- Run as root if avoidable
- Bloat images with unnecessary dependencies
- Commit sensitive data

## 7. Code Organization

```
data-pipeline/
├── config.py           # Configuration management
├── logger_setup.py     # Logging setup
├── spark_pipeline.py   # Main ETL logic
├── kafka_producer.py   # Kafka producer
├── kafka_consumer.py   # Kafka consumer
├── tests/
│   └── test_pipeline.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

## 8. Testing

✅ **DO:**
- Test transformations with sample data
- Mock external systems (Kafka, S3) when needed
- Use fixtures for reusable test data
- Test edge cases and error scenarios

```python
@pytest.fixture
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

def test_filter(spark):
    df = spark.createDataFrame([(1,), (2,)], ["value"])
    filtered = df.filter(df.value > 1)
    assert filtered.count() == 1
```

## 9. Environment Variables vs Secrets

```python
# Environment variables (safe)
DATABASE_URL = os.getenv("DATABASE_URL")

# Secrets (sensitive data)
API_KEY = os.getenv("API_KEY")  # Never hardcode!
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

# In K8s/EKS, use secrets:
# kubectl create secret generic kafka-secrets --from-literal=password=xxx
```

## 10. Deployment Patterns

### Local Development
```bash
docker-compose up
```

### Docker
```bash
docker build -t data-pipeline:latest .
docker run -e KAFKA_BROKERS=kafka:29092 data-pipeline:latest
```

### Kubernetes/EKS
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: data-pipeline
spec:
  containers:
  - name: pipeline
    image: data-pipeline:latest
    env:
    - name: KAFKA_BROKERS
      value: "kafka-service:9092"
    envFrom:
    - secretRef:
        name: kafka-secrets
```

## Performance Tips

1. **Partitioning**: Right-size partitions (aim for 128-256MB per partition)
2. **Caching**: Cache DataFrames you'll use multiple times
3. **Joins**: Broadcast small DataFrames to avoid shuffles
4. **Monitoring**: Track Spark UI (port 4040) for bottlenecks
5. **Sampling**: Test with sample data before full runs

```python
# Broadcast small dimension table
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "id")
```

## Common Gotchas

1. **Wide Transformations**: groupBy, join, repartition cause shuffles
2. **Small File Problem**: Write many small files (use coalesce)
3. **Memory**: Set executor memory appropriately for your data
4. **Serialization**: Use built-in types, avoid complex objects
5. **Lazy Evaluation**: Transformations don't execute until action() called

---

Remember: Start simple, measure performance, then optimize based on actual bottlenecks.
