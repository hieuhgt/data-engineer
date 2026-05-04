# Data Engineer Interview Questions & Answers

## System Design Questions

### Q1: Design a data ingestion system for a large e-commerce platform

**Scenario:** Ingest 100GB of data daily from 50 sources (APIs, databases, files) with 99.9% reliability and sub-4-hour latency.

**Answer Structure:**

1. **Understand Requirements**
   - Data volume: 100GB/day
   - Latency: 4 hours
   - Reliability: 99.9% (8.6 hours downtime/year)
   - Sources: Diverse (APIs, databases, files)

2. **Architecture**
   ```
   Data Sources (APIs, DBs, Files)
       ↓
   Message Queue (Kafka/SQS) - Decouple sources from processing
       ↓
   Stream Processing (Spark Streaming / Flink)
       ├ Parse (JSON, XML, CSV)
       ├ Validate (schema, data quality)
       └ Deduplicate
       ↓
   Data Warehouse (Snowflake / BigQuery)
       ├ Staging tables
       └ Fact/Dimension tables
       ↓
   Analytics Tools (Tableau, Looker)
   ```

3. **Reliability Features**
   - **Idempotency:** Use UPSERT (merge) to safely handle retries
   - **Retry Logic:** Exponential backoff (1s, 2s, 4s, ...)
   - **Circuit Breaker:** Stop trying after N failures, alert humans
   - **Partial Failures:** If 2/50 sources fail, load 48, alert on 2
   - **Data Quality Gates:** Reject bad data before warehouse

4. **Scalability**
   - Horizontal scaling: Add more worker nodes as volume grows
   - Partitioning: Partition by date, source, or user_id
   - Batch Compression: 100GB → 20GB compressed (80% savings)

5. **Monitoring**
   - Track: rows processed, latency, success rate, data quality
   - Alert on: SLA miss, quality drop, pipeline hung

**Code Sample:**
```python
async def ingest_pipeline(sources, warehouse):
    semaphore = asyncio.Semaphore(10)  # Max 10 concurrent

    async def fetch_source(source):
        try:
            async with semaphore:
                data = await source.fetch()
                valid = validate_schema(data)
                return valid
        except Exception as e:
            logger.error(f"Failed {source.name}: {e}")
            return None

    results = await asyncio.gather(
        *[fetch_source(s) for s in sources],
        return_exceptions=True
    )

    # Load successful results, log failures
    successful = [r for r in results if r]
    failed = len(results) - len(successful)

    warehouse.load_idempotent(successful)  # UPSERT, not INSERT

    if failed > 0:
        alert(f"Pipeline partial failure: {failed} sources")
```

---

### Q2: Optimize a slow SQL query

**Scenario:** This query takes 45 minutes:
```sql
SELECT * FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%'
```

**Answer:**

1. **Analyze with EXPLAIN**
   ```sql
   EXPLAIN SELECT ... -- Shows cost, seq scans, joins
   ```

2. **Root Causes**
   - No index on `created_at` (full table scan of users)
   - LIKE with wildcard (can't use index)
   - Subquery in WHERE clause (inefficient)

3. **Optimization Steps**
   ```sql
   -- Step 1: Add index on users.created_at
   CREATE INDEX idx_users_created_at ON users(created_at);

   -- Step 2: Add index on events for event_type
   CREATE INDEX idx_events_type ON events(event_type);

   -- Step 3: Rewrite query to avoid LIKE
   SELECT e.* FROM events e
   INNER JOIN users u ON e.user_id = u.id
   WHERE u.created_at >= '2024-01-01'
     AND e.event_type = 'purchase'  -- Not LIKE '%purchase%'
   ```

4. **Result:** 45 minutes → 2 seconds (1350x faster)

**Key Lesson:** Index on filter columns, avoid LIKE with wildcards, use JOIN instead of subqueries.

---

### Q3: Design a data pipeline that handles schema changes

**Scenario:** Data source changes schema every 3 months. How do you handle it without breaking downstream?

**Answer:**

1. **Schema Versioning**
   ```python
   class SchemaManager:
       SCHEMAS = {
           'v1': {'user_id', 'email', 'created_at'},
           'v2': {'user_id', 'email', 'created_at', 'phone'},  # Added phone
           'v3': {'user_id', 'email', 'created_at', 'phone', 'address'},  # Added address
       }

       @staticmethod
       def detect_schema(data):
           fields = set(data.keys())
           for version, required_fields in SchemaManager.SCHEMAS.items():
               if required_fields.issubset(fields):
                   return version
           return None

       @staticmethod
       def normalize(data):
           version = SchemaManager.detect_schema(data)
           if version == 'v1':
               return {**data, 'phone': None, 'address': None}
           return data
   ```

2. **Backward Compatibility**
   - Always add new columns as nullable
   - Never remove or rename columns
   - Use migration script if must change

3. **Testing**
   - Test with each schema version
   - Test schema transition (v1 → v2)

---

## Python & Code Questions

### Q4: Write a function to detect and handle duplicates

```python
def deduplicate_records(records, key_fields=['id', 'source_id', 'timestamp']):
    """Remove duplicates based on key fields."""
    seen = set()
    unique = []
    duplicates = []

    for record in records:
        # Create key from specified fields
        key = tuple(record.get(f) for f in key_fields)

        if key in seen:
            duplicates.append(record)
        else:
            unique.append(record)
            seen.add(key)

    return unique, duplicates

# Test
records = [
    {'id': 1, 'source_id': 'api', 'timestamp': '2024-01-01', 'value': 100},
    {'id': 1, 'source_id': 'api', 'timestamp': '2024-01-01', 'value': 100},  # Duplicate
    {'id': 2, 'source_id': 'api', 'timestamp': '2024-01-01', 'value': 200},
]

unique, dupes = deduplicate_records(records)
# unique: 2 records, dupes: 1 record
```

---

### Q5: Write an async function for API retries

```python
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
async def fetch_with_retry(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as response:
            if response.status >= 400:
                raise ValueError(f"HTTP {response.status}")
            return await response.json()

# Custom implementation (if no library)
async def fetch_manual_retry(url, max_attempts=3):
    for attempt in range(max_attempts):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status >= 400:
                        raise ValueError(f"HTTP {response.status}")
                    return await response.json()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            delay = 2 ** attempt  # 1s, 2s, 4s
            await asyncio.sleep(delay)
```

---

## Problem-Solving Questions

### Q6: You have 1 billion rows and need to find duplicates. How?

**Naive approach (FAILS):**
```python
# ❌ BAD: Load all data into memory, fails for 1B rows
duplicates = set()
seen = set()
for row in all_rows:
    if row in seen:
        duplicates.add(row)
    seen.add(row)
```

**Better approach:**
```python
# ✅ Use SQL (database handles it efficiently)
SELECT id, COUNT(*) as count FROM orders
GROUP BY id
HAVING count > 1

# ✅ Or use Spark (distributed)
df.groupBy('id').count().filter('count > 1').show()
```

**Key Insight:** Don't load all data into memory. Use database/Spark for distributed processing.

---

### Q7: Pipeline is slow. You have 30 minutes to diagnose. What do you do?

**Checklist (in order):**

1. **Check logs (2 min)**
   ```bash
   kubectl logs pod_name | grep -i error
   ```
   - Any obvious errors? Timeouts? OOM?

2. **Check metrics (3 min)**
   - Duration: Is it slow overall or specific stage?
   - CPU/Memory: Hitting limits?
   - Network: Slow API responses?

3. **Check data volume (2 min)**
   ```bash
   SELECT COUNT(*) FROM staging_table  -- Rows ingested?
   SELECT COUNT(DISTINCT source_id) FROM staging_table  -- All sources?
   ```
   - Is data flowing at expected rate?

4. **Profile slow stage (15 min)**
   ```python
   # Add timing logs
   import time
   start = time.time()
   result = slow_function()
   print(f"Duration: {time.time() - start}s")
   ```
   - Is it extraction, transformation, or loading?

5. **Optimize identified bottleneck (10 min)**
   - Add index? Increase parallelism? Reduce data?
   - Deploy fix, monitor

---

### Q8: Data quality is 89% (below 95% threshold). Root cause?

**Approach:**

1. **Understand what's failing**
   ```sql
   SELECT
       validation_rule,
       COUNT(*) as failure_count,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_data) as pct
   FROM validation_logs
   WHERE status = 'failed'
   GROUP BY validation_rule
   ORDER BY failure_count DESC
   ```

2. **Common root causes:**
   - API format changed (e.g., email field now nullable)
   - New data source with different schema
   - Data corruption upstream
   - Validation rule too strict

3. **Fix:**
   - Investigate upstream change with data team
   - Update schema/validation if intentional
   - Add data quality alert to catch future issues

---

## Behavioral Questions

### Q9: Tell us about a time you fixed a production bug

**STAR Format:**
- **Situation:** Pipeline failing in production, 10M rows not ingested
- **Task:** Fix within 2 hours to meet SLA
- **Action:**
  1. Checked logs → found timeout error
  2. Increased timeout from 10s to 30s
  3. Added circuit breaker to gracefully handle future timeouts
  4. Tested locally before deploy
- **Result:** Fixed in 45 min, no more timeouts, added monitoring

**Key Points to Mention:**
- ✅ Diagnosed systematically (logs first)
- ✅ Root cause (not just symptom)
- ✅ Prevention (won't happen again)
- ✅ Communication (kept team updated)

---

### Q10: Describe your data engineering philosophy

**Good Answer:**
- **Reliability > Speed:** Better to be slow and correct than fast and wrong
- **Simplicity:** If a function takes 5 minutes to understand, redesign it
- **Monitoring:** Can't fix what you can't see
- **Testing:** Production bugs are expensive; tests are cheap
- **Documentation:** Future-you will thank present-you

**Bad Answer:**
- "Just make it work" (no reliability focus)
- "Optimize prematurely" (premature optimization is evil)
- "No tests, just manual checking" (error-prone)

---

## Quick Fire Questions

| Question | Good Answer |
|----------|-------------|
| Batch vs streaming? | Batch for analytics, streaming for real-time alerts |
| Favorite data tech? | [Your honest answer + why] |
| Handle schema changes? | Versioning, backward compatibility, tests |
| Biggest pipeline failure? | Own mistake, how you fixed it, what you learned |
| Scale 10x overnight? | More partitions, increase resources, optimize queries |
| Sensitive data handling? | Encryption, PII masking, access controls, audit logs |

---

## Interviewer Questions (Ask Them!)

- What's a recent production incident?
- How do you measure pipeline success?
- What's your tech stack for data engineering?
- How much of the time do engineers spend on operational vs new features?
- What data quality tools do you use?
- How do you handle schema evolution?
- What's the biggest challenge your data team faces?

**Why Ask:**
- Shows you think about real-world problems
- Assesses if team/role is a good fit
- Demonstrates engagement

---

## Day-Of Interview Tips

1. **Listen carefully** — Make sure you understand the problem fully
2. **Think out loud** — Show your reasoning process
3. **Ask clarifying questions** — "Should I optimize for latency or cost?"
4. **Write clean code** — Readable beats clever
5. **Test your code** — Walk through with example
6. **Discuss trade-offs** — "This is faster but uses more memory"
7. **Stay humble** — "I haven't done this exact thing but here's how I'd approach it"
8. **Be honest** — "I don't know X, but I know Y which is related"

---

## Practice Problems

### Problem 1: Deduplicate 1B rows
- Constraint: Limited memory
- Solution: SQL GROUP BY or Spark

### Problem 2: Find top 10 products by sales (with ties)
- Constraint: Ties must all be included
- Solution: Use ROW_NUMBER() or RANK()

### Problem 3: Ingest API that returns different schemas
- Constraint: Can't break downstream
- Solution: Versioning + backward compatibility

### Problem 4: Pipeline took 10 hours (SLA is 4)
- Constraint: Must fix before tomorrow's run
- Solution: Optimize bottleneck (likely SQL query)

### Problem 5: Data quality 92% (threshold 95%)
- Constraint: Must understand root cause
- Solution: Analyze which records failing + why
