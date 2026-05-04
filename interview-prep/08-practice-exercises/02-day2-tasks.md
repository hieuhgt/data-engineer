# Day 2 Hands-On Exercises: SQL + Pipeline Design

## Exercise 1: SQL Query Optimization (60 min)

**Task:** Optimize 5 slow queries using EXPLAIN and indexing.

### Query 1: Simple Filter
```sql
-- ❌ SLOW (no index)
SELECT * FROM orders
WHERE user_id = 123;

-- TODO: Explain and see the cost
-- TODO: Add index on user_id
-- TODO: Re-run EXPLAIN and see improvement
```

**Solution:**
```sql
EXPLAIN SELECT * FROM orders WHERE user_id = 123;
-- Look for "Seq Scan" (bad) vs "Index Scan" (good)

CREATE INDEX idx_orders_user_id ON orders(user_id);

EXPLAIN SELECT * FROM orders WHERE user_id = 123;
-- Now should show "Index Scan" with lower cost
```

### Query 2: Date Range
```sql
-- ❌ SLOW (function prevents index use)
SELECT * FROM events
WHERE YEAR(created_at) = 2024
ORDER BY created_at DESC;

-- TODO: Rewrite without function
-- TODO: Use range query instead
-- TODO: Add index on created_at
```

**Solution:**
```sql
-- ✅ GOOD: Range query (allows index use)
SELECT * FROM events
WHERE created_at >= '2024-01-01'
  AND created_at < '2025-01-01'
ORDER BY created_at DESC;

CREATE INDEX idx_events_created ON events(created_at);
```

### Query 3: Subquery Problem
```sql
-- ❌ SLOW (subquery in SELECT for each row)
SELECT
    user_id,
    email,
    (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
FROM users;

-- TODO: Rewrite as JOIN + GROUP BY
-- TODO: Add appropriate indexes
```

**Solution:**
```sql
-- ✅ GOOD: JOIN + GROUP BY (single pass)
SELECT
    u.id,
    u.email,
    COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.email
ORDER BY order_count DESC;
```

### Query 4: LIKE with Wildcard
```sql
-- ❌ SLOW (LIKE at start, index ignored)
SELECT * FROM users
WHERE email LIKE '%@gmail.com';

-- TODO: Rewrite using reverse index or full-text search
```

**Solution:**
```sql
-- ✅ Option 1: If only filtering by domain
SELECT * FROM users
WHERE email LIKE '%@gmail.com';

-- ✅ Option 2: Extract domain (if DB supports)
SELECT * FROM users
WHERE SUBSTR(email, POSITION('@' IN email) + 1) = 'gmail.com';

-- ✅ Option 3: Add reverse index
CREATE INDEX idx_email_rev ON users(REVERSE(email));
SELECT * FROM users WHERE REVERSE(email) LIKE REVERSE('%@gmail.com');
```

### Query 5: Window Functions
```sql
-- ❌ SLOW (multiple JOINs to get rank)
SELECT
    user_id,
    order_date,
    amount,
    -- Get rank somehow...
FROM orders;

-- TODO: Use window function for efficiency
```

**Solution:**
```sql
-- ✅ GOOD: Window function (single pass)
SELECT
    user_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC) as purchase_rank,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as cumulative_amount
FROM orders;

-- Find top 3 purchases per user
SELECT * FROM (
    SELECT
        user_id,
        order_date,
        amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) as rank
    FROM orders
) WHERE rank <= 3;
```

**Evaluation:**
- ✅ Did you use EXPLAIN to understand costs?
- ✅ Did indexes reduce query time?
- ✅ Do you understand when to use JOIN vs subquery?
- ✅ Can you explain the trade-off between speed and complexity?

---

## Exercise 2: Data Quality Validation (45 min)

**Task:** Write validation rules for a fact table.

**Requirements:**
- Schema validation (required fields exist)
- Type validation (id is integer, amount is decimal)
- Range validation (amount > 0)
- Relationship validation (user_id exists in users table)
- Uniqueness validation (no duplicate orders)

**Skeleton:**
```python
import pandas as pd
import psycopg2

class OrderValidator:
    # Schema
    REQUIRED_FIELDS = ['order_id', 'user_id', 'amount', 'created_at']
    FIELD_TYPES = {
        'order_id': int,
        'user_id': int,
        'amount': float,
        'created_at': 'datetime64[ns]'
    }

    @staticmethod
    def validate_schema(df):
        """Check required fields exist."""
        missing = set(OrderValidator.REQUIRED_FIELDS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing fields: {missing}")
        return True

    @staticmethod
    def validate_types(df):
        """Check field types."""
        # TODO: Verify data types
        # TODO: Log type conversion errors
        # TODO: Return False if any invalid

    @staticmethod
    def validate_ranges(df):
        """Check value ranges."""
        # TODO: Amount must be > 0
        # TODO: created_at must be recent (not from future)
        # TODO: Count and log violations

    @staticmethod
    def validate_uniqueness(df):
        """Check no duplicates."""
        # TODO: order_id must be unique
        # TODO: Log duplicate count

    @staticmethod
    def validate_relationships(df, db_conn):
        """Check foreign keys exist."""
        # TODO: Query: all user_ids in df exist in users table
        # TODO: Log orphaned records

    @classmethod
    def validate_all(cls, df, db_conn):
        """Run all validations."""
        # TODO: Run each validation
        # TODO: Collect all errors
        # TODO: Return (valid_records, invalid_records, error_summary)

# Test
if __name__ == '__main__':
    df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'user_id': [10, 20, 30],
        'amount': [100.5, 200.75, -50],  # Negative amount (invalid)
        'created_at': pd.to_datetime(['2024-01-01', '2024-01-02', '2099-01-01'])  # Future date
    })

    try:
        validator = OrderValidator()
        valid, invalid, errors = validator.validate_all(df, db_conn)
        print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")
        print(f"Errors: {errors}")
    except Exception as e:
        print(f"Validation failed: {e}")
```

**Evaluation:**
- ✅ Did it catch missing fields?
- ✅ Did it catch type errors?
- ✅ Did it catch negative amounts?
- ✅ Did it catch duplicate records?
- ✅ Did it log errors without crashing?

---

## Exercise 3: Pipeline Design (90 min)

**Task:** Design a production data pipeline for a scenario.

### Scenario:
> You need to ingest 50GB daily from 10 sources (APIs + databases), transform the data, and load to warehouse. The pipeline must run within 4 hours, handle failures gracefully, and maintain data quality above 95%.

**Requirements:**
1. Draw architecture (describe components)
2. Explain each stage (ingest → validate → transform → load)
3. Handle failures (what if 1 source fails?)
4. Data quality (validation rules, gates)
5. Monitoring (key metrics, alerts)
6. Scalability (what if 10x volume next year?)

**Template:**
```python
# Design document (write it out)

class PipelineDesign:
    """
    ARCHITECTURE:

    Data Sources (10)
        ↓
    [Ingest Layer]
        - Parallel async fetch from each source
        - Semaphore: max 5 concurrent
        - Timeout: 10s per source
        - Retry: 3 attempts with backoff
        → Output: Raw data (CloudStorage or local)

    [Validation Layer]
        - Schema validation
        - Deduplication by (source_id, record_id)
        - Data quality gates (95% pass rate required)
        → Output: Valid + Invalid records

    [Transform Layer]
        - Cleanse (remove nulls, trim whitespace)
        - Enrich (join with reference data)
        - Aggregate (daily rollups)
        → Output: Fact/Dimension tables

    [Load Layer]
        - Idempotent load (UPSERT)
        - Partition by date
        → Output: Ready for analytics

    FAILURE HANDLING:
    - Partial failure: If source A fails, continue with others
    - Retry logic: Exponential backoff for transient errors
    - Circuit breaker: Stop retrying after 5 failures
    - Alerting: Email team if success rate < 90%

    DATA QUALITY:
    - Rules: No nulls in required fields, amount > 0, no duplicates
    - Gate: Block load if < 95% pass rate
    - Logging: Log all failures for debugging

    MONITORING:
    - Ingest time: Should be < 2 hours
    - Quality score: Should be > 95%
    - Success rate: Should be > 99%
    - Alert if: Any metric misses SLA

    SCALABILITY:
    - Increase semaphore (more parallel)
    - Partition data (by date, source)
    - Add more worker nodes
    - Cache reference data to avoid re-fetching
    """
    pass

# Key questions to answer:
# 1. Why batch vs streaming?
# 2. Why idempotent (UPSERT) not just INSERT?
# 3. What if quality drops to 80%?
# 4. How to debug if pipeline hangs?
# 5. How to scale to 500GB/day?
```

**Evaluation:**
- ✅ Did you address all 4 layers?
- ✅ Did you explain failure handling?
- ✅ Did you mention idempotency?
- ✅ Did you discuss monitoring?
- ✅ Can you justify your choices (why not streaming)?

---

## Day 2 Checklist

- [ ] Exercise 1: Optimize 5 SQL queries (all faster)
- [ ] Exercise 2: Write validation rules (all working)
- [ ] Exercise 3: Design pipeline (answers all 5 questions)
- [ ] Can explain tradeoffs between approaches?
- [ ] Can answer "Why UPSERT not INSERT?"

## After Day 2, You Should Know:

- ✅ How to optimize slow SQL queries
- ✅ How to write validation that catches real errors
- ✅ How to design a reliable pipeline
- ✅ Why idempotency matters
- ✅ What failure modes to plan for
