# SQL Optimization for Data Engineering

## The Real-World Problem

You have 50 million event records. This query takes 45 minutes:
```sql
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%'
```

After optimization: **12 seconds**. That's 225x faster. This is why SQL optimization matters.

---

## 1. Query Optimization Fundamentals

### Understanding EXPLAIN

```sql
-- ❌ SLOW: Full table scan (45 million rows processed)
EXPLAIN SELECT * FROM events WHERE user_id = 123;
-- Output: Seq Scan on events (cost=0.00..1500000.00)

-- ✅ FAST: Index lookup (1 microsecond)
CREATE INDEX idx_events_user_id ON events(user_id);
EXPLAIN SELECT * FROM events WHERE user_id = 123;
-- Output: Index Scan using idx_events_user_id (cost=0.29..8.31)
```

**Key Metric: Cost** — Lower cost = faster query

### ❌ ANTI-PATTERNS (Things to AVOID)

#### 1. Using LIKE with Wildcards at Start
```sql
-- ❌ BAD: Cannot use index
SELECT * FROM users WHERE email LIKE '%@gmail.com';

-- ✅ GOOD: Index-friendly
SELECT * FROM users WHERE email LIKE 'user%';

-- ✅ BEST: Use reverse index or full-text search
CREATE INDEX idx_email_reverse ON users(REVERSE(email));
SELECT * FROM users WHERE REVERSE(email) LIKE REVERSE('%@gmail.com');
```

#### 2. Subqueries in SELECT Clause (N+1 Problem)
```sql
-- ❌ BAD: Subquery runs for EACH row (1M queries for 1M rows)
SELECT
    user_id,
    (SELECT COUNT(*) FROM orders WHERE user_id = users.id) AS order_count
FROM users;
-- Time: 60+ seconds

-- ✅ GOOD: Single JOIN + GROUP BY
SELECT
    users.id as user_id,
    COUNT(orders.id) as order_count
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.id;
-- Time: 2 seconds
```

#### 3. Using Functions on Indexed Columns
```sql
-- ❌ BAD: Index ignored (function prevents index use)
SELECT * FROM users WHERE YEAR(created_at) = 2024;

-- ✅ GOOD: Range query uses index
SELECT * FROM users
WHERE created_at >= '2024-01-01'
  AND created_at < '2025-01-01';
```

#### 4. NOT IN with NULLs
```sql
-- ❌ BAD: Returns 0 rows if subquery has NULL
SELECT * FROM orders
WHERE user_id NOT IN (SELECT id FROM users WHERE status = 'deleted');
-- If subquery has NULL, ALL rows excluded

-- ✅ GOOD: Use NOT EXISTS (handles NULLs)
SELECT * FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM users u
    WHERE u.id = o.user_id AND u.status = 'deleted'
);
```

---

## 2. Indexing Strategy

### When to Index (Not Everything!)

| Column | Index? | Reason |
|--------|--------|--------|
| `id` (primary key) | ✅ Always | Already indexed as PK |
| `user_id` (foreign key) | ✅ Yes | Used in JOINs and WHERE |
| `created_at` | ✅ Yes | Used in range queries |
| `status` (low cardinality) | ❓ Maybe | Index if heavily filtered |
| `description` (text) | ❌ No | Indexes waste space, use full-text search |
| `is_active` (boolean) | ❌ No | Only 2 values, sequence scan faster |

### Composite Indexes (Multi-Column)

```sql
-- ❌ BAD: Multiple single-column indexes
CREATE INDEX idx_user_id ON orders(user_id);
CREATE INDEX idx_status ON orders(status);

-- If you always query: WHERE user_id = X AND status = 'pending'
-- Both indexes scanned, results merged (slow)

-- ✅ GOOD: Single composite index
CREATE INDEX idx_orders_user_status ON orders(user_id, status);

-- Column order matters!
-- Index is ordered by: (user_id, then status within each user_id)
-- Queries using BOTH columns are faster
```

### Index Maintenance Cost

```sql
-- ✅ Good: Minimal writes
CREATE INDEX idx_user_id ON users(user_id);

-- ❌ Bad: Expensive for high-write tables
-- Indexes slow down INSERT/UPDATE/DELETE
-- Only use if read queries benefit significantly
CREATE INDEX idx_address ON users(street, city, zip, country);
```

---

## 3. Real-World Optimization Examples

### Example 1: Slow Event Analytics Query

**Problem:** Get top 10 events by user count, taking 3 minutes

```sql
-- ❌ SLOW (3 minutes)
SELECT
    event_type,
    COUNT(DISTINCT user_id) as unique_users
FROM events
WHERE created_at > '2024-01-01'
GROUP BY event_type
ORDER BY unique_users DESC
LIMIT 10;
```

**Root Cause:** Scanning 50M rows, no index on `created_at`

**Solution:**
```sql
-- ✅ Step 1: Add index on filter column
CREATE INDEX idx_events_created_at ON events(created_at);

-- ✅ Step 2: Optimize GROUP BY (add event_type to index)
CREATE INDEX idx_events_created_type ON events(created_at, event_type);

-- Result: 5 seconds (36x faster)
-- Index jump to right date range, scan only relevant rows
```

### Example 2: N+1 Problem (Correlated Subquery)

**Problem:** Get all users with their recent purchases

```sql
-- ❌ SLOW: 1 + N queries (1M users = 1M queries)
SELECT
    id,
    email,
    (SELECT MAX(purchase_date) FROM purchases WHERE user_id = users.id) as last_purchase
FROM users;

-- ✅ FAST: Single pass with window function
SELECT
    u.id,
    u.email,
    MAX(p.purchase_date) OVER (PARTITION BY u.id) as last_purchase
FROM users u
LEFT JOIN purchases p ON u.id = p.user_id;

-- Even better (if you want 1 row per user)
SELECT
    u.id,
    u.email,
    p.last_purchase
FROM users u
LEFT JOIN (
    SELECT user_id, MAX(purchase_date) as last_purchase
    FROM purchases
    GROUP BY user_id
) p ON u.id = p.user_id;
```

### Example 3: Inefficient JOINs

**Problem:** Find users who have orders AND reviews

```sql
-- ❌ SLOW: Implicit Cartesian product
SELECT
    u.id,
    u.email,
    COUNT(*) as interaction_count
FROM users u, orders o, reviews r
WHERE u.id = o.user_id
  AND u.id = r.user_id;

-- ✅ FAST: Explicit JOINs + de-duplication
SELECT
    u.id,
    u.email,
    (SELECT COUNT(*) FROM orders WHERE user_id = u.id) +
    (SELECT COUNT(*) FROM reviews WHERE user_id = u.id) as interaction_count
FROM users u
WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = u.id)
  AND EXISTS (SELECT 1 FROM reviews WHERE user_id = u.id);
```

---

## 4. Data Quality in ETL

### Handling Duplicates

```sql
-- ❌ BAD: Duplicates silently corrupt analytics
INSERT INTO fact_orders SELECT * FROM raw_orders;

-- ✅ GOOD: Detect and deduplicate
INSERT INTO fact_orders
SELECT DISTINCT ON (order_id)
    *
FROM raw_orders
WHERE received_at = (SELECT MAX(received_at) FROM raw_orders)
ORDER BY order_id, received_at DESC;
-- DISTINCT ON keeps first occurrence
```

### Type Safety in ETL

```sql
-- ❌ BAD: Implicit type conversions (slow + wrong)
SELECT
    user_id,
    price * quantity,  -- What if price is string?
    created_at + '1 day'  -- Date arithmetic breaks
FROM orders;

-- ✅ GOOD: Explicit casting + validation
SELECT
    user_id::int,
    (price::decimal * quantity::int)::decimal(10,2) as total,
    (created_at::date + interval '1 day')::date as next_date
FROM orders
WHERE price ~ '^\d+\.\d{2}$';  -- Validate price format
```

---

## 5. Common SQL Interview Patterns

### Pattern 1: Running Totals
```sql
-- Running total of sales by day
SELECT
    date,
    sales,
    SUM(sales) OVER (ORDER BY date) as cumulative_sales
FROM daily_sales
ORDER BY date;
```

### Pattern 2: Rank by Group
```sql
-- Top 3 products per category
SELECT
    category,
    product,
    sales,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rank
FROM products
QUALIFY rank <= 3;
```

### Pattern 3: Date Range Matching
```sql
-- Find orders within 7 days of user signup
SELECT
    u.id,
    o.order_id,
    DATEDIFF(DAY, u.created_at, o.order_date) as days_to_purchase
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE DATEDIFF(DAY, u.created_at, o.order_date) <= 7;
```

---

## Interview Checklist

- ✅ Can read EXPLAIN output
- ✅ Know when to index and when not to
- ✅ Avoid LIKE, functions on indexed columns
- ✅ Prefer JOINs over subqueries
- ✅ Use window functions instead of N+1 queries
- ✅ Write SELECT before INSERT/UPDATE
- ✅ Always validate data quality at ingestion
- ✅ Can optimize slow queries (identify cost, add index, verify)

---

## Performance Monitoring

```sql
-- Find slowest queries in PostgreSQL
SELECT
    query,
    calls,
    mean_exec_time::int as avg_ms,
    max_exec_time::int as max_ms,
    total_exec_time::int / 1000 as total_sec
FROM pg_stat_statements
WHERE mean_exec_time > 100  -- > 100ms
ORDER BY total_exec_time DESC
LIMIT 10;

-- Identify unused indexes
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as scans
FROM pg_stat_user_indexes
WHERE idx_scan = 0
ORDER BY pg_relation_size(indexrelid) DESC;
```
