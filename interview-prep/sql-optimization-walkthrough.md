# Real Query Optimization: 45 Minutes → 12 Seconds

## The Problem Query

```sql
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%'
```

**Current Performance:** 45 minutes (unacceptable)

---

## Step 1: Diagnose with EXPLAIN

```sql
EXPLAIN ANALYZE SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%';
```

### ❌ BAD Output (What you'd see):
```
Seq Scan on events  (cost=0.00..15000000.00 rows=50000000)
  Filter: (event_type LIKE '%purchase%')
  SubPlan 1
    ->  Seq Scan on users  (cost=0.00..500000.00 rows=100000)
          Filter: (created_at > '2024-01-01')

Planning time: 0.234 ms
Execution time: 2745320.123 ms  (45 minutes!)
```

**Problems Identified:**
1. ✅ Seq Scan on events (full 50M row scan!)
2. ✅ Seq Scan on users (full user table scan!)
3. ✅ LIKE '%purchase%' (can't use index even if exists)
4. ✅ IN subquery (inefficient for large subsets)

---

## Step 2: Add Missing Indexes

### Index 1: Filter on users table
```sql
-- Users table: index on created_at
CREATE INDEX idx_users_created_at ON users(created_at);
```

### Index 2: Filter on events table
```sql
-- Events table: index on event_type (needed for LIKE optimization)
CREATE INDEX idx_events_event_type ON events(event_type);
```

### Index 3: Join column
```sql
-- Events table: index on user_id (for the IN join)
CREATE INDEX idx_events_user_id ON events(user_id);
```

**After adding indexes:**
```sql
EXPLAIN ANALYZE SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%';
```

```
Seq Scan on events  (cost=0.00..8000000.00 rows=5000000)  ← Still scanning!
  Filter: (event_type LIKE '%purchase%')
  SubPlan 1
    ->  Index Scan using idx_users_created_at on users  (cost=0.29..2000.00)
          Index Cond: (created_at > '2024-01-01')

Execution time: 480000.123 ms  (8 minutes - better but still bad)
```

**Issue:** LIKE with wildcard at start STILL can't use index effectively.

---

## Step 3: Rewrite Query - Replace LIKE with Exact Match

### Option A: Assume event_type is exact value
```sql
-- ✅ MUCH BETTER: Use exact match (if possible)
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type = 'purchase';

-- Result: 2 seconds (using index on event_type)
```

### Option B: If you must use LIKE, use anchor at start
```sql
-- ✅ BETTER: Anchor at start (allows index use)
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE 'purchase%';  -- ← Anchored, can use index

-- Result: 15 seconds (still using index scan)
```

**Why this matters:**
- `LIKE 'purchase%'` → Database can jump to 'purchase' entries (index seek)
- `LIKE '%purchase%'` → Database must scan all values (index scan → essentially useless)

---

## Step 4: Replace IN Subquery with JOIN

### ❌ Avoid: IN with large subquery
```sql
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type = 'purchase';

-- Execution time: 180000 ms (3 minutes)
```

### ✅ Better: Use JOIN instead
```sql
SELECT e.*
FROM events e
INNER JOIN users u ON e.user_id = u.id
WHERE u.created_at > '2024-01-01'
  AND e.event_type = 'purchase';

-- Execution time: 45000 ms (45 seconds)
```

**Why JOIN is faster:**
- JOIN: Database does nested loop join (optimized)
- IN: Database evaluates subquery for each row (less optimized)

---

## Step 5: Optimize Further with Composite Index

### Create multi-column index
```sql
-- Index on both user_id AND event_type
CREATE INDEX idx_events_user_event ON events(user_id, event_type);
```

**Why composite index helps:**
- Database can jump directly to rows where user_id matches AND event_type = 'purchase'
- Single index scan instead of two

```sql
EXPLAIN ANALYZE
SELECT e.*
FROM events e
INNER JOIN users u ON e.user_id = u.id
WHERE u.created_at > '2024-01-01'
  AND e.event_type = 'purchase';

-- Output with composite index:
Index Scan using idx_events_user_event on events e
  Index Cond: (event_type = 'purchase')
  ->  Hash Join
        Hash Cond: (e.user_id = u.id)
        ->  Index Scan using idx_users_created_at on users u
              Index Cond: (created_at > '2024-01-01')

Execution time: 12000 ms  (12 seconds! 225x faster!)
```

---

## The Complete Optimization Path

### Final Optimized Query:
```sql
-- Step 1: Create indexes
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_events_user_event ON events(user_id, event_type);

-- Step 2: Rewrite query
SELECT e.*
FROM events e
INNER JOIN users u ON e.user_id = u.id
WHERE u.created_at > '2024-01-01'
  AND e.event_type = 'purchase';
```

### Performance Journey:
```
Original:        45 minutes (2,700 seconds)
  ↓ Add indexes: 8 minutes (480 seconds)
  ↓ Fix LIKE:    15 seconds
  ↓ Use JOIN:    45 seconds
  ↓ Composite:   12 seconds ✅ (225x faster!)
```

---

## Key Lessons

### 1️⃣ Always Use EXPLAIN First
```sql
EXPLAIN ANALYZE <query>
```
Look for:
- Seq Scan (bad) vs Index Scan (good)
- Cost numbers (lower = faster)
- Actual execution time

### 2️⃣ Index the Right Columns
```sql
-- Indexes on WHERE clauses
CREATE INDEX idx_created_at ON users(created_at);

-- Indexes on JOIN columns
CREATE INDEX idx_user_id ON events(user_id);

-- Composite indexes for multiple filters
CREATE INDEX idx_user_event ON events(user_id, event_type);
```

### 3️⃣ Avoid These Patterns
```sql
-- ❌ LIKE with wildcard at start
WHERE event_type LIKE '%purchase%'

-- ❌ IN with large subquery
WHERE user_id IN (SELECT id FROM ... WHERE ...)

-- ❌ Function on indexed column
WHERE YEAR(created_at) = 2024

-- ❌ SELECT * (might pull unnecessary columns)
SELECT * FROM events
```

### 4️⃣ Prefer These Patterns
```sql
-- ✅ LIKE anchored at start
WHERE event_type LIKE 'purchase%'

-- ✅ Use JOIN instead of IN
WHERE user_id IN (
  SELECT id FROM users WHERE ...
)
-- Rewrite as:
INNER JOIN users u ON e.user_id = u.id
WHERE u.created_at > '2024-01-01'

-- ✅ Range query instead of function
WHERE created_at >= '2024-01-01'
AND created_at < '2025-01-01'

-- ✅ SELECT specific columns
SELECT e.id, e.user_id, e.event_type, e.amount
```

---

## Interview Answer Template

**Q: This query takes 45 minutes. How would you optimize it?**

**A:** I'd use EXPLAIN ANALYZE to diagnose first:

1. **Identify bottlenecks:**
   - Full table scan on events (no index)
   - LIKE with wildcard (can't use index)
   - IN subquery (inefficient)

2. **Add indexes:**
   - Index on `users.created_at` (for subquery)
   - Index on `events.user_id, event_type` (composite)

3. **Rewrite query:**
   - Replace LIKE '%purchase%' with LIKE 'purchase%' or = 'purchase'
   - Replace IN subquery with INNER JOIN
   - SELECT specific columns, not *

4. **Result:**
   - 45 minutes → 12 seconds (225x faster)
   - Most gain from replacing IN + adding composite index

5. **Verify:**
   - Run EXPLAIN ANALYZE again
   - Compare execution times
   - Monitor in production

---

## Before/After Comparison

### ❌ BEFORE (45 minutes)
```sql
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
  AND event_type LIKE '%purchase%';

-- Cost: 15,000,000
-- Execution: 2,745 seconds
-- Full table scans, no indexes
```

### ✅ AFTER (12 seconds)
```sql
SELECT e.id, e.user_id, e.event_type, e.amount, e.created_at
FROM events e
INNER JOIN users u ON e.user_id = u.id
WHERE u.created_at > '2024-01-01'
  AND e.event_type = 'purchase';

-- Cost: 8,500
-- Execution: 12 seconds
-- Index scans, composite index used
```

---

## Monitoring the Optimization

```sql
-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE tablename = 'events'
ORDER BY idx_scan DESC;

-- Check slow queries
SELECT query, calls, mean_exec_time
FROM pg_stat_statements
WHERE mean_exec_time > 1000  -- > 1 second
ORDER BY total_exec_time DESC
LIMIT 10;

-- Find unused indexes (remove these)
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE idx_scan = 0
ORDER BY pg_relation_size(indexrelid) DESC;
```

---

## Real-World Tips

1. **Don't over-index:** Each index slows down INSERT/UPDATE
2. **Test on real data:** Development database might be too small
3. **Monitor production:** Queries slow down as data grows
4. **Partition if needed:** 50M rows → consider partitioning by date
5. **Archive old data:** Delete old records instead of always filtering

---

## Practice Question

**Given this query taking 60 seconds:**
```sql
SELECT u.id, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01'
  AND o.amount > 100
  AND u.status = 'active'
GROUP BY u.id, u.email;
```

**What indexes would you add?**

**Answer:**
```sql
-- Index on users table (WHERE filters)
CREATE INDEX idx_users_created_status ON users(created_at, status);

-- Index on orders table (JOIN + WHERE filters)
CREATE INDEX idx_orders_user_amount ON orders(user_id, amount);
```

Composite indexes on the filter columns allow database to jump directly to relevant rows.
