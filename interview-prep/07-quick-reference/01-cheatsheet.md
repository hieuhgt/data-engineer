# Quick Reference Cheatsheet

## Python Async (5 min)

```python
# Semaphore (rate limiting)
semaphore = asyncio.Semaphore(5)
async with semaphore:
    await fetch(url)

# Gather (run multiple tasks)
results = await asyncio.gather(*tasks, return_exceptions=True)

# Timeout
async with asyncio.timeout(10):
    await long_operation()
```

---

## SQL Optimization (5 min)

| Problem | Solution |
|---------|----------|
| Slow query | Use EXPLAIN, add index on WHERE columns |
| LIKE '%text%' | Add reverse index or full-text search |
| Subquery in SELECT | Use JOIN + window function |
| NOT IN with NULLs | Use NOT EXISTS |
| Multiple JOINs | Check column orders, partition strategy |

---

## Data Validation (5 min)

```python
# Schema validation
required_fields = ['id', 'email', 'amount']
if not all(field in record for field in required_fields):
    raise ValueError("Missing required fields")

# Type checking
if not isinstance(record['amount'], (int, float)):
    raise ValueError("Amount must be numeric")

# Range checking
if record['amount'] < 0:
    raise ValueError("Amount cannot be negative")
```

---

## Resilient Ingestion (5 min)

```python
# Retry with exponential backoff
for attempt in range(3):
    try:
        return await fetch(url)
    except Exception:
        if attempt == 2: raise
        await asyncio.sleep(2 ** attempt)

# Circuit breaker (stop trying if failing)
if failure_count >= 5:
    raise RuntimeError("Circuit breaker OPEN")

# Idempotency (safe to retry)
warehouse.upsert(data)  # Not insert()
```

---

## Pipeline Design (5 min)

```
Ingest → Validate → Transform → Load

✅ Idempotent (can retry safely)
✅ Partial failure (continue if 1 source fails)
✅ Data quality gates (fail early)
✅ Deduplication (by unique key)
✅ Monitoring (track latency, success rate)
```

---

## Testing Pyramid (5 min)

```
    E2E (10%)
   /       \
  /Integration(20%)\
 /                   \
/____ Unit (70%) ____\

Unit: Test functions
Integration: Test with real DB
E2E: Test full pipeline
```

---

## Git Workflow (5 min)

```bash
git checkout -b feature/my-feature
# ... make changes ...
git add file.py
git commit -m "feat: add feature description"
git push origin feature/my-feature
# Create PR, wait for review & CI
git merge after approval
```

---

## Docker Basics (5 min)

```dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ src/
RUN useradd -m appuser
USER appuser
CMD ["python", "-m", "pipeline.main"]
```

```bash
docker build -t pipeline:1.0.0 .
docker run -v /data:/app/data pipeline:1.0.0
```

---

## Kubernetes Basics (5 min)

```bash
# Deploy
kubectl apply -f deployment.yaml

# Scale
kubectl scale deployment pipeline --replicas=5

# Update image
kubectl set image deployment/pipeline \
  pipeline=pipeline:1.1.0

# Check logs
kubectl logs pod_name
```

---

## Common Patterns (5 min)

### Deduplication
```python
seen = set()
unique = []
for record in records:
    key = (record['id'], record['source'])
    if key not in seen:
        unique.append(record)
        seen.add(key)
```

### Running Totals
```sql
SELECT
    date,
    sales,
    SUM(sales) OVER (ORDER BY date) as cumulative
FROM daily_sales
```

### Top N per Group
```sql
SELECT * FROM (
    SELECT
        category,
        product,
        sales,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rank
    FROM products
) WHERE rank <= 3
```

---

## Interview Red Flags to Avoid

❌ "I don't know"
✅ "I haven't done this, but here's how I'd approach it"

❌ "Just make it fast"
✅ "Trade-off analysis: faster but more memory"

❌ "No tests, manual checking only"
✅ "Unit + integration tests, CI/CD automated"

❌ "Copy-paste from Stack Overflow"
✅ "Understand code before using it"

---

## 3-Day Study Schedule

### Day 1: Python + Ingestion (6 hours)
- Morning (3h):
  - Read: 01-async-patterns.md
  - Code: Write async scraper with semaphore
- Afternoon (3h):
  - Read: 02-data-ingestion-scraping.md
  - Code: Implement retry + circuit breaker

### Day 2: SQL + Pipelines (6 hours)
- Morning (3h):
  - Read: 02-sql-optimization.md
  - Practice: Optimize 5 queries from EXPLAIN
- Afternoon (3h):
  - Read: 03-pipeline-design.md
  - Design: Sketch architecture for scenario

### Day 3: Testing + DevOps + Interview Prep (6 hours)
- Morning (2h):
  - Read: 04-testing-strategies.md
  - Code: Write unit + integration tests
- Mid-day (2h):
  - Read: 05-devops-tools.md
  - Hands-on: Build Docker image, deploy locally
- Afternoon (2h):
  - Read: 06-interview-questions.md
  - Practice: Answer 5 questions out loud
  - Review: 07-quick-reference.md

---

## Night Before Interview

- ✅ Review 07-quick-reference.md (30 min)
- ✅ Skim 06-interview-questions.md (30 min)
- ✅ Get good sleep (7-8 hours)
- ✅ Have water, snacks, pen/paper ready
- ❌ Don't cram new topics (brain is tired)

---

## During Interview

**Opening (2 min):**
- "I'm excited about this role because..."
- Show knowledge of company's data stack (Google Kafka? Spark?)

**Technical Questions (20-30 min):**
- Ask clarifying questions
- Think out loud (show reasoning)
- Write pseudocode before code
- Test with examples

**System Design (15-20 min):**
- Draw architecture
- Discuss trade-offs (speed vs cost)
- Mention monitoring/reliability

**Your Questions (10 min):**
- "What's a recent incident?"
- "How do you measure pipeline success?"
- "What does the data team look like?"

**Closing (1 min):**
- "Excited about opportunity"
- "When can I expect to hear?"

---

## Key Stats to Know

| Metric | Benchmark |
|--------|-----------|
| API latency | < 500ms |
| Data pipeline latency | < 4 hours |
| Query latency | < 10s |
| Success rate | > 99% |
| Data quality | > 95% |
| Test coverage | > 80% |
| Uptime SLA | 99.9% (8h/year down) |

---

## Books/Resources to Reference

- "Fundamentals of Data Engineering" — Joe Reis
- "Designing Data-Intensive Applications" — Martin Kleppmann
- "SQL Performance Explained" — Markus Winand
- "Release It!" — Michael T. Nygard (reliability patterns)

---

## Final Checklist (Day of Interview)

- ✅ Business casual or casual dress (ask recruiter)
- ✅ Arrive 10 min early (or join video call early)
- ✅ Have resume + portfolio links ready
- ✅ Pen + paper for notes
- ✅ Phone on silent
- ✅ Water nearby
- ✅ Calm down (you know this stuff!)

---

## Post-Interview

- Send thank you email (within 24h)
- Mention specific conversations/interests
- Reiterate why you're excited
- Ask timeline

Remember: You're interviewing them too!
