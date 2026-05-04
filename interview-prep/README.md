# Data Engineer - Python Interview Prep (3-Day Crash Course)

**Target Role:** Data Engineer – Python (Production-grade pipelines, web scraping, ETL, SQL optimization)

**Your Goal:** Go from foundational to confident in 3 days by learning REAL production patterns and pain points.

---

## 📚 Folder Structure

```
interview-prep/
├── 01-python-fundamentals/
│   ├── 01-async-patterns.md          → Async/await, semaphores, rate limiting
│   └── 02-data-ingestion-scraping.md → Web scraping, retries, error handling
│
├── 02-sql-optimization/
│   └── 01-sql-best-practices.md      → Query optimization, indexing, anti-patterns
│
├── 03-pipeline-design/
│   └── 01-architecture.md            → End-to-end pipeline architecture, failures
│
├── 04-testing-reliability/
│   └── 01-testing-strategies.md      → Unit + integration + E2E tests
│
├── 05-devops-tools/
│   └── 01-git-docker-kubernetes.md   → Git workflow, Docker, K8s, CI/CD
│
├── 06-interview-questions/
│   └── 01-common-questions.md        → Real interview Q&A, system design, behavioral
│
└── 07-quick-reference/
    └── 01-cheatsheet.md              → Quick lookup, study schedule, red flags
```

---

## 🎯 3-Day Study Plan

### **Day 1: Python + Data Ingestion (6 hours)**

**Morning (3 hours):**
1. Read: `01-async-patterns.md` (30 min)
2. Code: Write async API fetcher with semaphore (30 min)
3. Concepts: Rate limiting, backpressure, error handling (1 hour)

**Afternoon (3 hours):**
1. Read: `02-data-ingestion-scraping.md` (30 min)
2. Code: Build scraper with retries + circuit breaker (1 hour)
3. Practice: Handle JSON, XML, CSV parsing (1.5 hours)

**Topics Covered:**
- ✅ Async/await patterns
- ✅ Semaphores for rate limiting
- ✅ Web scraping best practices (don't get banned!)
- ✅ Resilient ingestion (retries, timeouts, circuit breakers)
- ✅ Multi-format data handling

---

### **Day 2: SQL + Pipeline Design (6 hours)**

**Morning (3 hours):**
1. Read: `02-sql-optimization.md` (30 min)
2. Practice: Optimize 5 slow queries using EXPLAIN (1.5 hours)
3. Concepts: Indexing strategy, window functions, common anti-patterns (1 hour)

**Afternoon (3 hours):**
1. Read: `03-pipeline-design.md` (30 min)
2. Code: Design pipeline architecture for a scenario (1 hour)
3. Concepts: Idempotency, data quality gates, failure handling (1.5 hours)

**Topics Covered:**
- ✅ Query optimization (EXPLAIN, indexing)
- ✅ Avoiding anti-patterns (LIKE, subqueries)
- ✅ Window functions, window functions
- ✅ Production pipeline architecture
- ✅ Idempotency and data quality
- ✅ Partial failure handling

---

### **Day 3: Testing, DevOps + Interview Prep (6 hours)**

**Morning (2 hours):**
1. Read: `04-testing-strategies.md` (30 min)
2. Code: Write unit + integration tests (1 hour)
3. Concepts: Testing pyramid, what to test (30 min)

**Mid-Morning (2 hours):**
1. Read: `05-devops-tools.md` (30 min)
2. Hands-on: Build Docker image, deploy locally (1 hour)
3. Concepts: Git, CI/CD, Kubernetes (30 min)

**Afternoon (2 hours):**
1. Read: `06-interview-questions.md` (30 min)
2. Practice: Answer 5 questions out loud to friend/mirror (1 hour)
3. Review: `07-quick-reference.md` (30 min)

**Topics Covered:**
- ✅ Unit, integration, E2E testing
- ✅ Test coverage, CI/CD
- ✅ Docker for reproducibility
- ✅ Git workflow, code review
- ✅ Kubernetes basics
- ✅ Interview preparation (behavioral + technical)

---

## 🔑 Key Pain Points & Solutions

### Pain Point 1: "My async code hammers the API"
**Solution:** Use `Semaphore` to limit concurrent requests (default 10)
```python
semaphore = asyncio.Semaphore(10)
async with semaphore:
    await fetch(url)  # Max 10 concurrent
```

### Pain Point 2: "Query takes 45 minutes, I don't know why"
**Solution:** Use `EXPLAIN` to find full table scans, add indexes on WHERE columns
```sql
EXPLAIN SELECT * FROM events WHERE created_at > '2024-01-01'
CREATE INDEX idx_events_created ON events(created_at)
```

### Pain Point 3: "Pipeline retries cause duplicates"
**Solution:** Use UPSERT (merge) instead of INSERT
```python
warehouse.upsert(data)  # Safe to retry
warehouse.insert(data)  # NOT safe (duplicates)
```

### Pain Point 4: "Tests only catch obvious bugs"
**Solution:** Test edge cases (nulls, empty data, timeouts), test error paths
```python
def test_null_handling(self):
    df = pd.DataFrame({'value': [1, 2, None]})
    result = remove_nulls(df)
    assert result is not None and len(result) == 2
```

### Pain Point 5: "Docker image is 2GB, too slow to deploy"
**Solution:** Use multi-stage builds, only copy what you need
```dockerfile
FROM python:3.10 as builder
RUN pip install --no-cache-dir -r requirements.txt
FROM python:3.10-slim
COPY --from=builder /app /app  # Only final files
```

---

## 💡 How to Use This Material

### **Option A: Deep Dive (Recommended for 3 days)**
1. Read file → Understand concepts → Code examples
2. Review quick reference
3. Practice interview questions
4. Success metric: Can explain any topic to a friend

### **Option B: Quick Review (Recommended if you have 1 day)**
1. Skim `07-quick-reference.md` (30 min)
2. Pick 2-3 files most relevant to job (Python? SQL? DevOps?)
3. Read those deeply (2 hours)
4. Review interview questions (30 min)

### **Option C: The Night Before (Emergency Mode)**
1. Read `07-quick-reference.md` (30 min)
2. Skim `06-interview-questions.md` (30 min)
3. Get 8 hours sleep
4. Do NOT panic/cram

---

## 📊 What You'll Learn

| Topic | What | Why |
|-------|------|-----|
| **Async** | Semaphore rate limiting | APIs ban you if too fast |
| **SQL** | Index optimization | 45 min query → 2 seconds |
| **Pipelines** | Idempotency, failure handling | One failure breaks everything |
| **Testing** | Unit + integration tests | Production bugs cost $500k |
| **DevOps** | Docker, K8s, CI/CD | Code must run same on all machines |
| **Interviews** | System design, behavioral | 60% technical, 40% soft skills |

---

## 🎬 Before the Interview

**Night Before:**
- ✅ Read: `07-quick-reference.md` (30 min)
- ✅ Skim: `06-interview-questions.md` (30 min)
- ✅ Sleep: 7-8 hours (brain needs rest)
- ❌ Don't: Cram new topics (too late)

**Day Of:**
- ✅ Think out loud (show reasoning)
- ✅ Ask clarifying questions ("Is latency or cost more important?")
- ✅ Draw architecture diagrams
- ✅ Test code with examples
- ✅ Be honest ("I haven't done X, but here's how I'd approach it")

---

## 📚 Recommended Reading Order

1. **Start here:** `07-quick-reference.md` (5 min orientation)
2. **Day 1:** `01-async-patterns.md` → `02-data-ingestion-scraping.md`
3. **Day 2:** `02-sql-optimization.md` → `03-pipeline-design.md`
4. **Day 3:** `04-testing-strategies.md` → `05-devops-tools.md`
5. **Before interview:** `06-interview-questions.md` + `07-quick-reference.md`

---

## 🎯 Success Metrics

After completing this prep:
- ✅ Can explain async/semaphore without notes
- ✅ Can optimize a slow SQL query
- ✅ Can design a production pipeline (idempotent, reliable)
- ✅ Can write unit + integration tests
- ✅ Can answer "Tell me about a time you fixed a bug"
- ✅ Can ask smart questions about data infrastructure

---

## 🚨 Common Interview Red Flags (Avoid These!)

| Red Flag | What to Say Instead |
|----------|-------------------|
| "I don't know" | "I haven't done this, but here's how I'd approach it" |
| "Just make it fast" | "Trade-off: faster but uses more memory, let's discuss" |
| "No tests needed" | "I write unit + integration tests, aim for 80%+ coverage" |
| Copy-paste code | "Let me understand what this does first" |

---

## 🤝 Ask Interviewer These Questions

- "What's a recent production incident your team dealt with?"
- "How do you measure pipeline success?"
- "What's your current data stack (Spark, Kafka, Snowflake)?"
- "How much time do engineers spend on ops vs new features?"
- "What's the biggest challenge your data team faces?"

**Why Ask:** Shows you think about real problems, not just coding.

---

## 🎓 Learning Mindset

> "It's not about knowing everything. It's about understanding the *why* behind decisions."

Focus on:
- ✅ Trade-offs (speed vs cost, complexity vs reliability)
- ✅ Pain points (what breaks in production?)
- ✅ Best practices (idempotency, testing, monitoring)

Don't focus on:
- ❌ Exact library names (you can Google that)
- ❌ Syntax details (IDE autocompletes that)
- ❌ Obscure edge cases (learn fundamentals first)

---

## 💬 Questions? Need Help?

If something is unclear:
1. Read it again (sometimes second reading clicks)
2. Code a small example (hands-on > reading)
3. Teach someone else (forces you to understand)
4. Google "X in data engineering" (see how others explain it)

---

**Good luck! You've got this. 🚀**
