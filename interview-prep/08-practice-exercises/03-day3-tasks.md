# Day 3 Hands-On Exercises: Testing, DevOps + Interview Prep

## Exercise 1: Unit Testing (45 min)

**Task:** Write unit tests for a data transformation function.

**Function to Test:**
```python
import pandas as pd

def clean_orders(df):
    """
    Transform raw orders:
    1. Remove rows with null amount
    2. Convert amount to float
    3. Remove duplicate order_ids (keep first)
    4. Filter for amounts > 0
    """
    df = df.dropna(subset=['amount'])
    df['amount'] = df['amount'].astype(float)
    df = df.drop_duplicates(subset=['order_id'], keep='first')
    df = df[df['amount'] > 0]
    return df
```

**Test Skeleton:**
```python
import unittest
import pandas as pd

class TestCleanOrders(unittest.TestCase):
    def setUp(self):
        """Create test data before each test."""
        # TODO: Create sample DataFrame with various issues

    def test_remove_nulls(self):
        """Test that null amounts are removed."""
        # TODO: Create input with null amount
        # TODO: Call clean_orders
        # TODO: Assert null rows removed

    def test_remove_duplicates(self):
        """Test that duplicates are handled."""
        # TODO: Create input with duplicate order_id
        # TODO: Call clean_orders
        # TODO: Assert only one row per order_id remains

    def test_filter_negative_amounts(self):
        """Test that negative amounts are removed."""
        # TODO: Create input with negative amount
        # TODO: Call clean_orders
        # TODO: Assert negative rows removed

    def test_type_conversion(self):
        """Test that string amounts are converted to float."""
        # TODO: Create input with string amounts ('100.5')
        # TODO: Call clean_orders
        # TODO: Assert dtype is float

    def test_empty_input(self):
        """Test handling of empty DataFrame."""
        # TODO: Create empty DataFrame
        # TODO: Call clean_orders
        # TODO: Assert returns empty DataFrame (not error)

    def test_all_pass_validation(self):
        """Test that valid data passes through unchanged."""
        # TODO: Create input with all valid data
        # TODO: Call clean_orders
        # TODO: Assert same length, same values

if __name__ == '__main__':
    unittest.main()
```

**Evaluation:**
- ✅ Did you test both success and failure paths?
- ✅ Did you test edge cases (empty data, null, duplicates)?
- ✅ Did you test type conversion?
- ✅ Do tests fail if code is broken?

---

## Exercise 2: Integration Testing (45 min)

**Task:** Test pipeline with real database (using fixtures).

**Requirements:**
- Create test database
- Insert test data
- Run pipeline transformation
- Verify output in database
- Clean up after test

**Skeleton:**
```python
import pytest
import psycopg2
from psycopg2.extras import execute_values

class TestPipelineIntegration:
    @pytest.fixture(scope='function')
    def db_connection(self):
        """Create test database connection."""
        # TODO: Connect to test_analytics database
        # TODO: Yield connection
        # TODO: Close after test

    @pytest.fixture(scope='function')
    def clean_tables(self, db_connection):
        """Clean tables before each test."""
        cursor = db_connection.cursor()
        # TODO: Truncate test tables
        db_connection.commit()
        yield
        # TODO: Cleanup after test

    def test_insert_and_duplicate_detection(self, db_connection, clean_tables):
        """Test inserting orders and detecting duplicates."""
        cursor = db_connection.cursor()

        # Insert test data
        test_data = [
            (1, 10, 100.50, '2024-01-01'),
            (1, 10, 100.50, '2024-01-01'),  # Duplicate
            (2, 20, 200.75, '2024-01-02'),
        ]

        execute_values(
            cursor,
            'INSERT INTO orders (order_id, user_id, amount, date) VALUES %s',
            test_data
        )
        db_connection.commit()

        # TODO: Call deduplication function
        # TODO: Verify only 2 unique orders remain

    def test_data_quality_gate(self, db_connection, clean_tables):
        """Test that invalid data is rejected."""
        cursor = db_connection.cursor()

        # Insert invalid data (negative amount)
        invalid_data = [
            (1, 10, -100, '2024-01-01'),  # Invalid
            (2, 20, 200, '2024-01-02'),
        ]

        # TODO: Insert data
        # TODO: Call quality gate
        # TODO: Assert exception raised (data rejected)

    def test_idempotent_load(self, db_connection, clean_tables):
        """Test that loading twice doesn't create duplicates."""
        cursor = db_connection.cursor()

        data = [(1, 10, 100.50, '2024-01-01')]

        # TODO: Load data first time
        # TODO: Load same data again (upsert)
        # TODO: Verify only 1 row exists (no duplicates)

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
```

**Evaluation:**
- ✅ Did you use fixtures for test setup/teardown?
- ✅ Did you test with real database?
- ✅ Did you verify idempotency (same data loaded twice = 1 row)?
- ✅ Did you clean up after tests?

---

## Exercise 3: Docker & Local Deployment (45 min)

**Task:** Create Dockerfile and test locally.

**Step 1: Create Dockerfile**
```dockerfile
# pipeline/Dockerfile

# TODO: Use Python 3.10 slim base image
# TODO: Set working directory to /app
# TODO: Copy requirements.txt
# TODO: Install dependencies with --no-cache-dir
# TODO: Copy source code
# TODO: Create non-root user
# TODO: Set CMD to run pipeline

# Remember: Multi-stage if building large image
# Remember: Non-root user for security
# Remember: COPY specific files, not entire directory
```

**Step 2: Create docker-compose.yml**
```yaml
# docker-compose.yml

version: '3.8'

services:
  # Your pipeline service
  pipeline:
    # TODO: Build from Dockerfile
    # TODO: Set environment variables (DATABASE_URL, etc)
    # TODO: Mount volumes for data
    # TODO: Depends on postgres

  # PostgreSQL for testing
  postgres:
    # TODO: Use postgres:13 image
    # TODO: Set POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    # TODO: Mount volume for persistence
    # TODO: Expose port 5432

# Remember: Non-root user in container
# Remember: Don't hardcode secrets (use .env file)
```

**Step 3: Build and Test**
```bash
# Build image
docker build -t pipeline:test .

# Run with docker-compose
docker-compose up -d

# Check logs
docker-compose logs -f pipeline

# Run manual test
docker exec pipeline python -m pytest tests/

# Cleanup
docker-compose down -v
```

**Evaluation:**
- ✅ Did Docker image build without errors?
- ✅ Did container run the pipeline?
- ✅ Did it connect to Postgres successfully?
- ✅ Can you reduce image size? (multi-stage, --no-cache-dir)

---

## Exercise 4: Interview Practice (60 min)

**Task:** Answer interview questions out loud.

### Q1: System Design (20 min)
**Question:** Design a data pipeline that ingests from 50 APIs, transforms data, and loads to warehouse. Requirements: 99.9% reliability, 4-hour latency, 100GB/day.

**Your Answer Should Include:**
- ✅ Architecture diagram (ingest → validate → transform → load)
- ✅ Reliability (retry logic, circuit breaker, partial failure handling)
- ✅ Idempotency (UPSERT not INSERT)
- ✅ Monitoring (metrics, alerts)
- ✅ Scalability (how to scale 10x)

### Q2: SQL Problem (10 min)
**Question:** Write a query to find the top 3 products by sales for each category.

**Your Answer Should Include:**
- ✅ Use window function (ROW_NUMBER or RANK)
- ✅ PARTITION BY category
- ✅ ORDER BY sales DESC
- ✅ FILTER where rank <= 3

### Q3: Behavioral (20 min)
**Question:** Tell me about a time you fixed a production bug.

**Your Answer Should Include (STAR format):**
- **S**ituation: What was the problem?
- **T**ask: What was your responsibility?
- **A**ction: What did you do? (steps taken)
- **R**esult: What was the outcome?

**Example:**
> "Pipeline failed to ingest 10M rows. I checked logs, found a timeout issue on API calls. I increased timeout from 10s to 30s, tested locally, deployed. Fixed in 45 min. Added monitoring to catch future timeouts."

### Q4: Failure Handling (10 min)
**Question:** Your pipeline processes data from 20 sources. One is down. What happens?

**Your Answer Should Include:**
- ✅ Don't block entire pipeline (partial failure)
- ✅ Retry that source with backoff
- ✅ Load successful sources
- ✅ Alert on failures
- ✅ Don't corrupt data

---

## Day 3 Checklist

- [ ] Exercise 1: Unit tests written and passing
- [ ] Exercise 2: Integration tests with real DB
- [ ] Exercise 3: Docker image builds and runs
- [ ] Exercise 4: Can answer 4 interview questions
- [ ] Practice: Answered questions out loud to someone?

## Before the Real Interview

**Tomorrow Morning:**
1. Review quick reference (5 min)
2. Skim one article that interested you most (10 min)
3. Eat good breakfast
4. Get to location early
5. Deep breath—you've got this!

**During Interview:**
- ✅ Listen fully before answering
- ✅ Think out loud (show reasoning)
- ✅ Ask clarifying questions
- ✅ Draw diagrams
- ✅ Test with examples
- ✅ Be honest ("I haven't done X, but here's how I'd approach it")
- ✅ Ask questions about their infrastructure/problems

**After Interview:**
- Send thank you email (within 24 hours)
- Mention specific things you discussed
- Reiterate why you're excited
- Ask about timeline

---

## Interview Success Indicators

You're ready if:
- ✅ Can explain async/semaphore concept
- ✅ Can optimize a slow SQL query
- ✅ Can design a production pipeline
- ✅ Can write unit tests for functions
- ✅ Can answer behavioral questions (STAR)
- ✅ Can discuss trade-offs (speed vs cost)
- ✅ Can ask smart questions about their infrastructure

---

## Common Interview Mistakes (Avoid These!)

| Mistake | Better Approach |
|---------|-----------------|
| "I don't know" | "I haven't done this, but here's my approach" |
| Too much detail | "Let me start with the high level, then dive in" |
| No diagrams | Draw architecture, DB schema, data flow |
| No questions | "What's your current data stack? Any recent incidents?" |
| Rushing | Pause to think, clarify ambiguities |
| Memorized answer | Talk conversationally, show real understanding |

---

## Resources for Final Review

- **Quick Reference:** `07-quick-reference.md` (5 min)
- **System Design:** `03-pipeline-design.md` (15 min)
- **SQL:** `02-sql-optimization.md` (15 min)
- **Interview Q&A:** `06-interview-questions.md` (30 min)

---

## You've Got This! 🚀

You've completed:
- ✅ Async Python (semaphores, retries, error handling)
- ✅ Web scraping (respectful, resilient)
- ✅ SQL optimization (EXPLAIN, indexing, window functions)
- ✅ Pipeline design (architecture, failure handling)
- ✅ Testing (unit + integration + E2E)
- ✅ DevOps (Docker, Git, basics of K8s)
- ✅ Interview preparation (50+ Q&A)

**You're ready. Go crush it! 💪**
