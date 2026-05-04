# Testing & Reliability for Data Pipelines

## Why Testing Matters (The Cost of Failure)

A data pipeline bug reaches production. Analysts run reports on wrong data for 2 days before discovering. Impact:
- $500k marketing budget wasted on bad insights
- Management loses trust in data team
- Audit log required, brand damage

**Cost of testing:** 4 hours to write tests
**Cost of failure:** $500k + reputation

---

## 1. Testing Strategy (Pyramid)

```
        /\
       /  \        End-to-End (E2E)
      /    \       (Rare, expensive, slow)
     /      \
    /________\

   /          \
  /    Unit    \   (Fast, cheap, many)
 /____Tests____\
```

### Test Distribution

- **Unit Tests (70%):** Test functions in isolation
- **Integration Tests (20%):** Test components together (DB, APIs)
- **E2E Tests (10%):** Test full pipeline end-to-end

---

## 2. Unit Testing (Data Validation)

### Testing Data Transformations

```python
import unittest
import pandas as pd
from datetime import datetime

class TestDataTransformation(unittest.TestCase):
    """Test data transformation functions."""

    def setUp(self):
        """Create test data."""
        self.input_df = pd.DataFrame({
            'user_id': [1, 2, 3, 4, 5],
            'amount': [100.5, 200.75, None, 50.0, 150.25],
            'created_at': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'status': ['completed', 'completed', 'failed', 'completed', 'pending']
        })

    def test_remove_nulls(self):
        """Test null removal."""
        def remove_nulls(df, columns):
            return df.dropna(subset=columns)

        result = remove_nulls(self.input_df, ['amount'])

        # Assert: 4 rows remain (1 null removed)
        self.assertEqual(len(result), 4)
        self.assertFalse(result['amount'].isna().any())

    def test_calculate_total(self):
        """Test aggregation."""
        def calculate_total(df):
            return df['amount'].sum()

        result = calculate_total(self.input_df.dropna(subset=['amount']))

        # Assert: Correct sum (ignore nulls)
        self.assertAlmostEqual(result, 501.5, places=2)

    def test_filter_completed(self):
        """Test filtering."""
        def filter_completed(df):
            return df[df['status'] == 'completed']

        result = filter_completed(self.input_df)

        # Assert: Only 3 completed orders
        self.assertEqual(len(result), 3)
        self.assertTrue((result['status'] == 'completed').all())

    def test_parse_date(self):
        """Test date parsing."""
        def parse_date(df):
            df['created_at'] = pd.to_datetime(df['created_at'])
            return df

        result = parse_date(self.input_df.copy())

        # Assert: Dates are datetime type
        self.assertEqual(result['created_at'].dtype, 'datetime64[ns]')

    def test_invalid_input_handling(self):
        """Test error cases."""
        def parse_int(value):
            try:
                return int(value)
            except (ValueError, TypeError):
                return None

        # Assert: Invalid inputs handled gracefully
        self.assertEqual(parse_int('123'), 123)
        self.assertEqual(parse_int('abc'), None)
        self.assertEqual(parse_int(None), None)

if __name__ == '__main__':
    unittest.main()
```

### Testing Data Quality Rules

```python
class TestDataQuality(unittest.TestCase):
    """Test data quality constraints."""

    def test_no_negative_prices(self):
        """Assert prices are non-negative."""
        df = pd.DataFrame({'price': [10.5, 20.0, 5.25]})

        self.assertTrue((df['price'] >= 0).all())

    def test_user_id_unique(self):
        """Assert user_id has no duplicates."""
        df = pd.DataFrame({'user_id': [1, 2, 3, 4, 5]})

        self.assertEqual(len(df), len(df['user_id'].unique()))

    def test_required_fields(self):
        """Assert no nulls in required columns."""
        df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'user_id': [10, 20, 30],
            'amount': [100, 200, 300]
        })

        required = ['order_id', 'user_id', 'amount']
        for col in required:
            self.assertFalse(df[col].isna().any(), f"Null found in required column: {col}")

    def test_email_format(self):
        """Assert emails match expected format."""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

        valid = 'user@example.com'
        invalid = 'user@invalid'

        self.assertTrue(re.match(pattern, valid))
        self.assertFalse(re.match(pattern, invalid))
```

---

## 3. Integration Testing (Pipeline Stages)

### Testing with Real Database

```python
import pytest
import psycopg2
from psycopg2.extras import execute_values

class TestPipelineIntegration:
    """Test pipeline stages with real database."""

    @pytest.fixture(scope='function')
    def db_connection(self):
        """Create test database connection."""
        conn = psycopg2.connect(
            dbname='test_db',
            user='test_user',
            password='test_pass',
            host='localhost'
        )
        yield conn
        conn.close()

    @pytest.fixture(scope='function')
    def clean_tables(self, db_connection):
        """Clean tables before each test."""
        cursor = db_connection.cursor()
        cursor.execute('TRUNCATE TABLE fact_orders CASCADE')
        db_connection.commit()
        cursor.close()
        yield
        cursor = db_connection.cursor()
        cursor.execute('TRUNCATE TABLE fact_orders CASCADE')
        db_connection.commit()
        cursor.close()

    def test_insert_orders(self, db_connection, clean_tables):
        """Test inserting orders into warehouse."""
        cursor = db_connection.cursor()

        test_data = [
            (1, 100.50, 'completed'),
            (2, 200.75, 'completed'),
            (3, 50.00, 'pending'),
        ]

        # Insert
        execute_values(
            cursor,
            'INSERT INTO fact_orders (user_id, amount, status) VALUES %s',
            test_data
        )
        db_connection.commit()

        # Verify
        cursor.execute('SELECT COUNT(*) FROM fact_orders')
        count = cursor.fetchone()[0]
        assert count == 3

        # Verify data integrity
        cursor.execute('SELECT SUM(amount) FROM fact_orders WHERE status = %s', ('completed',))
        total = cursor.fetchone()[0]
        assert total == 301.25

        cursor.close()

    def test_duplicate_detection(self, db_connection, clean_tables):
        """Test that duplicates are handled correctly."""
        cursor = db_connection.cursor()

        # Insert same order twice
        test_data = [(1, 100.50, 'completed'), (1, 100.50, 'completed')]

        # Upsert (update if exists, insert if not)
        for order_id, amount, status in test_data:
            cursor.execute(
                '''
                INSERT INTO fact_orders (order_id, amount, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET amount = %s, status = %s
                ''',
                (order_id, amount, status, amount, status)
            )
        db_connection.commit()

        # Verify: Only 1 row (duplicate handled)
        cursor.execute('SELECT COUNT(*) FROM fact_orders WHERE order_id = %s', (1,))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.close()
```

### Testing API Integration

```python
import responses
from unittest.mock import patch, MagicMock

class TestAPIIntegration:
    """Test API ingestion with mocking."""

    @responses.activate
    def test_fetch_api_success(self):
        """Test successful API response."""
        responses.add(
            responses.GET,
            'https://api.example.com/users/123',
            json={'id': 123, 'email': 'user@example.com'},
            status=200
        )

        from your_module import fetch_user
        result = fetch_user(123)

        assert result['email'] == 'user@example.com'

    @responses.activate
    def test_fetch_api_retry(self):
        """Test retry on timeout."""
        responses.add(
            responses.GET,
            'https://api.example.com/users/123',
            body='Connection timeout',
            status=504
        )
        responses.add(
            responses.GET,
            'https://api.example.com/users/123',
            json={'id': 123, 'email': 'user@example.com'},
            status=200
        )

        from your_module import fetch_user_with_retry
        result = fetch_user_with_retry(123)

        assert result['email'] == 'user@example.com'
        assert len(responses.calls) == 2  # Verify retry happened
```

---

## 4. End-to-End Testing (Full Pipeline)

```python
import pytest
import subprocess
from datetime import datetime

class TestEndToEnd:
    """Test full pipeline execution."""

    @pytest.mark.slow  # Mark as slow, run separately
    def test_pipeline_daily_run(self, tmp_path):
        """Test complete pipeline execution."""

        # Setup: Create test data files
        test_file = tmp_path / 'test_input.csv'
        test_file.write_text('order_id,user_id,amount\n1,10,100\n2,20,200\n')

        # Run pipeline
        result = subprocess.run(
            ['python', '-m', 'pipeline.main', f'--input={test_file}'],
            capture_output=True,
            timeout=60
        )

        # Verify: Exit code 0 (success)
        assert result.returncode == 0

        # Verify: Output file created
        output_file = tmp_path / 'output.parquet'
        assert output_file.exists()

        # Verify: Data quality
        df = pd.read_parquet(output_file)
        assert len(df) == 2
        assert df['amount'].sum() == 300

    @pytest.mark.slow
    def test_pipeline_with_errors(self):
        """Test pipeline gracefully handles errors."""
        bad_data = 'invalid,data\n1,abc,xyz\n'

        result = subprocess.run(
            ['python', '-m', 'pipeline.main', f'--input={bad_data}'],
            capture_output=True
        )

        # Verify: Pipeline fails gracefully (exit code 1)
        assert result.returncode == 1

        # Verify: Error message logged
        assert 'validation' in result.stderr.lower()
```

---

## 5. Testing Checklist & Best Practices

### Before Each Test
- ✅ Setup clean test data
- ✅ Use fixtures for reusability
- ✅ Isolate external dependencies (mock APIs, databases)
- ✅ Test both success and failure paths

### Test Naming
```python
# ✅ GOOD: Descriptive names
def test_remove_nulls_keeps_valid_rows(self):
def test_api_retry_on_timeout(self):
def test_invalid_email_raises_error(self):

# ❌ BAD: Vague names
def test_data(self):
def test_api(self):
def test_error(self):
```

### Coverage Requirements

```bash
# Measure test coverage
coverage run -m pytest tests/
coverage report

# Target: 80%+ coverage on data transformation code
# Target: 60%+ coverage on infrastructure code
```

### CI/CD Integration

```yaml
# GitHub Actions example
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pytest tests/ --cov=src --cov-report=xml
      - run: coverage report --fail-under=80  # Fail if < 80% coverage
```

---

## Interview Question

**Q: How would you test a data pipeline that ingests from 20 APIs, transforms data, and loads to warehouse?**

**Answer:**

1. **Unit Tests (70%):**
   - Test each transformation function with sample data
   - Test error handling (nulls, type errors, invalid formats)
   - Test data quality rules (no negatives, duplicates detected)

2. **Integration Tests (20%):**
   - Mock APIs (use responses library)
   - Test with real test database
   - Test duplicate handling (upsert logic)
   - Test partial failures (1 API down, others succeed)

3. **E2E Tests (10%):**
   - Run full pipeline on test data set
   - Verify output matches expected schema/counts
   - Test with realistic data volumes

4. **Reliability:**
   - Use fixtures for test data isolation
   - CI/CD pipeline enforces 80%+ coverage
   - Tests run on every commit
   - Separate "slow" E2E tests to run nightly

**Code Structure:**
```
tests/
├── unit/
│   ├── test_transformations.py
│   └── test_validation.py
├── integration/
│   ├── test_api_fetching.py
│   └── test_warehouse_loading.py
└── e2e/
    └── test_pipeline_full.py
```
