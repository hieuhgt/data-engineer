# Web Scraping & Data Ingestion Patterns

## Real-World Scenario: Why This Matters

You need to ingest product data from 100 e-commerce sites. Each site:
- Has different HTML structure
- Rate limits you after 50 requests/minute
- Blocks scrapers with User-Agent checks
- Has JavaScript-rendered content
- Changes structure every few months

**Goal:** Build resilient, maintainable ingestion that doesn't break after every site update.

---

## 1. Responsible Scraping (Not Getting Banned)

### ❌ BAD: Getting Blocked Within Minutes
```python
import requests
from bs4 import BeautifulSoup

def scrape_bad(url):
    response = requests.get(url)  # Default User-Agent = "python-requests/2.x"
    # Website immediately recognizes bot, returns empty page
    # IP blocked after 50 requests
    return BeautifulSoup(response.content, 'html.parser')
```

### ✅ GOOD: Respectful Scraping
```python
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
from random import uniform

class RespectfulScraper:
    def __init__(self, delay_range=(1, 3), max_concurrent=3):
        """
        delay_range: (min, max) seconds between requests to same domain
        max_concurrent: Max concurrent requests globally
        """
        self.delay_range = delay_range
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.last_request_time = {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml'
        }

    async def fetch(self, url):
        """Fetch with rate limiting and proper headers."""
        domain = url.split('/')[2]

        # Respect domain rate limits
        if domain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[domain]
            delay_needed = uniform(*self.delay_range)
            if elapsed < delay_needed:
                await asyncio.sleep(delay_needed - elapsed)

        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(
                        url,
                        headers=self.headers,
                        timeout=aiohttp.ClientTimeout(total=15),
                        ssl=False  # Some sites have cert issues
                    ) as response:
                        self.last_request_time[domain] = time.time()

                        if response.status == 429:  # Rate limited
                            logger.warning(f"Rate limited on {domain}")
                            return None

                        return await response.text()
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    return None

    async def scrape_multiple(self, urls):
        """Scrape multiple URLs respecting rate limits."""
        tasks = [self.fetch(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)
```

**Key Practices:**
- ✅ Proper User-Agent (mimics real browser)
- ✅ Delay between requests to same domain
- ✅ Handle 429 (Too Many Requests) gracefully
- ✅ Respect `robots.txt`
- ✅ Check site terms before scraping
- ✅ Contact site owner if long-term data needed

---

## 2. Handling Different Data Formats

### JSON API (Clean Case)
```python
async def ingest_json_api(base_url, endpoints):
    """Most straightforward: structured data."""
    scraper = RespectfulScraper()

    results = {}
    for endpoint in endpoints:
        url = f"{base_url}{endpoint}"
        html = await scraper.fetch(url)
        try:
            results[endpoint] = json.loads(html)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {endpoint}: {e}")
            results[endpoint] = None

    return results
```

### HTML with BeautifulSoup (Messy Case)
```python
def parse_html_table(html):
    """
    Pain Point: HTML structure changes = parsing breaks
    Solution: Flexible selectors + validation
    """
    soup = BeautifulSoup(html, 'html.parser')

    # ❌ FRAGILE: Assumes exact HTML structure
    # rows = soup.find_all('tr')[1:]  # Skip header

    # ✅ ROBUST: Target specific identifiers
    table = soup.find('table', {'class': 'product-table'})
    if not table:
        # Fallback: Look for table by ID
        table = soup.find('table', {'id': 'products'})

    if not table:
        logger.warning("Could not find product table")
        return []

    rows = []
    for row in table.find_all('tr')[1:]:  # Skip header
        try:
            cells = row.find_all('td')
            if len(cells) < 3:  # Validate structure
                continue

            rows.append({
                'name': cells[0].text.strip(),
                'price': cells[1].text.strip(),
                'stock': cells[2].text.strip(),
            })
        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            continue

    return rows
```

### XML with ElementTree
```python
import xml.etree.ElementTree as ET

def parse_xml_feed(xml_string):
    """XML can be deeply nested—use XPath for clarity."""
    try:
        root = ET.fromstring(xml_string)

        items = []
        # XPath notation: .// means search anywhere in tree
        for item in root.findall('.//item'):
            items.append({
                'id': item.findtext('id'),
                'name': item.findtext('name'),
                'price': item.findtext('price'),
            })

        return items
    except ET.ParseError as e:
        logger.error(f"Invalid XML: {e}")
        return []
```

### CSV with Pandas (Best for Large Files)
```python
import pandas as pd
from io import StringIO

def parse_csv_stream(csv_string):
    """
    Pandas is optimized for data processing.
    Better for: large files, type conversion, filtering
    """
    try:
        df = pd.read_csv(
            StringIO(csv_string),
            dtype={'id': int, 'price': float},  # Type safety
            na_values=['N/A', 'null'],  # Handle missing
            skipinitialspace=True  # Clean whitespace
        )

        # Validate schema
        required_cols = {'id', 'name', 'price'}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        return []
```

---

## 3. Resilient Ingestion with Retries

### Exponential Backoff (Don't Hammer Servers)
```python
import asyncio
from functools import wraps
from datetime import datetime, timedelta

def retry_with_backoff(max_attempts=3, base_delay=1, max_delay=60):
    """
    Decorator: automatically retry failed requests
    Waits: 1s, 2s, 4s, ... (exponential)
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Final attempt failed: {e}")
                        raise

                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)

        return wrapper
    return decorator

@retry_with_backoff(max_attempts=3)
async def fetch_with_retry(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as response:
            if response.status >= 400:
                raise ValueError(f"HTTP {response.status}")
            return await response.text()
```

### Circuit Breaker Pattern (Avoid Cascading Failures)
```python
from enum import Enum
from datetime import datetime, timedelta

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failures detected, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.last_failure_time = None

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""

        if self.state == CircuitState.OPEN:
            # Check if we should try to recover
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout):
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise RuntimeError("Circuit breaker OPEN - service unavailable")

        try:
            result = await func(*args, **kwargs)

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker recovered to CLOSED")

            return result

        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.now()

            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.error(f"Circuit breaker OPEN after {self.failure_count} failures")

            raise

# Usage
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

async def fetch_with_breaker(url):
    return await breaker.call(fetch_with_retry, url)
```

---

## 4. Data Quality Validation

```python
import jsonschema

class DataValidator:
    """
    Pain Point: Bad data silently corrupts your pipeline
    Solution: Validate at ingestion, not downstream
    """

    PRODUCT_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string", "minLength": 1},
            "price": {"type": "number", "minimum": 0},
            "currency": {"type": "string", "enum": ["USD", "EUR", "GBP"]}
        },
        "required": ["id", "name", "price"]
    }

    @classmethod
    def validate_record(cls, record):
        """Validate single record."""
        try:
            jsonschema.validate(record, cls.PRODUCT_SCHEMA)
            return True, None
        except jsonschema.ValidationError as e:
            return False, str(e)

    @classmethod
    def validate_batch(cls, records):
        """Validate batch, collect errors."""
        valid = []
        invalid = []

        for i, record in enumerate(records):
            is_valid, error = cls.validate_record(record)
            if is_valid:
                valid.append(record)
            else:
                invalid.append({"index": i, "error": error, "record": record})

        if invalid:
            logger.warning(f"Invalid records: {invalid}")

        return valid, invalid
```

---

## Interview Question

**Q: Design an ingestion pipeline that scrapes 10,000 product pages daily, handling rate limiting, format variety, and data quality.**

**Answer Structure:**
1. Use async with semaphore for concurrency control
2. Implement respectful scraping (delays, headers, robots.txt)
3. Handle multiple formats (JSON, HTML, XML, CSV)
4. Add retry logic with exponential backoff
5. Validate data at ingestion (schema validation)
6. Log failures separately for alerting
7. Store raw data before transformation (debug access)

**Key Metrics to Mention:**
- Success rate (e.g., 99.5% of pages ingested)
- Average ingest time (e.g., 2 hours for 10k pages)
- Failure rate and causes (e.g., 0.3% timeouts, 0.2% bad data)
- Data quality score (e.g., 98% pass validation)
