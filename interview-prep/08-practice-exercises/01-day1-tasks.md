# Day 1 Hands-On Exercises: Python + Data Ingestion

## Exercise 1: Async API Fetcher with Semaphore (30 min)

**Task:** Write an async function that fetches from 20 APIs while respecting rate limits.

**Requirements:**
- Max 5 concurrent requests at a time
- 2-second delay between requests to same domain
- Handle timeouts (10s per request)
- Return both successes and failures

**Skeleton:**
```python
import asyncio
import aiohttp
import time
from urllib.parse import urlparse
from random import uniform

class APIFetcher:
    def __init__(self, max_concurrent=5, delay_between_requests=2):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay_between_requests
        self.last_request = {}  # Track last request per domain

    async def fetch(self, url):
        """Fetch URL with semaphore and delay."""
        # TODO: Extract domain from URL
        domain = url.split('/')[2]
        # TODO: Check if enough time passed since last request
        if domain in self.last_request:
            elapsed = time.time() - self.last_request[domain]
            delay_needed = uniform(*self.delay_range)
            if elapsed < self.delay:
                await asyncio.sleep(delay_needed - elapsed)
        # TODO: Acquire semaphore before fetching
        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 429:
                            logger.warning(f"Rate limited on {url}")
                            return None
                        return await response.text()
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                return None

    async def fetch_all(self, urls):
        """Fetch all URLs and return (successes, failures)."""
        # TODO: Create tasks for each URL
        tasks = [self.fetch(url) for url in urls]
        # TODO: Gather with return_exceptions=True
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # TODO: Separate successes from failures
        successes = [result for result in results if result is not None]
        failures = [result for result in results if result is None]
        # TODO: Log statistics
        logger.info(f"Success: {len(successes)}, Failures: {len(failures)}")
        # TODO: Return both

# Test
async def main():
    urls = [f"https://jsonplaceholder.typicode.com/users/{i}" for i in range(1, 21)]
    fetcher = APIFetcher(max_concurrent=5)
    results, failures = await fetcher.fetch_all(urls)
    print(f"Success: {len(results)}, Failures: {len(failures)}")

if __name__ == '__main__':
    asyncio.run(main())
```

**Evaluation:**
- ✅ Did it respect max 5 concurrent?
- ✅ Did it handle timeouts?
- ✅ Did it return both successes and failures?
- ✅ Can you add retry logic? (Bonus)

---

## Exercise 2: Resilient Scraper with Retries (45 min)

**Task:** Build a scraper that retries on failures, with exponential backoff.

**Requirements:**
- Retry up to 3 times on failure
- Exponential backoff: 1s, 2s, 4s
- Log each attempt
- Use `return_exceptions=True` for partial failures
- Parse HTML table from response

**Skeleton:**
```python
import aiohttp
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, max_attempts: int = 3):
    """Fetch with exponential backoff retry."""
    for attempt in range(max_attempts):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 429:
                    logger.warning(f"Rate limited on {url}")
                    raise Exception(f"Rate limited on {url}")
                return await response.text()
        except Exception as e:
            if attempt == max_attempts - 1:
                logger.error(f"Failed to fetch {url} after {max_attempts} attempts: {e}")
                return None
            
            # TODO: Calculate backoff delay
            backoff_delay = 2 ** attempt
            # TODO: Log retry attempt
            logger.warning(f"Retrying {url} in {backoff_delay}s (attempt {attempt + 1}/{max_attempts})")
            # TODO: Sleep before retry
            await asyncio.sleep(backoff_delay)

async def scrape_table(html):
    """Extract table from HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    # TODO: Find table by class or ID
    table = soup.find('table', {'class': 'product-table'})
    if not table:
        logger.warning("Could not find product table")
        return []
    headers = [th.text.strip() for th in table.find_all('th')]
    if not headers:
        # Fallback if no <th> tags exist
        headers = ["column_1", "column_2", "column_3"]
    # TODO: Extract rows
    data = []
    rows = table.find_all('tr')  # Skip header
    # TODO: Handle missing table gracefully
    if not rows:
        logger.info()
    # TODO: Return list of dicts
    for row in rows:
        cols = row.find_all('td')
        if not cols:
            continue
        item = {headers[i]: col.text.strip() for i, col in enumerate(cols) if i < len(headers)}
        data.append(item)

    logger.info(f"Scraped {len(data)} items from {url}")
    return data


async def main():
    urls = [
        "https://example.com/products",
        "https://example.com/users",
        "https://example.com/orders",
    ]
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_retry(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successes = [result for result in results if result is not None]
        failures = [result for result in results if result is None]
        logger.info(f"Success: {len(successes)}, Failures: {len(failures)}")
        return successes, failures

if __name__ == '__main__':
    asyncio.run(main())
```

**Evaluation:**
- ✅ Did retry happen 3 times on failure?
- ✅ Were backoff delays correct (1, 2, 4)?
- ✅ Did it handle partial failures (some succeed, some fail)?
- ✅ Did it parse HTML table correctly?
- ✅ Bonus: Add circuit breaker (stop after 5 consecutive failures)

---

## Exercise 3: Multi-Format Data Parser (45 min)

**Task:** Write parsers for JSON, XML, CSV that handle errors gracefully.

**Requirements:**
- Parse JSON API response
- Parse XML feed
- Parse CSV file
- Handle invalid data (log warnings, don't crash)
- Return consistent format (list of dicts)

**Skeleton:**
```python
import json
import xml.etree.ElementTree as ET
import pandas as pd
from io import StringIO

class DataParser:
    @staticmethod
    def parse_json(json_str):
        """Parse JSON response."""
        try:
            # TODO: Parse JSON
            # TODO: Validate has expected fields
            # TODO: Return as list of dicts
        except json.JSONDecodeError as e:
            # TODO: Log error
            # TODO: Return empty list
            pass

    @staticmethod
    def parse_xml(xml_str):
        """Parse XML feed."""
        try:
            # TODO: Parse XML
            # TODO: Find items element
            # TODO: Extract fields using XPath
            # TODO: Handle missing elements gracefully
            # TODO: Return list of dicts
        except ET.ParseError as e:
            # TODO: Log error
            # TODO: Return empty list
            pass

    @staticmethod
    def parse_csv(csv_str):
        """Parse CSV data."""
        try:
            # TODO: Parse with pandas
            # TODO: Type conversion (int, float)
            # TODO: Handle missing values
            # TODO: Return as list of dicts
        except Exception as e:
            # TODO: Log error
            # TODO: Return empty list
            pass

# Test
if __name__ == '__main__':
    # Test JSON
    json_data = '{"items": [{"id": 1, "name": "Product A"}]}'
    result = DataParser.parse_json(json_data)
    print(f"JSON parsed: {result}")

    # Test XML
    xml_data = '''<feed>
        <item>
            <id>1</id>
            <name>Product A</name>
        </item>
    </feed>'''
    result = DataParser.parse_xml(xml_data)
    print(f"XML parsed: {result}")

    # Test CSV
    csv_data = 'id,name,price\n1,Product A,100.5\n2,Product B,200'
    result = DataParser.parse_csv(csv_data)
    print(f"CSV parsed: {result}")
```

**Evaluation:**
- ✅ Did it parse all three formats?
- ✅ Did it return consistent format (list of dicts)?
- ✅ Did it handle invalid data gracefully?
- ✅ Did it log errors but not crash?

---

## Bonus: Circuit Breaker Pattern (30 min, Optional)

**Task:** Implement circuit breaker to prevent hammering a failing API.

**States:**
- CLOSED: Normal operation
- OPEN: Too many failures, reject requests
- HALF_OPEN: Testing if recovered

**Skeleton:**
```python
from enum import Enum
from datetime import datetime, timedelta

class State(Enum):
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.state = State.CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker."""
        # TODO: Check if OPEN, if so check recovery
        if self.state == State.OPEN:
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.recovery_timeout):
                self.state = State.HALF_OPEN        
        # TODO: Execute function
        try:
            result = await func(*args, **kwargs)
            if self.state == State.HALF_OPEN:
                self.state = State.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker recovered to CLOSED")

            return result
        except Exception as e:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.state = State.OPEN
                self.last_failure_time = datetime.now()
        # TODO: On success, reset failures
        # TODO: On failure, increment failures and check threshold

# Usage
breaker = CircuitBreaker(failure_threshold=3)

async def fetch(url):
    return await breaker.call(fetch_with_retry, url)
```

---

## Day 1 Checklist

- [ ] Exercise 1: Async fetcher with semaphore (working)
- [ ] Exercise 2: Scraper with retries (working)
- [ ] Exercise 3: Multi-format parser (working)
- [ ] Bonus: Circuit breaker (optional)
- [ ] Run all code without errors
- [ ] Can explain each component to friend?

## After Day 1, You Should Know:

- ✅ Why semaphores matter (don't get banned!)
- ✅ How exponential backoff works
- ✅ How to handle partial failures
- ✅ How to parse multiple data formats
- ✅ How to log and debug async code
