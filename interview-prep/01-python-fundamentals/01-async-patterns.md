# Async Patterns for Data Engineering

## Why Async Matters in Data Engineering

**Pain Point:** Sequential processing of API calls = slow ingestion. If you have 1000 API endpoints and each takes 1s, sequential takes 1000s. Async + concurrent execution can do it in ~5-10s.

---

## Core Concepts

### 1. **Asyncio Basics**
```python
import asyncio
import time

# ❌ BAD: Sequential (blocks everything)
def fetch_data_slow(urls):
    results = []
    for url in urls:
        data = requests.get(url)  # Blocks for 1-2s each
        results.append(data)
    return results  # Total: 10-20s for 10 requests

# ✅ GOOD: Async with semaphore (respects rate limits)
import aiohttp

async def fetch_with_limit(session, url, semaphore):
    async with semaphore:  # Max 5 concurrent requests
        async with session.get(url) as response:
            return await response.json()

async def fetch_data_fast(urls, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_limit(session, url, semaphore) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=True)

# Usage
results = asyncio.run(fetch_data_fast(urls, max_concurrent=10))
```

**Key Learning:**
- `asyncio.Semaphore()` prevents hammering APIs (rate limiting)
- `return_exceptions=True` prevents one failure from breaking all tasks
- Always use context managers (`async with`) for resource cleanup

### 2. **Error Handling in Async**

```python
# ❌ BAD: Errors silently lost
async def risky_fetch(urls):
    tasks = [fetch_with_timeout(url) for url in urls]
    results = await asyncio.gather(*tasks)  # One exception fails all
    return results

# ✅ GOOD: Capture and log errors separately
async def robust_fetch(urls, timeout=10):
    tasks = [
        fetch_with_timeout(url, timeout)
        for url in urls
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Separate successes and failures
    successes = [r for r in results if not isinstance(r, Exception)]
    failures = [(i, r) for i, r in enumerate(results) if isinstance(r, Exception)]

    if failures:
        logger.error(f"Failed {len(failures)} requests: {failures}")

    return successes, failures

async def fetch_with_timeout(url, timeout):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                return await response.json()
    except asyncio.TimeoutError:
        raise ValueError(f"Timeout fetching {url}")
    except Exception as e:
        raise RuntimeError(f"Error fetching {url}: {e}")
```

**Pain Point:** Debugging async failures is harder—errors can happen in background. Always log and track partial failures.

### 3. **Backpressure (Controlling Flow)**

```python
# ❌ BAD: Queue explodes if consumer is slow
async def producer(queue):
    for i in range(10000):
        await queue.put(i)  # Queue fills up = memory explosion

# ✅ GOOD: Producer respects consumer speed
async def producer_with_backpressure(queue, max_size=100):
    for i in range(10000):
        if queue.qsize() >= max_size:
            await asyncio.sleep(0.1)  # Pause producer
        await queue.put(i)

async def consumer(queue):
    while True:
        item = await queue.get()
        await process_slow(item)  # Slow processing
        queue.task_done()

# Usage
queue = asyncio.Queue(maxsize=100)
await asyncio.gather(
    producer_with_backpressure(queue),
    consumer(queue)
)
```

**Real-world scenario:** Scraping 1M pages, storing to database. Without backpressure, you queue all 1M in memory before writing to DB.

---

## Best Practices Checklist

- ✅ Always use `Semaphore` for API rate limiting
- ✅ Use `return_exceptions=True` in `gather()` to handle partial failures
- ✅ Always set timeouts on network calls (`timeout=10`)
- ✅ Implement backpressure (pause producer if consumer can't keep up)
- ✅ Log failures separately from successes
- ✅ Use `aiohttp` instead of `requests` for async work
- ✅ Don't mix sync and async without `asyncio.run()` or `nest_asyncio`
- ✅ Consider `asyncio.wait()` for timeout-aware task management

---

## Interview Question

**Q: You're ingesting data from 50 APIs, each can take 1-10 seconds. How would you design this?**

**Good Answer:**
- Use `aiohttp` with async/await
- Semaphore to limit concurrent requests (e.g., 10 at a time to respect rate limits)
- Timeout per request (10s) to avoid hanging forever
- Return exceptions to distinguish failed vs successful ingestions
- Log failures separately for alerting/retry logic
- Implement exponential backoff for retries

**Sample Code:**
```python
async def ingest_all_apis(api_configs, max_concurrent=10):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_retry(config):
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with semaphore:
                        async with session.get(
                            config['url'],
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            return await resp.json()
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed {config['name']}: {e}")
                    return None
                wait = (2 ** attempt)  # Exponential backoff
                await asyncio.sleep(wait)

    results = await asyncio.gather(
        *[fetch_with_retry(cfg) for cfg in api_configs],
        return_exceptions=True
    )
    return results
```

---

## Common Pitfalls

| Pitfall | What Goes Wrong | Fix |
|---------|-----------------|-----|
| No semaphore | Hammer API, get rate-limited or IP banned | Add `Semaphore(max_concurrent)` |
| No timeout | Request hangs forever, deadlock | Use `timeout=aiohttp.ClientTimeout(total=10)` |
| All-or-nothing gathering | One failure breaks entire ingestion | Use `return_exceptions=True` |
| Sync + async mixed | "RuntimeError: no running event loop" | Use `asyncio.run()` at top level |
| No backpressure | Queue/memory explosion | Pause producer when queue fills |
