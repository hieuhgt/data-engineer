import asyncio
import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def retry_with_backoff(max_attempts=3, base_delay=1, max_delay=60, exceptions=(Exception,)):
    """Retry async function with exponential backoff."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


def rate_limit(calls_per_second=1):
    """Limit how fast a function can be called."""
    min_interval = 1.0 / calls_per_second
    last_called = {}

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            key = func.__name__
            if key in last_called:
                elapsed = now - last_called[key]
                if elapsed < min_interval:
                    await asyncio.sleep(min_interval - elapsed)
            last_called[key] = time.time()
            return await func(*args, **kwargs)
        return wrapper
    return decorator
