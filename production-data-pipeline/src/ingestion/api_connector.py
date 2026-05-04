import asyncio
import logging
from typing import Dict, List, Any

import aiohttp

from .base_connector import DataConnector
from .decorators import retry_with_backoff

logger = logging.getLogger(__name__)


class APIConnector(DataConnector):
    """Fetches data from REST APIs with semaphore, retries, and rate limiting."""

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.endpoint = config["endpoint"]
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.retries = config.get("retries", 3)
        self.timeout = config.get("timeout", 30)
        self.max_concurrent = config.get("max_concurrent", 5)
        self._semaphore: asyncio.Semaphore | None = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)
        return self._semaphore

    def fetch(self) -> List[Dict]:
        return asyncio.run(self._fetch_async())

    async def _fetch_async(self) -> List[Dict]:
        @retry_with_backoff(max_attempts=self.retries)
        async def _get():
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.endpoint,
                        headers=self.headers,
                        params=self.params,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as resp:
                        if resp.status == 429:
                            raise RuntimeError(f"Rate limited by {self.endpoint}")
                        resp.raise_for_status()
                        return await resp.json()

        raw = await _get()
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            # Unwrap common envelope patterns: {"data": [...]} or {"results": [...]}
            for key in ("data", "results", "items", "records"):
                if key in raw and isinstance(raw[key], list):
                    return raw[key]
        return [raw]

    def validate_schema(self, data: List[Dict]) -> bool:
        required = self.config.get("validation", {}).get("required_fields", [])
        if not data or not required:
            return True
        missing = [f for f in required if f not in data[0]]
        if missing:
            logger.error(f"[{self.name}] Missing required fields: {missing}")
            return False
        return True
