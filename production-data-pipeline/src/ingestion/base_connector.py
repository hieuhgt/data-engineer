# Base connector for all data sources

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Iterator
import json

logger = logging.getLogger(__name__)

class DataConnector(ABC):
    """Base class for all data connectors"""

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.metrics = {
            'rows_processed': 0,
            'errors': 0,
            'duration_sec': 0,
            'last_run': None,
            'status': 'pending'
        }

    @abstractmethod
    def fetch(self) -> List[Dict]:
        """Fetch data from source"""
        pass

    @abstractmethod
    def validate_schema(self, data: List[Dict]) -> bool:
        """Validate schema matches expected"""
        pass

    def log_lineage(self, data: List[Dict]) -> Dict:
        """Track data lineage"""
        return {
            'source': self.name,
            'timestamp': datetime.now().isoformat(),
            'rows': len(data),
            'status': 'success',
            'schema': list(data[0].keys()) if data else []
        }

    def execute(self) -> tuple[List[Dict], Dict]:
        """Execute fetch with error handling and metrics"""
        start_time = datetime.now()

        try:
            logger.info(f"Starting ingestion from {self.name}")

            # Fetch data
            data = self.fetch()
            logger.info(f"Fetched {len(data)} rows from {self.name}")

            # Validate schema
            if not self.validate_schema(data):
                raise ValueError(f"Schema validation failed for {self.name}")

            # Record metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.update({
                'rows_processed': len(data),
                'duration_sec': duration,
                'last_run': datetime.now().isoformat(),
                'status': 'success',
                'errors': 0
            })

            # Log lineage
            lineage = self.log_lineage(data)

            logger.info(f"Successfully ingested from {self.name}: {len(data)} rows in {duration:.1f}s")

            return data, lineage

        except Exception as e:
            logger.error(f"Error ingesting from {self.name}: {e}", exc_info=True)
            self.metrics.update({
                'status': 'failed',
                'errors': 1,
                'duration_sec': (datetime.now() - start_time).total_seconds()
            })
            raise

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class APIConnector(DataConnector):
    """Base class for REST API connectors"""

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.endpoint = config.get('endpoint')
        self.retries = config.get('retries', 3)
        self.timeout = config.get('timeout', 30)
        self.page_size = config.get('page_size', 1000)

    def _parse_response(self, data) -> List[Dict]:
        data_key = self.config.get('data_key')
        if data_key and isinstance(data, dict) and data_key in data:
            return data[data_key]
        elif isinstance(data, dict) and 'data' in data:
            return data['data']
        elif isinstance(data, list):
            return data
        else:
            return [data]

    def fetch(self) -> List[Dict]:
        """Fetch all data (single request). Use fetch_pages() for large datasets."""
        import aiohttp
        import asyncio

        async def fetch_async():
            for attempt in range(self.retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.endpoint,
                            params=self.config.get('params', {}),
                            timeout=aiohttp.ClientTimeout(total=self.timeout),
                        ) as response:
                            if response.status != 200:
                                raise ValueError(f"HTTP {response.status}")
                            return await response.json()
                except Exception as e:
                    if attempt == self.retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1}/{self.retries}: {e}")

        return self._parse_response(asyncio.run(fetch_async()))

    def fetch_pages(self) -> Iterator[List[Dict]]:
        """
        Yield one page at a time using limit/skip params (offset-based pagination).
        Avoids loading the full dataset into memory — use for large sources.
        Stops when the API returns fewer records than page_size (last page).
        """
        import aiohttp
        import asyncio

        async def fetch_page(skip: int) -> List[Dict]:
            params = {**self.config.get('params', {}), 'limit': self.page_size, 'skip': skip}
            for attempt in range(self.retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.endpoint,
                            params=params,
                            timeout=aiohttp.ClientTimeout(total=self.timeout),
                        ) as response:
                            if response.status != 200:
                                raise ValueError(f"HTTP {response.status}")
                            return self._parse_response(await response.json())
                except Exception as e:
                    if attempt == self.retries - 1:
                        raise
                    logger.warning(f"[{self.name}] skip={skip} retry {attempt + 1}: {e}")

        skip = 0
        while True:
            data = asyncio.run(fetch_page(skip))
            if not data:
                break
            yield data
            if len(data) < self.page_size:
                break  # partial page = last page
            skip += self.page_size

    def validate_schema(self, data: List[Dict]) -> bool:
        """Validate API response schema"""
        if not data:
            logger.warning(f"Empty data from {self.name}")
            return True  # Allow empty

        expected_schema = self.config.get('schema', {})
        if not expected_schema:
            return True  # No schema validation configured

        first_record = data[0]
        required_fields = self.config.get('validation', {}).get('required_fields', [])

        # Check required fields
        for field in required_fields:
            if field not in first_record:
                logger.error(f"Missing required field: {field}")
                return False

        return True


class FileConnector(DataConnector):
    """Base class for file-based connectors (S3, GCS, local)"""

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.bucket = config.get('bucket')
        self.prefix = config.get('prefix')
        self.format = config.get('format', 'parquet')

    def fetch(self) -> List[Dict]:
        """Fetch from S3/GCS/local file"""
        import pandas as pd

        if self.bucket:
            # S3 file
            path = f"s3://{self.bucket}/{self.prefix}"
        else:
            # Local file
            path = self.prefix

        logger.info(f"Reading {self.format} from {path}")

        try:
            if self.format == 'parquet':
                df = pd.read_parquet(path)
            elif self.format == 'csv':
                df = pd.read_csv(path)
            elif self.format == 'json':
                df = pd.read_json(path)
            else:
                raise ValueError(f"Unsupported format: {self.format}")

            return df.to_dict('records')

        except Exception as e:
            logger.error(f"Error reading file from {path}: {e}")
            raise

    def validate_schema(self, data: List[Dict]) -> bool:
        """Validate file schema"""
        if not data:
            return True

        expected_schema = self.config.get('schema', {})
        if not expected_schema:
            return True

        first_record = data[0]
        for field in expected_schema:
            if field not in first_record:
                logger.error(f"Missing field: {field}")
                return False

        return True


class DatabaseConnector(DataConnector):
    """Base class for database connectors"""

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.connection_string = config.get('connection_string')
        self.query = config.get('query')

    def fetch(self) -> List[Dict]:
        """Fetch from database"""
        import sqlalchemy as sa

        try:
            engine = sa.create_engine(self.connection_string)
            with engine.connect() as conn:
                result = conn.execute(sa.text(self.query))
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in result]
            return data

        except Exception as e:
            logger.error(f"Error querying database: {e}")
            raise

    def validate_schema(self, data: List[Dict]) -> bool:
        """Validate database schema"""
        if not data:
            return True

        expected_schema = self.config.get('schema', {})
        if not expected_schema:
            return True

        first_record = data[0]
        for field in expected_schema:
            if field not in first_record:
                logger.error(f"Missing field: {field}")
                return False

        return True
