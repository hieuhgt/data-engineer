# Base connector for all data sources

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Optional
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

    def fetch(self) -> List[Dict]:
        """Fetch from REST API with retries"""
        import aiohttp
        import asyncio
        from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential

        async def fetch_async():
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self.retries),
                wait=wait_exponential(multiplier=1, min=2, max=10),
            ):
                with attempt:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            self.endpoint,
                            timeout=aiohttp.ClientTimeout(total=self.timeout)
                        ) as response:
                            if response.status != 200:
                                raise ValueError(f"HTTP {response.status}")
                            return await response.json()

        data = asyncio.run(fetch_async())
        if isinstance(data, dict) and 'data' in data:
            return data['data']
        elif isinstance(data, list):
            return data
        else:
            return [data]

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
