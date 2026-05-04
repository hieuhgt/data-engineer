import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class FileConnector:
    """Reads Parquet / CSV / JSON from local disk or S3 (via s3fs)."""

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.bucket = config.get("bucket")
        self.prefix = config.get("prefix", "")
        self.fmt = config.get("format", "parquet")
        self.metrics: Dict[str, Any] = {"rows_processed": 0, "status": "pending"}

    def _build_path(self) -> str:
        if self.bucket:
            return f"s3://{self.bucket}/{self.prefix}"
        return self.prefix

    def fetch(self) -> List[Dict]:
        import pandas as pd

        path = self._build_path()
        logger.info(f"[{self.name}] Reading {self.fmt} from {path}")

        readers = {
            "parquet": lambda p: pd.read_parquet(p),
            "csv": lambda p: pd.read_csv(p),
            "json": lambda p: pd.read_json(p),
        }
        if self.fmt not in readers:
            raise ValueError(f"Unsupported format: {self.fmt}")

        df = readers[self.fmt](path)
        return df.to_dict("records")

    def validate_schema(self, data: List[Dict]) -> bool:
        expected = self.config.get("schema", {})
        if not data or not expected:
            return True
        missing = [f for f in expected if f not in data[0]]
        if missing:
            logger.error(f"[{self.name}] Missing fields: {missing}")
            return False
        return True

    def execute(self) -> tuple[List[Dict], Dict]:
        from datetime import datetime
        start = datetime.now()
        try:
            data = self.fetch()
            self.validate_schema(data)
            self.metrics = {
                "rows_processed": len(data),
                "status": "success",
                "duration_sec": (datetime.now() - start).total_seconds(),
            }
            lineage = {"source": self.name, "rows": len(data), "timestamp": start.isoformat()}
            return data, lineage
        except Exception as e:
            self.metrics["status"] = "failed"
            logger.error(f"[{self.name}] Failed: {e}", exc_info=True)
            raise
