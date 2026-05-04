import json
import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class LineageTracker:
    """Record data lineage for every pipeline step."""

    def __init__(self):
        self._records: List[Dict[str, Any]] = []

    def record(
        self,
        step: str,
        source: str,
        target: str,
        rows: int,
        status: str,
        errors: List[str] | None = None,
    ):
        entry = {
            "step": step,
            "source": source,
            "target": target,
            "timestamp": datetime.utcnow().isoformat(),
            "rows_processed": rows,
            "status": status,
            "errors": errors or [],
        }
        self._records.append(entry)
        logger.info(f"lineage | step={step} source={source} target={target} rows={rows} status={status}")

    def all_records(self) -> List[Dict]:
        return list(self._records)

    def to_json(self) -> str:
        return json.dumps(self._records, indent=2)

    def save(self, path: str):
        with open(path, "w") as f:
            f.write(self.to_json())
        logger.info(f"Lineage saved to {path}")
