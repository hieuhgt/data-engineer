import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DataProfiler:
    """Compute simple statistics over a batch of records."""

    def profile(self, data: List[Dict]) -> Dict[str, Any]:
        if not data:
            return {}

        columns = list(data[0].keys())
        report: Dict[str, Any] = {"total_rows": len(data), "columns": {}}

        for col in columns:
            values = [r.get(col) for r in data]
            null_count = sum(1 for v in values if v is None)
            numeric = [v for v in values if isinstance(v, (int, float)) and v is not None]

            col_stats: Dict[str, Any] = {
                "null_count": null_count,
                "null_rate": round(null_count / len(data), 4),
                "unique_count": len(set(str(v) for v in values if v is not None)),
            }

            if numeric:
                col_stats["min"] = min(numeric)
                col_stats["max"] = max(numeric)
                col_stats["mean"] = round(sum(numeric) / len(numeric), 4)

            report["columns"][col] = col_stats

        logger.info(f"Profiled {len(data)} rows, {len(columns)} columns")
        return report
