import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _PROMETHEUS = True
except ImportError:
    _PROMETHEUS = False
    logger.warning("prometheus_client not installed – metrics will only be logged")


class PipelineMetrics:
    """Record and expose pipeline health metrics."""

    def __init__(self, expose_port: Optional[int] = None):
        self._store: Dict[str, Any] = {}

        if _PROMETHEUS and expose_port:
            start_http_server(expose_port)

        if _PROMETHEUS:
            self.rows_processed = Counter("pipeline_rows_processed_total", "Total rows", ["source"])
            self.duration = Histogram("pipeline_duration_seconds", "Task duration", ["task"])
            self.quality_rate = Gauge("pipeline_quality_pass_rate", "Quality pass rate", ["gate"])
            self.freshness = Gauge("pipeline_freshness_hours", "Hours since last load", ["table"])

    def record(self, name: str, value: float, tags: Optional[Dict] = None):
        self._store[name] = {"value": value, "timestamp": datetime.now().isoformat(), "tags": tags or {}}
        logger.info(f"metric | {name}={value} {tags or ''}")

        if _PROMETHEUS:
            tag_values = list((tags or {}).values())
            if "rows" in name:
                self.rows_processed.labels(*tag_values).inc(value)
            elif "quality" in name:
                self.quality_rate.labels(*tag_values).set(value)
            elif "freshness" in name:
                self.freshness.labels(*tag_values).set(value)

    def timer(self, task_name: str):
        """Context manager that records task duration."""
        return _Timer(self, task_name)

    def snapshot(self) -> Dict:
        return dict(self._store)


class _Timer:
    def __init__(self, metrics: PipelineMetrics, task: str):
        self._metrics = metrics
        self._task = task
        self._start: float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        elapsed = time.perf_counter() - self._start
        self._metrics.record(f"{self._task}.duration_sec", elapsed, {"task": self._task})
