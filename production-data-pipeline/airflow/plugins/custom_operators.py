import logging
from typing import Any, Dict, List

from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """Run quality gates and fail the task if critical gates don't pass."""

    template_fields = ("source_table", "min_rows")

    def __init__(self, source_table: str, min_rows: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.source_table = source_table
        self.min_rows = min_rows

    def execute(self, context: Dict[str, Any]):
        from src.validation.quality_gates import NullCheckGate, QualityGateChecker

        ti = context["task_instance"]
        data: List[Dict] = ti.xcom_pull(key="ingested_data") or []

        logger.info(f"Running quality check on {len(data)} rows from {self.source_table}")

        if len(data) < self.min_rows:
            raise ValueError(f"Too few rows: {len(data)} < min {self.min_rows}")

        gates = [NullCheckGate(required_fields=["user_id"])]
        checker = QualityGateChecker(gates)
        _, results = checker.validate_all(data)

        score = checker.get_quality_score()
        ti.xcom_push(key="quality_score", value=score)

        logger.info(f"Quality score: {score:.1%}")
        return {"quality_score": score, "gates_run": len(results)}


class SourceIngestionOperator(BaseOperator):
    """Ingest one named source from pipeline_config.yaml."""

    template_fields = ("source_name",)

    def __init__(self, source_name: str, config_path: str, **kwargs):
        super().__init__(**kwargs)
        self.source_name = source_name
        self.config_path = config_path

    def execute(self, context: Dict[str, Any]):
        import yaml

        with open(self.config_path) as f:
            cfg = yaml.safe_load(f)

        source_cfg = next(
            (s for s in cfg["sources"] if s["name"] == self.source_name), None
        )
        if not source_cfg:
            raise ValueError(f"Source '{self.source_name}' not in config")

        src_type = source_cfg.get("type")

        if src_type == "rest_api":
            from src.ingestion.api_connector import APIConnector
            connector = APIConnector(self.source_name, source_cfg)
        elif src_type in ("s3", "file"):
            from src.ingestion.file_connector import FileConnector
            connector = FileConnector(self.source_name, source_cfg)
        elif src_type == "kafka":
            from src.ingestion.kafka_connector import KafkaConnector
            connector = KafkaConnector(self.source_name, source_cfg)
        else:
            raise ValueError(f"Unknown source type: {src_type}")

        data, lineage = connector.execute()
        context["task_instance"].xcom_push(key="ingested_data", value=data)
        context["task_instance"].xcom_push(key="lineage", value=lineage)

        logger.info(f"Ingested {len(data)} rows from {self.source_name}")
        return {"rows": len(data), "source": self.source_name}
