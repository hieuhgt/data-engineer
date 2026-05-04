import json
import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class KafkaConnector:
    """Reads a batch of messages from a Kafka topic."""

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.topic = config["topic"]
        self.bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")
        self.consumer_group = config.get("consumer_group", "pipeline")
        self.max_messages = config.get("max_messages", 10_000)
        self.timeout_sec = config.get("timeout_sec", 30)
        self.metrics: Dict[str, Any] = {"rows_processed": 0, "status": "pending"}

    def fetch(self) -> List[Dict]:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit after processing
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=self.timeout_sec * 1000,
        )

        records: List[Dict] = []
        try:
            for msg in consumer:
                records.append(msg.value)
                if len(records) >= self.max_messages:
                    break
        finally:
            consumer.close()

        logger.info(f"[{self.name}] Consumed {len(records)} messages from {self.topic}")
        return records

    def validate_schema(self, data: List[Dict]) -> bool:
        required = self.config.get("validation", {}).get("required_fields", [])
        if not data or not required:
            return True
        missing = [f for f in required if f not in data[0]]
        if missing:
            logger.error(f"[{self.name}] Missing fields: {missing}")
            return False
        return True

    def execute(self) -> tuple[List[Dict], Dict]:
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
