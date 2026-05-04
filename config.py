"""Configuration management for data pipeline."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SparkConfig:
    """Spark configuration."""
    app_name: str = "data_pipeline"
    master: str = os.getenv("SPARK_MASTER", "local[4]")
    memory: str = "2g"
    shuffle_partitions: int = 200
    sql_adaptive_enabled: bool = True


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: list = None
    group_id: str = os.getenv("KAFKA_GROUP_ID", "data-pipeline-group")
    topic: str = os.getenv("KAFKA_TOPIC", "events")
    auto_offset_reset: str = "earliest"

    def __post_init__(self):
        if self.bootstrap_servers is None:
            servers = os.getenv("KAFKA_BROKERS", "localhost:9092")
            self.bootstrap_servers = servers.split(",")


@dataclass
class AppConfig:
    """Application configuration."""
    spark: SparkConfig = None
    kafka: KafkaConfig = None
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"

    def __post_init__(self):
        if self.spark is None:
            self.spark = SparkConfig()
        if self.kafka is None:
            self.kafka = KafkaConfig()


def get_config() -> AppConfig:
    """Get application configuration."""
    return AppConfig()
