"""Kafka consumer - Consume events from Kafka."""
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger
from config import get_config


class EventConsumer:
    """Consumes events from Kafka."""

    def __init__(self, group_id: str = None):
        self.config = get_config()
        self.group_id = group_id or self.config.kafka.group_id
        self.consumer = self._initialize_consumer()
        self.logger = logger

    def _initialize_consumer(self) -> KafkaConsumer:
        """Initialize Kafka consumer with best practices."""
        try:
            return KafkaConsumer(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.config.kafka.auto_offset_reset,
                enable_auto_commit=True,
                max_poll_records=500,  # Fetch more records per poll
                session_timeout_ms=30000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize consumer: {e}")
            raise

    def consume(self, topic: str, timeout_ms: int = 5000, max_messages: int = None):
        """Consume messages from topic."""
        self.consumer.subscribe([topic])
        self.logger.info(f"Subscribed to topic: {topic}")

        message_count = 0
        try:
            while True:
                messages = self.consumer.poll(timeout_ms=timeout_ms)

                if not messages:
                    self.logger.debug("No messages received")
                    continue

                for topic_partition, records in messages.items():
                    for message in records:
                        try:
                            yield message.value
                            message_count += 1

                            if max_messages and message_count >= max_messages:
                                return
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            self.logger.info("Consumer interrupted")
        finally:
            self.close()

    def close(self) -> None:
        """Close consumer connection."""
        self.consumer.close()
        self.logger.info("Consumer closed")


# Example usage
if __name__ == "__main__":
    consumer = EventConsumer(group_id="example-group")

    # Consume 10 messages
    for i, message in enumerate(consumer.consume("events", max_messages=10)):
        print(f"Message {i}: {message}")
