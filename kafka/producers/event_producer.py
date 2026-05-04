"""Kafka producer - Send events to Kafka."""
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger
from config import get_config


class EventProducer:
    """Produces events to Kafka."""

    def __init__(self):
        self.config = get_config()
        self.producer = self._initialize_producer()
        self.logger = logger

    def _initialize_producer(self) -> KafkaProducer:
        """Initialize Kafka producer with error handling."""
        try:
            return KafkaProducer(
                bootstrap_servers=self.config.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize producer: {e}")
            raise

    def send_event(self, event: dict, topic: str = None, key: str = None) -> bool:
        """Send single event to Kafka with callback."""
        topic = topic or self.config.kafka.topic

        try:
            future = self.producer.send(
                topic,
                value=event,
                key=key.encode() if key else None
            )
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            self.logger.debug(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")
            return True
        except KafkaError as e:
            self.logger.error(f"Failed to send event: {e}")
            return False

    def send_batch(self, events: list, topic: str = None) -> int:
        """Send multiple events efficiently."""
        topic = topic or self.config.kafka.topic
        sent_count = 0

        for event in events:
            try:
                self.producer.send(topic, value=event)
                sent_count += 1
            except KafkaError as e:
                self.logger.error(f"Failed to send event: {e}")

        # Ensure all messages are sent
        self.producer.flush()
        self.logger.info(f"Sent {sent_count}/{len(events)} events")
        return sent_count

    def close(self) -> None:
        """Close producer connection."""
        self.producer.close()
        self.logger.info("Producer closed")


# Example usage
if __name__ == "__main__":
    producer = EventProducer()

    # Send single event
    event = {
        "user_id": "user_123",
        "event_type": "click",
        "value": 42.5,
        "timestamp": time.time(),
    }
    producer.send_event(event, key=event["user_id"])

    # Send batch
    events = [
        {"user_id": f"user_{i}", "event_type": "purchase", "value": i * 10, "timestamp": time.time()}
        for i in range(100)
    ]
    producer.send_batch(events)

    producer.close()
