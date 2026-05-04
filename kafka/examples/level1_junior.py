"""
LEVEL 1: JUNIOR - Kafka Fundamentals
Complete working examples for beginners
"""

import json
import time
from kafka import KafkaProducer, KafkaConsumer

print("""
╔════════════════════════════════════════════════════════════╗
║  LEVEL 1: JUNIOR - Kafka Fundamentals                    ║
║  Topics: Basic producer, consumer, simple examples        ║
╚════════════════════════════════════════════════════════════╝
""")

# ============================================================================
# EXAMPLE 1: Basic Producer
# ============================================================================

print("\n[EXAMPLE 1] Basic Producer\n")

def example_basic_producer():
    """Send messages to Kafka."""

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Send 5 simple messages
    for i in range(5):
        message = {
            'id': i,
            'message': f'Hello Kafka {i}',
            'timestamp': time.time()
        }

        # Send to topic (creates topic if doesn't exist)
        producer.send('my_topic', value=message)
        print(f"✓ Sent: {message}")

    # Ensure all messages are sent
    producer.flush()

    # Close connection
    producer.close()
    print("\n✓ Producer closed")


# ============================================================================
# EXAMPLE 2: Basic Consumer
# ============================================================================

print("\n[EXAMPLE 2] Basic Consumer\n")

def example_basic_consumer():
    """Read messages from Kafka."""

    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # Start from beginning
        group_id='my_group',
        consumer_timeout_ms=5000  # Stop after 5 seconds of no messages
    )

    print("Waiting for messages... (timeout after 5 seconds)")

    for message in consumer:
        print(f"✓ Received: {message.value}")

    consumer.close()
    print("\n✓ Consumer closed")


# ============================================================================
# EXAMPLE 3: Multiple Consumers (Consumer Group)
# ============================================================================

print("\n[EXAMPLE 3] Consumer Group (Load Balancing)\n")

def example_consumer_group():
    """Multiple consumers sharing work."""

    # Consumer 1 and 2 have same group_id = they split the messages
    for consumer_id in range(2):
        consumer = KafkaConsumer(
            'my_topic',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='load_balanced_group',  # Same group!
            consumer_timeout_ms=3000
        )

        print(f"\nConsumer {consumer_id + 1}:")
        for message in consumer:
            print(f"  ✓ Received: {message.value['message']}")

        consumer.close()


# ============================================================================
# EXAMPLE 4: Producer with Callbacks
# ============================================================================

print("\n[EXAMPLE 4] Producer with Success/Error Callbacks\n")

def example_producer_callbacks():
    """Handle send success and errors."""

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    def on_send_success(record_metadata):
        print(f"✓ SUCCESS")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partition: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")

    def on_send_error(exc):
        print(f"✗ ERROR: {exc}")

    # Send with callback
    for i in range(3):
        future = producer.send('my_topic', value={'id': i})
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    producer.flush()
    producer.close()


# ============================================================================
# EXAMPLE 5: Offset Management
# ============================================================================

print("\n[EXAMPLE 5] Offset Management\n")

def example_offset_management():
    """Track message position."""

    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='offset_demo',
        enable_auto_commit=False  # Manual offset management
    )

    message_count = 0

    for message in consumer:
        print(f"Message: {message.value['message']}")
        print(f"  Offset: {message.offset}")
        print(f"  Partition: {message.partition}")

        message_count += 1

        # Manually commit after processing
        if message_count % 2 == 0:
            consumer.commit()
            print(f"  ✓ Offset committed")

        if message_count >= 4:
            break

    consumer.close()


# ============================================================================
# EXAMPLE 6: Error Handling
# ============================================================================

print("\n[EXAMPLE 6] Error Handling\n")

def example_error_handling():
    """Handle errors gracefully."""

    try:
        consumer = KafkaConsumer(
            'my_topic',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='error_demo',
            consumer_timeout_ms=3000
        )

        for message in consumer:
            try:
                # Process message
                data = message.value

                # Simulate error on certain messages
                if data['id'] == 2:
                    raise ValueError("Invalid data at id=2")

                print(f"✓ Processed: {data}")

            except ValueError as e:
                print(f"⚠ Data error: {e} - Skipping message")
                # Skip and continue

            except Exception as e:
                print(f"✗ Unexpected error: {e}")
                # Log and continue

        consumer.close()

    except Exception as e:
        print(f"✗ Consumer error: {e}")


# ============================================================================
# EXAMPLE 7: Simple End-to-End Pipeline
# ============================================================================

print("\n[EXAMPLE 7] Simple End-to-End Pipeline\n")

def example_end_to_end():
    """Complete pipeline: produce → consume."""

    # Producer: Send user events
    print("--- PRODUCER: Sending user events ---")

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    user_events = [
        {'user_id': 1, 'action': 'login', 'timestamp': time.time()},
        {'user_id': 1, 'action': 'view_product', 'product_id': 123},
        {'user_id': 2, 'action': 'purchase', 'product_id': 123, 'amount': 99.99},
        {'user_id': 1, 'action': 'logout'},
    ]

    for event in user_events:
        producer.send('user_events', value=event)
        print(f"✓ Produced: {event}")

    producer.flush()
    producer.close()

    # Consumer: Process events
    print("\n--- CONSUMER: Processing events ---")

    consumer = KafkaConsumer(
        'user_events',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='event_processor',
        consumer_timeout_ms=3000
    )

    event_count = 0

    for message in consumer:
        event = message.value
        action = event.get('action', 'unknown')

        if action == 'login':
            print(f"✓ User {event['user_id']} logged in")
        elif action == 'purchase':
            print(f"✓ User {event['user_id']} purchased product {event['product_id']} for ${event['amount']}")
        else:
            print(f"✓ User {event['user_id']} performed: {action}")

        event_count += 1

    consumer.close()
    print(f"\n✓ Processed {event_count} events")


# ============================================================================
# RUN EXAMPLES
# ============================================================================

if __name__ == "__main__":
    import sys

    examples = {
        '1': ('Basic Producer', example_basic_producer),
        '2': ('Basic Consumer', example_basic_consumer),
        '3': ('Consumer Group', example_consumer_group),
        '4': ('Producer Callbacks', example_producer_callbacks),
        '5': ('Offset Management', example_offset_management),
        '6': ('Error Handling', example_error_handling),
        '7': ('End-to-End Pipeline', example_end_to_end),
    }

    print("\nAvailable Examples:")
    for key, (name, _) in examples.items():
        print(f"  {key}. {name}")

    choice = input("\nChoose example (1-7) or 'all' for all examples: ").strip()

    if choice.lower() == 'all':
        for key, (name, func) in examples.items():
            try:
                print(f"\n{'='*60}")
                print(f"Running: {name}")
                print(f"{'='*60}")
                func()
            except Exception as e:
                print(f"✗ Error: {e}")
    else:
        name, func = examples.get(choice, (None, None))
        if func:
            try:
                func()
            except Exception as e:
                print(f"✗ Error: {e}")
        else:
            print("Invalid choice")

    print("\n✓ Examples complete!")
