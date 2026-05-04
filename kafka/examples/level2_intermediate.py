"""
LEVEL 2: INTERMEDIATE - Advanced Kafka Patterns
Consumer groups, offset management, error handling, monitoring
"""

import json
import time
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from loguru import logger

print("""
╔════════════════════════════════════════════════════════════╗
║  LEVEL 2: INTERMEDIATE - Advanced Patterns                ║
║  Topics: Consumer groups, offsets, monitoring, errors      ║
╚════════════════════════════════════════════════════════════╝
""")

# ============================================================================
# EXAMPLE 1: Consumer Groups with Load Balancing
# ============================================================================

print("\n[EXAMPLE 1] Consumer Groups & Load Balancing\n")

def example_consumer_groups():
    """Distribute messages across multiple consumers."""

    # Simulate 3 consumers in same group
    consumers = []
    for i in range(3):
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=['localhost:9092'],
            group_id='order_processors',  # Same group = load balanced
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=2000
        )
        consumers.append((i, consumer))

    # Process in parallel (would be in separate threads/processes)
    for consumer_id, consumer in consumers:
        print(f"\nConsumer {consumer_id}:")
        for message in consumer:
            order = message.value
            print(f"  ✓ Processing order {order['id']} (partition {message.partition})")

        consumer.close()


# ============================================================================
# EXAMPLE 2: Manual Offset Management
# ============================================================================

print("\n[EXAMPLE 2] Manual Offset Management\n")

def example_manual_offsets():
    """Manually commit offsets after processing."""

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='manual_offset_group',
        enable_auto_commit=False,  # Disable auto-commit
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=3000
    )

    batch = []
    batch_size = 5

    for message in consumer:
        order = message.value
        batch.append(order)

        print(f"Processing order {order['id']} (offset: {message.offset})")

        if len(batch) >= batch_size:
            # Process batch
            print(f"✓ Processed batch of {len(batch)} orders")

            # Commit offset AFTER successful processing
            consumer.commit()
            print(f"✓ Offset committed")

            batch = []

    consumer.close()


# ============================================================================
# EXAMPLE 3: Consumer Lag Monitoring
# ============================================================================

print("\n[EXAMPLE 3] Monitor Consumer Lag\n")

def example_consumer_lag():
    """Track how far behind the consumer is."""

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='lag_monitor',
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )

    print("Topic partitions and lag:\n")

    # Get partition assignment
    partitions = consumer.assignment()

    if not partitions:
        # If not assigned yet, read one message first
        try:
            next(consumer)
        except StopIteration:
            pass
        partitions = consumer.assignment()

    for partition in partitions:
        tp = TopicPartition('orders', partition.partition)

        # Current position (where consumer is)
        current = consumer.position(tp)

        # End position (latest message)
        consumer.seek_to_end(tp)
        end = consumer.position(tp)

        # Calculate lag
        lag = end - current

        print(f"Partition {partition.partition}:")
        print(f"  Current offset: {current}")
        print(f"  End offset: {end}")
        print(f"  Lag: {lag} messages")

    consumer.close()


# ============================================================================
# EXAMPLE 4: Seek to Specific Offset
# ============================================================================

print("\n[EXAMPLE 4] Seek to Specific Offset\n")

def example_seek_offset():
    """Jump to specific position in partition."""

    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        group_id='seek_demo'
    )

    tp = TopicPartition('orders', partition=0)

    # Seek to beginning
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)
    print("✓ Seeked to beginning")

    # Read first 3 messages
    count = 0
    for message in consumer:
        print(f"  Message at offset {message.offset}: {message.value}")
        count += 1
        if count >= 3:
            break

    # Seek to specific offset
    consumer.seek(tp, 10)
    print("\n✓ Seeked to offset 10")

    count = 0
    for message in consumer:
        print(f"  Message at offset {message.offset}: {message.value}")
        count += 1
        if count >= 2:
            break

    consumer.close()


# ============================================================================
# EXAMPLE 5: Producer with Retries
# ============================================================================

print("\n[EXAMPLE 5] Producer with Retries\n")

def example_producer_retries():
    """Reliable producer with retries."""

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',              # Wait for all replicas
        retries=3,               # Retry 3 times
        retry_backoff_ms=100,    # Wait 100ms between retries
        max_in_flight_requests_per_connection=1  # Preserve order
    )

    def on_success(metadata):
        logger.info(f"✓ Sent to partition {metadata.partition} offset {metadata.offset}")

    def on_error(exc):
        logger.error(f"✗ Send failed: {exc}")

    # Send with error handling
    for i in range(5):
        order = {'id': i, 'amount': 100 * (i + 1)}

        future = producer.send('orders', value=order)
        future.add_callback(on_success)
        future.add_errback(on_error)

    producer.flush()
    producer.close()


# ============================================================================
# EXAMPLE 6: Consumer Error Handling & Dead Letter Queue
# ============================================================================

print("\n[EXAMPLE 6] Dead Letter Queue Pattern\n")

def example_dlq():
    """Route problematic messages to DLQ."""

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='dlq_processor',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=3000
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        try:
            order = message.value

            # Validate order
            if not order.get('id'):
                raise ValueError("Missing order ID")

            # Process
            print(f"✓ Processing order {order['id']}")
            consumer.commit()

        except ValueError as e:
            # Business logic error - send to DLQ
            logger.warning(f"Invalid order: {e}")

            dlq_producer.send('orders_dlq', value={
                'error': str(e),
                'original_message': message.value,
                'timestamp': time.time()
            })

            consumer.commit()  # Skip to next message

        except Exception as e:
            # Unexpected error - don't commit, will retry
            logger.error(f"Unexpected error: {e}")

    consumer.close()
    dlq_producer.close()


# ============================================================================
# EXAMPLE 7: Batch Processing for Performance
# ============================================================================

print("\n[EXAMPLE 7] Batch Processing\n")

def example_batch_processing():
    """Process messages in batches for better throughput."""

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='batch_processor',
        max_poll_records=100,  # Fetch 100 at a time
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000
    )

    batch = []
    batch_size = 10

    for message in consumer:
        batch.append(message.value)

        if len(batch) >= batch_size:
            # Process entire batch at once
            total_amount = sum(b.get('amount', 0) for b in batch)
            print(f"✓ Processed batch of {len(batch)} orders. Total: ${total_amount}")

            batch = []

    consumer.close()


# ============================================================================
# EXAMPLE 8: Monitor Multiple Metrics
# ============================================================================

print("\n[EXAMPLE 8] Performance Monitoring\n")

def example_monitoring():
    """Monitor consumer health metrics."""

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='monitor',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000
    )

    metrics = {
        'messages_processed': 0,
        'messages_failed': 0,
        'total_amount': 0,
        'start_time': time.time()
    }

    for message in consumer:
        try:
            order = message.value
            metrics['messages_processed'] += 1
            metrics['total_amount'] += order.get('amount', 0)

        except Exception as e:
            metrics['messages_failed'] += 1

    # Print metrics
    elapsed = time.time() - metrics['start_time']

    print(f"\n=== METRICS ===")
    print(f"Messages processed: {metrics['messages_processed']}")
    print(f"Messages failed: {metrics['messages_failed']}")
    print(f"Total amount: ${metrics['total_amount']:.2f}")
    print(f"Processing time: {elapsed:.2f}s")
    print(f"Throughput: {metrics['messages_processed']/elapsed:.2f} msg/sec")

    consumer.close()


# ============================================================================
# RUN EXAMPLES
# ============================================================================

if __name__ == "__main__":
    examples = {
        '1': ('Consumer Groups', example_consumer_groups),
        '2': ('Manual Offsets', example_manual_offsets),
        '3': ('Consumer Lag', example_consumer_lag),
        '4': ('Seek Offset', example_seek_offset),
        '5': ('Producer Retries', example_producer_retries),
        '6': ('Dead Letter Queue', example_dlq),
        '7': ('Batch Processing', example_batch_processing),
        '8': ('Monitoring', example_monitoring),
    }

    print("\nAvailable Examples:")
    for key, (name, _) in examples.items():
        print(f"  {key}. {name}")

    choice = input("\nChoose example (1-8): ").strip()

    name, func = examples.get(choice, (None, None))
    if func:
        try:
            func()
        except Exception as e:
            logger.error(f"Error: {e}")
    else:
        print("Invalid choice")
