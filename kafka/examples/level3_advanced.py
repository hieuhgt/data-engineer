"""
LEVEL 3: ADVANCED - Production Patterns
Idempotence, transactions, streaming, exactly-once semantics
"""

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
from loguru import logger
from dataclasses import dataclass

print("""
╔════════════════════════════════════════════════════════════╗
║  LEVEL 3: ADVANCED - Production Patterns                  ║
║  Topics: Idempotence, transactions, streaming              ║
╚════════════════════════════════════════════════════════════╝
""")

# ============================================================================
# EXAMPLE 1: Idempotent Producer (Exactly-Once Semantics)
# ============================================================================

print("\n[EXAMPLE 1] Idempotent Producer\n")

def example_idempotent_producer():
    """
    Idempotent producer ensures each message sent exactly once,
    even if there are retries.
    """

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        enable_idempotence=True,           # Enable idempotence
        acks='all',
        retries=2147483647,                # Max retries
        max_in_flight_requests_per_connection=5
    )

    print("Sending messages with idempotent producer...")

    for i in range(5):
        payment = {
            'transaction_id': f'txn_{i:03d}',
            'amount': 100.0 + i * 10,
            'timestamp': time.time()
        }

        producer.send('payments', value=payment)
        logger.info(f"✓ Sent payment: {payment['transaction_id']}")

    producer.flush()
    producer.close()

    # Even if producer retries, Kafka deduplicates internally
    print("\n✓ All messages sent exactly once (even with retries)")


# ============================================================================
# EXAMPLE 2: Transactions (Atomic Multi-Write)
# ============================================================================

print("\n[EXAMPLE 2] Transactional Writes\n")

def example_transactions():
    """
    Transactions ensure all-or-nothing behavior across multiple topics.
    """

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        transactional_id='payment_processor_1',  # Unique ID
        acks='all'
    )

    print("Processing order with transaction...\n")

    try:
        producer.begin_transaction()

        order = {'order_id': 123, 'amount': 99.99}
        payment = {'order_id': 123, 'amount': 99.99, 'status': 'completed'}

        # Send both messages
        producer.send('orders', value=order)
        logger.info("✓ Order message sent")

        producer.send('payments', value=payment)
        logger.info("✓ Payment message sent")

        # Commit transaction (both messages or neither)
        producer.commit_transaction()
        logger.info("✓ Transaction committed - both messages persisted")

    except Exception as e:
        producer.abort_transaction()
        logger.error(f"✗ Transaction aborted: {e}")

    finally:
        producer.close()


# ============================================================================
# EXAMPLE 3: Exactly-Once Consumer (Offset Sync)
# ============================================================================

print("\n[EXAMPLE 3] Exactly-Once Consumer Processing\n")

def example_exactly_once_consumer():
    """
    Process each message exactly once by syncing offset with processing.
    """

    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['localhost:9092'],
        group_id='exactly_once_processor',
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=3000
    )

    # Simulated database
    processed_orders = set()

    for message in consumer:
        order = message.value
        order_id = order['id']

        try:
            # Check if already processed (idempotency check)
            if order_id in processed_orders:
                logger.info(f"⚠ Order {order_id} already processed, skipping")
                # Still commit to move forward
                consumer.commit()
                continue

            # Process order
            logger.info(f"✓ Processing order {order_id}")
            process_order(order)

            # Mark as processed
            processed_orders.add(order_id)

            # Commit offset AFTER processing
            consumer.commit()

        except Exception as e:
            logger.error(f"✗ Error processing {order_id}: {e}")
            # Don't commit = will retry

    consumer.close()


# ============================================================================
# EXAMPLE 4: Stream Processing (Windowing)
# ============================================================================

print("\n[EXAMPLE 4] Stream Processing with Windowing\n")

def example_stream_processing():
    """
    Process streaming data in time windows.
    """

    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=['localhost:9092'],
        group_id='stream_processor',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000
    )

    window_duration = 10  # 10 seconds
    window_data = {'start_time': time.time(), 'transactions': [], 'total': 0}

    print(f"Processing transactions in {window_duration}s windows...\n")

    for message in consumer:
        tx = message.value
        current_time = time.time()

        # Check if we're still in current window
        if current_time - window_data['start_time'] >= window_duration:
            # Close and emit previous window
            if window_data['transactions']:
                print(f"✓ Window closed")
                print(f"  Transactions: {len(window_data['transactions'])}")
                print(f"  Total: ${window_data['total']:.2f}\n")

            # Start new window
            window_data = {
                'start_time': current_time,
                'transactions': [],
                'total': 0
            }

        # Add to current window
        window_data['transactions'].append(tx)
        window_data['total'] += tx.get('amount', 0)

    consumer.close()


# ============================================================================
# EXAMPLE 5: Dead Letter Queue with Retry
# ============================================================================

print("\n[EXAMPLE 5] Dead Letter Queue with Exponential Backoff\n")

@dataclass
class RetryMessage:
    """Message that failed and needs retry."""
    original_message: dict
    error: str
    retry_count: int
    next_retry_time: float


def example_dlq_with_retry():
    """
    Route failed messages to DLQ and retry with exponential backoff.
    """

    consumer = KafkaConsumer(
        'tasks',
        bootstrap_servers=['localhost:9092'],
        group_id='task_processor',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=3000
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    max_retries = 3

    for message in consumer:
        task = message.value
        task_id = task.get('id')
        retry_count = task.get('retry_count', 0)

        try:
            # Process task
            if task_id % 3 == 0:  # Simulate failure for some tasks
                raise Exception(f"Processing error for task {task_id}")

            logger.info(f"✓ Processed task {task_id}")
            consumer.commit()

        except Exception as e:
            retry_count += 1

            if retry_count >= max_retries:
                # Final failure - send to DLQ
                logger.error(f"✗ Task {task_id} failed after {max_retries} retries")

                dlq_producer.send('tasks_dlq', value={
                    'task_id': task_id,
                    'error': str(e),
                    'retry_count': retry_count,
                    'original_task': task
                })

                consumer.commit()

            else:
                # Retry with exponential backoff
                wait_time = 2 ** retry_count  # 2s, 4s, 8s

                logger.warning(f"⚠ Task {task_id} failed, retry {retry_count} in {wait_time}s")

                # Send to retry topic with delay
                dlq_producer.send('tasks_retry', value={
                    'id': task_id,
                    'retry_count': retry_count,
                    'next_retry_time': time.time() + wait_time,
                    **task
                })

                consumer.commit()

    consumer.close()
    dlq_producer.close()


# ============================================================================
# EXAMPLE 6: Multi-Partition Consumer Tracking
# ============================================================================

print("\n[EXAMPLE 6] Multi-Partition Monitoring\n")

def example_partition_tracking():
    """
    Track offset and lag across multiple partitions.
    """

    consumer = KafkaConsumer(
        'events',
        bootstrap_servers=['localhost:9092'],
        group_id='multi_partition',
        auto_offset_reset='earliest',
        consumer_timeout_ms=2000
    )

    print("Reading from partitions and tracking lag...\n")

    messages_by_partition = {}

    for message in consumer:
        partition = message.partition

        if partition not in messages_by_partition:
            messages_by_partition[partition] = []

        messages_by_partition[partition].append(message.offset)

    # Display partition information
    for partition, offsets in sorted(messages_by_partition.items()):
        tp = TopicPartition('events', partition)

        consumer.assign([tp])
        current = consumer.position(tp)
        consumer.seek_to_end(tp)
        end = consumer.position(tp)

        lag = end - current

        print(f"Partition {partition}:")
        print(f"  Messages read: {len(offsets)}")
        print(f"  Current offset: {current}")
        print(f"  End offset: {end}")
        print(f"  Lag: {lag}\n")

    consumer.close()


# ============================================================================
# EXAMPLE 7: Performance Analysis
# ============================================================================

print("\n[EXAMPLE 7] Performance & Throughput Analysis\n")

def example_performance_analysis():
    """
    Measure throughput and latency.
    """

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=32768,  # 32KB
        linger_ms=10,
        compression_type='snappy'
    )

    print("Measuring producer throughput...\n")

    message_count = 100
    start_time = time.time()
    latencies = []

    for i in range(message_count):
        msg_start = time.time()

        future = producer.send('perf_test', value={'id': i})
        record_metadata = future.get(timeout=10)

        latency = (time.time() - msg_start) * 1000  # ms
        latencies.append(latency)

    producer.flush()
    elapsed = time.time() - start_time

    # Calculate metrics
    avg_latency = sum(latencies) / len(latencies)
    max_latency = max(latencies)
    min_latency = min(latencies)
    throughput = message_count / elapsed

    print(f"=== PERFORMANCE METRICS ===")
    print(f"Messages sent: {message_count}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {throughput:.2f} msg/sec")
    print(f"Avg latency: {avg_latency:.2f}ms")
    print(f"Min latency: {min_latency:.2f}ms")
    print(f"Max latency: {max_latency:.2f}ms")

    producer.close()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def process_order(order):
    """Simulate order processing."""
    time.sleep(0.1)  # Simulate work
    return True


if __name__ == "__main__":
    examples = {
        '1': ('Idempotent Producer', example_idempotent_producer),
        '2': ('Transactions', example_transactions),
        '3': ('Exactly-Once Consumer', example_exactly_once_consumer),
        '4': ('Stream Processing', example_stream_processing),
        '5': ('DLQ with Retry', example_dlq_with_retry),
        '6': ('Partition Tracking', example_partition_tracking),
        '7': ('Performance Analysis', example_performance_analysis),
    }

    print("\nAvailable Examples:")
    for key, (name, _) in examples.items():
        print(f"  {key}. {name}")

    choice = input("\nChoose example (1-7): ").strip()

    name, func = examples.get(choice, (None, None))
    if func:
        try:
            func()
        except Exception as e:
            logger.error(f"Error: {e}")
    else:
        print("Invalid choice")
