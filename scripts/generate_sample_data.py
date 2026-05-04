"""Generate sample data for testing."""

import json
import random
from datetime import datetime, timedelta

def generate_sample_events(count: int = 100):
    """Generate sample events for testing."""
    events = []
    users = [f"user_{i}" for i in range(1, 21)]
    event_types = ["click", "purchase", "view", "signup", "logout"]

    for _ in range(count):
        event = {
            "user_id": random.choice(users),
            "event_type": random.choice(event_types),
            "value": round(random.uniform(1.0, 100.0), 2),
            "timestamp": (
                datetime.now() - timedelta(minutes=random.randint(0, 1440))
            ).isoformat(),
        }
        events.append(event)

    return events


def save_to_csv():
    """Save sample data as CSV."""
    import csv

    events = generate_sample_events(500)

    with open("data/sample.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "event_type", "value", "timestamp"])
        writer.writeheader()
        writer.writerows(events)

    print(f"✓ Saved {len(events)} events to data/sample.csv")


def save_to_json():
    """Save sample data as JSONL (one JSON per line)."""
    events = generate_sample_events(500)

    with open("data/sample.jsonl", "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"✓ Saved {len(events)} events to data/sample.jsonl")


def save_to_parquet():
    """Save sample data as Parquet."""
    try:
        import pandas as pd
        from pyspark.sql import SparkSession

        events = generate_sample_events(500)
        df = pd.DataFrame(events)

        # Save with Pandas
        df.to_parquet("data/sample.parquet", index=False)
        print(f"✓ Saved {len(events)} events to data/sample.parquet")
    except ImportError:
        print("⚠ PySpark/Pandas not available, skipping Parquet")


if __name__ == "__main__":
    import os

    # Create data directory
    os.makedirs("data", exist_ok=True)

    print("Generating sample data...")
    save_to_csv()
    save_to_json()
    save_to_parquet()

    print("\n✓ Sample data generation complete!")
    print("Files created:")
    print("  - data/sample.csv")
    print("  - data/sample.jsonl")
    print("  - data/sample.parquet")
