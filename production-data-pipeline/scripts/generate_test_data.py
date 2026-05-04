#!/usr/bin/env python3
"""
Generate realistic test data and write to MinIO (local S3).

Usage:
  python scripts/generate_test_data.py --size small
  python scripts/generate_test_data.py --size medium
  python scripts/generate_test_data.py --size large
"""
import argparse
import json
import logging
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

SIZES = {"small": 10_000, "medium": 1_000_000, "large": 10_000_000}
EVENT_TYPES = ["purchase", "view", "add_to_cart", "search", "checkout", "refund"]
STATUSES = ["completed", "pending", "failed"]


def random_email() -> str:
    user = "".join(random.choices(string.ascii_lowercase, k=7))
    domain = random.choice(["gmail.com", "yahoo.com", "company.com"])
    return f"{user}@{domain}"


def make_event(i: int, base_date: datetime) -> dict:
    offset = timedelta(seconds=random.randint(0, 86400))
    return {
        "event_id": f"evt_{i:010d}",
        "user_id": random.randint(1, 50_000),
        "event_type": random.choice(EVENT_TYPES),
        "amount": round(random.uniform(0, 500), 2),
        "timestamp": (base_date + offset).isoformat(),
        "product_id": random.randint(1, 10_000),
        "source_id": random.choice(["web", "mobile", "api"]),
    }


def make_user(i: int) -> dict:
    return {
        "user_id": i,
        "name": f"User {i}",
        "email": random_email(),
        "status": random.choice(STATUSES),
        "created_at": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).isoformat(),
    }


def write_jsonl(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    logger.info(f"Wrote {len(records):,} records → {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--out", default="data/test")
    args = parser.parse_args()

    n = SIZES[args.size]
    out = Path(args.out)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"Generating {n:,} rows ({args.size} dataset)")

    # Events (main fact)
    events = [make_event(i, today) for i in range(1, n + 1)]
    write_jsonl(events, out / "events" / "events.jsonl")

    # Users (dimension)
    users = [make_user(i) for i in range(1, min(n // 10, 50_001))]
    write_jsonl(users, out / "users" / "users.jsonl")

    # Introduce ~2% duplicates for realistic dedup testing
    dup_count = max(1, n // 50)
    dupes = random.choices(events, k=dup_count)
    write_jsonl(events + dupes, out / "events_with_dupes" / "events.jsonl")
    logger.info(f"Added {dup_count:,} duplicate events for dedup testing")

    logger.info(f"Test data ready in {out}/")


if __name__ == "__main__":
    main()
