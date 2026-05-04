#!/usr/bin/env python3
"""
Check pipeline health from the command line.

Usage:
  python scripts/monitor_pipeline.py --status
  python scripts/monitor_pipeline.py --quality
  python scripts/monitor_pipeline.py --timeline
"""
import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path


def _check_file(path: str) -> dict:
    if not os.path.exists(path):
        return {"exists": False}
    age_sec = time.time() - os.path.getmtime(path)
    return {"exists": True, "age_hours": round(age_sec / 3600, 2)}


def status():
    """Check warehouse freshness and service availability."""
    tables = ["fact_events", "fact_transactions", "dim_users", "agg_daily_metrics"]
    print("\n=== Warehouse Status ===")
    for table in tables:
        info = _check_file(f"/tmp/warehouse/{table}.parquet")
        if info["exists"]:
            fresh = "✓ FRESH" if info["age_hours"] < 4 else "✗ STALE"
            print(f"  {table:30s}  {fresh}  ({info['age_hours']}h old)")
        else:
            print(f"  {table:30s}  ✗ NOT FOUND")

    print("\n=== Service URLs ===")
    services = {
        "Airflow UI": "http://localhost:8080",
        "Spark UI":   "http://localhost:8088",
        "MinIO":      "http://localhost:9001",
        "Prometheus": "http://localhost:9090",
        "Grafana":    "http://localhost:3000",
    }
    for name, url in services.items():
        print(f"  {name:20s}  {url}")


def quality():
    """Show last quality check results."""
    path = "/tmp/quality_report.json"
    print("\n=== Data Quality ===")
    if not os.path.exists(path):
        print("  No quality report found – run the pipeline first")
        return
    with open(path) as f:
        report = json.load(f)
    print(f"  Overall quality score: {report.get('quality_score', 'N/A')}")
    for gate in report.get("gates", []):
        icon = "✓" if gate["passed"] else "✗"
        print(f"  {icon} {gate['name']:30s}  {gate['pass_rate']}  (threshold {gate['threshold']})")


def timeline():
    """Print rough pipeline stage durations from lineage log."""
    path = "/tmp/lineage.json"
    print("\n=== Pipeline Timeline ===")
    if not os.path.exists(path):
        print("  No lineage log found – run the pipeline first")
        return
    with open(path) as f:
        records = json.load(f)
    for r in records:
        print(f"  {r['step']:15s}  {r['source']} → {r['target']}  rows={r['rows_processed']}  status={r['status']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--status",   action="store_true")
    parser.add_argument("--quality",  action="store_true")
    parser.add_argument("--timeline", action="store_true")
    args = parser.parse_args()

    if args.status or not any([args.status, args.quality, args.timeline]):
        status()
    if args.quality:
        quality()
    if args.timeline:
        timeline()
