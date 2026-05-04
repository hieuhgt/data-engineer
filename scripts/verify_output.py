"""Verify pipeline output."""

import os
from datetime import datetime

def check_output_files():
    """Check what files were created by pipeline."""
    print("=" * 60)
    print("PIPELINE OUTPUT VERIFICATION")
    print("=" * 60)

    output_path = "output"

    if not os.path.exists(output_path):
        print(f"✗ No output directory found at {output_path}/")
        return

    print(f"\n📁 Checking {output_path}/\n")

    # Walk through output directory
    for root, dirs, files in os.walk(output_path):
        level = root.replace(output_path, "").count(os.sep)
        indent = " " * 2 * level
        print(f"{indent}📂 {os.path.basename(root)}/")

        # Show files
        subindent = " " * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            size = os.path.getsize(file_path)
            size_mb = size / (1024 * 1024)
            print(f"{subindent}📄 {file} ({size_mb:.2f} MB)")

    print("\n" + "=" * 60)
    print("OUTPUT VERIFICATION COMPLETE")
    print("=" * 60)


def check_logs():
    """Check pipeline logs."""
    log_file = "logs/pipeline.log"

    if not os.path.exists(log_file):
        print(f"✗ No log file found at {log_file}")
        return

    print(f"\n📋 Recent logs from {log_file}:\n")

    with open(log_file, "r") as f:
        lines = f.readlines()
        # Show last 30 lines
        for line in lines[-30:]:
            print(line.rstrip())


def summary():
    """Print summary."""
    print("\n" + "=" * 60)
    print("EXPECTED OUTPUT STRUCTURE")
    print("=" * 60)

    print("""
    output/
    ├── processed/
    │   ├── _SUCCESS
    │   └── part-00000.parquet  (Cleaned data)
    └── (other subdirectories)

    logs/
    └── pipeline.log  (Execution logs)
    """)

    print("\nTo see more details:")
    print("  - Check Airflow UI: http://localhost:8080")
    print("  - Check logs: tail -f logs/pipeline.log")
    print("  - Check Spark UI: http://localhost:4040")


if __name__ == "__main__":
    check_output_files()
    print("\n")
    check_logs()
    print("\n")
    summary()
