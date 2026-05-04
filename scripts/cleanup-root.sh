#!/bin/bash

# Cleanup root directory - remove files that belong in component directories

set -e

echo "🧹 Cleaning up root directory..."
echo ""

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Files to remove from root (they're duplicated in component directories)
FILES_TO_REMOVE=(
    "spark_pipeline.py"
    "kafka_consumer.py"
    "kafka_producer.py"
)

# Files to KEEP in root (shared across components)
FILES_TO_KEEP=(
    "config.py"
    "logger_setup.py"
)

echo "❌ Files to REMOVE from root:"
echo "   (they're better in component directories)"
for file in "${FILES_TO_REMOVE[@]}"; do
    if [ -f "$PROJECT_ROOT/$file" ]; then
        echo "   ✓ $file → will be removed"
    fi
done

echo ""
echo "✅ Files to KEEP in root:"
echo "   (shared across all components)"
for file in "${FILES_TO_KEEP[@]}"; do
    if [ -f "$PROJECT_ROOT/$file" ]; then
        echo "   ✓ $file → keeping"
    fi
done

echo ""
read -p "Continue with cleanup? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "🔄 Removing unnecessary files..."

    for file in "${FILES_TO_REMOVE[@]}"; do
        if [ -f "$PROJECT_ROOT/$file" ]; then
            rm -v "$PROJECT_ROOT/$file"
            echo "   ✓ Removed: $file"
        fi
    done

    echo ""
    echo "✅ Cleanup complete!"
    echo ""
    echo "📂 Root directory is now clean"

else
    echo "Cleanup cancelled"
fi
