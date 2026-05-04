#!/bin/bash

# Reorganize project files to match monorepo structure

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "🏗️  Reorganizing project to match monorepo structure..."
echo ""
echo "Current directory: $PROJECT_ROOT"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Step 1: Create proper directory structure
echo "📁 Creating directory structure..."

mkdir -p "$PROJECT_ROOT/spark/jobs"
mkdir -p "$PROJECT_ROOT/spark/tests"
mkdir -p "$PROJECT_ROOT/airflow/dags"
mkdir -p "$PROJECT_ROOT/airflow/plugins"
mkdir -p "$PROJECT_ROOT/kafka/producers"
mkdir -p "$PROJECT_ROOT/kafka/consumers"
mkdir -p "$PROJECT_ROOT/kafka/schemas"
mkdir -p "$PROJECT_ROOT/kafka/examples"
mkdir -p "$PROJECT_ROOT/requirements"
mkdir -p "$PROJECT_ROOT/scripts"
mkdir -p "$PROJECT_ROOT/tests"
mkdir -p "$PROJECT_ROOT/docs"
mkdir -p "$PROJECT_ROOT/data"
mkdir -p "$PROJECT_ROOT/output"
mkdir -p "$PROJECT_ROOT/logs"
mkdir -p "$PROJECT_ROOT/kubernetes/airflow"
mkdir -p "$PROJECT_ROOT/kubernetes/spark"
mkdir -p "$PROJECT_ROOT/kubernetes/kafka"
mkdir -p "$PROJECT_ROOT/kubernetes/configmaps"

echo -e "${GREEN}✓${NC} Directory structure created"

# Step 2: Move root files to proper locations
echo ""
echo "🔄 Moving files to proper locations..."

# Move Python files
if [ -f "$PROJECT_ROOT/spark_pipeline.py" ]; then
    mv "$PROJECT_ROOT/spark_pipeline.py" "$PROJECT_ROOT/spark/spark_pipeline.py"
    echo -e "${GREEN}✓${NC} spark_pipeline.py → spark/"
fi

if [ -f "$PROJECT_ROOT/kafka_producer.py" ]; then
    mv "$PROJECT_ROOT/kafka_producer.py" "$PROJECT_ROOT/kafka/producers/event_producer.py"
    echo -e "${GREEN}✓${NC} kafka_producer.py → kafka/producers/event_producer.py"
fi

if [ -f "$PROJECT_ROOT/kafka_consumer.py" ]; then
    mv "$PROJECT_ROOT/kafka_consumer.py" "$PROJECT_ROOT/kafka/consumers/event_consumer.py"
    echo -e "${GREEN}✓${NC} kafka_consumer.py → kafka/consumers/event_consumer.py"
fi

# Step 3: Verify key files are in place
echo ""
echo "✅ Verifying structure..."

check_file() {
    if [ -f "$1" ]; then
        echo -e "${GREEN}✓${NC} $(basename $1)"
    else
        echo -e "${RED}✗${NC} $(basename $1) - NOT FOUND"
    fi
}

echo ""
echo "Root (shared files):"
check_file "$PROJECT_ROOT/config.py"
check_file "$PROJECT_ROOT/logger_setup.py"

echo ""
echo "Spark:"
check_file "$PROJECT_ROOT/spark/spark_pipeline.py"
check_file "$PROJECT_ROOT/spark/requirements.txt"

echo ""
echo "Airflow:"
check_file "$PROJECT_ROOT/airflow/requirements.txt"

echo ""
echo "Kafka:"
check_file "$PROJECT_ROOT/kafka/requirements.txt"
check_file "$PROJECT_ROOT/kafka/producers/event_producer.py"
check_file "$PROJECT_ROOT/kafka/consumers/event_consumer.py"

echo ""
echo "Requirements (Monorepo):"
check_file "$PROJECT_ROOT/requirements/base.txt"
check_file "$PROJECT_ROOT/requirements/dev.txt"

echo ""
echo "Scripts:"
check_file "$PROJECT_ROOT/scripts/install.sh"
check_file "$PROJECT_ROOT/scripts/verify-deps.sh"

echo ""
echo -e "${GREEN}✅ Reorganization complete!${NC}"
echo ""
echo "📋 Next steps:"
echo "  1. Review new structure: tree -L 3 (if installed)"
echo "  2. Update any imports in your code"
echo "  3. Run: ./scripts/install.sh --all"
echo "  4. Run: ./scripts/verify-deps.sh"
echo "  5. Test: docker-compose -f docker-compose.full.yml up -d"
echo ""
