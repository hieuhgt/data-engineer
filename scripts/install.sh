#!/bin/bash

# Installation helper for monorepo components

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  echo "Usage: ./install.sh [--spark|--airflow|--kafka|--all|--dev|--prod]"
  echo ""
  echo "Options:"
  echo "  --spark       Install only Spark component"
  echo "  --airflow     Install only Airflow component"
  echo "  --kafka       Install only Kafka component"
  echo "  --all         Install all components"
  echo "  --dev         Install all + development tools"
  echo "  --prod        Install all + production dependencies"
  echo ""
  exit 1
}

if [ $# -eq 0 ]; then
  usage
fi

echo "📦 Installing dependencies..."

# Always install base
echo "  📌 Installing base dependencies..."
pip install -r "$PROJECT_ROOT/requirements/base.txt"

case "$1" in
  --spark)
    echo "  🔥 Installing Spark dependencies..."
    pip install -r "$PROJECT_ROOT/spark/requirements.txt"
    echo "✅ Spark installed successfully"
    ;;
  --airflow)
    echo "  ✈️  Installing Airflow dependencies..."
    pip install -r "$PROJECT_ROOT/airflow/requirements.txt"
    echo "✅ Airflow installed successfully"
    ;;
  --kafka)
    echo "  📨 Installing Kafka dependencies..."
    pip install -r "$PROJECT_ROOT/kafka/requirements.txt"
    echo "✅ Kafka installed successfully"
    ;;
  --all)
    echo "  🔥 Installing Spark dependencies..."
    pip install -r "$PROJECT_ROOT/spark/requirements.txt"
    echo "  ✈️  Installing Airflow dependencies..."
    pip install -r "$PROJECT_ROOT/airflow/requirements.txt"
    echo "  📨 Installing Kafka dependencies..."
    pip install -r "$PROJECT_ROOT/kafka/requirements.txt"
    echo "✅ All components installed successfully"
    ;;
  --dev)
    echo "  🔥 Installing Spark dependencies..."
    pip install -r "$PROJECT_ROOT/spark/requirements.txt"
    echo "  ✈️  Installing Airflow dependencies..."
    pip install -r "$PROJECT_ROOT/airflow/requirements.txt"
    echo "  📨 Installing Kafka dependencies..."
    pip install -r "$PROJECT_ROOT/kafka/requirements.txt"
    echo "  🛠️  Installing development tools..."
    pip install -r "$PROJECT_ROOT/requirements/dev.txt"
    echo "✅ All components + dev tools installed successfully"
    ;;
  --prod)
    echo "  🔥 Installing Spark dependencies..."
    pip install -r "$PROJECT_ROOT/spark/requirements.txt"
    echo "  ✈️  Installing Airflow dependencies..."
    pip install -r "$PROJECT_ROOT/airflow/requirements.txt"
    echo "  📨 Installing Kafka dependencies..."
    pip install -r "$PROJECT_ROOT/kafka/requirements.txt"
    echo "  🏭 Installing production dependencies..."
    pip install -r "$PROJECT_ROOT/requirements/prod.txt"
    echo "✅ Production setup installed successfully"
    ;;
  *)
    usage
    ;;
esac

echo ""
echo "Next steps:"
echo "  1. Verify installation: ./scripts/verify-deps.sh"
echo "  2. Start services: docker-compose -f docker-compose.full.yml up -d"
echo "  3. Run tests: pytest tests/"
echo ""
