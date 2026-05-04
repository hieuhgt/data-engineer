#!/bin/bash

# Verify installed dependencies

echo "========================================"
echo "  Dependency Verification"
echo "========================================"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

verify_package() {
  local package=$1
  local import_name=${2:-$1}

  echo -n "  $package... "
  if python -c "import $import_name" 2>/dev/null; then
    echo -e "${GREEN}✓${NC}"
    return 0
  else
    echo -e "${RED}✗${NC}"
    return 1
  fi
}

echo "Core Dependencies:"
verify_package "dotenv" "dotenv" || true
verify_package "loguru" "loguru" || true
verify_package "pydantic" "pydantic" || true

echo ""
echo "Spark Dependencies:"
verify_package "pyspark" "pyspark" || true
verify_package "pandas" "pandas" || true
verify_package "pyarrow" "pyarrow" || true

echo ""
echo "Airflow Dependencies:"
verify_package "airflow" "airflow" || true

echo ""
echo "Kafka Dependencies:"
verify_package "kafka" "kafka" || true

echo ""
echo "Development Tools:"
verify_package "pytest" "pytest" || true
verify_package "black" "black" || true
verify_package "mypy" "mypy" || true

echo ""
echo "========================================"
echo "  Verification Complete"
echo "========================================"
