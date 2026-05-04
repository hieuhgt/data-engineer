#!/bin/bash

# Setup Poetry environments for each component
# Run this once to setup all isolated virtual environments

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "🎯 Data Engineer Project - Poetry Setup"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Check if Poetry is installed
echo "📦 Checking Poetry installation..."
if ! command -v poetry &> /dev/null; then
    echo -e "${YELLOW}⚠️  Poetry not found. Installing...${NC}"
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
    echo -e "${GREEN}✓ Poetry installed${NC}"
else
    echo -e "${GREEN}✓ Poetry found: $(poetry --version)${NC}"
fi

echo ""
echo "📁 Setting up project environments..."
echo ""

# Array of components
components=("pandas" "kafka" "spark" "airflow")

# Function to setup component
setup_component() {
    local component=$1
    local component_path="$PROJECT_ROOT/$component"

    if [ ! -d "$component_path" ]; then
        echo -e "${YELLOW}⊘ Skipping $component (directory not found)${NC}"
        return
    fi

    echo -e "${BLUE}Setting up: $component${NC}"

    cd "$component_path"

    # Install dependencies and create venv
    poetry install --no-interaction --verbose

    echo -e "${GREEN}✓ $component environment ready${NC}"
    echo "  Location: $component_path"
    echo "  Run with: poetry run python <script.py>"
    echo ""
}

# Setup root environment
echo -e "${BLUE}Setting up: root project${NC}"
cd "$PROJECT_ROOT"
poetry install --no-interaction --verbose
echo -e "${GREEN}✓ Root environment ready${NC}"
echo ""

# Setup each component
for component in "${components[@]}"; do
    setup_component "$component"
done

echo ""
echo "========================================"
echo -e "${GREEN}✅ All environments ready!${NC}"
echo ""
echo "📖 Usage:"
echo ""
echo "Pandas:"
echo "  cd pandas"
echo "  poetry run python examples/level1_fundamentals.py"
echo ""
echo "Kafka:"
echo "  cd kafka"
echo "  poetry run python producers/event_producer.py"
echo ""
echo "Spark:"
echo "  cd spark"
echo "  poetry run python jobs/ingest_kafka.py"
echo ""
echo "Airflow:"
echo "  cd airflow"
echo "  poetry run airflow dags list"
echo ""
echo "Or activate virtual env:"
echo "  source $(poetry env info -p)/bin/activate"
echo ""
