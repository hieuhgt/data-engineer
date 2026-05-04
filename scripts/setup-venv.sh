#!/bin/bash

# Setup Python virtual environments for each component
# Simpler approach than Poetry - uses standard venv + pip

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "🎯 Data Engineer Project - Virtual Environment Setup"
echo "====================================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Check Python version
echo "✓ Python version:"
python3 --version
echo ""

# Create function for component setup
setup_venv() {
    local component=$1
    local component_path="$PROJECT_ROOT/$component"
    local venv_path="$component_path/venv"

    if [ ! -d "$component_path" ]; then
        echo -e "${YELLOW}⊘ Skipping $component (directory not found)${NC}"
        return
    fi

    echo -e "${BLUE}Setting up: $component${NC}"

    # Create virtual environment
    python3 -m venv "$venv_path"
    echo -e "  ${GREEN}✓ Created venv at: $venv_path${NC}"

    # Activate and upgrade pip
    source "$venv_path/bin/activate"
    pip install --upgrade pip setuptools wheel > /dev/null 2>&1
    echo -e "  ${GREEN}✓ Upgraded pip${NC}"

    # Install requirements
    if [ -f "$component_path/requirements.txt" ]; then
        pip install -r "$component_path/requirements.txt" > /dev/null 2>&1
        echo -e "  ${GREEN}✓ Installed requirements${NC}"
    fi

    # Deactivate
    deactivate

    echo -e "${GREEN}✓ $component environment ready${NC}"
    echo "  To activate: source $component_path/venv/bin/activate"
    echo ""
}

# Array of components
components=("pandas" "kafka" "spark" "airflow")

# Setup root virtual environment
echo -e "${BLUE}Setting up: root project venv${NC}"
ROOT_VENV="$PROJECT_ROOT/venv"

python3 -m venv "$ROOT_VENV"
echo -e "  ${GREEN}✓ Created venv at: $ROOT_VENV${NC}"

source "$ROOT_VENV/bin/activate"
pip install --upgrade pip setuptools wheel > /dev/null 2>&1
echo -e "  ${GREEN}✓ Upgraded pip${NC}"

# Install base requirements
if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
    pip install -r "$PROJECT_ROOT/requirements.txt" > /dev/null 2>&1
    echo -e "  ${GREEN}✓ Installed base requirements${NC}"
fi

deactivate
echo -e "${GREEN}✓ Root environment ready${NC}"
echo ""

# Setup each component
for component in "${components[@]}"; do
    setup_venv "$component"
done

echo ""
echo "====================================================="
echo -e "${GREEN}✅ All environments ready!${NC}"
echo ""
echo "📖 Usage:"
echo ""
echo "Option 1: Activate venv then run"
echo "  source pandas/venv/bin/activate"
echo "  python examples/level1_fundamentals.py"
echo "  deactivate"
echo ""
echo "Option 2: Run with venv python directly"
echo "  pandas/venv/bin/python examples/level1_fundamentals.py"
echo ""
echo "Component paths:"
echo "  - pandas:   $PROJECT_ROOT/pandas/venv"
echo "  - kafka:    $PROJECT_ROOT/kafka/venv"
echo "  - spark:    $PROJECT_ROOT/spark/venv"
echo "  - airflow:  $PROJECT_ROOT/airflow/venv"
echo "  - root:     $PROJECT_ROOT/venv"
echo ""
echo "💡 Pro tip: Use .envrc with direnv for automatic activation"
echo ""
