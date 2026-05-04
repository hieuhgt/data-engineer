# Monorepo Structure Guide

## Current Issue
```
data-engineer/
├── requirements.txt           ← All deps together
├── spark/
├── airflow/
├── kafka/
└── kubernetes/
```

**Problems**:
- ❌ Can't install only Spark deps
- ❌ Mixing unrelated dependencies
- ❌ Harder to manage in CI/CD
- ❌ Conflicts between versions

## Monorepo Solution

```
data-engineer/
├── requirements/              ← Shared & common
│   ├── base.txt             ← Core dependencies
│   ├── dev.txt              ← Testing, linting, etc
│   └── prod.txt             ← Production only
│
├── spark/
│   ├── requirements.txt      ← Spark-specific
│   ├── jobs/
│   └── Dockerfile
│
├── airflow/
│   ├── requirements.txt      ← Airflow-specific
│   ├── dags/
│   └── Dockerfile
│
├── kafka/
│   ├── requirements.txt      ← Kafka-specific
│   ├── examples/
│   └── producers/
│
├── docker-compose.yml
└── docker-compose.full.yml
```

## Benefits of Monorepo

| Benefit | Explanation |
|---------|-------------|
| **Dependency Isolation** | Each component declares what it needs |
| **Clear Dependencies** | Easy to see what each service requires |
| **CI/CD Efficiency** | Install only what you need |
| **Version Management** | Can use different versions per component |
| **Development** | Run multiple components independently |
| **Documentation** | Clear separation of concerns |

## Installation Strategy

```bash
# Option 1: Install only what you need
pip install -r spark/requirements.txt         # Just Spark

# Option 2: Install for development
pip install -r requirements/base.txt          # Core
pip install -r spark/requirements.txt         # + Spark
pip install -r requirements/dev.txt           # + Dev tools

# Option 3: Install everything
pip install -r requirements/prod.txt \
            -r spark/requirements.txt \
            -r airflow/requirements.txt \
            -r kafka/requirements.txt
```

## CI/CD Optimization

```yaml
# Example: GitHub Actions
name: Test Spark Only
on: [push]

jobs:
  test-spark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      # Install only Spark dependencies
      - run: pip install -r spark/requirements.txt

      # Run Spark tests
      - run: pytest spark/tests/

      # Build only Spark image
      - run: docker build -f spark/Dockerfile .
```

## Docker Integration

```dockerfile
# spark/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Only install Spark deps
COPY spark/requirements.txt .
RUN pip install -r requirements.txt

COPY spark/ ./spark/
COPY config.py logger_setup.py ./

CMD ["python", "spark/jobs/etl.py"]
```

## Shared Dependencies

**requirements/base.txt**:
```
python-dotenv==1.0.0
loguru==0.7.2
pydantic==2.0.0
```

Used by all components.

**Component Requirements**:

spark/requirements.txt:
```
# Include base
-r ../requirements/base.txt

# Spark-specific
pyspark==3.5.0
pandas==2.0.3
pyarrow==13.0.0
```

airflow/requirements.txt:
```
# Include base
-r ../requirements/base.txt

# Airflow-specific
apache-airflow==2.8.1
apache-airflow-providers-apache-spark==4.4.3
```

kafka/requirements.txt:
```
# Include base
-r ../requirements/base.txt

# Kafka-specific
kafka-python==2.0.2
```

## Development Workflow

```bash
# Clone project
git clone <repo>
cd data-engineer

# Install base dependencies
pip install -r requirements/base.txt

# Install specific component
pip install -r spark/requirements.txt

# Or install all
for dir in spark airflow kafka; do
  pip install -r $dir/requirements.txt
done

# Or use install.sh (see below)
./scripts/install.sh --all
```

## Helper Scripts

### install.sh
```bash
#!/bin/bash

set -e

usage() {
  echo "Usage: ./install.sh [--spark|--airflow|--kafka|--all|--dev]"
  exit 1
}

if [ $# -eq 0 ]; then
  usage
fi

# Install base
pip install -r requirements/base.txt

case "$1" in
  --spark)
    pip install -r spark/requirements.txt
    echo "✓ Spark installed"
    ;;
  --airflow)
    pip install -r airflow/requirements.txt
    echo "✓ Airflow installed"
    ;;
  --kafka)
    pip install -r kafka/requirements.txt
    echo "✓ Kafka installed"
    ;;
  --all)
    pip install -r spark/requirements.txt
    pip install -r airflow/requirements.txt
    pip install -r kafka/requirements.txt
    echo "✓ All installed"
    ;;
  --dev)
    pip install -r spark/requirements.txt
    pip install -r airflow/requirements.txt
    pip install -r kafka/requirements.txt
    pip install -r requirements/dev.txt
    echo "✓ All + dev tools installed"
    ;;
  *)
    usage
    ;;
esac
```

### verify-deps.sh
```bash
#!/bin/bash

echo "=== Verifying Dependencies ==="

echo -n "Spark... "
python -c "import pyspark; print('✓')" 2>/dev/null || echo "✗ Not installed"

echo -n "Airflow... "
python -c "import airflow; print('✓')" 2>/dev/null || echo "✗ Not installed"

echo -n "Kafka... "
python -c "import kafka; print('✓')" 2>/dev/null || echo "✗ Not installed"

echo -n "Base (loguru)... "
python -c "import loguru; print('✓')" 2>/dev/null || echo "✗ Not installed"
```

## Docker Compose

```yaml
version: '3.8'

services:
  spark:
    build:
      context: .
      dockerfile: spark/Dockerfile  # Uses spark/requirements.txt
    environment:
      - PYTHONUNBUFFERED=1
    volumes:
      - ./spark:/app/spark
      - ./data:/data

  airflow:
    build:
      context: .
      dockerfile: airflow/Dockerfile  # Uses airflow/requirements.txt
    environment:
      - AIRFLOW_HOME=/app/airflow
    volumes:
      - ./airflow:/app/airflow
      - ./logs:/app/logs
```

## Migration Plan

### Step 1: Create requirements structure
```bash
mkdir -p requirements
```

### Step 2: Create base requirements
Create `requirements/base.txt`:
```
python-dotenv==1.0.0
loguru==0.7.2
```

### Step 3: Create component requirements
```bash
# Spark
cat > spark/requirements.txt << EOF
-r ../requirements/base.txt
pyspark==3.5.0
pandas==2.0.3
pyarrow==13.0.0
pytest==7.4.0
EOF

# Airflow
cat > airflow/requirements.txt << EOF
-r ../requirements/base.txt
apache-airflow==2.8.1
apache-airflow-providers-apache-spark==4.4.3
EOF

# Kafka
cat > kafka/requirements.txt << EOF
-r ../requirements/base.txt
kafka-python==2.0.2
EOF
```

### Step 4: Update Docker setup
```bash
# Update docker-compose to use component Dockerfiles
# Each service specifies its own requirements.txt
```

### Step 5: Create helper scripts
```bash
chmod +x scripts/install.sh
chmod +x scripts/verify-deps.sh
```

## Best Practices

### ✅ DO
- ✅ Keep base requirements minimal
- ✅ Use `-r ../requirements/base.txt` to include base
- ✅ Specify exact versions (except for development)
- ✅ Document why each dependency is needed
- ✅ Test installation with fresh venv

### ❌ DON'T
- ❌ Duplicate common dependencies
- ❌ Use wildcard versions (==, not >=)
- ❌ Mix incompatible versions
- ❌ Keep unused dependencies
- ❌ Ignore dependency conflicts

## Example: Complete Setup

```bash
# 1. Clone and navigate
git clone <repo>
cd data-engineer

# 2. Install what you need
./scripts/install.sh --spark  # Only Spark
# or
./scripts/install.sh --all    # Everything

# 3. Verify installation
./scripts/verify-deps.sh

# 4. Run component
python spark/jobs/etl.py

# 5. Build Docker image
docker build -f spark/Dockerfile -t spark-pipeline:latest .
```

## CI/CD Integration

```yaml
# .github/workflows/test.yml
name: Test All Components

on: [push]

jobs:
  test-spark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r spark/requirements.txt
      - run: pytest spark/tests/

  test-airflow:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r airflow/requirements.txt
      - run: pytest airflow/tests/

  test-kafka:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r kafka/requirements.txt
      - run: pytest kafka/tests/
```

## Summary

**Old Way** (Flat):
```
requirements.txt  ← Everything mixed
```

**New Way** (Monorepo):
```
requirements/
├── base.txt       ← Shared
├── dev.txt        ← Dev tools
└── prod.txt       ← Production only

spark/
└── requirements.txt  ← Spark-specific

airflow/
└── requirements.txt  ← Airflow-specific

kafka/
└── requirements.txt  ← Kafka-specific
```

**Benefits**:
- Clear dependencies
- Easy to install only what you need
- Better CI/CD
- Easier to manage versions
- Professional structure
