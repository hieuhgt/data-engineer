# Setup Guide - Python Virtual Environments

Choose one approach to setup isolated Python environments for each component.

---

## 🎯 Quick Choice

| Approach | Best For | Setup Time | Complexity |
|----------|----------|-----------|-----------|
| **venv (Simple)** | Quick start, learning | 2 minutes | Low |
| **Poetry (Modern)** | Production, lock files | 5 minutes | Medium |

---

## Option 1: Simple venv + pip (Recommended for Learning)

### Step 1: Make script executable
```bash
cd /Users/hieuht/workspace/personal/data-engineer
chmod +x scripts/setup-venv.sh
```

### Step 2: Run setup
```bash
./scripts/setup-venv.sh
```

### Step 3: Use environments

**Activate and run:**
```bash
# Activate Pandas environment
source pandas/venv/bin/activate

# Run the code
python examples/level1_fundamentals.py

# Deactivate when done
deactivate
```

**Or run directly (no activation needed):**
```bash
pandas/venv/bin/python examples/level1_fundamentals.py
```

### What gets created
```
data-engineer/
├── venv/                    ← Root venv
├── pandas/
│   └── venv/                ← Pandas-only packages
├── kafka/
│   └── venv/                ← Kafka-only packages
├── spark/
│   └── venv/                ← Spark-only packages
└── airflow/
    └── venv/                ← Airflow-only packages
```

---

## Option 2: Modern Poetry (Recommended for Production)

### Step 1: Install Poetry (if not already installed)
```bash
curl -sSL https://install.python-poetry.org | python3 -
export PATH="$HOME/.local/bin:$PATH"

# Verify
poetry --version
```

### Step 2: Make script executable
```bash
cd /Users/hieuht/workspace/personal/data-engineer
chmod +x scripts/setup-poetry.sh
```

### Step 3: Run setup
```bash
./scripts/setup-poetry.sh
```

### Step 4: Use environments

**Option A: Run with poetry run**
```bash
cd pandas
poetry run python examples/level1_fundamentals.py
```

**Option B: Activate virtual environment**
```bash
cd pandas
poetry shell
python examples/level1_fundamentals.py
exit  # Exit the poetry shell
```

**Option C: Use poetry directly**
```bash
poetry -C pandas run python examples/level1_fundamentals.py
```

### What gets created
```
data-engineer/
├── poetry.lock              ← Locked dependency versions (important!)
├── pyproject.toml           ← Root config
├── pandas/
│   ├── pyproject.toml       ← Pandas config
│   └── .venv/               ← Pandas venv (created by Poetry)
├── kafka/
│   ├── pyproject.toml       ← Kafka config
│   └── .venv/               ← Kafka venv
└── ... (etc)
```

---

## 🚀 Real-World Usage Examples

### Run Pandas Level 1
```bash
# Simple venv approach
source pandas/venv/bin/activate
python examples/level1_fundamentals.py
deactivate

# Poetry approach
cd pandas && poetry run python examples/level1_fundamentals.py
```

### Run Kafka Producer
```bash
# Simple venv approach
source kafka/venv/bin/activate
python producers/event_producer.py
deactivate

# Poetry approach
cd kafka && poetry run python producers/event_producer.py
```

### Run Spark Job
```bash
# Simple venv approach
source spark/venv/bin/activate
python jobs/ingest_kafka.py
deactivate

# Poetry approach
cd spark && poetry run python jobs/ingest_kafka.py
```

### Jupyter Notebooks
```bash
# Install Jupyter first
pip install jupyter

# Or in Poetry (includes in pyproject.toml dev deps)
poetry add --group dev jupyter

# Run Jupyter from component
cd pandas
poetry shell
jupyter notebook

# Or with venv
source pandas/venv/bin/activate
jupyter notebook
```

---

## 📦 Component Dependencies

### Pandas
```
- pandas (data manipulation)
- numpy (numerical computing)
- scipy (scientific computing)
```

### Kafka
```
- kafka-python (client library)
- confluent-kafka (alternative)
- python-dotenv (environment config)
```

### Spark
```
- pyspark (distributed computing)
- pandas (dataframe operations)
- pyarrow (columnar format)
```

### Airflow
```
- apache-airflow (orchestration)
- airflow-providers (integrations)
- psycopg2 (PostgreSQL support)
```

---

## 🔄 Updating Dependencies

### venv approach
```bash
# Update requirements.txt, then reinstall
source pandas/venv/bin/activate
pip install --upgrade -r requirements.txt
deactivate
```

### Poetry approach
```bash
cd pandas

# Add new package
poetry add pandas==2.1.0

# Update all packages
poetry update

# Update lock file
poetry lock
```

---

## 🔍 Checking Environment

### venv approach
```bash
source pandas/venv/bin/activate
python -c "import pandas; print(pandas.__version__)"
pip list  # Show all packages
deactivate
```

### Poetry approach
```bash
cd pandas
poetry show  # List all dependencies
poetry env info  # Environment information
```

---

## 🗑️ Cleanup

### Remove venv environments (space-saving)
```bash
# Remove individual venv
rm -rf pandas/venv

# Remove all venvs
rm -rf */venv
rm -rf venv

# Then recreate with setup script
./scripts/setup-venv.sh
```

### Remove Poetry environments
```bash
# Remove Poetry cache
poetry cache clear . --all

# Remove specific env
poetry env remove <python-version>

# Reinstall
./scripts/setup-poetry.sh
```

---

## 💡 Which Should I Use?

### Use **venv** if you:
- ✅ Just learning
- ✅ Don't need reproducible builds
- ✅ Want minimal setup
- ✅ Only running locally

### Use **Poetry** if you:
- ✅ Building for production
- ✅ Need lock files (reproducible)
- ✅ Managing complex dependencies
- ✅ Team collaboration
- ✅ Want to publish packages

---

## 🔗 .envrc for Auto-Activation (Optional)

If you use **direnv**, create `.envrc`:

```bash
# .envrc in root of project
export PYTHONPATH="$(pwd):$PYTHONPATH"

# Auto-activate pandas venv when in pandas/
if [ -d "./venv" ]; then
    source ./venv/bin/activate
fi

# Or with Poetry
layout poetry
```

Then:
```bash
direnv allow
```

Now venv activates automatically when you `cd` into the directory!

---

## ⚠️ Troubleshooting

### "ModuleNotFoundError: No module named 'pandas'"
```bash
# Make sure venv is activated
source pandas/venv/bin/activate

# Or verify installation
pandas/venv/bin/python -c "import pandas"

# Reinstall if needed
source pandas/venv/bin/activate
pip install pandas
```

### "poetry: command not found"
```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add to PATH
export PATH="$HOME/.local/bin:$PATH"
```

### "Permission denied: './scripts/setup-venv.sh'"
```bash
# Make executable
chmod +x scripts/setup-venv.sh
```

### Different Python versions?
```bash
# Check which Python is used
which python
python --version

# Use specific Python version with venv
python3.11 -m venv pandas/venv

# With Poetry
poetry env use python3.11
```

---

## 📚 Next Steps

1. **Choose an approach** (venv or Poetry)
2. **Run the setup script**
3. **Test with Pandas**
   ```bash
   # venv
   source pandas/venv/bin/activate
   python examples/level1_fundamentals.py

   # Poetry
   cd pandas && poetry run python examples/level1_fundamentals.py
   ```
4. **Repeat for other components** (kafka, spark, airflow)

---

## 🎯 Quick Start (venv)

```bash
# One-liner setup
chmod +x scripts/setup-venv.sh && ./scripts/setup-venv.sh

# Run Pandas
source pandas/venv/bin/activate
python examples/level1_fundamentals.py
```

---

## 🎯 Quick Start (Poetry)

```bash
# Install Poetry (if needed)
curl -sSL https://install.python-poetry.org | python3 -

# Setup
chmod +x scripts/setup-poetry.sh && ./scripts/setup-poetry.sh

# Run Pandas
cd pandas && poetry run python examples/level1_fundamentals.py
```

---

**Ready? Pick one and run the setup script!** 🚀
