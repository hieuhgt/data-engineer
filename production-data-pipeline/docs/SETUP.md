# Setup & Getting Started

## Prerequisites

- Docker & Docker Compose (v20+)
- Python 3.9–3.12 (Airflow 2.x does not yet support Python 3.13)
- [Poetry](https://python-poetry.org/) 1.8+ — `curl -sSL https://install.python-poetry.org | python3 -`
- Git
- 8 GB RAM minimum (16 GB recommended)
- 20 GB free disk space

## 1. Clone & Initialize

```bash
git clone <repo-url>
cd production-data-pipeline

cp config/env.example .env   # fill in secrets before proceeding
mkdir -p logs data/lake data/test data/checkpoints
```

## 2. Install Dependencies (Poetry)

```bash
# Core runtime + dev/test tools
poetry install --with dev

# Add optional groups as needed:
#   --with airflow    Airflow + providers (heavy, needs Python <3.13)
#   --with warehouse  Snowflake + psycopg2
#   --with quality    ydata-profiling, great-expectations
poetry install --with dev,airflow,warehouse

# Activate the managed virtualenv
poetry shell
# or prefix every command: poetry run python ...
```

### Dependency groups

| Group | Contents | When to install |
|---|---|---|
| *(default)* | PySpark, pandas, Kafka, Prometheus, etc. | Always |
| `dev` | pytest, black, ruff, mypy | Local development & CI |
| `airflow` | apache-airflow + providers | Running the orchestration layer |
| `warehouse` | Snowflake + psycopg2 | Connecting to real warehouses |
| `quality` | ydata-profiling, great-expectations | Advanced profiling |

## 3. Start Services

```bash
# Start all containers
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

## 4. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO (S3) | http://localhost:9001 | minioadmin / minioadmin |
| Spark | http://localhost:8088 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |

## 5. Generate Test Data

```bash
# Generate sample data
python scripts/generate_test_data.py --size small

# Options:
# --size small    (100k records)
# --size medium   (1M records)
# --size large    (10M records)
```

## 6. Trigger Pipeline

```bash
# Option A: Via Airflow UI
# 1. Go to http://localhost:8080
# 2. Find "daily_batch_pipeline"
# 3. Click play button

# Option B: Via CLI
airflow dags trigger daily_batch_pipeline

# Monitor execution
airflow dags list-runs daily_batch_pipeline
```

## 7. Verify Results

```bash
# Check pipeline ran successfully
python scripts/monitor_pipeline.py --status

# View data quality metrics
python scripts/monitor_pipeline.py --quality

# Check warehouse (MinIO in this case)
# Navigate to http://localhost:9001/browser
# Bucket: processed-data
# Folder: transformed/
```

## Troubleshooting

### Airflow not starting
```bash
# Check logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver

# Reset Airflow database (⚠️ destroys data)
docker-compose down -v
docker-compose up -d
```

### Spark job failures
```bash
# Check Spark master logs
docker-compose logs spark-master

# Check worker resources
docker stats pipeline-spark-worker

# Increase memory if needed
# Edit docker-compose.yml: SPARK_WORKER_MEMORY: 4G (was 2G)
```

### PostgreSQL connection issues
```bash
# Verify database is up
docker-compose exec postgres pg_isready

# Reset data (⚠️ careful)
docker-compose down -v
docker-compose up postgres -d
docker-compose up -d
```

### Out of disk space
```bash
# Clean up old logs
docker exec pipeline-airflow-scheduler find /opt/airflow/logs -mtime +7 -delete

# Clear MinIO test files
# Via UI: http://localhost:9001

# Prune Docker volumes
docker system prune -a --volumes
```

## Performance Tuning

### For Local Testing (Small Data)
```yaml
# docker-compose.yml
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: 2G
    SPARK_WORKER_CORES: 2
```

### For Production Simulation (Medium Data)
```yaml
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: 8G
    SPARK_WORKER_CORES: 4
```

### Increase Airflow parallelism
```bash
# Edit airflow/airflow.cfg
parallelism = 32
dag_concurrency = 16
max_active_tasks_per_dag = 16
```

## Common Issues & Solutions

### 1. "Address already in use"
```bash
# Port already taken
# Solution: Change port in docker-compose.yml or kill process
lsof -i :8080  # Find process
kill -9 <PID>
```

### 2. "Connection refused"
```bash
# Service not fully started
# Solution: Wait a bit, check healthchecks
docker-compose ps --no-trunc

# Check individual service
docker-compose exec postgres pg_isready -U airflow
```

### 3. "Memory error in Spark"
```bash
# Job exceeds memory
# Solution: Reduce data size or increase worker memory
python scripts/generate_test_data.py --size small
```

### 4. "Data not appearing in warehouse"
```bash
# Check MinIO bucket
docker-compose exec minio mc ls minio/processed-data

# Check Airflow logs
docker-compose logs airflow-scheduler | grep -i error
```

## Next Steps

1. **Understand the code**: Read src/ modules
2. **Customize config**: Edit config/pipeline_config.yaml
3. **Add your sources**: Extend src/ingestion/ with your APIs/databases
4. **Run tests**: `pytest tests/ -v`
5. **Deploy to cloud**: Follow DEPLOYMENT.md

## Getting Help

- **Logs**: `docker-compose logs -f <service>`
- **Metrics**: http://localhost:9090 (Prometheus)
- **Dashboards**: http://localhost:3000 (Grafana)
- **Documentation**: Check docs/ folder
