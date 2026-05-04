# Project Structure Guide

## Complete Directory Layout

```
data-engineer/
│
├── 📄 README.md                        ← Start here! Project overview
├── 📄 WALKTHROUGH.md                   ← Step-by-step guide (YOU ARE HERE)
├── 📄 BEST_PRACTICES.md                ← Code patterns & optimization tips
├── 📄 PROJECT_STRUCTURE.md             ← This file
│
├── 📁 docs/
│   ├── PIPELINE_ARCHITECTURE.md        ← Architecture patterns & layers
│   └── DEPLOYMENT_GUIDE.md             ← EKS deployment instructions
│
├── 📁 spark/                           ← PySpark ETL Jobs
│   ├── 📁 jobs/
│   │   ├── ingest_kafka.py            ← Kafka → Bronze layer
│   │   ├── bronze_to_silver.py        ← Clean & deduplicate
│   │   └── silver_to_gold.py          ← Aggregate & enrich
│   ├── Dockerfile                      ← Build Spark image
│   ├── requirements.txt                ← Python deps for Spark
│   └── 📄 README.md                    ← Spark job docs
│
├── 📁 kafka/                           ← Kafka Producer/Consumer
│   ├── 📁 producers/
│   │   └── event_producer.py          ← Generate test events
│   ├── 📁 consumers/
│   │   └── event_consumer.py          ← Consume & display events
│   ├── 📁 schemas/
│   │   └── events.avsc                ← Avro schema definition
│   ├── 📁 examples/
│   │   ├── level1_junior.py           ← 7 basic Kafka examples
│   │   ├── level2_intermediate.py     ← 8 intermediate examples
│   │   ├── level3_advanced.py         ← 7 advanced examples
│   │   └── level4_senior.py           ← Production patterns
│   ├── 📄 KAFKA_COMPLETE_GUIDE.md     ← 4-level Kafka reference
│   ├── 📄 KAFKA_LEARNING_PATH.md      ← 20-week Kafka roadmap
│   ├── 📄 KAFKA_SUMMARY.md            ← Quick reference
│   └── docker-compose.yml              ← Local Kafka setup
│
├── 📁 pandas/                          ← Pandas Data Manipulation (NEW!)
│   ├── 📁 examples/
│   │   ├── level1_fundamentals.py     ← Series, DataFrames, basic ops
│   │   ├── level2_intermediate.py     ← Cleaning, groupby, merging
│   │   ├── level3_advanced.py         ← Time series, windows, custom ops
│   │   └── level4_mastery.py          ← Optimization, production patterns
│   ├── 📄 README.md                   ← Pandas materials overview
│   ├── 📄 PANDAS_COMPLETE_GUIDE.md    ← 4-level Pandas reference (2600+ lines)
│   └── 📄 PANDAS_LEARNING_PATH.md     ← 20-week structured learning roadmap
│
├── 📁 airflow/                         ← Apache Airflow Orchestration
│   ├── 📁 dags/
│   │   ├── daily_data_pipeline_dag.py        ← Main pipeline DAG
│   │   └── multi_stage_pipeline_dag.py       ← Advanced patterns
│   ├── 📁 plugins/
│   │   ├── 📁 operators/               ← Custom operators
│   │   └── 📁 sensors/                 ← Custom sensors
│   ├── 📁 logs/                        ← Airflow logs (gitignored)
│   ├── Dockerfile                      ← Airflow image
│   ├── requirements.txt                ← Airflow + providers
│   └── 📄 README.md                    ← Airflow setup guide
│
├── 📁 kubernetes/                      ← EKS/K8s Manifests
│   ├── namespace.yaml                  ← Create namespace
│   ├── 📁 airflow/
│   │   ├── airflow-deployment.yaml    ← Webserver + Scheduler
│   │   └── postgres-statefulset.yaml  ← Metadata database
│   ├── 📁 spark/
│   │   ├── spark-job.yaml             ← Spark job template
│   │   └── spark-rbac.yaml            ← RBAC for Spark
│   ├── 📁 kafka/
│   │   ├── kafka-statefulset.yaml
│   │   └── zookeeper-statefulset.yaml
│   ├── 📁 configmaps/
│   │   └── pipeline-config.yaml       ← Config + Secrets
│   └── 📁 monitoring/
│       └── prometheus.yml              ← Monitoring config
│
├── 📁 scripts/                         ← Utility Scripts
│   ├── generate_sample_data.py        ← Create test data
│   ├── verify_output.py               ← Check pipeline results
│   └── data_quality_checks.py         ← Validate data
│
├── 📁 tests/                           ← Unit & Integration Tests
│   ├── test_pipeline.py               ← Spark transformation tests
│   └── 📁 integration/
│       └── test_end_to_end.py         ← Full pipeline tests
│
├── 📁 data/                            ← Local test data (gitignored)
│   ├── sample.csv
│   ├── sample.jsonl
│   └── sample.parquet
│
├── 📁 output/                          ← Pipeline output (gitignored)
│   ├── processed/
│   └── ...
│
├── 📁 logs/                            ← Execution logs (gitignored)
│   └── pipeline.log
│
├── 📁 monitoring/                      ← Prometheus/Grafana config
│   └── prometheus.yml
│
├── 🔧 config.py                        ← Centralized configuration
├── 🔧 logger_setup.py                  ← Logging configuration
├── 📄 requirements.txt                 ← Main Python dependencies
├── 📄 Dockerfile                       ← Spark base image
├── 📄 docker-compose.yml               ← Simple local setup
├── 📄 docker-compose.full.yml          ← Full stack (Kafka + Airflow)
├── 📄 .env.example                     ← Environment template
├── 📄 .gitignore                       ← Git ignore rules
└── 📄 .dockerignore                    ← Docker ignore rules
```

---

## What Each Layer Does

### Layer 1: Configuration & Setup
```
config.py              ← Environment variables & dataclass configs
logger_setup.py        ← Structured logging with rotation
requirements.txt       ← Python package versions
.env.example          ← Environment template
```

### Layer 2: Data Ingestion
```
kafka/
  ├── producers/event_producer.py    ← Send events to Kafka
  ├── consumers/event_consumer.py    ← Read from Kafka
  └── schemas/events.avsc           ← Event format definition
```

### Layer 3: Processing
```
spark/jobs/
  ├── ingest_kafka.py       ← Kafka → Bronze (Raw data)
  ├── bronze_to_silver.py   ← Clean & transform
  └── silver_to_gold.py     ← Aggregate & optimize
```

### Layer 4: Orchestration
```
airflow/dags/
  ├── daily_data_pipeline_dag.py     ← Schedule Bronze→Silver→Gold
  └── multi_stage_pipeline_dag.py    ← Advanced patterns
```

### Layer 5: Infrastructure (Local)
```
docker-compose.full.yml  ← Define services (Kafka, Airflow, Spark, etc)
```

### Layer 6: Infrastructure (Production)
```
kubernetes/
  ├── airflow/          ← Deploy Airflow to K8s
  ├── spark/            ← Deploy Spark jobs to K8s
  ├── kafka/            ← Deploy Kafka to K8s
  └── configmaps/       ← Configuration & secrets
```

### Layer 7: Utilities
```
scripts/
  ├── generate_sample_data.py  ← Create test CSV/Parquet files
  ├── verify_output.py        ← Check what pipeline created
  └── data_quality_checks.py  ← Validate outputs
```

### Layer 8: Testing
```
tests/
  ├── test_pipeline.py        ← Test Spark transformations
  └── integration/            ← End-to-end tests
```

---

## File Relationships

### Configuration Flow
```
.env.example
    ↓
.env (your custom config)
    ↓
config.py (loads and validates)
    ↓
Used by: spark/jobs, kafka, airflow/dags
```

### Data Flow
```
Kafka (raw events)
    ↓ [ingest_kafka.py]
Bronze Layer (s3://bucket/bronze/)
    ↓ [bronze_to_silver.py]
Silver Layer (s3://bucket/silver/)
    ↓ [silver_to_gold.py]
Gold Layer (s3://bucket/gold/)
    ↓
Analytics / BI / ML
```

### Orchestration Flow
```
daily_data_pipeline_dag.py (Airflow DAG)
    ├── Task 1: validate_inputs (PythonOperator)
    ├── Task 2: ingest_kafka_to_bronze (SparkSubmitOperator)
    │   ↓ runs → spark/jobs/ingest_kafka.py
    ├── Task 3: bronze_to_silver (SparkSubmitOperator)
    │   ↓ runs → spark/jobs/bronze_to_silver.py
    ├── Task 4: silver_to_gold (SparkSubmitOperator)
    │   ↓ runs → spark/jobs/silver_to_gold.py
    ├── Task 5: quality_checks (PythonOperator)
    └── Task 6: notify (PythonOperator)
```

---

## How to Navigate

### "How do I modify X?"

| Objective | Edit File | Section |
|-----------|-----------|---------|
| Change Spark memory | `config.py` | `SparkConfig.memory` |
| Add transformation | `spark/jobs/bronze_to_silver.py` | `clean_data()` function |
| Change DAG schedule | `airflow/dags/daily_data_pipeline_dag.py` | `schedule_interval` parameter |
| Add Kafka config | `.env` | `KAFKA_BROKERS=...` |
| Change logging output | `logger_setup.py` | Handlers section |
| Deploy to Kubernetes | `kubernetes/` | See `DEPLOYMENT_GUIDE.md` |

### "Where do I find X?"

| Looking For | Location |
|------------|----------|
| Architecture explanation | `docs/PIPELINE_ARCHITECTURE.md` |
| Code best practices | `BEST_PRACTICES.md` |
| EKS setup instructions | `docs/DEPLOYMENT_GUIDE.md` |
| Step-by-step walkthrough | `WALKTHROUGH.md` |
| Spark job examples | `spark/jobs/*.py` |
| Airflow DAG examples | `airflow/dags/*.py` |
| Docker setup | `docker-compose.full.yml` |
| Kubernetes manifests | `kubernetes/` |
| Logging config | `logger_setup.py` |
| Environment config | `.env` (after copying from `.env.example`) |

---

## Development Workflow

### Day-to-Day Tasks

```
1. Modify code
   ├── spark/jobs/
   ├── airflow/dags/
   └── config.py / .env

2. Test locally
   docker-compose -f docker-compose.full.yml up -d
   python kafka/producers/event_producer.py
   trigger DAG in Airflow UI

3. Check results
   python scripts/verify_output.py
   tail -f logs/pipeline.log

4. Debug issues
   - Check Airflow UI (http://localhost:8080)
   - Check Spark UI (http://localhost:4040)
   - Check Kafka (kafka/consumers/event_consumer.py)

5. Commit changes
   git add .
   git commit -m "description"
   git push
```

---

## Quick Reference: Important Files

### Must Read First
1. `README.md` - Project overview
2. `WALKTHROUGH.md` - Step-by-step guide
3. `docs/PIPELINE_ARCHITECTURE.md` - How it works

### Key Implementation Files
1. `spark/jobs/*.py` - ETL logic
2. `airflow/dags/*.py` - Scheduling logic
3. `config.py` - Configuration
4. `kafka/producers/*.py` - Data source

### Deployment
1. `docker-compose.full.yml` - Local development
2. `kubernetes/` - Production on EKS
3. `docs/DEPLOYMENT_GUIDE.md` - How to deploy

### Testing & Verification
1. `scripts/generate_sample_data.py` - Create test data
2. `scripts/verify_output.py` - Check results
3. `tests/` - Unit tests

---

## File Sizes & Growth

| Component | Size | Growth | Notes |
|-----------|------|--------|-------|
| Spark code | ~2 KB each | Minimal | Add new jobs as needed |
| Airflow DAGs | ~5 KB each | Minimal | DAGs stay small |
| Kubernetes manifests | ~3 KB each | Minimal | Stable configuration |
| Logs | ~5 MB/day | Rotating | Auto-rotates, see logger_setup.py |
| Data output | ~50 MB/day | Configurable | Depends on data volume |
| Kafka storage | ~100 MB/day | Configurable | Retention policy in docker-compose.yml |

---

## Common Mistakes & How to Avoid

| Mistake | Prevention |
|---------|-----------|
| Hardcoded values | Use `config.py` and `.env` |
| Duplicate code | Extract to functions in shared modules |
| Missing error handling | See `BEST_PRACTICES.md` - Error Handling |
| Untracked credentials | Never commit `.env`, use `.env.example` |
| Slow Spark jobs | Profile with Spark UI, see Performance tips |
| Lost logs | Logging auto-rotates, check `logs/pipeline.log` |
| DAG test failures | Test with `airflow dags test <dag_id> <date>` |

---

## Next Steps

1. **Understand the flow**: Read `PIPELINE_ARCHITECTURE.md`
2. **Try it locally**: Follow `WALKTHROUGH.md`
3. **Write your own**: Modify `spark/jobs/bronze_to_silver.py`
4. **Deploy**: Follow `docs/DEPLOYMENT_GUIDE.md`

Happy data engineering! 🚀
