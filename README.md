# Data Pipeline Engineering - Complete Guide

A comprehensive, production-ready data pipeline implementation using Spark, Kafka, Airflow, and Kubernetes/EKS.

## 📁 Project Structure

```
data-engineer/
├── docs/
│   ├── PIPELINE_ARCHITECTURE.md     # Architecture patterns & best practices
│   └── DEPLOYMENT_GUIDE.md          # EKS deployment instructions
│
├── spark/
│   ├── jobs/
│   │   ├── ingest_kafka.py          # Kafka → Bronze layer
│   │   ├── bronze_to_silver.py      # Clean & transform data
│   │   └── silver_to_gold.py        # Aggregate & enrich
│   ├── Dockerfile
│   └── requirements.txt
│
├── kafka/
│   ├── producers/
│   │   └── event_producer.py        # Produce test events
│   ├── consumers/
│   │   └── event_consumer.py        # Consume events
│   ├── schemas/
│   │   └── events.avsc              # Avro schema
│   └── docker-compose.yml
│
├── airflow/
│   ├── dags/
│   │   ├── daily_data_pipeline_dag.py       # Main pipeline DAG
│   │   └── multi_stage_pipeline_dag.py      # Advanced patterns
│   ├── plugins/
│   │   ├── operators/
│   │   └── sensors/
│   ├── Dockerfile
│   └── requirements.txt
│
├── kubernetes/
│   ├── airflow/
│   │   ├── airflow-deployment.yaml
│   │   └── postgres-statefulset.yaml
│   ├── spark/
│   │   ├── spark-job.yaml
│   │   └── spark-rbac.yaml
│   ├── configmaps/
│   │   └── pipeline-config.yaml
│   └── namespace.yaml
│
├── config.py                # Centralized configuration
├── logger_setup.py          # Logging configuration
├── requirements.txt         # Python dependencies
├── docker-compose.full.yml  # Full stack for local dev
├── Dockerfile              # Spark Docker image
├── .env.example            # Environment template
├── BEST_PRACTICES.md       # Detailed best practices guide
└── README.md              # This file
```

---

## 🚀 Quick Start

### 1. Local Development (Docker Compose)

```bash
# Setup
cp .env.example .env
docker-compose -f docker-compose.full.yml up -d

# Wait for services to start
sleep 30

# Access services
# Airflow UI:        http://localhost:8080 (airflow / airflow)
# Spark Master UI:   http://localhost:8081
# Grafana:           http://localhost:3000 (admin / admin)
# Kafka:             localhost:9092
```

### 2. Test the Pipeline

```bash
# Produce test events
python kafka/producers/event_producer.py

# Check in Airflow UI:
# 1. Go to http://localhost:8080
# 2. Find 'daily_data_pipeline' DAG
# 3. Click "Trigger DAG"
# 4. Monitor execution

# View results
python scripts/verify_output.py
```

### 3. Check Logs

```bash
# Airflow scheduler
docker-compose -f docker-compose.full.yml logs -f airflow-scheduler

# Spark
docker-compose -f docker-compose.full.yml logs -f spark

# Kafka
docker-compose -f docker-compose.full.yml logs -f kafka
```

---

## 📊 Data Pipeline Flow

```
Events/Data Source
    ↓
Kafka (Streaming Queue)
    ↓
[Airflow Orchestration]
    ↓
Spark Jobs (Processing)
    ├── Bronze (Raw) → Kafka → ingest_kafka.py
    ├── Silver (Cleaned) → bronze_to_silver.py
    └── Gold (Aggregated) → silver_to_gold.py
    ↓
S3/Data Lake
    └── /bronze/ → /silver/ → /gold/
    ↓
Analytics / BI / ML
```

### Task Dependencies

```
Validate Inputs
    ↓
Ingest Kafka → Bronze
    ↓
Transform Bronze → Silver
    ↓
Aggregate Silver → Gold
    ↓
Quality Checks
    ↓
Success/Failure Notification
```

---

## 🔧 Configuration

### Environment Variables

Create `.env` from template:
```bash
cp .env.example .env
```

### Key Variables
```env
SPARK_MASTER=local[4]
KAFKA_BROKERS=localhost:9092
S3_BUCKET=company-datalake
ENVIRONMENT=development
DEBUG=false
```

---

## 🐳 Docker

### Build Images

```bash
# Spark image
cd spark
docker build -t data-pipeline/spark:latest .

# Airflow image (uses official Apache image)
```

### Run Individual Containers

```bash
# Spark job
docker run -e BRONZE_PATH=s3://... \
  -e SILVER_PATH=s3://... \
  data-pipeline/spark:latest

# Kafka producer
docker run -e KAFKA_BROKERS=kafka:9092 \
  data-pipeline/producer:latest
```

---

## ☸️ Kubernetes/EKS Deployment

### Prerequisites
```bash
aws --version
kubectl version --client
helm version
eksctl --version
```

### Deploy to EKS

```bash
# 1. Create EKS cluster
eksctl create cluster --name data-pipeline --region us-east-1 --nodes 3

# 2. Create namespace
kubectl apply -f kubernetes/namespace.yaml

# 3. Create secrets
kubectl create secret generic aws-credentials \
  --from-literal=access-key=YOUR_KEY \
  --from-literal=secret-key=YOUR_SECRET \
  -n data-pipeline

# 4. Deploy Kafka
kubectl apply -f kubernetes/kafka/

# 5. Deploy PostgreSQL
kubectl apply -f kubernetes/airflow/postgres-statefulset.yaml

# 6. Deploy Airflow
kubectl apply -f kubernetes/airflow/airflow-deployment.yaml

# 7. Deploy Spark RBAC
kubectl apply -f kubernetes/spark/spark-rbac.yaml

# 8. Verify
kubectl get all -n data-pipeline
```

### Access Airflow on EKS

```bash
# Port forward
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline

# Or create Ingress (see DEPLOYMENT_GUIDE.md)
```

---

## 🧪 Testing

### Unit Tests (Spark Transformations)
```bash
pytest tests/test_pipeline.py -v
```

### Integration Tests
```bash
# Start local services
docker-compose -f docker-compose.full.yml up -d

# Run integration tests
pytest tests/integration/

# Cleanup
docker-compose -f docker-compose.full.yml down
```

### Data Quality Tests
```bash
# Using Great Expectations
python scripts/data_quality_checks.py
```

---

## 📈 Monitoring

### Airflow Monitoring
- Dashboard: http://localhost:8080
- Monitor DAG runs, task status, SLA
- Alert on failures

### Spark Monitoring
- UI: http://localhost:4040 (running job)
- Master UI: http://localhost:8081
- Check executor memory, task duration

### Prometheus & Grafana
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Dashboards for pipeline health

### CloudWatch (EKS)
```bash
# View logs in CloudWatch
aws logs tail /aws/eks/data-pipeline/airflow --follow

# CloudWatch metrics for pod resources
```

---

## 🔍 Troubleshooting

### Common Issues

#### Spark job fails
```bash
# Check logs
docker-compose logs spark

# Verify Kafka connectivity
docker-compose exec spark \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic events
```

#### Airflow DAG not running
```bash
# Check scheduler
docker-compose logs airflow-scheduler

# Verify DAG syntax
docker-compose exec airflow-scheduler airflow dags list

# Check for parsing errors
docker-compose exec airflow-scheduler airflow dags test daily_data_pipeline 2024-01-01
```

#### Kafka connection timeout
```bash
# Check Kafka health
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check connectivity
docker-compose exec spark nc -zv kafka 9092
```

---

## 📚 Documentation

- **PIPELINE_ARCHITECTURE.md** - Architecture patterns, medallion model, layer explanations
- **DEPLOYMENT_GUIDE.md** - EKS setup, scaling, monitoring, troubleshooting
- **BEST_PRACTICES.md** - Code patterns, performance tips, common gotchas

---

## 🎯 Next Steps

1. **Understand Architecture** → Read `docs/PIPELINE_ARCHITECTURE.md`
2. **Run Locally** → Follow "Quick Start" above
3. **Modify DAGs** → Edit `airflow/dags/daily_data_pipeline_dag.py`
4. **Deploy to EKS** → Follow `docs/DEPLOYMENT_GUIDE.md`
5. **Add Data Quality** → Integrate Great Expectations
6. **Monitor Production** → Setup CloudWatch alerts

---

## 📊 Real-World Enhancements

Ready to add? These are common next steps:

```python
# 1. Data Quality Framework
# - Great Expectations for validation
# - Custom quality metrics

# 2. Feature Store
# - ML-ready aggregations
# - Point-in-time correctness

# 3. Real-time Serving
# - RediSearch for hot queries
# - Stream aggregations

# 4. Advanced Scheduling
# - Dynamic DAG generation
# - Cross-DAG dependencies
# - SLA management

# 5. Cost Optimization
# - Spot instances on EKS
# - Auto-scaling policies
# - Resource quotas
```

---

## 🤝 Contributing

See BEST_PRACTICES.md for code standards before submitting changes.

---

## 📝 License

Internal use - Data Engineering Team

---

## 🆘 Support

- Check troubleshooting section
- Review logs in respective services
- Consult BEST_PRACTICES.md for patterns
