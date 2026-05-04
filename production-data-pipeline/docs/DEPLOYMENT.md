# Deployment

## Environments

| Environment | Orchestrator | Warehouse | Storage | Notes |
|---|---|---|---|---|
| Local | Docker Compose | Parquet files | MinIO | `scripts/setup.sh` |
| Staging | Kubernetes (EKS) | Snowflake dev DB | S3 staging bucket | Mirror of prod with sampled data |
| Production | Kubernetes (EKS) | Snowflake prod DB | S3 production bucket | 99.9 % SLA target |

---

## Local (Docker Compose)

```bash
cp config/env.example .env  # fill in values
bash scripts/setup.sh       # one-time setup
docker-compose up -d        # start all services
```

Services available after startup:

| Service | URL |
|---|---|
| Airflow | http://localhost:8080 (admin/admin) |
| Spark UI | http://localhost:8088 |
| MinIO | http://localhost:9001 (minioadmin/minioadmin) |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

---

## Staging / Production (AWS EKS)

### Prerequisites

- AWS CLI configured with deployment role
- `kubectl` connected to the target cluster
- Helm 3.x
- Secrets loaded into AWS Secrets Manager

### 1. Build and push Docker image

```bash
IMAGE="123456789.dkr.ecr.us-east-1.amazonaws.com/data-pipeline"
TAG=$(git rev-parse --short HEAD)

aws ecr get-login-password | docker login --username AWS --password-stdin "$IMAGE"
docker build -t "$IMAGE:$TAG" -t "$IMAGE:latest" .
docker push "$IMAGE:$TAG"
docker push "$IMAGE:latest"
```

### 2. Deploy Airflow (via Helm)

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow \
  --namespace pipeline \
  --create-namespace \
  --values deploy/helm/airflow-values.yaml \
  --set images.airflow.repository="$IMAGE" \
  --set images.airflow.tag="$TAG"
```

### 3. Deploy Spark on Kubernetes

```bash
spark-submit \
  --master k8s://https://your-eks-api-endpoint \
  --deploy-mode cluster \
  --name daily-transform \
  --conf spark.kubernetes.container.image="$IMAGE:$TAG" \
  --conf spark.kubernetes.namespace=pipeline \
  --conf spark.executor.instances=5 \
  spark/transform_daily.py --date $(date +%Y-%m-%d)
```

### 4. Environment variables (Kubernetes Secrets)

```bash
kubectl create secret generic pipeline-secrets \
  --namespace pipeline \
  --from-env-file .env
```

Reference in pod spec:
```yaml
envFrom:
  - secretRef:
      name: pipeline-secrets
```

---

## CI/CD (GitHub Actions)

The pipeline runs on every push to `main`:

1. **Install deps** — `poetry install --with dev`
2. **Lint** — `poetry run ruff check src/ tests/`
3. **Type check** — `poetry run mypy src/`
4. **Unit tests** — `poetry run pytest tests/ -v --ignore=tests/integration`
5. **Integration tests** — `poetry run pytest tests/integration/ -v`
6. **Build image** — `docker build`
7. **Push to ECR** — on `main` branch only
8. **Deploy to staging** — Helm upgrade, runs smoke test DAG
9. **Deploy to production** — Manual approval gate, then Helm upgrade

---

## Rollback

```bash
# List Helm releases
helm history airflow -n pipeline

# Roll back to previous revision
helm rollback airflow -n pipeline

# Or deploy a specific image tag
helm upgrade airflow apache-airflow/airflow \
  --namespace pipeline \
  --reuse-values \
  --set images.airflow.tag=abc1234
```

---

## Scaling

### Add more Spark executors

Update `SPARK_EXECUTOR_INSTANCES` in the Helm values or pass `--conf spark.executor.instances=N` at submit time.

### Add more Airflow workers

```bash
helm upgrade airflow apache-airflow/airflow \
  --namespace pipeline \
  --reuse-values \
  --set workers.replicas=5
```

### Add a new data source

1. Add a source block to `config/pipeline_config.yaml`
2. Implement a connector in `src/ingestion/` (subclass `DataConnector`)
3. Add an ingest task to `airflow/dags/daily_batch_pipeline.py`
4. Add unit tests in `tests/test_ingestion.py`
5. Deploy — no infrastructure changes needed
