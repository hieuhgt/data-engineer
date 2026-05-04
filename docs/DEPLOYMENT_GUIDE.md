# Data Pipeline Deployment Guide

## 1. Local Development Setup (Docker Compose)

### Quick Start

```bash
cd /Users/hieuht/workspace/personal/data-engineer

# Copy environment template
cp .env.example .env

# Start services
docker-compose -f docker-compose.full.yml up -d

# Check services
docker-compose ps
```

### Services Available
- **Zookeeper**: Port 2181
- **Kafka**: Port 9092
- **Airflow Webserver**: http://localhost:8080
- **Spark Master**: http://localhost:8080 (wait, this will conflict!)
- **Postgres** (Airflow metadata): localhost:5432

### Local Testing Flow

```bash
# 1. Create test data
python scripts/generate_sample_data.py

# 2. Run producer
python kafka/producers/event_producer.py

# 3. Trigger Airflow DAG
# Go to http://localhost:8080
# Click "Trigger DAG" on daily_data_pipeline

# 4. Monitor in Spark UI
# http://localhost:4040

# 5. Verify output
python scripts/verify_output.py
```

---

## 2. AWS EKS Deployment

### Prerequisites

```bash
# Install AWS CLI, kubectl, helm
aws --version
kubectl version --client
helm version

# Configure AWS credentials
aws configure

# Create EKS cluster (or use existing)
eksctl create cluster --name data-pipeline --region us-east-1 --nodes 3
```

### Architecture on EKS

```
┌─ EKS Cluster (us-east-1) ─────────────────────────────────┐
│                                                             │
│  Namespace: data-pipeline                                  │
│  ├─ Airflow Deployment                                    │
│  │  ├─ airflow-webserver (1 pod)                         │
│  │  ├─ airflow-scheduler (1 pod)                         │
│  │  ├─ airflow-workers (3+ pods, KubernetesExecutor)    │
│  │  └─ postgres-statefulset (1 pod)                      │
│  │                                                         │
│  ├─ Kafka Cluster (StatefulSet)                          │
│  │  ├─ kafka-0, kafka-1, kafka-2                         │
│  │  ├─ zookeeper-statefulset                             │
│  │  └─ kafka-service (headless)                          │
│  │                                                         │
│  ├─ Spark Jobs (launched on-demand)                      │
│  │  ├─ spark-etl-job-1 (Pod)                             │
│  │  └─ spark-etl-job-2 (Pod)                             │
│  │                                                         │
│  ├─ Monitoring                                            │
│  │  ├─ Prometheus                                         │
│  │  └─ Grafana                                            │
│  │                                                         │
│  └─ CloudWatch Logs Integration                          │
│                                                             │
│  Ingress: airflow.company.com → webserver                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Step-by-Step EKS Deployment

#### 1. Create Namespace

```bash
kubectl create namespace data-pipeline
kubectl label namespace data-pipeline env=production
```

#### 2. Deploy Kafka

```bash
# Using Helm (recommended)
helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts
helm repo update

helm install kafka confluentinc/cp-kafka \
  --namespace data-pipeline \
  --values kubernetes/kafka/values.yaml

# Or use YAML manifests
kubectl apply -f kubernetes/kafka/

# Verify
kubectl get pods -n data-pipeline
kubectl logs -n data-pipeline kafka-0
```

#### 3. Deploy PostgreSQL (Airflow Metadata)

```bash
# Using StatefulSet for persistence
kubectl apply -f kubernetes/airflow/postgres-statefulset.yaml

# Create PersistentVolume (adjust storage class)
kubectl apply -f kubernetes/airflow/postgres-pv.yaml

# Verify
kubectl get pvc -n data-pipeline
```

#### 4. Deploy Airflow

```bash
# Create secrets for sensitive data
kubectl create secret generic airflow-secrets \
  --from-literal=AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())") \
  --from-literal=AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@postgres:5432/airflow \
  -n data-pipeline

# Deploy Airflow
kubectl apply -f kubernetes/airflow/airflow-deployment.yaml

# Wait for rollout
kubectl rollout status deployment/airflow-webserver -n data-pipeline

# Port forward to access UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline
# http://localhost:8080

# Default credentials: airflow / airflow
```

#### 5. Deploy ConfigMaps & Secrets

```bash
# Store pipeline configuration
kubectl apply -f kubernetes/configmaps/pipeline-config.yaml

# Store Kafka credentials
kubectl create secret generic kafka-credentials \
  --from-literal=bootstrap-servers=kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092 \
  -n data-pipeline
```

#### 6. Deploy Spark Jobs

```bash
# Create RBAC for Spark
kubectl apply -f kubernetes/spark/spark-rbac.yaml

# Deploy Spark job example
kubectl apply -f kubernetes/spark/spark-job.yaml

# Check logs
kubectl logs -f spark-etl-job-1 -n data-pipeline
```

### Verification

```bash
# Check all resources
kubectl get all -n data-pipeline

# Check Airflow UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline

# Check Kafka connectivity
kubectl run -it kafka-client --image=confluentinc/cp-kafka:latest \
  --restart=Never -n data-pipeline -- \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic events

# Check logs
kubectl logs -f deployment/airflow-scheduler -n data-pipeline
kubectl logs -f statefulset/kafka -n data-pipeline
```

---

## 3. Scaling & Performance

### Auto-scaling

```yaml
# HPA for Airflow workers
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-workers-hpa
  namespace: data-pipeline
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### Kafka Scaling

```bash
# Scale Kafka StatefulSet
kubectl scale statefulset kafka --replicas=5 -n data-pipeline

# Rebalance partitions (handled automatically)
```

### Storage

```yaml
# EBS Storage for Kafka persistence
storageClassName: ebs-gp3
volumeSize: 100Gi
```

---

## 4. Monitoring & Observability

### CloudWatch Integration

```bash
# Deploy CloudWatch agent
kubectl apply -f kubernetes/monitoring/cloudwatch-agent.yaml

# View logs in CloudWatch
aws logs tail /aws/eks/data-pipeline/airflow --follow
```

### Prometheus & Grafana

```bash
# Deploy Prometheus
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace

# Deploy Grafana dashboards for:
# - Airflow tasks, DAG runs, SLA
# - Kafka topics, lag, throughput
# - Spark jobs, executors, memory
# - Node health, pod resource usage
```

### Alerts

```yaml
# Example: Alert if pipeline fails
- alert: DataPipelineFailure
  expr: airflow_dag_failed_runs_total > 0
  for: 5m
  annotations:
    summary: "Data pipeline {{ $labels.dag_id }} failed"
```

---

## 5. CICD Integration

### GitHub Actions / GitLab CI

```yaml
name: Deploy Data Pipeline

on:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: pytest tests/

  build-and-deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Build Docker image
        run: docker build -t ${{ secrets.ECR_REGISTRY }}/data-pipeline:${{ github.sha }} .
      - name: Push to ECR
        run: docker push ${{ secrets.ECR_REGISTRY }}/data-pipeline:${{ github.sha }}
      - name: Update EKS deployment
        run: |
          kubectl set image deployment/airflow \
            airflow=${{ secrets.ECR_REGISTRY }}/data-pipeline:${{ github.sha }} \
            -n data-pipeline
```

---

## 6. Cost Optimization

### Reserved Capacity
```bash
# Use spot instances for batch processing
eksctl create nodegroup --cluster data-pipeline \
  --name spot-workers \
  --spot \
  --instance-types m5.large,m5.xlarge
```

### Resource Limits

```yaml
# Set pod resource requests/limits
resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 2Gi
```

---

## 7. Troubleshooting

### Common Issues

#### Issue: Spark job fails to start
```bash
# Check logs
kubectl logs -f <spark-pod> -n data-pipeline

# Check resource availability
kubectl describe nodes | grep -A 5 "Allocatable"

# Increase cluster nodes
eksctl scale nodegroup --cluster data-pipeline --name workers --nodes 5
```

#### Issue: Kafka broker not available
```bash
# Check Kafka health
kubectl exec kafka-0 -n data-pipeline -- \
  kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Check Zookeeper
kubectl logs -f zookeeper-0 -n data-pipeline
```

#### Issue: Airflow tasks not executing
```bash
# Check scheduler
kubectl logs -f deployment/airflow-scheduler -n data-pipeline

# Check worker logs
kubectl logs -f deployment/airflow-worker -n data-pipeline

# Verify DAG syntax
airflow dags list
```

---

## 8. Production Checklist

- [ ] Namespace created with RBAC
- [ ] Secrets stored in AWS Secrets Manager
- [ ] Persistent volumes configured
- [ ] Backups enabled (Kafka, PostgreSQL)
- [ ] Monitoring & alerting configured
- [ ] HTTPS/TLS enabled
- [ ] Network policies configured
- [ ] Resource quotas set per namespace
- [ ] Auto-scaling configured
- [ ] Disaster recovery plan documented
- [ ] Load testing completed
- [ ] Security scan passed (Trivy, Snyk)

---

## 9. Cleanup

```bash
# Delete EKS cluster
eksctl delete cluster --name data-pipeline --region us-east-1

# Delete S3 buckets (if created)
aws s3 rm s3://company-datalake --recursive

# Delete RDS (if used for warehouse)
aws rds delete-db-instance --db-instance-identifier data-warehouse --skip-final-snapshot
```

