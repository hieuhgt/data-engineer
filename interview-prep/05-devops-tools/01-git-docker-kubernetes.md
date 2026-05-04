# DevOps for Data Engineers: Git, Docker, Kubernetes

## 1. Git Best Practices

### Commit Strategy (Atomic Commits)

```bash
# ❌ BAD: One commit with everything
git commit -m "add pipeline" -A

# ✅ GOOD: Logical, reviewable commits
git add src/ingestion/api_fetcher.py
git commit -m "feat: add exponential backoff to API fetching"

git add src/validation/schema.py
git commit -m "feat: add schema validation for orders"

git add tests/test_api_fetcher.py
git commit -m "test: add retry tests for API fetcher"
```

**Each commit should:**
- Be logically related (one feature/fix per commit)
- Have tests passing
- Have clear message describing the "why"

### Branch Strategy (Git Flow)

```bash
# Main branches
main/            # Production-ready code
  └─ staging/    # Pre-production testing

# Feature branches
feature/async-ingest           # New feature
feature/sql-optimization       # Enhancement
bugfix/duplicate-detection     # Bug fix
hotfix/api-timeout-crash       # Urgent production fix

# Naming convention: type/brief-description

# Creating feature branch
git checkout -b feature/async-ingest
# ... make changes ...
git push origin feature/async-ingest
# ... create PR on GitHub ...
```

### Code Review Checklist

```markdown
# PR Checklist (for reviewer)
- [ ] Code is readable and well-structured
- [ ] Tests cover new functionality (unit + integration)
- [ ] No hardcoded credentials or secrets
- [ ] Handles edge cases (nulls, empty data, timeouts)
- [ ] Error messages are helpful for debugging
- [ ] Performance impact considered (any slow loops?)
- [ ] Data quality validated before warehouse load
- [ ] Documentation updated if needed
```

### Common Git Mistakes (and fixes)

```bash
# ❌ Committed credentials
git rm --cached .env
echo '.env' >> .gitignore
git commit --amend --no-edit  # Update commit

# ❌ Wrong branch
git stash                     # Save changes
git checkout correct-branch   # Switch branch
git stash pop                 # Apply changes

# ❌ Merge conflict
# Fix conflicts manually, then:
git add resolved_file.py
git commit -m "resolve merge conflict"

# ❌ Need to undo last commit
git reset --soft HEAD~1       # Undo commit, keep changes
# or
git revert HEAD               # Create new commit that reverts changes
```

---

## 2. Docker for Data Pipelines

### Dockerfile Structure

```dockerfile
# ✅ GOOD: Multi-stage build (smaller final image)
FROM python:3.10-slim as builder

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/

# Final stage (lightweight)
FROM python:3.10-slim
WORKDIR /app

# Copy only needed files from builder
COPY --from=builder /app /app

# Non-root user (security)
RUN useradd -m datauser
USER datauser

# Run pipeline
CMD ["python", "-m", "pipeline.main"]
```

**Key Points:**
- ✅ Multi-stage: Reduces final image size (1GB → 500MB)
- ✅ Non-root user: Security (can't access host system)
- ✅ Specific Python version: Reproducible builds

### Building & Running

```bash
# Build image with version tag
docker build -t data-pipeline:1.0.0 .

# Run container (with volume mount for data)
docker run \
  -v /local/data:/app/data \  # Mount local data directory
  -e DATABASE_URL="postgresql://..."  # Pass secrets via env
  data-pipeline:1.0.0

# Check logs
docker logs container_id
docker logs -f container_id  # Follow logs

# Debug: Interactive shell
docker run -it data-pipeline:1.0.0 /bin/bash
```

### Docker Compose (Multi-Service)

```yaml
# docker-compose.yml
version: '3.8'

services:
  # Pipeline service
  pipeline:
    build: .
    environment:
      DATABASE_URL: postgresql://user:pass@postgres:5432/analytics
      KAFKA_BROKERS: kafka:9092
    depends_on:
      - postgres
      - kafka
    volumes:
      - ./data:/app/data
    restart: unless-stopped

  # PostgreSQL (test database)
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
      POSTGRES_DB: analytics
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  # Kafka (test message broker)
  kafka:
    image: confluentinc/cp-kafka:7.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper

  zookeeper:
    image: confluentinc/cp-zookeeper:7.0
    ports:
      - "2181:2181"

volumes:
  postgres_data:

# Run all services
# docker-compose up

# Run specific service
# docker-compose up pipeline

# Cleanup
# docker-compose down -v
```

---

## 3. Kubernetes Basics (EKS)

### Deployment Manifest

```yaml
# pipeline-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-pipeline
spec:
  replicas: 3  # Run 3 instances for HA
  selector:
    matchLabels:
      app: data-pipeline
  template:
    metadata:
      labels:
        app: data-pipeline
    spec:
      containers:
      - name: pipeline
        image: myrepo/data-pipeline:1.0.0
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-secrets
              key: url
        livenessProbe:  # Restart if unhealthy
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:  # Remove from load balancer if not ready
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5

---
# Service (expose pipeline internally)
apiVersion: v1
kind: Service
metadata:
  name: data-pipeline-service
spec:
  selector:
    app: data-pipeline
  ports:
  - protocol: TCP
    port: 8080
    targetPort: 8080
  type: ClusterIP
```

### Deploying to EKS

```bash
# Apply deployment
kubectl apply -f pipeline-deployment.yaml

# Check status
kubectl get pods
kubectl describe pod <pod_name>
kubectl logs <pod_name>

# Scale pipeline
kubectl scale deployment data-pipeline --replicas=5

# Update image (rolling update)
kubectl set image deployment/data-pipeline \
  pipeline=myrepo/data-pipeline:1.1.0

# Rollback if issues
kubectl rollout undo deployment/data-pipeline
```

### CronJob (Scheduled Pipeline)

```yaml
# schedule-pipeline.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-analytics-pipeline
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM UTC
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: pipeline
            image: myrepo/data-pipeline:1.0.0
            command: ["python", "-m", "pipeline.main"]
          restartPolicy: OnFailure
      backoffLimit: 3  # Retry 3 times on failure
```

---

## 4. CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/build-and-deploy.yml
name: Build and Deploy

on:
  push:
    branches: [main, staging]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s

    steps:
    - uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.10'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov

    - name: Run tests
      run: pytest tests/ --cov=src --cov-report=xml

    - name: Check code quality
      run: |
        pip install flake8 black
        flake8 src/ --max-line-length=100
        black --check src/

  build:
    needs: test
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    steps:
    - uses: actions/checkout@v2

    - name: Build Docker image
      run: docker build -t myrepo/data-pipeline:${{ github.sha }} .

    - name: Push to registry
      run: |
        docker login -u ${{ secrets.DOCKER_USER }} -p ${{ secrets.DOCKER_PASS }}
        docker push myrepo/data-pipeline:${{ github.sha }}
        docker tag myrepo/data-pipeline:${{ github.sha }} myrepo/data-pipeline:latest
        docker push myrepo/data-pipeline:latest

  deploy:
    needs: build
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'

    steps:
    - name: Deploy to EKS
      run: |
        aws eks update-kubeconfig --name production
        kubectl set image deployment/data-pipeline \
          pipeline=myrepo/data-pipeline:${{ github.sha }} \
          --record
```

---

## 5. Monitoring & Logging

### Logging Best Practices

```python
import logging
import json
from datetime import datetime

# Structured logging (JSON for easy parsing)
class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        })

# Setup
logger = logging.getLogger('pipeline')
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)

# Usage
logger.info('Pipeline started', extra={'rows_to_process': 1000})
logger.warning('Retry attempt', extra={'attempt': 2, 'url': 'api.example.com'})
logger.error('Failed to ingest', extra={'source': 'api', 'error': 'timeout'})
```

### Key Metrics to Monitor

```bash
# Pipeline health
- Pipeline duration (should be < 4 hours)
- Success rate (should be > 99%)
- Data quality score (should be > 95%)

# Data freshness
- Last successful run timestamp
- Rows ingested per source
- Duplicate detection rate

# System health
- CPU usage
- Memory usage
- Disk space remaining

# Alerting rules
- Pipeline took > 5 hours → Alert
- Success rate < 95% → Alert
- Data quality < 90% → Alert
- No data for 6+ hours → Alert
```

---

## Interview Question

**Q: You deployed a pipeline update that crashed in production. How would you roll back?**

**Answer:**

1. **Immediate:** Check what failed
   ```bash
   kubectl logs pod_name
   kubectl describe pod pod_name
   ```

2. **Rollback:**
   ```bash
   kubectl rollout undo deployment/data-pipeline
   # This reverts to previous image
   ```

3. **Root cause:** Read code diff
   ```bash
   git log --oneline -5
   git diff HEAD~1 HEAD src/
   ```

4. **Prevent future issues:**
   - Add more thorough tests (unit + integration)
   - Use canary deploy (1% of traffic first)
   - Add healthcheck that catches errors
   - Require code review before prod deploy

**Testing before deploy:**
```bash
# 1. Local testing
pytest tests/ --cov=80

# 2. Build Docker image
docker build -t pipeline:test .

# 3. Test with docker-compose
docker-compose up
# Verify works with real Postgres/Kafka

# 4. Deploy to staging (replica of prod)
kubectl apply -f pipeline-deployment.yaml -n staging

# 5. Smoke test on staging
curl staging-pipeline.internal/health
# Check data quality metrics

# 6. Deploy to prod (if staging passed)
kubectl apply -f pipeline-deployment.yaml -n production
```
