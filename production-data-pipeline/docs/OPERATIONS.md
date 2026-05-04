# Operations & Monitoring Guide

## Daily Operations

### Morning Checklist
```bash
# 1. Verify pipeline ran
python scripts/monitor_pipeline.py --status

# 2. Check data quality
python scripts/monitor_pipeline.py --quality

# 3. Review alerts
# Check Slack #data-alerts channel

# 4. Verify freshness
# Data should be < 1 hour old
```

### Monitoring Dashboard

**Prometheus** (http://localhost:9090)
- Query: `pipeline_duration_seconds` (should be < 14400s or 4 hours)
- Query: `quality_pass_rate` (should be > 0.95)
- Query: `rows_processed` (should be > 1M)

**Grafana** (http://localhost:3000)
- Dashboard: "Pipeline Health" → shows all key metrics
- Dashboard: "Cost Tracking" → compute & storage costs

### Alerting

**Critical Alerts** (Page on-call):
```
- Pipeline duration > 4 hours (SLA miss)
- Data quality < 95%
- No data for 6+ hours
```

**Warning Alerts** (Slack notification):
```
- 1+ source failed
- Quality score 85-95%
- Performance degradation
```

## Handling Failures

### Scenario 1: Source API Down

```bash
# Symptoms:
# - Airflow shows "api_users" task failed
# - Quality score dropped
# - Partial data loaded

# Solution:
# 1. Check API status with source team
# 2. Airflow automatically retries (3 times)
# 3. Pipeline continues with other sources
# 4. Check alert severity:
#    - 1-2 sources: WARNING (acceptable)
#    - 3+ sources: CRITICAL (escalate)

# Force retry:
airflow tasks clear daily_batch_pipeline \
  --task-id ingest_data \
  --downstream \
  --start-date 2024-01-15 \
  --end-date 2024-01-15
```

### Scenario 2: Data Quality Failure

```bash
# Symptoms:
# - Pipeline stops at validate_data
# - Alert: "Quality gate failed"
# - 85% of records passed

# Root cause analysis:
# 1. Check validation logs
docker-compose logs airflow-scheduler | grep "quality"

# 2. Investigate bad records
python scripts/analyze_failures.py --source api_events

# 3. Common fixes:
#    a) Schema changed at source → update config
#    b) Data corruption → investigate source
#    c) Rule too strict → relax threshold in config

# 4. Fix and backfill:
#    - Update config/pipeline_config.yaml
#    - Redeploy containers
#    - Trigger backfill for past dates
airflow dags backfill daily_batch_pipeline \
  --start-date 2024-01-01 \
  --end-date 2024-01-15
```

### Scenario 3: Spark Job Failure

```bash
# Symptoms:
# - Airflow task "transform" fails
# - Spark UI shows error
# - Alert: "Spark job failed"

# Solution:
# 1. Check Spark logs
docker-compose logs spark-master

# 2. Common issues:
#    a) Out of memory → increase SPARK_WORKER_MEMORY
#    b) Data skew → check partition strategy
#    c) Timeout → increase timeout in Spark config

# 3. Fix and retry:
#    - Edit docker-compose.yml or spark job
#    - Restart Spark:
docker-compose restart spark-master spark-worker

#    - Trigger backfill
```

### Scenario 4: Warehouse Not Accepting Data

```bash
# Symptoms:
# - Load task fails
# - Alert: "Warehouse connection failed"

# Solution:
# 1. Check warehouse health
#    - For MinIO: http://localhost:9001
#    - For Snowflake: Check Snowflake status page

# 2. Check credentials
docker-compose exec airflow-scheduler env | grep WAREHOUSE

# 3. Check logs
docker-compose logs airflow-scheduler | grep -i warehouse

# 4. Retry:
airflow tasks clear daily_batch_pipeline \
  --task-id load_data \
  --start-date 2024-01-15
```

## Performance Tuning

### Slow Ingestion (> 1 hour)

```bash
# Increase parallel connectors
# Edit: config/pipeline_config.yaml
# Increase: sources parallelism

# Or increase Airflow pool:
# UI: Admin → Pools → data_pipeline
# Change: 128 slots (was 32)

# Monitor:
python scripts/monitor_pipeline.py --timeline
```

### Slow Transform (> 1.5 hours)

```bash
# Increase Spark workers
# Edit: docker-compose.yml
# scale: docker-compose up -d --scale spark-worker=4

# Or:
# Edit Spark job parallelism
# In spark/transform_daily.py:
# spark.repartition(200)  # Increase partitions

# Monitor:
# Spark UI: http://localhost:8088
# Check: Executor memory, task times
```

### High Cost

```bash
# Check cost drivers:
python scripts/analyze_costs.py

# Optimizations:
# 1. Right-size executors (memory, cores)
# 2. Use spot instances (EC2 only)
# 3. Archive old data (S3 → Glacier)
# 4. Schedule less frequently (if acceptable)

# Example: Reduce cost 30%
# Before: 20 executors * 4GB each = 80GB
# After: 10 executors * 8GB each = 80GB
# Reason: Larger executors more efficient
```

## Maintenance

### Weekly

```bash
# Clean up old logs
docker exec pipeline-airflow-scheduler \
  find /opt/airflow/logs -mtime +7 -delete

# Check storage usage
docker-compose exec minio mc du minio/

# Review cost report
python scripts/generate_cost_report.py --period weekly
```

### Monthly

```bash
# Backup Airflow database
docker-compose exec postgres pg_dump -U airflow airflow > backup_airflow_$(date +%Y%m%d).sql

# Archive old data to cheaper storage
# S3 → S3 Glacier
python scripts/archive_old_data.py --older-than 30days

# Update dependencies
pip list --outdated
pip install --upgrade -r requirements.txt

# Run full test suite
pytest tests/ -v --cov=src
```

### Quarterly

```bash
# Update Docker images
docker-compose pull
docker-compose up -d

# Review and optimize configs
# - Check: pipeline_config.yaml
# - Review: alerts, thresholds
# - Adjust: for upcoming season/changes

# Disaster recovery drill
# 1. Backup current state
# 2. Simulate failure (delete warehouse data)
# 3. Test recovery (restore from backup)
# 4. Document recovery time
```

## Escalation

### Alert Severity & Escalation

| Severity | Alert | Escalation | Action |
|----------|-------|------------|--------|
| CRITICAL | SLA miss (> 4h) | Page on-call | Immediate debugging |
| CRITICAL | Quality < 90% | Page on-call | Investigate source |
| WARNING | Quality < 95% | Slack | Monitor, plan fix |
| WARNING | 1-2 sources failed | Slack | Note for review |
| INFO | Minor performance | Dashboard | Review daily |

### On-Call Runbook

**Problem: Pipeline took 5 hours (SLA miss)**
1. Check: Which task was slow?
   ```bash
   airflow dag-stats daily_batch_pipeline --stat duration
   ```
2. Investigate bottleneck:
   - Ingestion: Check source APIs
   - Transform: Check Spark metrics
   - Load: Check warehouse capacity
3. Scale if needed:
   - Increase executors
   - Increase memory
4. Document incident

**Problem: Data quality failed**
1. Check: Which gate failed?
   ```bash
   docker-compose logs airflow-scheduler | grep quality
   ```
2. Analyze bad data:
   ```bash
   python scripts/analyze_failures.py
   ```
3. Fix:
   - Update schema if changed
   - Relax rule if too strict
   - Contact source if corrupted
4. Backfill past data:
   ```bash
   airflow dags backfill daily_batch_pipeline --start-date 2024-01-01 --end-date 2024-01-31
   ```

## Metrics Reference

Key metrics to monitor:

```
Pipeline Duration (seconds)
├─ Ingest: should be 1-3h
├─ Validate: should be 5-15min
├─ Transform: should be 1-1.5h
└─ Load: should be 15-30min

Data Quality
├─ Completeness: > 99% (no nulls)
├─ Accuracy: > 98% (no invalid values)
└─ Consistency: > 97% (matches expectations)

Reliability
├─ Success rate: > 99%
├─ MTTR (mean time to recover): < 30min
└─ Uptime: > 99.9% (< 8.6h downtime/year)

Cost
├─ Compute: should be $50-150/day
├─ Storage: should be $30-100/day
└─ Total: should be $1-3k/month
```

## Resources

- **Logs**: `docker-compose logs -f <service>`
- **Metrics**: http://localhost:9090 (Prometheus)
- **Dashboards**: http://localhost:3000 (Grafana)
- **Job tracker**: http://localhost:8080 (Airflow)
- **Spark UI**: http://localhost:8088 (Spark Master)
