# Troubleshooting

## Quick Diagnostics

```bash
# Check all container health
docker-compose ps

# Tail logs for a specific service
docker-compose logs -f airflow-scheduler

# Run the built-in monitor
python scripts/monitor_pipeline.py --status
python scripts/monitor_pipeline.py --quality
python scripts/monitor_pipeline.py --timeline
```

---

## Common Issues

### Docker / startup

**Containers keep restarting**
```bash
docker-compose logs <service_name>
# Look for: port conflicts, missing env vars, failed healthchecks
```
Fix: ensure `.env` is populated and no other process is using ports 8080, 9000, 9090, etc.

**`docker-compose up` fails with "network not found"**
```bash
docker-compose down
docker network prune
docker-compose up -d
```

**MinIO bucket not created**
```bash
docker-compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker-compose exec minio mc mb local/data-lake
```

---

### Airflow

**DAG not appearing in the UI**
- Check the DAG file has no Python syntax errors: `python airflow/dags/daily_batch_pipeline.py`
- Look for import errors: `docker-compose logs airflow-scheduler | grep ERROR`
- Ensure `AIRFLOW__CORE__DAGS_FOLDER` points to `airflow/dags/`

**Tasks stuck in "queued"**
- The scheduler may be down: `docker-compose restart airflow-scheduler`
- Worker capacity: check `docker-compose logs airflow-worker`

**"Critical quality gate failed" stops the DAG**
- Inspect the quality report: `python scripts/monitor_pipeline.py --quality`
- Fix the upstream data issue or temporarily lower the threshold in `config/pipeline_config.yaml`
- Re-run the DAG from the failed task (not from the start)

**SLA miss alert not firing**
- Verify `SLACK_WEBHOOK_URL` is set in `.env`
- Test the webhook: `curl -X POST -H 'Content-type: application/json' --data '{"text":"test"}' $SLACK_WEBHOOK_URL`

---

### Spark

**`SparkSubmitOperator` fails with "Connection refused"**
- Confirm spark-master is running: `docker-compose ps spark-master`
- Check `SPARK_MASTER_URL` in `.env` matches the compose service name

**Out of memory error on executor**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```
Increase executor memory in `docker-compose.yml`:
```yaml
SPARK_EXECUTOR_MEMORY: 6g
```
Or reduce data volume with partition pruning: pass a narrower `--date` range.

**Broadcast join causes driver OOM**
- The dimension table grew too large to broadcast. Remove `F.broadcast()` from `src/transformation/enrichment.py` for that join.

**Slow shuffle / many small tasks**
- Check `spark.sql.shuffle.partitions` — default 200 is too high for small datasets.
- Set it to `2 * num_executors * cores_per_executor` in `spark/transform_daily.py`.

---

### Kafka

**Consumer lag keeps growing**
```bash
docker-compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group pipeline_consumer
```
- Add more partitions to the topic or scale the streaming job's parallelism.
- Check `streaming_job.py` checkpoint path is writable.

**"Topic not found" error**
```bash
docker-compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic raw_events --partitions 4 --replication-factor 1
```

---

### Data Quality

**Quality score unexpectedly low**
1. `python scripts/monitor_pipeline.py --quality` — find which gate failed
2. Check recent source data: did a field get renamed or go nullable?
3. Rerun the profiler against raw data: `DataProfiler.profile(raw_records)`

**Duplicate rows in warehouse**
- `idempotent_load` uses `event_id` as merge key by default. Check the key is actually unique in the source.
- Run `Deduplicator(key_fields=["event_id"]).deduplicate(records)` manually and compare counts.

---

### Tests

**`pytest` passes locally but fails in CI**
- Ensure the same lockfile: CI should run `poetry install --with dev` (not pip).
- Check Python version: project requires `>=3.9,<3.13` (see `pyproject.toml`).
- Spark tests skip automatically if PySpark is not installed (`pytest.importorskip`).

**Integration tests fail with import errors**
```
ModuleNotFoundError: No module named 'src'
```
Run from the project root using `poetry run` (Poetry adds `src/` to `sys.path` via the `packages` declaration):
```bash
cd production-data-pipeline
poetry run pytest tests/ -v
```

**Wrong virtualenv / packages not found after `poetry install`**
```bash
poetry env info          # show which venv Poetry is using
poetry env list          # list all managed envs
poetry env remove --all  # nuke and reinstall
poetry install --with dev
```

---

## Log Locations

| Log | Default path |
|---|---|
| Pipeline structured log | `/tmp/pipeline.log` |
| Pipeline errors | `/tmp/pipeline_errors.log` |
| Quality report | `/tmp/quality_report.json` |
| Lineage records | `/tmp/lineage.json` |
| Warehouse files (dev) | `/tmp/warehouse/*.parquet` |
| Airflow task logs | Airflow UI → DAG → task → Log |
| Spark logs | http://localhost:8088 → application |

---

## Getting Help

1. Check this doc and `docs/OPERATIONS.md`
2. Search Airflow / Spark / Kafka official docs
3. Look at the test files — they show expected behaviour for every module
4. Open an issue with: error message, `docker-compose ps` output, and relevant log snippet
