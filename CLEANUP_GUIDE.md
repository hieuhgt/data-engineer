# Cleanup Guide - Remove Confusing Root Files

## 🧹 What to Remove

Your root directory has some files that should **NOT** be there. They belong in component directories.

### Files to DELETE (❌ Not needed in root)
```
spark_pipeline.py        → Move to spark/
kafka_consumer.py        → Move to kafka/consumers/
kafka_producer.py        → Move to kafka/producers/
requirements.txt         → Replace with monorepo structure
```

### Files to KEEP (✅ Needed in root)
```
config.py               ← Shared configuration
logger_setup.py         ← Shared logging
.env.example            ← Environment template
Dockerfile              ← Base Spark image
docker-compose.yml      ← Docker setup
```

---

## 🎯 Why Remove Them?

**Current Confusing State**:
```
data-engineer/
├── spark_pipeline.py        ❓ Is this a root tool? A job? Confusing!
├── kafka_consumer.py        ❓ Where does this belong?
├── kafka_producer.py        ❓ Why not in kafka/ folder?
├── spark/
│   └── jobs/
│       ├── ingest_kafka.py  ← Already have it here!
│       └── ...
└── kafka/
    └── producers/
        └── event_producer.py ← Already have it here!
```

**Clean State**:
```
data-engineer/
├── config.py            ✓ Shared
├── logger_setup.py      ✓ Shared
├── spark/
│   └── jobs/
│       └── ingest_kafka.py
└── kafka/
    ├── producers/
    │   └── event_producer.py
    └── consumers/
        └── event_consumer.py
```

---

## 🔄 How to Cleanup

### Option 1: **Automatic Cleanup** (Easiest)

```bash
cd /Users/hieuht/workspace/personal/data-engineer

# Run cleanup script
chmod +x scripts/cleanup-root.sh
./scripts/cleanup-root.sh

# It will ask for confirmation before deleting
```

### Option 2: **Manual Cleanup** (3 steps)

```bash
cd /Users/hieuht/workspace/personal/data-engineer

# 1. Remove spark_pipeline.py (it's in spark/ already)
rm spark_pipeline.py

# 2. Remove kafka_consumer.py (it's in kafka/consumers/ already)
rm kafka_consumer.py

# 3. Remove kafka_producer.py (it's in kafka/producers/ already)
rm kafka_producer.py

# KEEP these:
# - config.py (shared)
# - logger_setup.py (shared)
# - .env.example
# - Dockerfile
# - docker-compose.yml
# - etc.
```

### Option 3: **VSCode** (Visual)

In VSCode file explorer:
1. Find `spark_pipeline.py` in root
2. Right-click → Delete
3. Find `kafka_consumer.py` in root
4. Right-click → Delete
5. Find `kafka_producer.py` in root
6. Right-click → Delete

---

## ✅ After Cleanup

Your root directory should look like:

```
data-engineer/
├── Documentation
│   ├── README.md
│   ├── START_HERE.md
│   ├── WALKTHROUGH.md
│   ├── CLEANUP_GUIDE.md (this file)
│   └── ...
│
├── Configuration (Shared)
│   ├── config.py          ✓ Keep
│   ├── logger_setup.py    ✓ Keep
│   └── .env.example       ✓ Keep
│
├── Docker & Compose
│   ├── Dockerfile         ✓ Keep
│   ├── docker-compose.yml ✓ Keep
│   └── docker-compose.full.yml ✓ Keep
│
├── Components (Organized!)
│   ├── spark/
│   │   ├── spark_pipeline.py  (moved here - don't duplicate!)
│   │   └── jobs/
│   ├── kafka/
│   │   ├── producers/event_producer.py
│   │   ├── consumers/event_consumer.py
│   │   └── examples/
│   └── airflow/
│
├── Infrastructure
│   ├── kubernetes/
│   ├── requirements/
│   └── scripts/
│
└── ... (other files)
```

---

## 🚨 IMPORTANT: Don't Duplicate!

After cleanup, **do NOT**:
- ❌ Create `spark_pipeline.py` in root again
- ❌ Create `kafka_consumer.py` in root again
- ❌ Create `kafka_producer.py` in root again

These belong in their component directories:
- ✅ Use `spark/` for Spark files
- ✅ Use `kafka/producers/` for producers
- ✅ Use `kafka/consumers/` for consumers
- ✅ Use `airflow/` for Airflow files

---

## 📋 Checklist

Before cleanup:
- [ ] Read this guide
- [ ] Understand why files need to be removed
- [ ] Backup if concerned (optional)

During cleanup:
- [ ] Run cleanup script OR manually delete files
- [ ] Verify files are removed

After cleanup:
- [ ] Run `./scripts/verify-deps.sh`
- [ ] Run `docker-compose -f docker-compose.full.yml up -d`
- [ ] Test everything still works
- [ ] Verify imports in code still work

---

## 🎉 Result

**Clean, organized monorepo** with:
- ✅ No root clutter
- ✅ Clear file organization
- ✅ Easy to understand structure
- ✅ Professional setup
- ✅ No confusion!

---

## 🆘 If Something Breaks

After cleanup, if code doesn't run:

1. **Check imports**: Did files import from root?
   ```python
   # OLD (wrong after cleanup)
   from kafka_producer import KafkaProducer

   # NEW (correct)
   from kafka.producers.event_producer import KafkaProducer
   ```

2. **Check references**: Update any documentation or scripts that reference root files

3. **Verify installation**: Run `./scripts/verify-deps.sh`

All the files still exist - they're just in better locations! 🎯
