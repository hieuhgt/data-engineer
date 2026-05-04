# Airflow Learning Path: Week-by-Week Guidance

## Overview
This path takes you from "what is Airflow?" to "I can manage enterprise data platforms" in 20 weeks.

---

## LEVEL 1: JUNIOR (Weeks 1-2)

### Week 1: Fundamentals

**Monday-Tuesday: Understand Architecture**
- Read: AIRFLOW_COMPLETE_GUIDE.md sections on Airflow architecture
- Concepts: DAGs, tasks, operators, scheduler, executor
- Time: 3 hours
- Goal: Understand the distributed model

**Wednesday-Thursday: Create First DAG**
- Read: DAG section in guide
- Do: Create simple DAG with 2-3 tasks
- Code: Examples from dags/01_level1_basics.py
- Time: 4 hours
- Goal: Comfortable creating basic DAG

**Friday: Task Dependencies & Scheduling**
- Read: Task dependencies and scheduling sections
- Do: Create DAG with multiple dependencies, test locally
- Code: Practice DAGs
- Time: 3 hours
- Goal: Understand dependencies and scheduling

**Weekend: Review**
- Review concepts
- Test all examples locally
- Time: 2 hours

### Week 2: Core Operators

**Monday-Tuesday: Python & Bash Operators**
- Learn: PythonOperator, BashOperator, DummyOperator
- Do: Create DAG mixing Python and Bash tasks
- Time: 4 hours

**Wednesday-Thursday: SQL Operators**
- Learn: PostgresOperator, other DB operators
- Do: Create DAG that runs SQL
- Time: 3 hours

**Friday: End-to-End Project**
- Project: Multi-task DAG (extract → transform → load)
- Time: 4 hours

**Checkpoint:** Can you create DAG, define dependencies, schedule execution?

---

## LEVEL 2: INTERMEDIATE (Weeks 3-5)

### Week 3: Error Handling

**Monday-Tuesday: Retries & Trigger Rules**
- Learn: Retry configuration, trigger_rule options
- Do: Create DAG with error handling
- Time: 3 hours

**Wednesday-Thursday: Monitoring & Callbacks**
- Learn: Callbacks, SLA monitoring, alerting
- Do: Add failure/success callbacks
- Time: 3 hours

**Friday: Sensors**
- Learn: S3Sensor, FileSensor, SqlSensor
- Do: Create DAG with sensors
- Time: 3 hours

### Week 4: Advanced Concepts

**Monday-Tuesday: XCom (Data Passing)**
- Learn: Push/pull XCom
- Do: Create DAG passing data between tasks
- Time: 3 hours

**Wednesday-Thursday: Task Groups**
- Learn: Organize complex DAGs
- Do: Refactor previous DAG with task groups
- Time: 3 hours

**Friday: Branching**
- Learn: BranchPythonOperator, conditional execution
- Do: Create DAG with conditional logic
- Time: 3 hours

### Week 5: Integration Project

**Monday-Friday: Real Pipeline**
- Project: ETL with multiple sources, error handling, monitoring
- Time: 20 hours
- Goal: Build production-like DAG

**Checkpoint:** Can you handle errors, branch logic, monitor health?

---

## LEVEL 3: ADVANCED (Weeks 6-12)

### Week 6-7: Spark Integration

**Focus: Orchestrate Spark jobs**
- Learn: SparkSubmitOperator
- Do: Create DAG submitting Spark job
- Code: Integration with Spark jobs
- Time: 15 hours

### Week 8-9: Dynamic DAGs

**Focus: Generate tasks programmatically**
- Learn: Dynamic task generation loops
- Do: Create DAG that generates tasks from config
- Time: 15 hours

### Week 10-11: Production Patterns

**Focus: Reliability and monitoring**
- Learn: SLA monitoring, backfill, recovery
- Do: Set up SLA alerts, test backfill
- Time: 20 hours

### Week 12: Capstone Project

**Build:** Complete data pipeline
- Multiple sources (API, database, files)
- Error handling and retries
- SLA monitoring
- Spark transformation
- Multiple outputs
- Time: 20 hours

**Checkpoint:** Can you orchestrate complex workflows?

---

## LEVEL 4: SENIOR (Weeks 13-20)

### Weeks 13-15: Custom Development

**Focus: Build custom operators**
- Learn: BaseOperator, hooks, authentication
- Do: Create custom operator
- Time: 20 hours

### Weeks 16-17: Enterprise Operations

**Focus: HA, security, cost optimization**
- Learn: Multi-environment setup, security policies
- Do: Design production deployment
- Time: 20 hours

### Weeks 18-20: Capstone Project

**Build:** Enterprise platform
- Multi-tenant architecture
- Custom operators
- HA configuration
- Cost optimization
- Security implementation
- Time: 60 hours

**Checkpoint:** Can you design and operate enterprise platform?

---

## Recommended Practice Path

### If you have 1 week:
- Days 1-2: AIRFLOW_COMPLETE_GUIDE.md LEVEL 1
- Days 3-4: dags/01_level1_basics.py + dags/02_level2_intermediate.py
- Days 5-7: Build small DAG project

### If you have 3 weeks:
- Week 1: LEVEL 1 (complete)
- Week 2: LEVEL 2 (complete)
- Week 3: Build medium project + practice monitoring

### If you have 2 months:
- Weeks 1-2: LEVEL 1
- Weeks 3-4: LEVEL 2 + error handling
- Weeks 5-6: LEVEL 3 + Spark integration
- Weeks 7-8: Capstone project

### For Interview Prep (3 days):
- Day 1: AIRFLOW_COMPLETE_GUIDE.md (skim all levels, focus on concepts)
- Day 2: Create 2-3 small DAGs, practice error handling
- Day 3: Review quick reference, practice interview questions

---

## Interview Preparation Schedule

### 1-Day Intensive (Before Interview Tomorrow)
```
Morning (2 hours):
- Read AIRFLOW_COMPLETE_GUIDE.md LEVEL 1 + 2

Afternoon (2 hours):
- Do 3 practice DAGs
- Review common mistakes

Evening (1 hour):
- Practice interview questions
```

### 3-Day Preparation (Recommended)
```
Day 1 (4 hours):
- LEVEL 1 (fundamentals)
- dags/01_level1_basics.py

Day 2 (4 hours):
- LEVEL 2 (error handling)
- dags/02_level2_intermediate.py
- Practice creating DAGs

Day 3 (2 hours):
- Quick review of all levels
- Interview questions practice
```

---

## Key Milestones

### After Week 2 (JUNIOR)
- [ ] Create basic DAG with multiple tasks
- [ ] Define task dependencies
- [ ] Understand scheduling
- [ ] Use common operators

### After Week 5 (INTERMEDIATE)
- [ ] Handle failures with retries
- [ ] Use sensors and branching
- [ ] Pass data between tasks with XCom
- [ ] Organize complex DAGs with task groups
- [ ] Monitor with SLA and callbacks

### After Week 12 (ADVANCED)
- [ ] Orchestrate Spark jobs
- [ ] Generate dynamic tasks
- [ ] Backfill data
- [ ] Monitor production workflows
- [ ] Handle failures gracefully

### After Week 20 (SENIOR)
- [ ] Design HA architecture
- [ ] Create custom operators
- [ ] Implement security policies
- [ ] Optimize costs
- [ ] Troubleshoot production issues
- [ ] Lead infrastructure initiatives

---

## Common Interview Questions

**Q: What is a DAG?**
A: Directed acyclic graph - your workflow with task dependencies

**Q: How does Airflow schedule jobs?**
A: Scheduler parses DAGs every 30s, creates task instances based on schedule_interval

**Q: How to handle task failures?**
A: Retries (automatic), trigger_rule (conditional execution), callbacks (notifications)

**Q: Difference between Airflow and Cron?**
A: Airflow: Dependencies, monitoring, UI, error handling, SLAs
   Cron: Simple, no dependencies, no monitoring

**Q: How to pass data between tasks?**
A: XCom for small data, files/DB for large data

**Q: When to use sensors?**
A: Wait for external conditions (file appears, SQL returns data, etc.)

**Q: How to scale Airflow?**
A: CeleryExecutor with Kafka queue, multiple workers, database replication

---

## Next Steps

1. **Install Airflow**: `pip install apache-airflow`
2. **Initialize**: `airflow db init`
3. **Read Level 1**: AIRFLOW_COMPLETE_GUIDE.md LEVEL 1
4. **Create DAG**: First simple DAG
5. **Integrate with Spark**: Use SparkSubmitOperator
6. **Deploy**: Move to production

---

## Integration with Other Topics

```
Data Engineer Learning Path:

Python Basics
    ↓
SQL Optimization
    ↓
Spark (Process data)
    ↓
Airflow (Orchestrate Spark)
    ↓
Kafka (Real-time data)
    ↓
Docker/Kubernetes (Deploy)
```

**Recommended Learning Order:**
1. Python + SQL (fundamentals)
2. Spark (process data)
3. Airflow (orchestrate workflows)
4. Kafka (real-time integration)
5. Docker + K8s (deployment)

---

## Success Metrics

You're ready for interviews when you can:
- [ ] Explain DAG architecture
- [ ] Create multi-task DAG with dependencies
- [ ] Handle failures and retries
- [ ] Pass data between tasks
- [ ] Monitor with SLA and callbacks
- [ ] Integrate with Spark
- [ ] Answer "How would you debug a failing DAG?"

---

**Now go build something with Airflow!** 🚀
