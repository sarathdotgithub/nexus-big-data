"""
Operational Runbooks and Procedures for PI-1 Platform

Provides step-by-step guides for platform operations and troubleshooting.
"""

# OPERATIONS.md

## PI-1 Platform Operations Guide

### 1. Platform Startup

```bash
# Start the Spark cluster and supporting services
docker-compose up -d

# Verify cluster is running
curl http://localhost:8080  # Spark Master UI

# Monitor logs
docker-compose logs -f

# Access Jupyter notebook
# Navigate to http://localhost:8888
```

### 2. Daily Operations

#### Morning Briefing (7:00 AM)
1. Check Spark cluster health
   - Verify all workers are running
   - Check available resources
2. Review overnight ingestion status
   - Check for any failed jobs
   - Verify data freshness
3. Review risk dashboard
   - Identify hospitals at HIGH/CRITICAL risk
   - Prepare escalation list

#### Throughout Day
1. Monitor data quality metrics
   - Check for anomalies in consumption patterns
   - Verify data completeness
2. Review AI decision copilot recommendations
   - Evaluate suggested actions
   - Execute recommendations
3. Track operational efficiency
   - Monitor delivery times
   - Track plant capacity utilization

#### Evening Review (5:00 PM)
1. Prepare next day's delivery plan
2. Review financial metrics
3. Prepare executive summary

### 3. Emergency Procedures

#### Hospital at CRITICAL Risk (< 24 hours autonomy)

**Immediate Actions (0-30 min):**
1. Alert hospital operations team
2. Initiate emergency delivery from nearest plant
3. Divert all available trucks to affected hospital
4. Contact plant managers for capacity confirmation

**Follow-up (30 min - 2 hours):**
1. Monitor delivery progress
2. Update hospital with ETA
3. Coordinate with medical staff on patient care
4. Document incident in audit log

**Post-Incident (Within 24 hours):**
1. Root cause analysis
2. Network rebalancing
3. Lessons learned review

#### System Failure (Platform Outage)

**For Spark Cluster Failure:**
```bash
# Stop containers
docker-compose down

# Check for corrupted data
ls -la data/bronze data/silver data/gold

# Restart cluster
docker-compose up -d

# Rerun latest failed jobs
# (Implement checkpoint recovery)
```

**For Data Loss:**
1. Restore from backup (if available)
2. Re-ingest from source systems
3. Rerun transformation pipelines
4. Validate data integrity

### 4. Maintenance Windows

#### Weekly Maintenance (Sunday 2:00-4:00 AM)
- Backup all Gold layer tables
- Run data quality comprehensive audit
- Cleanup old logs
- Update documentation

#### Monthly Maintenance (First Sunday, 1:00-6:00 AM)
- Full cluster restart (graceful shutdown)
- Disk space cleanup
- Index optimization
- Performance benchmarking

#### Quarterly Maintenance (q1: Jan, Apr, Jul, Oct)
- Major Spark version updates
- Infrastructure optimization review
- Cost analysis and reporting
- Governance audit

### 5. Troubleshooting Guide

#### Issue: High query latency (>10 seconds)

**Check:**
1. Spark executor memory
   ```bash
   docker-compose logs spark-master | grep memory
   ```
2. Data skew in partitions
   ```python
   df.groupBy("hospital_id").count().show()
   ```
3. Unoptimized queries
   - Check execution plan with EXPLAIN

**Solutions:**
1. Increase executor memory
2. Re-partition tables
3. Create indexes/materialized views
4. Cache frequently accessed tables

#### Issue: Ingestion lag (data older than 1 hour)

**Check:**
1. Batch job status
   ```bash
   docker-compose logs spark-master | grep ingestion
   ```
2. Source system availability
   - Verify CSV files exist and are readable
3. Disk space
   ```bash
   df -h data/
   ```

**Solutions:**
1. Increase batch frequency
2. Implement streaming instead of batch
3. Scale worker nodes
4. Archive old data

#### Issue: Data quality drops below 99%

**Check:**
1. Null rate by column
   ```python
   df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]).show()
   ```
2. Duplicate rate
   ```python
   df.count() - df.dropDuplicates().count()
   ```
3. Schema mismatches
   ```python
   df.printSchema()
   ```

**Solutions:**
1. Increase validation rules
2. Contact source system owners
3. Implement data cleansing rules
4. Manual data audit

### 6. Monitoring & Alerting

**Key Metrics to Monitor:**

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| Cluster uptime | <99% | Investigate node failures |
| Ingestion lag | >1 hour | Scale cluster or optimize jobs |
| Data quality | <99% | Review quality checks |
| Query latency p95 | >10 sec | Optimize queries/indexes |
| Disk space | >80% | Archive/delete old data |
| Autonomy forecast error | >10% | Retrain models |
| Risk score drift | >20% from baseline | Review model calibration |

**Alerting Setup:**

```python
# In governance/monitoring.py
ALERT_RULES = {
    "ingestion_lag_hours": {
        "threshold": 1,
        "severity": "HIGH",
        "notify": ["platform_team", "ops_manager"],
    },
    "data_quality_score": {
        "threshold": 99,
        "severity": "HIGH",
        "notify": ["data_quality_owner"],
    },
    "autonomy_forecast_accuracy": {
        "threshold": 0.90,
        "severity": "MEDIUM",
        "notify": ["analytics_team"],
    },
}
```

### 7. Backup & Disaster Recovery

#### Backup Strategy

**Frequency:**
- Bronze layer: Continuous (append-only, recoverable from source)
- Silver layer: Daily (can be regenerated from Bronze)
- Gold layer: Daily (pre-aggregates, cannot be regenerated)

**Backup Implementation:**
```bash
# Daily backup of Gold layer (via cron)
0 1 * * * tar -czf /backups/gold-$(date +%Y%m%d).tar.gz data/gold/
0 2 * * * tar -czf /backups/silver-$(date +%Y%m%d).tar.gz data/silver/

# Retention: 30 days of daily backups
find /backups -name "*.tar.gz" -mtime +30 -delete
```

#### Disaster Recovery Plan

**RTO (Recovery Time Objective):** 4 hours
**RPO (Recovery Point Objective):** 1 hour

**Recovery Procedures:**
1. Restore backups from S3/NAS
2. Validate data integrity
3. Restart Spark cluster
4. Rerun failed ingestion jobs
5. Validate end-to-end pipeline
6. Communication to stakeholders

### 8. Performance Tuning

#### Spark Configuration Optimization

```bash
# In docker-compose.yml
SPARK_WORKER_CORES=4      # Increase from 2
SPARK_WORKER_MEMORY=4G    # Increase from 2G
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_MEMORY=2G
```

#### Data Optimization

```python
# Parquet compression
df.write.option("compression", "snappy").parquet(path)

# Partitioning strategy
df.write.partitionBy("date", "hospital_id").parquet(path)

# Statistics collection
df.describe().show()
```

### 9. Cost Management

#### Resource Sizing

- Current: 1 Master + 2 Workers (dev/test)
- Production: 1 Master + 4-6 Workers (depending on data volume)

#### Cost Optimization

1. **Storage:** Compress Parquet files (snappy: 30-50% reduction)
2. **Compute:** Right-size executors based on job profiles
3. **Archiving:** Move old data to cheaper storage
4. **Scheduling:** Run batch jobs during off-peak hours

### 10. Contact & Escalation

**Platform Support Contacts:**

| Role | Contact | Hours |
|------|---------|-------|
| Platform Team | platform@nexus.io | 24/7 |
| Ops Manager | ops-mgr@nexus.io | 9-5 |
| Data Quality | dq-team@nexus.io | 9-5 |
| AI/Analytics | ai-team@nexus.io | 9-5 |

**Escalation Path:**
1. Detection → Alert sent
2. <15 min → Platform team investigates
3. <30 min → Decision on escalation
4. <60 min → Incident commander assigned (if critical)
5. <4 hrs → RCA and mitigation plan

---

*Last Updated: 2026-01-01*
