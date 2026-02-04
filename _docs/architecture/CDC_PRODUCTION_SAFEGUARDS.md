# CDC Pipeline Production Safeguards

**Document Version**: 1.0  
**Last Updated**: 2026-01-28  
**Status**: ⚠️ CRITICAL - Must Implement Before Production

---

## Executive Summary

This document outlines critical production safeguards for the Redpanda Connect CDC pipeline. The current implementation works well for development but requires specific configurations to prevent data consistency issues and performance problems in production.

**Risk Level**:
- ✅ **Development**: LOW - Current setup is adequate
- 🔴 **Production WITHOUT safeguards**: HIGH - Risk of data inconsistency and outages
- 🟢 **Production WITH safeguards**: LOW - Well protected

---

## Understanding the Risk: LSN Cache Loss

### What is LSN Cache?

The LSN (Log Sequence Number) cache tracks the last CDC event processed from MSSQL. It's stored in `.lsn_cache/` directory and mounted to the source pipeline container at `/data/lsn_cache`.

**Current behavior**:
```bash
# LSN cache file content example:
$ cat .lsn_cache/actor_last_lsn
0x00004AD30000435D000A
```

### What Happens If Cache is Lost?

When LSN cache is deleted or corrupted:

1. Pipeline resets to LSN `0x00000000000000000000` (beginning of time)
2. Re-reads **entire CDC history** from MSSQL CDC tables
3. Replays all historical changes to Kafka and PostgreSQL

### Production Scenarios Where Cache Loss Occurs:

❌ **Pod restart without persistent volume** (Kubernetes)  
❌ **Volume deletion during maintenance**  
❌ **Filesystem corruption**  
❌ **Accidental `docker volume rm`**  
❌ **Manual cleanup scripts** (like `cdc nuke-local`)

---

## Critical Production Risks

### 1. Data Consistency Issues 🔴 HIGH RISK

**Problem**: During CDC replay, old data versions temporarily overwrite current data.

**Example Timeline**:
```
10:00 AM - Record created:  Actor #123 name="John Smith", email="john@old.com"
11:00 AM - Record updated:  Actor #123 name="Jane Smith", email="jane@new.com"
12:00 PM - LSN cache lost, pipeline restarts

Replay sequence:
  12:01 PM - INSERT: name="John Smith", email="john@old.com"  ← STALE DATA!
  12:02 PM - UPDATE: name="Jane Smith", email="jane@new.com" ← Correct data restored
```

**Impact**:
- ⚠️ Applications see stale data during replay (1-30 minutes depending on CDC table size)
- ⚠️ Reports/analytics may capture incorrect snapshots
- ⚠️ Audit logs show "updates" that are actually old replays

**Mitigation**: See Critical Safeguard #1 (Persistent Volumes)

---

### 2. Performance Degradation 🔴 HIGH RISK

**Problem**: Replaying months of CDC history overwhelms the pipeline.

**Real-World Example**:
```
Scenario: 6 months of CDC data accumulated
- Total CDC records: 1,000,000 events
- Normal processing rate: 1,000 events/sec
- Replay time: ~17 minutes of full load

During replay:
- ❌ PostgreSQL connection pool saturated
- ❌ Kafka lag increases
- ❌ Real-time CDC delayed (stuck processing old data)
- ❌ Application queries timeout or slow
```

**Metrics Impact**:
```
Normal operation:
  - CDC Lag: < 5 seconds
  - PostgreSQL connections: 2-5 active
  - CPU usage: 10-20%

During replay:
  - CDC Lag: 10-30 minutes
  - PostgreSQL connections: 10/10 (maxed out)
  - CPU usage: 80-100%
```

**Mitigation**: See Critical Safeguard #2 (CDC Retention) + #4 (Monitoring)

---

### 3. MSSQL CDC Table Growth 🟡 MEDIUM RISK

**Problem**: MSSQL CDC tables grow indefinitely without cleanup jobs.

**Growth Pattern**:
```sql
-- Example CDC table sizes over time (1000 changes/day per table):

After 1 week:    7,000 rows    (acceptable)
After 1 month:   30,000 rows   (acceptable)
After 6 months:  180,000 rows  (concerning)
After 1 year:    365,000 rows  (problematic)
After 2 years:   730,000 rows  (severe performance impact)
```

**Impact**:
- Slow CDC queries (full table scans)
- Increased storage costs
- Longer replay times if cache lost
- MSSQL backup size inflation

**Mitigation**: See Critical Safeguard #2 (CDC Retention Policy)

---

## 🔴 CRITICAL SAFEGUARDS - Must Implement

### Safeguard #1: Persistent Volume for LSN Cache (Kubernetes)

**Priority**: 🔴 CRITICAL  
**Effort**: LOW (1-2 hours)  
**Environment**: Production Kubernetes

#### Implementation:

**Step 1**: Create PersistentVolumeClaim

```yaml
# kubernetes/base/lsn-cache-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: lsn-cache-pvc
  namespace: cdc-pipeline
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard  # Use your cluster's storage class
```

**Step 2**: Mount in Source Pipeline Deployment

```yaml
# kubernetes/overlays/prod/redpanda-connect-source-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redpanda-connect-source
spec:
  template:
    spec:
      volumes:
        - name: lsn-cache
          persistentVolumeClaim:
            claimName: lsn-cache-pvc
      containers:
        - name: redpanda-connect
          volumeMounts:
            - name: lsn-cache
              mountPath: /data/lsn_cache
```

**Step 3**: Verify Persistence

```bash
# Test: Delete pod and verify cache survives
kubectl delete pod -l app=redpanda-connect-source
kubectl exec -it <new-pod> -- ls -la /data/lsn_cache/
# Should show existing LSN files
```

#### Backup Strategy:

```bash
# Automated backup script (run daily via CronJob)
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
kubectl cp cdc-pipeline/redpanda-connect-source-xxx:/data/lsn_cache \
  ./backups/lsn_cache_${DATE}.tar.gz

# Retention: Keep last 30 days
find ./backups -name "lsn_cache_*.tar.gz" -mtime +30 -delete
```

---

### Safeguard #2: Configure MSSQL CDC Retention

**Priority**: 🔴 CRITICAL  
**Effort**: LOW (30 minutes)  
**Environment**: All MSSQL databases with CDC enabled

#### Recommended Retention Periods:

| Environment | Retention | Rationale |
|-------------|-----------|-----------|
| Development | 1-2 days  | Fast cleanup, minimal disk usage |
| Staging     | 3-7 days  | Allow testing scenarios |
| Production  | 2-7 days  | Balance between recovery window and performance |

#### Implementation:

```sql
-- Run on each MSSQL database with CDC enabled
USE AdOpusTest;
GO

-- Check current CDC cleanup job settings
EXEC sys.sp_cdc_help_jobs;
GO

-- Set retention to 2 days (2880 minutes)
EXEC sys.sp_cdc_change_job 
    @job_type = N'cleanup',
    @retention = 2880,           -- Minutes to retain CDC data
    @threshold = 5000,            -- Delete in batches of 5000
    @pollinginterval = 3600;     -- Run cleanup every hour
GO

-- Verify job is enabled
SELECT name, enabled 
FROM msdb.dbo.sysjobs 
WHERE name LIKE 'cdc%cleanup%';
GO

-- For production: Use 7 days retention
EXEC sys.sp_cdc_change_job 
    @job_type = N'cleanup',
    @retention = 10080;  -- 7 days = 10080 minutes
GO
```

#### Monitoring CDC Table Sizes:

```sql
-- Create monitoring query (run weekly)
USE AdOpusTest;
GO

SELECT 
    OBJECT_NAME(ct.source_object_id) AS source_table,
    ct.capture_instance,
    OBJECT_NAME(ct.object_id) AS cdc_table,
    p.rows AS row_count,
    (p.reserved * 8.0 / 1024) AS reserved_mb,
    (p.data * 8.0 / 1024) AS data_mb
FROM cdc.change_tables ct
CROSS APPLY (
    SELECT 
        SUM(reserved) as reserved, 
        SUM(data) as data,
        SUM(rows) as rows
    FROM sys.partitions p
    WHERE p.object_id = ct.object_id
    AND p.index_id IN (0,1)
) p
ORDER BY p.rows DESC;
GO
```

**Alert Thresholds**:
- ⚠️ Warning: > 100,000 rows in any CDC table
- 🔴 Critical: > 500,000 rows in any CDC table

---

### Safeguard #3: Kafka Topic Retention Policy

**Priority**: 🔴 CRITICAL  
**Effort**: LOW (15 minutes)  
**Environment**: Production Kafka/Redpanda

#### Recommended Settings:

```bash
# Set retention for CDC topics (7 days)
docker exec cdc-redpanda rpk topic alter-config \
  nonprod.avansas.AdOpusTest.dbo.Actor \
  --set retention.ms=604800000  # 7 days in milliseconds

# Verify settings
docker exec cdc-redpanda rpk topic describe nonprod.avansas.AdOpusTest.dbo.Actor
```

#### Production Kafka Configuration:

```yaml
# For Kubernetes Redpanda deployment
apiVersion: v1
kind: ConfigMap
metadata:
  name: redpanda-config
data:
  redpanda.yaml: |
    redpanda:
      default_topic_replications: 3
      default_topic_partitions: 3
      log_retention_ms: 604800000  # 7 days default
      log_segment_size: 1073741824  # 1GB segments
```

**Rationale**:
- Prevents infinite replay from Kafka
- Allows reasonable recovery window
- Controls disk usage

---

## 🟡 RECOMMENDED SAFEGUARDS - Should Implement

### Safeguard #4: Monitoring & Alerting

**Priority**: 🟡 HIGH  
**Effort**: MEDIUM (4-8 hours)  
**Environment**: Production

#### Key Metrics to Monitor:

```yaml
# Prometheus metrics to track

# 1. LSN Cache Status
- Alert: lsn_cache_file_missing
  Query: absent(lsn_cache_last_modified_seconds)
  Action: Immediate notification - cache may be lost

# 2. CDC Processing Lag
- Alert: cdc_lag_high
  Query: time() - cdc_last_processed_timestamp > 300
  Action: Warning if lag > 5 minutes, Critical if > 15 minutes

# 3. PostgreSQL Connection Saturation
- Alert: postgres_connections_saturated
  Query: postgres_connections_active / postgres_connections_max > 0.8
  Action: Scale up or investigate slow queries

# 4. CDC Table Size
- Alert: cdc_table_size_large
  Query: mssql_cdc_table_rows > 100000
  Action: Check retention policy, consider manual cleanup
```

#### Example Prometheus Alert Rules:

```yaml
# alerts/cdc-pipeline-alerts.yaml
groups:
  - name: cdc_pipeline
    interval: 60s
    rules:
      - alert: CDCLagHigh
        expr: (time() - cdc_last_processed_timestamp) > 300
        for: 5m
        labels:
          severity: warning
          component: cdc-pipeline
        annotations:
          summary: "CDC processing lag is high"
          description: "CDC lag is {{ $value }}s for {{ $labels.customer }}"
          
      - alert: LSNCacheBackwardJump
        expr: delta(cdc_lsn_value[5m]) < -1000
        labels:
          severity: critical
          component: cdc-pipeline
        annotations:
          summary: "LSN jumped backward - possible cache loss!"
          description: "LSN decreased, indicating cache reset or corruption"
```

#### Grafana Dashboard Panels:

```json
{
  "panels": [
    {
      "title": "CDC Processing Rate",
      "targets": [
        "rate(cdc_events_processed_total[5m])"
      ]
    },
    {
      "title": "Current CDC Lag",
      "targets": [
        "time() - cdc_last_processed_timestamp"
      ]
    },
    {
      "title": "PostgreSQL Connections",
      "targets": [
        "postgres_connections_active",
        "postgres_connections_max"
      ]
    }
  ]
}
```

---

### Safeguard #5: Graceful Degradation on Cache Loss

**Priority**: 🟡 MEDIUM  
**Effort**: MEDIUM (4-6 hours)  
**Environment**: All

#### Implement Smart LSN Recovery:

Instead of always starting from `0x00000000000000000000`, add logic to start from current LSN if cache is missing.

**Template Modification** (`pipeline-templates/source-pipeline.yaml`):

```yaml
processors:
  # Initialize with default LSN OR get current LSN from MSSQL
  - bloblang: |
      # Check if this is first run (no cache)
      root.is_first_run = true
      root.last_lsn = "0x00000000000000000000"
  
  # Try to get last processed LSN from cache
  - try:
      - branch:
          request_map: 'root = ""'
          processors:
            - cache:
                resource: lsn_cache
                operator: get
                key: "actor_last_lsn"
          result_map: |
            root.last_lsn = content().string()
            root.is_first_run = false
  
  # If cache missing (first run), get CURRENT LSN instead of replaying all history
  - branch:
      request_map: 'root = this'
      processors:
        - bloblang: |
            # Only execute if is_first_run = true
            root = if this.is_first_run { this } else { deleted() }
        
        - sql_raw:
            driver: mssql
            dsn: "${MSSQL_DSN}"
            query: |
              -- Get current maximum LSN to start from NOW
              SELECT CONVERT(VARCHAR(22), sys.fn_cdc_get_max_lsn(), 1) AS current_lsn
            args_mapping: 'root = []'
      
      result_map: |
        # Use current LSN if we got it, otherwise keep default
        root.last_lsn = if this.from.0.current_lsn != null { 
          this.from.0.current_lsn 
        } else { 
          root.last_lsn 
        }
```

**Configuration Flag** (environment variable):

```yaml
# Environment variable to control behavior
LSN_RECOVERY_MODE: "current"  # Options: "beginning" or "current"

# If "current": Start from current LSN on cache loss (no replay)
# If "beginning": Start from beginning (replay all history)
```

---

### Safeguard #6: Documented Recovery Procedures

**Priority**: 🟡 MEDIUM  
**Effort**: LOW (2 hours)  
**Environment**: All

#### Create Runbook for Common Scenarios:

**Runbook: LSN Cache Lost in Production**

```markdown
## Incident: LSN Cache Lost

### Detection
- Alert: `LSNCacheBackwardJump` or `lsn_cache_file_missing`
- Symptom: Pipeline processing old CDC events

### Immediate Actions
1. Check if cache volume still exists:
   ```bash
   kubectl get pvc lsn-cache-pvc
   kubectl exec -it <pod> -- ls -la /data/lsn_cache/
   ```

2. If cache exists but empty - check for recent backups:
   ```bash
   ls -lh backups/lsn_cache_*.tar.gz | tail -5
   ```

3. Restore from backup (if available):
   ```bash
   kubectl cp backups/lsn_cache_20260128.tar.gz \
     <pod>:/data/lsn_cache/
   kubectl delete pod <pod>  # Restart to load cache
   ```

### If No Backup Available

**Option A: Fast Forward (Recommended for Production)**
```bash
# Set LSN to current to skip replay
kubectl exec -it <pod> -- sh
# Inside pod:
echo "0x$(date +%s)" > /data/lsn_cache/actor_last_lsn
# This will skip old data but resume from now
```

**Option B: Full Replay (Use only if data completeness is critical)**
```bash
# Let pipeline replay from beginning
# Monitor closely:
kubectl logs -f <pod> | grep "Processing CDC"
```

### Prevention Checklist
- [ ] Verify PVC is bound
- [ ] Check backup CronJob is running
- [ ] Test restore procedure monthly
```

---

## 📊 Testing & Validation

### Pre-Production Testing Checklist

Before deploying to production, test these scenarios:

#### Test 1: Cache Loss Recovery
```bash
# 1. Run pipeline and process some CDC events
# 2. Delete LSN cache
kubectl exec -it <pod> -- rm -rf /data/lsn_cache/*
# 3. Restart pod
kubectl delete pod <pod>
# 4. Verify behavior (should start from beginning or current based on config)
# 5. Measure replay time
```

**Success Criteria**:
- ✅ Pipeline resumes without errors
- ✅ Replay completes in acceptable time (< 30 minutes for expected CDC volume)
- ✅ No data corruption in PostgreSQL

#### Test 2: CDC Retention Cleanup
```bash
# 1. Generate test CDC data
# 2. Wait for retention period to pass
# 3. Verify old CDC data is cleaned up
SELECT COUNT(*) FROM cdc.dbo_Actor_CT 
WHERE __$start_lsn < CONVERT(VARBINARY(10), '<old-lsn>', 1);
```

**Success Criteria**:
- ✅ Old CDC records removed after retention period
- ✅ Active CDC still works
- ✅ Pipeline not affected by cleanup

#### Test 3: Performance Under Load
```bash
# Simulate replay of large CDC dataset
# Monitor metrics during replay
```

**Success Criteria**:
- ✅ PostgreSQL connections < 80% max
- ✅ CDC lag recovers to normal after replay
- ✅ No application timeouts

---

## Implementation Roadmap

### Phase 1: Critical Safeguards (Week 1)
**Must complete before production deployment**

- [ ] Implement persistent volume for LSN cache (Safeguard #1)
- [ ] Configure MSSQL CDC retention (Safeguard #2)
- [ ] Set Kafka topic retention (Safeguard #3)
- [ ] Test cache loss recovery scenario
- [ ] Document rollback procedures

### Phase 2: Monitoring (Week 2)
**Deploy alongside production launch**

- [ ] Set up Prometheus metrics (Safeguard #4)
- [ ] Configure Grafana dashboards
- [ ] Set up alerting rules
- [ ] Test alerts with simulated failures

### Phase 3: Enhancements (Week 3-4)
**Improve operational resilience**

- [ ] Implement smart LSN recovery (Safeguard #5)
- [ ] Create runbooks (Safeguard #6)
- [ ] Automated backup for LSN cache
- [ ] Regular disaster recovery drills

---

## Acceptance Criteria for Production

Before deploying CDC pipeline to production, verify:

### Infrastructure Checklist
- [ ] LSN cache uses persistent volume with automatic backups
- [ ] MSSQL CDC retention configured (2-7 days)
- [ ] Kafka topic retention set (7 days minimum)
- [ ] PostgreSQL connection pooling properly sized

### Monitoring Checklist
- [ ] CDC lag monitoring with alerts
- [ ] LSN cache status monitoring
- [ ] PostgreSQL connection monitoring
- [ ] CDC table size monitoring

### Documentation Checklist
- [ ] Recovery runbooks written and tested
- [ ] Escalation procedures defined
- [ ] Team trained on CDC troubleshooting

### Testing Checklist
- [ ] Cache loss recovery tested successfully
- [ ] Performance under load validated
- [ ] Failover procedures tested
- [ ] Disaster recovery drill completed

---

## Appendix A: Current Architecture Gaps

### Development vs. Production Comparison

| Component | Development | Production Needed |
|-----------|-------------|-------------------|
| LSN Cache | Local directory (`.lsn_cache/`) | PersistentVolume + backups |
| MSSQL CDC | No retention policy | 2-7 day retention |
| Kafka Topics | No retention | 7-day retention |
| Monitoring | Manual logs | Prometheus + Grafana |
| Alerts | None | PagerDuty/Slack integration |
| Recovery | Manual intervention | Automated + runbooks |

---

## Appendix B: Contact & Support

**Document Owner**: CDC Pipeline Team  
**Review Frequency**: Quarterly  
**Next Review**: 2026-04-28

**Escalation**:
1. On-call Engineer: [PagerDuty rotation]
2. Database Team: [Contact info]
3. Platform Team: [Contact info]

---

## Change Log

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2026-01-28 | 1.0 | Initial document creation | GitHub Copilot |

