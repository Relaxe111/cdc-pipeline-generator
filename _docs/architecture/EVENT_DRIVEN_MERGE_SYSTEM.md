# Event-Driven CDC Merge System - Implementation Summary

## Overview
Successfully implemented an event-driven merge control system that automatically merges staging tables to final tables using PostgreSQL triggers and pg_cron.

## Components Created/Updated

### 1. Merge Control Migration (`generated/pg-migrations/0-cdc-merge-control.sql`)
**Status**: ✅ Generated automatically by apply-tables script

**Components**:
- `cdc_merge_control` table: Tracks merge status per schema/table
- `mark_table_for_merge()` function: Trigger function that sets has_changes=true
- `trigger_pending_merges()` function: Executor that calls merge procedures
- `v_cdc_merge_status` view: Monitoring view for current status
- `manual_merge()` function: Utility for manual merge triggering
- pg_cron job template (commented out, requires manual enablement)

**Auto-registration**: Tables are automatically added to control table on first staging INSERT (via trigger's ON CONFLICT INSERT)

### 2. Staging Table Template (`scripts/sql/staging-tables-setup.sql`)
**Status**: ✅ Updated with trigger creation

**Added**:
```sql
CREATE TRIGGER trg_mark_for_merge
    AFTER INSERT ON {{SCHEMA}}."{{STG_TABLE}}"
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.mark_table_for_merge();
```

**Benefits**:
- Fires once per INSERT statement (not per row)
- Automatically marks table for merge when new data arrives
- No manual intervention needed

### 3. Generation Script (`scripts/2-apply-cdc-tables.py`)
**Status**: ✅ Updated to generate merge control migration

**Added Method**: `generate_merge_control_migration()`
- Creates `0-cdc-merge-control.sql` on first run
- Skips if file already exists
- Called before processing log generation (execution order)

**Integration**:
- Runs automatically when executing: `python3 scripts/2-apply-cdc-tables.py`
- File prefix `0-` ensures it runs before staging tables (`3-*-staging.sql`)

### 4. Generated Staging Tables
**Status**: ✅ All staging tables include trigger

**Example**: `generated/pg-migrations/3-Fraver-staging.sql`
```sql
CREATE TRIGGER trg_mark_for_merge
    AFTER INSERT ON {{SCHEMA}}."stg_Fraver"
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.mark_table_for_merge();
```

**Runtime**: `{{SCHEMA}}` placeholder replaced with customer schema (e.g., `avansas`) during migration application

## Workflow

### Generation (Developer)
```bash
# Generate all migrations including merge control
python3 scripts/2-apply-cdc-tables.py

# Output:
# - generated/pg-migrations/0-cdc-merge-control.sql (control system)
# - generated/pg-migrations/1-cdc-processing-log.sql (metrics)
# - generated/pg-migrations/2-{Table}.sql (final tables)
# - generated/pg-migrations/3-{Table}-staging.sql (staging + trigger)
```

### Deployment (Operations)
```bash
# Apply migrations to customer schema
cdc enable-cdc avansas --env nonprod --migrations-only

# Migrations execute in order:
# 1. 0-cdc-merge-control.sql (creates control table + functions)
# 2. 1-cdc-processing-log.sql (creates processing log)
# 3. 2-*.sql (creates final tables)
# 4. 3-*-staging.sql (creates staging tables + triggers)
```

### Runtime (Automatic)
```
1. Redpanda Connect → INSERT to avansas.stg_Fraver
2. Trigger fires → mark_table_for_merge()
3. Control table updated:
   - has_changes = true
   - last_change_time = NOW()
   - schema_name = 'avansas'
   - table_name = 'Fraver'
4. pg_cron runs trigger_pending_merges() every 5 seconds
5. Checks for tables where:
   - has_changes = true
   - last_change_time < NOW() - 5 seconds (batching window)
6. Calls CALL avansas.sp_merge_fraver()
7. Updates metrics:
   - has_changes = false
   - last_merge_time = NOW()
   - merge_count += 1
   - last_merge_duration_ms = duration
```

## Configuration

### Enable pg_cron (One-time setup per database)
```sql
-- 1. Enable extension
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 2. Uncomment in 0-cdc-merge-control.sql or run manually:
SELECT cron.schedule(
    'cdc_auto_merge',
    '*/5 * * * * *',  -- Every 5 seconds
    $$SELECT public.trigger_pending_merges(5)$$
);

-- 3. Verify job is running
SELECT * FROM cron.job;

-- 4. Check job run history
SELECT * FROM cron.job_run_details ORDER BY start_time DESC LIMIT 10;
```

### Batching Window
Default: 5 seconds (prevents merge-per-insert spam)

**Adjust if needed**:
- More frequent: `trigger_pending_merges(1)` = merge after 1 second
- Less frequent: `trigger_pending_merges(10)` = merge after 10 seconds
- Immediate: `trigger_pending_merges(0)` = merge immediately (use with caution)

## Monitoring

### Check Merge Status
```sql
-- Real-time view
SELECT * FROM v_cdc_merge_status;

-- Columns:
--   schema_name, table_name, has_changes, is_merging
--   last_change_time, last_merge_time, seconds_since_change
--   merge_count, last_merge_duration_ms, last_merge_rows
--   merge_status: MERGING | PENDING | BATCHING | IDLE
```

### Manual Merge (Testing)
```sql
-- Force immediate merge for specific table
SELECT manual_merge('avansas', 'Fraver');

-- Check execution
SELECT * FROM trigger_pending_merges(0);
```

### Metrics
```sql
-- Top tables by merge count
SELECT schema_name, table_name, merge_count, last_merge_duration_ms, last_merge_rows
FROM cdc_merge_control
ORDER BY merge_count DESC;

-- Tables with pending changes
SELECT schema_name, table_name, 
       EXTRACT(EPOCH FROM (NOW() - last_change_time))::INT AS seconds_pending
FROM cdc_merge_control
WHERE has_changes = true
ORDER BY last_change_time ASC;

-- Merge performance
SELECT schema_name, table_name, 
       last_merge_duration_ms, 
       last_merge_rows,
       (last_merge_duration_ms::FLOAT / NULLIF(last_merge_rows, 0))::NUMERIC(10,2) AS ms_per_row
FROM cdc_merge_control
WHERE last_merge_time IS NOT NULL
ORDER BY last_merge_duration_ms DESC;
```

## Benefits

### Event-Driven
- ✅ Automatic triggering when data arrives
- ✅ No manual intervention needed
- ✅ No polling overhead from application code

### Batching Window
- ✅ Prevents merge-per-insert spam (5-second window)
- ✅ Batches multiple inserts together
- ✅ Reduces merge overhead for high-frequency tables

### Per-Table Granularity
- ✅ Each table merges independently
- ✅ No blocked tables waiting for slow merges
- ✅ Parallel execution possible

### Concurrency Control
- ✅ `is_merging` flag prevents duplicate executions
- ✅ `FOR UPDATE SKIP LOCKED` prevents lock contention
- ✅ Safe for multiple pg_cron workers

### Metrics & Monitoring
- ✅ Tracks merge count, duration, rows per table
- ✅ Real-time status view
- ✅ Identifies slow/stuck merges
- ✅ Audit trail for troubleshooting

## Testing

### Verify Script Integration
The verify script (`scripts/6-verify-pipeline.py`) already calls merge procedures after staging check:

```bash
# Test with single record
cdc verify avansas --env nonprod --table Fraver

# Test with batch (10 records)
cdc verify avansas --env nonprod --table Fraver --records 10
```

**Flow**:
1. INSERT to MSSQL
2. Wait for Kafka
3. Wait for staging INSERT
4. **Call merge procedure** (manual trigger for testing)
5. Verify final table count
6. Verify staging cleanup

### Event-Driven Test
```bash
# 1. Enable pg_cron (see Configuration section)

# 2. Insert to staging manually
psql -c "INSERT INTO avansas.stg_Fraver (...) VALUES (...);"

# 3. Check control table immediately
psql -c "SELECT * FROM cdc_merge_control WHERE table_name = 'Fraver';"
# Expected: has_changes = true, last_change_time = NOW()

# 4. Wait 6+ seconds, check again
sleep 6
psql -c "SELECT * FROM v_cdc_merge_status WHERE table_name = 'Fraver';"
# Expected: has_changes = false, merge_count = 1

# 5. Verify staging is empty
psql -c "SELECT COUNT(*) FROM avansas.stg_Fraver;"
# Expected: 0

# 6. Verify final table has data
psql -c "SELECT COUNT(*) FROM avansas.Fraver WHERE ...;"
# Expected: 1+
```

## Troubleshooting

### Merges not running
```sql
-- Check if pg_cron job exists
SELECT * FROM cron.job WHERE jobname = 'cdc_auto_merge';

-- Check job run history
SELECT * FROM cron.job_run_details 
WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'cdc_auto_merge')
ORDER BY start_time DESC LIMIT 10;

-- Manually trigger merge
SELECT * FROM trigger_pending_merges(0);
```

### Stuck in is_merging=true
```sql
-- Reset stuck flag (if merge process crashed)
UPDATE cdc_merge_control
SET is_merging = false, updated_at = NOW()
WHERE is_merging = true 
  AND last_merge_time < NOW() - INTERVAL '5 minutes';
```

### High merge frequency
```sql
-- Identify tables merging too often
SELECT schema_name, table_name, merge_count, 
       EXTRACT(EPOCH FROM (NOW() - created_at))::INT AS seconds_active,
       (merge_count::FLOAT / NULLIF(EXTRACT(EPOCH FROM (NOW() - created_at)), 0) * 60)::NUMERIC(10,2) AS merges_per_minute
FROM cdc_merge_control
ORDER BY merges_per_minute DESC;

-- Solution: Increase batching window for specific tables
-- Or: Reduce CDC change frequency on MSSQL side
```

### Verify trigger exists
```sql
-- Check triggers on staging tables
SELECT schemaname, tablename, trigname 
FROM pg_triggers 
WHERE tablename LIKE 'stg_%'
  AND schemaname = 'avansas'
ORDER BY tablename;

-- Expected: trg_mark_for_merge on each stg_* table
```

## Migration Execution Order
Files in `generated/pg-migrations/` execute in alphabetical order:

1. `0-cdc-merge-control.sql` - Control system (public schema)
2. `0-create-customer-schemas.sql` - Create schemas
3. `1-cdc-processing-log.sql` - Processing log per schema
4. `2-Actor.sql` - Final table (Actor)
5. `2-Fraver.sql` - Final table (Fraver)
6. `3-Actor-staging.sql` - Staging + trigger (Actor)
7. `3-Fraver-staging.sql` - Staging + trigger (Fraver)

The prefix numbering ensures correct dependency order.

## Next Steps

1. ✅ **DONE**: Generate merge control migration
2. ✅ **DONE**: Add triggers to staging table template
3. ✅ **DONE**: Integrate into generation pipeline
4. ⏳ **TODO**: Enable pg_cron on target databases
5. ⏳ **TODO**: Test with production-like load
6. ⏳ **TODO**: Document in main README
7. ⏳ **TODO**: Add alerting for stuck merges
