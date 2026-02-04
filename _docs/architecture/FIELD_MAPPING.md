# Dynamic Field Mapping System - Implementation Summary

## Overview

Enhanced the CDC management system with **automatic field mapping** from MSSQL schema inspection. The system now:

1. **Inspects MSSQL tables** to discover actual column names and types
2. **Generates field mappings** (MSSQL → PostgreSQL) automatically
3. **Stores mappings** in customer YAML files
4. **Generates dynamic sink pipelines** using the field mappings

## What Changed

### 1. Enhanced apply-cdc-tables.py

**New capabilities:**
- Inspects MSSQL tables and extracts column metadata
- Converts MSSQL column names to PostgreSQL snake_case
- Maps MSSQL data types to PostgreSQL equivalents
- Stores field mappings in customer YAML files

**Field mapping structure:**
```yaml
cdc_tables:
  - schema: dbo
    table: Actor
    primary_key: actno
    fields:
      - mssql: ActorId          # Original MSSQL column name
        postgres: actor_id      # PostgreSQL snake_case name
        type: INTEGER           # PostgreSQL data type
        nullable: false
        primary_key: false
```

### 2. Updated Sink Template

**Before:** Hardcoded field names
```yaml
# Old approach - hardcoded
query: |
  INSERT INTO schema.actor (
    actor_id, actno, firstname, lastname
  ) VALUES ($1, $2, $3, $4)
```

**After:** Dynamic field mapping
```yaml
# New approach - dynamic placeholder
output:
  switch:
    cases:
{{TABLE_CASES}}  # ← Generated from field mappings
```

### 3. Enhanced generate_pipelines.py

**New function: `build_sink_table_cases()`**

Generates switch cases for each table:
- **DELETE operations** - Uses primary key
- **UPSERT operations** - Uses all fields with conflict resolution

**Dynamic SQL generation:**
```python
# Reads field mappings from customer YAML
for table_config in cdc_tables:
    fields = table_config.get('fields', [])
    
    # Builds INSERT with all columns
    # Builds ON CONFLICT with primary key
    # Builds UPDATE SET for all non-PK fields
    # Maps MSSQL field names to PostgreSQL
```

## Workflow

### Complete End-to-End Flow

```bash
# 1. Define tables in cdc_tables.yaml (simple list)
vim pipelines/cdc-management/cdc_tables.yaml

tables:
  - name: Actor
    schema: dbo
    primary_key: actno

# 2. Inspect MSSQL and generate field mappings
cd pipelines/cdc-management
python3 apply-cdc-tables.py avansas local

# What happens:
# - Connects to avansas MSSQL database
# - Inspects Actor table columns
# - Detects: ActorId, actno, FirstName, LastName, Email, etc.
# - Maps to: actor_id, actno, first_name, last_name, email, etc.
# - Stores in avansas.yaml under cdc_tables[].fields
# - Generates PostgreSQL migration script

# 3. Review customer config (now has field mappings)
cat pipelines/customers/avansas.yaml

# 4. Generate pipelines with dynamic field mapping
cd pipelines
python3 generate_pipelines.py avansas local

# What happens:
# - Reads field mappings from avansas.yaml
# - Generates DELETE case for each table
# - Generates UPSERT case for each table
# - Uses actual column names from MSSQL
# - Maps to PostgreSQL snake_case names
# - Creates proper conflict resolution

# 5. Result: Dynamic sink pipeline
cat generated/local/avansas/sink-pipeline.yaml
```

## Example: Generated Sink Case

For Actor table with field mappings, generates:

```yaml
# DELETE for Actor
- check: 'meta("table_name") == "Actor" && meta("source_op") == "d"'
  output:
    sql_raw:
      driver: postgres
      dsn: "postgres://..."
      query: |
        DELETE FROM avansas.actor
        WHERE actno = $1
      args_mapping: |
        root = [this.actno]
      exec_only: true

# UPSERT for Actor
- check: 'meta("table_name") == "Actor" && meta("source_op") != "d"'
  output:
    sql_raw:
      driver: postgres
      dsn: "postgres://..."
      query: |
        INSERT INTO avansas.actor (
          actor_id, actno, first_name, last_name, email, phone_number, created_date,
          __sync_timestamp, __source, __source_db, __source_table, __source_ts_ms, __cdc_operation
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
        )
        ON CONFLICT (actno) DO UPDATE SET
          actor_id = EXCLUDED.actor_id,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          email = EXCLUDED.email,
          phone_number = EXCLUDED.phone_number,
          created_date = EXCLUDED.created_date,
          __sync_timestamp = EXCLUDED.__sync_timestamp,
          __source = EXCLUDED.__source,
          __source_db = EXCLUDED.__source_db,
          __source_table = EXCLUDED.__source_table,
          __source_ts_ms = EXCLUDED.__source_ts_ms,
          __cdc_operation = EXCLUDED.__cdc_operation
      args_mapping: |
        root = [
          this.ActorId,
          this.actno,
          this.FirstName,
          this.LastName,
          this.Email,
          this.PhoneNumber,
          this.CreatedDate,
          this.__sync_timestamp,
          this.__source,
          this.__source_db,
          this.__source_table,
          this.__source_ts_ms,
          this.__cdc_operation
        ]
      exec_only: true
```

## Key Benefits

### 1. No More Hardcoding
- **Before:** Manually code each table's fields in template
- **After:** Automatically discovered from MSSQL schema

### 2. Accurate Field Mapping
- **Before:** Guess field names and types
- **After:** Inspect actual MSSQL schema

### 3. Automatic Type Conversion
- **Before:** Manual type mapping
- **After:** Automatic MSSQL → PostgreSQL type mapping

### 4. Consistent Naming
- **Before:** Inconsistent PascalCase/snake_case
- **After:** Consistent snake_case in PostgreSQL

### 5. Correct Primary Keys
- **Before:** Assume 'id' or guess
- **After:** Detect from MSSQL schema (e.g., 'actno' for Actor)

## Type Mapping Examples

| MSSQL Type | PostgreSQL Type |
|------------|----------------|
| `int` | `INTEGER` |
| `varchar(100)` | `VARCHAR(100)` |
| `nvarchar(255)` | `VARCHAR(255)` |
| `datetime`, `datetime2` | `TIMESTAMP` |
| `bit` | `BOOLEAN` |
| `decimal(10,2)` | `DECIMAL(10,2)` |
| `uniqueidentifier` | `UUID` |

## Field Name Conversion Examples

| MSSQL (PascalCase) | PostgreSQL (snake_case) |
|-------------------|------------------------|
| `ActorId` | `actor_id` |
| `FirstName` | `first_name` |
| `PhoneNumber` | `phone_number` |
| `BrukerNavn` | `bruker_navn` |
| `AccessLevel` | `access_level` |

## Customer YAML Structure

### Before Enhancement
```yaml
customer: avansas
schema: avansas

cdc_tables:
  - schema: dbo
    table: Actor
    primary_key: actno
```

### After Enhancement
```yaml
customer: avansas
schema: avansas

cdc_tables:
  - schema: dbo
    table: Actor
    primary_key: actno
    fields:  # ← NEW: Field mappings from MSSQL inspection
      - mssql: ActorId
        postgres: actor_id
        type: INTEGER
        nullable: false
        primary_key: false
      - mssql: actno
        postgres: actno
        type: VARCHAR(50)
        nullable: false
        primary_key: true
      - mssql: FirstName
        postgres: first_name
        type: VARCHAR(100)
        nullable: true
        primary_key: false
      # ... more fields
```

## Usage

### Prerequisites

```bash
# Install required Python packages
pip install pymssql pyyaml

# Or add to requirements.txt:
# pymssql>=2.2.0
# PyYAML>=6.0
```

### Basic Workflow

```bash
# 1. Add table to template
vim pipelines/cdc-management/cdc_tables.yaml

# 2. Inspect and generate mappings
cd pipelines/cdc-management
python3 apply-cdc-tables.py avansas local

# 3. Generate pipelines
cd ../
python3 generate_pipelines.py avansas local

# 4. Review generated pipeline
cat generated/local/avansas/sink-pipeline.yaml
```

### Advanced Options

```bash
# Process all customers
python3 apply-cdc-tables.py

# Specific customers
python3 apply-cdc-tables.py avansas fretex

# Preview without changes
python3 apply-cdc-tables.py --dry-run

# Use different environment
python3 apply-cdc-tables.py avansas --env nonprod
```

## Error Handling

### Missing pymssql
```
[WARNING] pymssql not installed - MSSQL inspection will not be available
[INFO] Install with: pip install pymssql
```

**Solution:** Install pymssql or skip MSSQL inspection for --list operations

### Cannot Connect to MSSQL
```
[ERROR] Failed to inspect MSSQL table [dbo].[Actor]: Login failed
```

**Check:**
- MSSQL server is running
- Connection info in customer YAML is correct
- Network/firewall allows connection

### No Field Mappings Generated
```
[WARNING] Could not inspect table [dbo].[Actor] - skipping migration
```

**Causes:**
- Table doesn't exist in MSSQL
- No permissions to access table
- Wrong database/schema name

## Migration Path

### For Existing Customers

If you already have customer configs without field mappings:

```bash
# 1. Run apply-cdc-tables.py to add field mappings
python3 apply-cdc-tables.py

# 2. Backup is created automatically in .backups/

# 3. Regenerate all pipelines
cd ../
python3 generate_pipelines.py

# 4. Review changes
git diff generated/
```

## Files Modified

### Created
- `pipelines/cdc-management/apply-cdc-tables.py` (enhanced)
- `pipelines/cdc-management/EXAMPLE_customer_with_field_mappings.yaml`
- `FIELD_MAPPING_IMPLEMENTATION.md` (this file)

### Modified
- `pipelines/templates/sink-pipeline.yaml` (now dynamic)
- `pipelines/generate_pipelines.py` (added `build_sink_table_cases()`)

### Auto-Generated
- `pipelines/customers/*.yaml` (field mappings added)
- `pipelines/customers/.backups/*.yaml` (automatic backups)

## Testing

### Verify Field Mappings

```bash
# Check customer config has field mappings
cat pipelines/customers/avansas.yaml | grep -A 10 "fields:"

# Should show:
#     fields:
#       - mssql: ActorId
#         postgres: actor_id
#         type: INTEGER
#         ...
```

### Verify Generated Pipeline

```bash
# Generate pipeline
python3 generate_pipelines.py avansas local

# Check sink has dynamic cases
cat generated/local/avansas/sink-pipeline.yaml | grep -A 20 "check:"

# Should show dynamic switch cases with actual field names
```

## Troubleshooting

### Q: Sink pipeline still has hardcoded fields?
**A:** Regenerate pipelines after running apply-cdc-tables.py:
```bash
python3 apply-cdc-tables.py avansas
python3 generate_pipelines.py avansas
```

### Q: Wrong field names in PostgreSQL?
**A:** Check field mappings in customer YAML. The `postgres` field controls the PostgreSQL column name. Edit if needed.

### Q: Wrong primary key in ON CONFLICT?
**A:** Update `primary_key` field in customer YAML cdc_tables section, then regenerate.

### Q: Missing columns in sink?
**A:** Re-run apply-cdc-tables.py to refresh field mappings from current MSSQL schema.

## Future Enhancements

Potential improvements:
1. **Custom field transformations** - Add transform rules in customer YAML
2. **Exclude fields** - Mark fields to skip in CDC
3. **Field-level type overrides** - Override auto-detected types
4. **Composite primary keys** - Support multi-column PKs
5. **Source template dynamic generation** - Apply same approach to source pipeline

## Summary

The dynamic field mapping system:
- ✅ Eliminates hardcoded field names
- ✅ Automatically discovers schema from MSSQL
- ✅ Generates accurate PostgreSQL migrations
- ✅ Creates dynamic sink pipelines
- ✅ Supports all 26 customers
- ✅ Handles gracefully when pymssql not available
- ✅ Provides automatic backups
- ✅ Works with remote MSSQL databases

**Result:** Add one table to template → inspect once → applies to all customers with correct field mappings!
