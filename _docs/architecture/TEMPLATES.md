# CDC Tables Template Management

## Overview

The CDC pipeline now supports centralized CDC table management through a template system. Instead of defining CDC tables in each customer configuration file, you can:

1. Define tables once in [`cdc_tables.yaml`](cdc_tables.yaml)
2. Apply them to all customers or specific customers using [`scripts/apply-cdc-tables.py`](scripts/apply-cdc-tables.py)
3. Add customer-specific overrides when needed

## Quick Start

### 1. View Current Template

```bash
cat cdc_tables.yaml
```

### 2. Apply to All Customers

```bash
# Dry run first (see what would change)
python3 scripts/apply-cdc-tables.py --dry-run

# Apply changes
python3 scripts/apply-cdc-tables.py
```

### 3. Apply to Specific Customer

```bash
# Single customer
python3 scripts/apply-cdc-tables.py avansas

# Multiple customers
python3 scripts/apply-cdc-tables.py avansas fretex genesis
```

### 4. Apply by Environment

```bash
# Apply only to customers with 'local' environment
python3 scripts/apply-cdc-tables.py --env local

# Apply only to customers with 'prod' environment
python3 scripts/apply-cdc-tables.py --env prod
```

## File Structure

### `cdc_tables.yaml` - Template Definition

```yaml
# Standard tables applied to ALL customers
standard_tables:
  - schema: dbo
    table: Actor
    primary_key: actno
    description: "Person/company master data"

# Optional tables (commented out, uncomment to use)
optional_tables: []

# Customer-specific tables
customer_overrides:
  fretex:
    - schema: dbo
      table: Donations
      primary_key: donation_id
      description: "Fretex-specific donations"

# Environment-specific tables
environment_tables:
  local:
    - schema: dbo
      table: TestData
      primary_key: id
  prod: []
```

### `scripts/apply-cdc-tables.py` - Application Script

Features:
- ✅ Dry run mode to preview changes
- ✅ Backup original files before modification
- ✅ Filter by customer name(s)
- ✅ Filter by environment
- ✅ Shows added/removed tables
- ✅ Prevents duplicate tables

## Common Workflows

### Workflow 1: Add a New Standard Table

**Scenario**: Add `Customers` table to all customer configurations

```bash
# 1. Edit cdc_tables.yaml
vim cdc_tables.yaml

# Add to standard_tables:
# - schema: dbo
#   table: Customers
#   primary_key: customer_id
#   description: "Customer master data"

# 2. Preview changes
python3 scripts/apply-cdc-tables.py --dry-run

# 3. Apply to all customers
python3 scripts/apply-cdc-tables.py

# 4. Enable CDC on MSSQL for all customers
python3 scripts/enable-cdc-mssql.py avansas local
python3 scripts/enable-cdc-mssql.py fretex local
# ... or create a loop script
```

### Workflow 2: Add Customer-Specific Table

**Scenario**: Fretex needs a `Donations` table that other customers don't have

```bash
# 1. Edit cdc_tables.yaml
vim cdc_tables.yaml

# Add to customer_overrides:
# customer_overrides:
#   fretex:
#     - schema: dbo
#       table: Donations
#       primary_key: donation_id

# 2. Apply only to fretex
python3 scripts/apply-cdc-tables.py fretex

# 3. Enable CDC
python3 scripts/enable-cdc-mssql.py fretex local

# 4. Regenerate pipelines
python3 scripts/5-generate-pipelines.py fretex local
```

### Workflow 3: Update All Customer Configs

**Scenario**: New project requirement - all customers need 2 new tables

```bash
# 1. Update template
vim cdc_tables.yaml

# standard_tables:
#   - ... existing tables ...
#   - schema: dbo
#     table: Orders
#     primary_key: order_id
#   - schema: dbo
#     table: Invoices
#     primary_key: invoice_id

# 2. Preview changes for all customers
python3 scripts/apply-cdc-tables.py --dry-run | less

# 3. Apply to all (creates backups automatically)
python3 scripts/apply-cdc-tables.py

# 4. Check backups
ls -la pipelines/customers/.backups/
```

### Workflow 4: Test Changes on One Customer First

**Scenario**: Want to test new table on avansas before rolling out

```bash
# 1. Add table to template
vim cdc_tables.yaml

# 2. Apply only to avansas
python3 scripts/apply-cdc-tables.py avansas

# 3. Test locally
python3 scripts/enable-cdc-mssql.py avansas local
python3 scripts/setup-local.py
python3 scripts/verify-pipeline.py

# 4. If successful, apply to all others
python3 scripts/apply-cdc-tables.py --dry-run
python3 scripts/apply-cdc-tables.py
```

## Script Usage Reference

### Command Syntax

```bash
python3 scripts/apply-cdc-tables.py [OPTIONS] [CUSTOMERS...]
```

### Options

| Option | Description | Example |
|--------|-------------|---------|
| (none) | Apply to all customers | `python3 scripts/apply-cdc-tables.py` |
| `--dry-run` | Preview changes without modifying files | `--dry-run` |
| `--list` | List all available customers | `--list` |
| `--env ENV` | Filter by environment (local/nonprod/prod) | `--env local` |

### Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `CUSTOMER` | One or more customer names | `avansas fretex genesis` |

### Examples

```bash
# List all customers
python3 scripts/apply-cdc-tables.py --list

# Dry run on all customers
python3 scripts/apply-cdc-tables.py --dry-run

# Apply to single customer
python3 scripts/apply-cdc-tables.py avansas

# Apply to multiple customers
python3 scripts/apply-cdc-tables.py avansas fretex genesis

# Apply to all customers with local environment
python3 scripts/apply-cdc-tables.py --env local

# Dry run for specific customer
python3 scripts/apply-cdc-tables.py --dry-run avansas
```

## Output Examples

### Dry Run Output

```
[INFO] Loading CDC tables template from cdc_tables.yaml
[INFO] Template has 2 standard tables, 0 optional tables
[WARNING] DRY RUN MODE - No files will be modified

================================================================================
Applying CDC Tables to 3 customer(s)
================================================================================

[CHANGE] avansas:
  Added tables:
    + [dbo].[Customers] (PK: customer_id) (Customer master data)
[INFO] [DRY RUN] Would update avansas.yaml

[INFO] fretex: CDC tables already up to date

[CHANGE] genesis:
  Removed tables:
    - [dbo].[OldTable] (PK: id)
  Added tables:
    + [dbo].[Customers] (PK: customer_id)
[INFO] [DRY RUN] Would update genesis.yaml

================================================================================
[INFO] DRY RUN: Would update 2/3 customer(s)
[INFO] Run without --dry-run to apply changes
================================================================================
```

### Actual Apply Output

```
[INFO] Loading CDC tables template from cdc_tables.yaml
[INFO] Template has 2 standard tables, 0 optional tables

================================================================================
Applying CDC Tables to 26 customer(s)
================================================================================

[CHANGE] avansas:
  Added tables:
    + [dbo].[Customers] (PK: customer_id)
[INFO] Backed up to: pipelines/customers/.backups/avansas_20260125_132500.yaml
[SUCCESS] Updated avansas.yaml

[INFO] fretex: CDC tables already up to date

...

================================================================================
[SUCCESS] Updated 25/26 customer(s)
[INFO] Backups saved to: pipelines/customers/.backups
================================================================================
```

## Template Sections Explained

### Standard Tables

Tables that **all customers** should have:

```yaml
standard_tables:
  - schema: dbo
    table: Actor
    primary_key: actno
    description: "Person/company master data"
```

### Optional Tables

Tables that can be enabled on demand (keep commented out):

```yaml
optional_tables: []
  # Uncomment to add to standard_tables:
  # - schema: dbo
  #   table: Customers
  #   primary_key: customer_id
```

### Customer Overrides

Tables specific to individual customers:

```yaml
customer_overrides:
  fretex:
    - schema: dbo
      table: Donations
      primary_key: donation_id
  avansas:
    - schema: dbo
      table: SpecialTable
      primary_key: id
```

### Environment Tables

Tables only needed in specific environments:

```yaml
environment_tables:
  local:
    - schema: dbo
      table: TestData
      primary_key: id
  nonprod: []
  prod: []
```

## Backup System

### Automatic Backups

Every time the script modifies a customer config, it creates a timestamped backup:

```
pipelines/customers/.backups/
├── avansas_20260125_132500.yaml
├── avansas_20260125_134500.yaml
├── fretex_20260125_132500.yaml
└── genesis_20260125_132500.yaml
```

### Restore from Backup

```bash
# List backups
ls -lt pipelines/customers/.backups/

# Restore specific backup
cp pipelines/customers/.backups/avansas_20260125_132500.yaml \
   pipelines/customers/avansas.yaml
```

## Integration with Other Scripts

### After Applying Tables

Once you've applied CDC tables to customer configs, follow these steps:

```bash
# 1. Apply CDC tables to configs
python3 scripts/apply-cdc-tables.py

# 2. Enable CDC on MSSQL for each customer
python3 scripts/enable-cdc-mssql.py avansas local

# 3. Regenerate pipelines
cd pipelines
python3 generate_pipelines.py avansas local

# 4. Restart pipelines
docker compose restart cdc-redpanda-connect-source

# 5. Verify
python3 scripts/verify-pipeline.py
```

### Full Automation Example

```bash
#!/bin/bash
# Apply CDC tables and update all local customers

CUSTOMERS="avansas fretex genesis"

# 1. Apply template to all local customers
python3 scripts/apply-cdc-tables.py $CUSTOMERS

# 2. Enable CDC for each
for customer in $CUSTOMERS; do
  python3 scripts/enable-cdc-mssql.py $customer local
done

# 3. Regenerate all pipelines
cd pipelines
for customer in $CUSTOMERS; do
  python3 generate_pipelines.py $customer local
done
cd ..

# 4. Restart pipelines
docker compose restart cdc-redpanda-connect-source cdc-redpanda-connect-sink

# 5. Verify
python3 scripts/verify-pipeline.py
```

## Best Practices

1. **Always use --dry-run first** to preview changes
2. **Test on one customer** before applying to all
3. **Check backups** are created before applying to production
4. **Document custom overrides** with good descriptions
5. **Version control** the cdc_tables.yaml file
6. **Review changes** in git diff before committing

## Troubleshooting

### "Customer config not found"

```bash
# List available customers
python3 scripts/apply-cdc-tables.py --list

# Check customer file exists
ls pipelines/customers/customer_name.yaml
```

### "No environments defined"

Customer YAML must have `environments` section:

```yaml
customer: name
environments:
  local:
    # ... config
```

### Restore from Backup

```bash
# Find the backup
ls -lt pipelines/customers/.backups/

# Restore
cp pipelines/customers/.backups/customer_TIMESTAMP.yaml \
   pipelines/customers/customer.yaml
```

### Clear All Backups

```bash
rm -rf pipelines/customers/.backups/
```

## Related Documentation

- [CDC Table Management Guide](CDC_TABLE_MANAGEMENT.md) - Detailed CDC guide
- [CDC Quick Reference](CDC_QUICK_REF.md) - Quick commands
- [Customer Config Workflow](CUSTOMER_CONFIG_WORKFLOW.md) - Customer setup
