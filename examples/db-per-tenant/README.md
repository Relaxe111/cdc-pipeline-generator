# db-per-tenant Reference Implementation

**Pattern:** Each customer has a dedicated source database  
**Example:** AdOpus with 26 customer databases  
**Pipeline Generation:** N source + N sink pipelines (one per customer)

## Overview

This pattern is used when your multi-tenant system isolates customers at the database level. Each customer (tenant) has their own dedicated database with identical schema structure.

### When to Use This Pattern

✅ **Use db-per-tenant when:**
- Each customer has a separate database
- Strong data isolation requirements
- Different customers may have different database sizes/performance needs
- Easier customer-specific backups and migrations

❌ **Don't use db-per-tenant when:**
- All customers share one database (use `db-shared` instead)
- You need cross-customer queries
- Managing hundreds of databases becomes complex

## Architecture

```
Source (MSSQL):                     Kafka:                      Sink (PostgreSQL):
┌──────────────────┐               ┌────────────────┐          ┌──────────────────┐
│ AdOpusBrukerforum│──CDC──────────│ cdc.adopus.    │─────────▶│ brukerforum      │
│ (Database)       │               │ brukerforum.*  │          │ (Schema)         │
└──────────────────┘               └────────────────┘          └──────────────────┘

┌──────────────────┐               ┌────────────────┐          ┌──────────────────┐
│ AdOpusFretexDev  │──CDC──────────│ cdc.adopus.    │─────────▶│ fretexdev        │
│ (Database)       │               │ fretexdev.*    │          │ (Schema)         │
└──────────────────┘               └────────────────┘          └──────────────────┘

... (26 total customers)
```

**Flow:**
1. Each customer database has CDC enabled
2. Source pipeline reads CDC per database
3. Kafka topics namespaced by customer (`cdc.adopus.{customer}.{table}`)
4. Sink pipeline writes to customer-specific PostgreSQL schema

## Configuration Files

### 1. source-groups.yaml

Defines the server group with `server_group_type: db-per-tenant`:

```yaml
server_groups:
  - name: adopus
    server_group_type: db-per-tenant  # Key: defines pattern
    service: adopus
    database_ref: AdOpusTest           # Reference DB for validation
    include_pattern: AdOpus*            # Match customer databases
    server:
      type: mssql
      host: ${MSSQL_NONPROD_HOST}
    databases:
      - name: AdOpusBrukerforum        # Auto-discovered
        service: adopus
        schemas: [dbo]
```

### 2. services/adopus.yaml

Service configuration with table definitions:

```yaml
service: adopus
server_group: adopus
reference: brukerforum  # Reference customer for schema validation

source_tables:
  - schema: dbo
    tables:
      - name: Actor
      - name: Fraver

# Customers auto-discovered from databases matching include_pattern
# in source-groups.yaml
```

### 3. templates/*.yaml

Pipeline templates with placeholders:

- `source-pipeline.yaml` - CDC reader template (`{{CUSTOMER}}`, `{{DATABASE}}`)
- `sink-pipeline.yaml` - PostgreSQL writer template (`{{SCHEMA}}`)

## Generated Output

For each customer database, the generator creates:

```
generated/pipelines/adopus/
├── nonprod/
│   ├── brukerforum/
│   │   ├── source-brukerforum.yaml       # Reads AdOpusBrukerforum
│   │   └── sink-brukerforum.yaml         # Writes to brukerforum schema
│   └── fretexdev/
│       ├── source-fretexdev.yaml         # Reads AdOpusFretexDev
│       └── sink-fretexdev.yaml           # Writes to fretexdev schema
└── prod/
    └── ... (production customers)
```

## Usage

### 1. Initialize Server Groups

```bash
# Discover databases from MSSQL server
cdc-generator manage-source-groups --update

# This auto-populates databases: section in source-groups.yaml
```

### 2. Configure Service

```bash
# Add tables to CDC
cdc-generator manage-service --service adopus --add-table Actor --primary-key actno
cdc-generator manage-service --service adopus --add-table Fraver --primary-key FraverId

# List available tables
cdc-generator manage-service --service adopus --inspect --schema dbo
```

### 3. Generate Pipelines

```bash
# Generate for all customers and environments
cdc-generator generate --service adopus

# Generate for specific environment
cdc-generator generate --service adopus --environment nonprod

# Generate for specific customer
cdc-generator generate --service adopus --customer brukerforum
```

### 4. Deploy Pipelines

```bash
# Apply generated pipelines to Redpanda Connect
kubectl apply -f generated/pipelines/adopus/nonprod/
```

## Key Features

### Auto-Discovery

Databases matching `include_pattern` are automatically discovered:

```bash
# Run discovery
cdc-generator manage-source-groups --update

# Adds to source-groups.yaml:
databases:
  - name: AdOpusBrukerforum     # ✅ Matches AdOpus*
  - name: AdOpusFretexDev       # ✅ Matches AdOpus*
  - name: AdPraksis             # ❌ Excluded by database_exclude_patterns
```

### Environment-Specific Customers

Different customers per environment (nonprod vs prod):

```yaml
# In service config (optional)
environments:
  nonprod:
    customers: [brukerforum, fretexdev]
  prod:
    customers: [brukerforum, podium, osloprod]
```

### Schema Validation

Uses `database_ref` for schema inspection without accessing all databases:

```bash
# Generates validation schema from reference database
cdc-generator manage-service --service adopus --generate-validation --schema dbo

# Uses AdOpusTest as source of truth for table structure
```

## Environment Variables

Required variables (referenced in source-groups.yaml):

```bash
# Source MSSQL
export MSSQL_NONPROD_HOST=mssql-nonprod.example.com
export MSSQL_NONPROD_PORT=1433
export MSSQL_NONPROD_USER=cdc_user
export MSSQL_NONPROD_PASSWORD=secret

# Sink PostgreSQL
export POSTGRES_URL=postgres://postgres-nonprod.example.com:5432
export POSTGRES_USER=cdc_user
export POSTGRES_PASSWORD=secret

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=kafka-nonprod.example.com:9092
```

## Advantages

✅ **Strong isolation** - Customer data physically separated  
✅ **Independent scaling** - Scale databases individually  
✅ **Easier migrations** - Move one customer at a time  
✅ **Customer-specific operations** - Backups, restores per customer  

## Disadvantages

❌ **More pipelines** - N customers = N source + N sink pipelines  
❌ **Resource overhead** - Each pipeline consumes resources  
❌ **Complex management** - 26 customers = 52 pipelines  
❌ **Cross-customer queries** - Not possible in source system  

## See Also

- **db-shared pattern** - For single-database multi-tenancy (asma example)
- **Pipeline templates** - Template syntax and variables
- **CLI reference** - All available commands
