# db-shared Reference Implementation

**Pattern:** All customers share a single database  
**Example:** ASMA directory service  
**Pipeline Generation:** 1 source + 1 sink pipeline (for all customers)

## Overview

This pattern is used when your multi-tenant system keeps all customers in a single database, with isolation via schema separation, customer_id columns, or row-level security.

### When to Use This Pattern

✅ **Use db-shared when:**
- All customers in one database
- Customer isolation via schema or `customer_id` column
- Cross-customer queries needed
- Simpler infrastructure (one database to manage)
- Lower resource overhead

❌ **Don't use db-shared when:**
- Customers need dedicated databases (use `db-per-tenant` instead)
- Strong physical isolation required
- Individual customer scaling needed

## Architecture

```
Source (PostgreSQL):                Kafka:                      Sink (PostgreSQL):
┌──────────────────────┐           ┌────────────────┐          ┌──────────────────┐
│ adopus_db_directory  │           │ cdc.directory. │          │ directory        │
│                      │           │ organizations  │          │                  │
│ ├─ public.           │──CDC──────│                │─────────▶│ ├─ organizations │
│ │  organizations     │           │ cdc.directory. │          │ └─ users         │
│ └─ public.users      │           │ users          │          │                  │
└──────────────────────┘           └────────────────┘          └──────────────────┘
      (All customers                   (Shared topics)              (Shared schema)
       in same DB)
```

**Flow:**
1. Single database with CDC enabled on all tables
2. One source pipeline reads all customer data
3. Kafka topics not customer-specific (`cdc.directory.organizations`)
4. One sink pipeline writes to shared PostgreSQL schema
5. Customer isolation maintained via `customer_id` or schema

## Configuration Files

### 1. server-groups.yaml

Defines the server group with `server_group_type: db-shared`:

```yaml
server_groups:
  - name: asma
    server_group_type: db-shared  # Key: defines pattern
    service: directory
    server:
      type: postgres
      host: ${POSTGRES_ASMA_HOST}
    databases:
      - name: adopus_db_directory_dev  # Single shared database
        service: directory
        schemas:
          - public
          - customer_a  # Optional: schema-based isolation
          - customer_b
```

### 2. services/directory.yaml

Service configuration with table definitions:

```yaml
service: directory
server_group: asma

source:
  type: postgres
  tables:
    - schema: public
      name: organizations
      primary_key: id
      # Optional: Filter for specific customer
      # filter: "customer_id = '{{CUSTOMER_ID}}'"
```

### 3. templates/*.yaml

Pipeline templates (simpler than db-per-tenant):

- `source-pipeline.yaml` - CDC reader (no customer placeholders)
- `sink-pipeline.yaml` - PostgreSQL writer (single schema)

## Generated Output

Single set of pipelines for all customers:

```
generated/pipelines/directory/
├── nonprod/
│   ├── source-directory.yaml     # Reads all customers
│   └── sink-directory.yaml       # Writes to shared schema
└── prod/
    ├── source-directory.yaml
    └── sink-directory.yaml
```

## Usage

### 1. Configure Service

```bash
# Add tables to CDC
cdc-generator manage-service --service directory --add-table organizations --primary-key id
cdc-generator manage-service --service directory --add-table users --primary-key id

# List available tables
cdc-generator manage-service --service directory --inspect --schema public
```

### 2. Generate Pipelines

```bash
# Generate for all environments
cdc-generator generate --service directory

# Generate for specific environment
cdc-generator generate --service directory --environment nonprod
```

### 3. Deploy Pipelines

```bash
# Apply generated pipelines
kubectl apply -f generated/pipelines/directory/nonprod/
```

## Multi-Tenancy Strategies

### Strategy 1: customer_id Column

All customers share the same tables with a `customer_id` discriminator:

```sql
-- Source table structure
CREATE TABLE organizations (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,  -- Tenant identifier
    name VARCHAR(255),
    created_at TIMESTAMP
);

-- Sink: Same structure preserved
CREATE TABLE organizations (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMP
);

-- Query for specific customer
SELECT * FROM organizations WHERE customer_id = 'acme_corp';
```

### Strategy 2: Schema-Based Isolation

Each customer gets their own schema:

```sql
-- Customer A schema
CREATE SCHEMA customer_a;
CREATE TABLE customer_a.organizations (
    id UUID PRIMARY KEY,
    name VARCHAR(255)
);

-- Customer B schema
CREATE SCHEMA customer_b;
CREATE TABLE customer_b.organizations (
    id UUID PRIMARY KEY,
    name VARCHAR(255)
);

-- CDC replicates all schemas
```

### Strategy 3: Row-Level Security (PostgreSQL)

Use PostgreSQL RLS for transparent customer isolation:

```sql
CREATE TABLE organizations (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    name VARCHAR(255)
);

-- Enable RLS
ALTER TABLE organizations ENABLE ROW LEVEL SECURITY;

-- Policy: users can only see their customer's data
CREATE POLICY customer_isolation ON organizations
    USING (customer_id = current_setting('app.current_customer'));
```

## Key Features

### Single Pipeline

One source + one sink pipeline handles all customers:

```bash
# Generates only 2 files (vs 52 in db-per-tenant with 26 customers)
generated/pipelines/directory/nonprod/
├── source-directory.yaml
└── sink-directory.yaml
```

### Optional Filtering

Filter CDC events by customer (if needed):

```yaml
# In service config
source:
  tables:
    - schema: public
      name: organizations
      filter: "customer_id IN ('customer_a', 'customer_b')"
```

### Cross-Customer Queries

Easy to query across customers:

```sql
-- Count organizations per customer
SELECT customer_id, COUNT(*) 
FROM organizations 
GROUP BY customer_id;

-- Not possible in db-per-tenant pattern
```

## Environment Variables

Required variables:

```bash
# Source PostgreSQL
export POSTGRES_ASMA_HOST=postgres-asma-nonprod.example.com
export POSTGRES_ASMA_PORT=5432
export POSTGRES_ASMA_USER=cdc_user
export POSTGRES_ASMA_PASSWORD=secret

# Sink PostgreSQL
export POSTGRES_URL=postgres://postgres-nonprod.example.com:5432
export POSTGRES_USER=cdc_user
export POSTGRES_PASSWORD=secret

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=kafka-nonprod.example.com:9092
```

## Advantages

✅ **Simpler infrastructure** - One database to manage  
✅ **Fewer pipelines** - 1 source + 1 sink (vs N in db-per-tenant)  
✅ **Lower resource usage** - Single pipeline for all customers  
✅ **Cross-customer queries** - Easy analytics across tenants  
✅ **Easier schema migrations** - Apply once to shared database  

## Disadvantages

❌ **Weaker isolation** - All customers in same database  
❌ **Scaling limits** - Can't scale customers independently  
❌ **Noisy neighbor** - One customer can affect others  
❌ **Complex security** - Must ensure proper data isolation  
❌ **Harder migrations** - Can't move one customer separately  

## Comparison: db-shared vs db-per-tenant

| Aspect | db-shared | db-per-tenant |
|--------|-----------|---------------|
| **Pipelines** | 1 source + 1 sink | N source + N sink |
| **Databases** | 1 shared | N dedicated |
| **Isolation** | Logical (schema/column) | Physical (database) |
| **Scaling** | All together | Per customer |
| **Cross-queries** | ✅ Easy | ❌ Not possible |
| **Management** | ✅ Simple | ❌ Complex |
| **Resource usage** | ✅ Low | ❌ High |

## See Also

- **db-per-tenant pattern** - For database-per-customer (adopus example)
- **Pipeline templates** - Template syntax and variables
- **CLI reference** - All available commands
