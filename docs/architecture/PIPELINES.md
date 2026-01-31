# Redpanda Connect Pipelines

This directory contains Redpanda Connect (formerly Benthos) pipeline configurations for the CDC pipeline.

## Overview

**Redpanda Connect** replaces the previous Kafka Connect + Debezium setup with a simpler, unified streaming solution:

- **Source Pipeline**: Captures CDC events from MSSQL and publishes to Redpanda
- **Sink Pipeline**: Consumes events from Redpanda and writes to PostgreSQL

## Quick Start

### 1. Define Customer Configuration

Create a YAML file in `customers/<customer>.yaml`:

```yaml
customer: mycustomer
schema: mycustomer

environments:
  local:
    sink_tasks: 8
    database_name: mycustomer
    topic_prefix: local.mycustomer.mycustomer
    mssql:
      host: ${MSSQL_HOST}
      port: ${MSSQL_PORT}
      user: ${MSSQL_USER}
      password: ${MSSQL_PASSWORD}
    postgres:
      url: ${POSTGRES_URL}?currentSchema=mycustomer
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
    kafka:
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
```

### 2. Validate Configuration

```bash
python3 scripts/4-validate-customers.py
```

This will check all customer YAML files for required fields.

### 3. Generate Pipelines

```bash
# Generate for specific customer and environment
python3 ../scripts/5-generate-pipelines.py mycustomer local

# Generate for all environments of a customer
python3 ../scripts/5-generate-pipelines.py mycustomer

# Generate for all customers
python3 ../scripts/5-generate-pipelines.py

# List all customers
python3 ../scripts/5-generate-pipelines.py --list
```

That's it! The scripts handle everything else automatically.

## Architecture

```
┌─────────────┐
│   MSSQL     │
│  (Source)   │
└──────┬──────┘
       │
       │ SQL CDC polling
       ▼
┌─────────────────────┐
│ Redpanda Connect    │
│  (Source Pipeline)  │
└──────┬──────────────┘
       │
       │ Kafka topics
       ▼
┌─────────────────────┐
│    Redpanda         │
│  (Message Broker)   │
└──────┬──────────────┘
       │
       │ Consume topics
       ▼
┌─────────────────────┐
│ Redpanda Connect    │
│  (Sink Pipeline)    │
└──────┬──────────────┘
       │
       │ SQL INSERT/UPDATE
       ▼
┌─────────────────────┐
│   PostgreSQL        │
│    (Target)         │
└─────────────────────┘
```

## Directory Structure

```
pipelines/
├── README.md                          # This file
├── validate_customers.py              # Validate customer YAML configs
├── generate_pipelines.py              # Generate pipeline configs
├── convert_to_yaml.py                 # Migrate JSON configs to YAML
│
├── customers/                         # Customer configurations (YAML)
│   ├── avansas.yaml
│   ├── fretex.yaml
│   ├── genesis.yaml
│   └── ...
│
├── templates/                         # Pipeline templates
│   ├── source-pipeline.yaml          # MSSQL CDC source template
│   └── sink-pipeline.yaml            # PostgreSQL sink template
│
└── generated/                         # Generated pipeline configs (DO NOT EDIT)
    ├── local/
    │   └── avansas/
    │       ├── source-pipeline.yaml
    │       └── sink-pipeline.yaml
    ├── nonprod/
    ├── prod/
    └── prod-fretex/
```

## Workflow

### Adding a New Customer

1. **Create customer YAML file**: `customers/newcustomer.yaml`
   ```yaml
   customer: newcustomer
   schema: newcustomer
   environments:
     prod:
       sink_tasks: 3
       database_name: AdOpusNewcustomer
       topic_prefix: prod.newcustomer.AdOpusNewcustomer
       mssql:
         host: ${MSSQL_HOST}
         port: ${MSSQL_PORT}
         user: ${MSSQL_USER}
         password: ${MSSQL_PASSWORD}
       postgres:
         url: ${POSTGRES_URL}?currentSchema=newcustomer
         user: ${POSTGRES_USER}
         password: ${POSTGRES_PASSWORD}
       kafka:
         bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
   ```

2. **Validate**: `python3 scripts/4-validate-customers.py`

3. **Generate pipelines**: `python3 ../scripts/5-generate-pipelines.py newcustomer`

4. **Deploy** (in docker-compose or Kubernetes)

### Modifying Pipeline Templates

If you need to change transformations or add tables:

1. Edit `templates/source-pipeline.yaml` or `templates/sink-pipeline.yaml`
2. Regenerate all pipelines: `python3 ../scripts/5-generate-pipelines.py`
3. Restart Redpanda Connect services

### Migration from JSON to YAML

If you have existing JSON customer configs:

```bash
python3 convert_to_yaml.py
```

This will:
- Convert all `.json` files to `.yaml`
- Move original JSON files to `customers/legacy/`
- Preserve all configuration data

```
redpanda-connect/
├── templates/
│   ├── source-pipeline.yaml       # Source pipeline template
│   └── sink-pipeline.yaml         # Sink pipeline template
├── generated/
│   └── local/
│       └── avansas/
│           ├── source-pipeline.yaml
│           └── sink-pipeline.yaml
├── generate_pipelines.py          # Pipeline generator script
└── README.md                       # This file
```

## Generating Pipelines

Pipelines are generated from templates using customer configurations in `pipelines/customers/`:

```bash
# Generate all customers, all environments
python3 ../scripts/5-generate-pipelines.py

# Generate specific customer
python3 ../scripts/5-generate-pipelines.py avansas

# Generate specific environment for customer
python3 ../scripts/5-generate-pipelines.py avansas local

# List available customers
python3 ../scripts/5-generate-pipelines.py --list
```

## Pipeline Configuration

### Source Pipeline

The source pipeline:
1. **Polls MSSQL CDC tables** using `sql_raw` input
2. **Transforms CDC data** to Debezium-compatible envelope format
3. **Applies schema changes** (snake_case conversion for column names)
4. **Publishes to Redpanda** topics

Key features:
- Tracks LSN (Log Sequence Number) for incremental changes
- Supports INSERT, UPDATE, DELETE operations
- Maintains Debezium envelope compatibility
- Separate inputs for each table (Actor, AdgangLinjer)

### Sink Pipeline

The sink pipeline:
1. **Consumes from Redpanda** topics using regex pattern
2. **Unwraps Debezium envelope** to extract payload
3. **Converts field names** from PascalCase to snake_case
4. **Executes SQL** INSERT/UPDATE/DELETE to PostgreSQL

Key features:
- UPSERT mode with ON CONFLICT handling
- Delete support
- Adds metadata fields (__sync_timestamp, __source, etc.)
- Table-specific SQL statements for proper field mapping

## Transformations

All transformations from the previous Kafka Connect setup are preserved:

### 1. Unwrap Debezium Envelope
Extracts the `after` payload from Debezium envelope (or `before` for deletes).

### 2. Extract Table Name
Extracts table name from the topic pattern `{{TOPIC_PREFIX}}.{{DATABASE_NAME}}.dbo.{{TABLE}}`.

### 3. Add Sync Timestamp
Adds `__sync_timestamp` field with current Unix timestamp.

### 4. Convert to snake_case
Converts all field names from PascalCase/camelCase to snake_case:
- `ActorId` → `actor_id`
- `FirstName` → `first_name`
- `SoknadId` → `soknad_id`
- `BrukerNavn` → `bruker_navn`

### 5. Add Source Metadata
Adds source tracking fields:
- `__source`: Environment identifier (e.g., "local-mssql")
- `__source_db`: Source database name
- `__source_table`: Source table name
- `__source_ts_ms`: Source CDC timestamp
- `__cdc_operation`: CDC operation type (c/r/u/d)

## Monitoring

Both pipelines expose HTTP endpoints for monitoring:

- **Source Pipeline**: http://localhost:4195/benthos
  - `/ping` - Health check
  - `/stats` - Runtime statistics
  - `/metrics` - Prometheus metrics

- **Sink Pipeline**: http://localhost:4196/benthos
  - `/ping` - Health check
  - `/stats` - Runtime statistics
  - `/metrics` - Prometheus metrics

## Running Locally

1. **Generate pipelines:**
   ```bash
   python3 ../scripts/5-generate-pipelines.py avansas local
   ```

2. **Start services:**
   ```bash
   docker compose up -d
   ```

3. **Check pipeline health:**
   ```bash
   # Source pipeline
   curl http://localhost:4195/ping
   curl http://localhost:4195/stats | jq
   
   # Sink pipeline
   curl http://localhost:4196/ping
   curl http://localhost:4196/stats | jq
   ```

4. **View logs:**
   ```bash
   docker logs cdc-redpanda-connect-source -f
   docker logs cdc-redpanda-connect-sink -f
   ```

## Configuration Variables

Pipelines use template variables that are substituted during generation:

| Variable | Description | Example |
|----------|-------------|---------|
| `{{CUSTOMER}}` | Customer identifier | avansas |
| `{{ENV}}` | Environment | local, nonprod, prod |
| `{{SCHEMA}}` | PostgreSQL schema | avansas |
| `{{DATABASE_NAME}}` | MSSQL database name | avansas |
| `{{TOPIC_PREFIX}}` | Kafka topic prefix | local.avansas |
| `{{MSSQL_HOST}}` | MSSQL hostname | mssql |
| `{{MSSQL_PORT}}` | MSSQL port | 1433 |
| `{{MSSQL_USER}}` | MSSQL username | sa |
| `{{MSSQL_PASSWORD}}` | MSSQL password | ... |
| `{{POSTGRES_URL}}` | PostgreSQL connection URL | postgres://... |
| `{{KAFKA_BOOTSTRAP_SERVERS}}` | Redpanda brokers | redpanda:9092 |

## Adding New Tables

To add support for new tables:

1. **Update source-pipeline.yaml template:**
   - Add new input under `broker.inputs`
   - Configure CDC table polling
   - Map fields and set topic

2. **Update sink-pipeline.yaml template:**
   - Add new case under `output.switch.cases`
   - Define table-specific INSERT/UPSERT SQL
   - Map all fields in args_mapping

3. **Regenerate pipelines:**
   ```bash
   python3 ../scripts/5-generate-pipelines.py avansas local
   ```

4. **Restart services:**
   ```bash
   docker compose restart redpanda-connect-source redpanda-connect-sink
   ```

## Advantages over Kafka Connect

1. **Simpler Architecture**: Single binary instead of JVM + plugins
2. **Better Performance**: Native Go implementation, lower latency
3. **Easier Configuration**: YAML instead of JSON REST API
4. **Built-in Monitoring**: Prometheus metrics out of the box
5. **Lower Resource Usage**: ~50MB RAM vs ~1GB for Kafka Connect
6. **No Plugin Management**: Everything built-in
7. **Better Error Handling**: Configurable retry/backoff strategies

## Troubleshooting

### Pipeline not starting

Check logs:
```bash
docker logs cdc-redpanda-connect-source
docker logs cdc-redpanda-connect-sink
```

Common issues:
- YAML syntax errors
- Missing environment variables
- Database connection failures
- CDC not enabled on MSSQL tables

### No data flowing

1. Check if CDC is enabled on MSSQL tables:
   ```sql
   SELECT name, is_tracked_by_cdc 
   FROM sys.tables 
   WHERE name IN ('Actor', 'AdgangLinjer');
   ```

2. Check Redpanda topics:
   ```bash
   docker exec cdc-redpanda rpk topic list
   docker exec cdc-redpanda rpk topic consume nonprod.avansas.AdOpusTest.dbo.Actor -n 5
   ```

3. Check pipeline stats:
   ```bash
   curl http://localhost:4195/stats | jq
   curl http://localhost:4196/stats | jq
   ```

### Data in wrong format

Check the Bloblang transformations in the pipeline YAML files. Use the Redpanda Connect online Bloblang playground to test transformations:
https://www.benthos.dev/docs/guides/bloblang/about

## Resources

- [Redpanda Connect Documentation](https://docs.redpanda.com/redpanda-connect/)
- [Bloblang Language Guide](https://www.benthos.dev/docs/guides/bloblang/about)
- [SQL Input Documentation](https://docs.redpanda.com/redpanda-connect/components/inputs/sql_raw/)
- [Kafka Output Documentation](https://docs.redpanda.com/redpanda-connect/components/outputs/kafka/)
