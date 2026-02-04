# SQL and PostgreSQL Patterns

Patterns for SQL database integration in Redpanda Connect pipelines.

---

## SQL Input Components

### sql_select - Table Consumption

Best for simple table reads with WHERE clause:

```yaml
input:
  sql_select:
    driver: postgres
    dsn: postgres://user:pass@localhost:5432/mydb?sslmode=disable
    table: events
    columns:
      - "*"  # Or specific: [id, name, created_at]
    where: created_at >= $1
    args_mapping: |
      root = [
        now().ts_sub_iso8601("PT1H").ts_format("2006-01-02T15:04:05Z")
      ]
```

**Key options:**
- `prefix` - Add before SELECT (e.g., `DISTINCT`)
- `suffix` - Add after query (e.g., `ORDER BY id`)
- `init_statement` - Run on first connection

### sql_raw - Complex Queries

For JOINs, aggregations, and complex SQL:

```yaml
input:
  sql_raw:
    driver: postgres
    dsn: postgres://user:pass@localhost:5432/mydb?sslmode=disable
    query: |
      SELECT 
        e.id,
        e.name,
        c.category_name,
        COUNT(*) OVER() as total_count
      FROM events e
      JOIN categories c ON e.category_id = c.id
      WHERE e.created_at >= $1
      ORDER BY e.created_at DESC
      LIMIT 1000
    args_mapping: |
      root = [
        now().ts_sub_iso8601("P1D").ts_format("2006-01-02T15:04:05Z")
      ]
```

---

## SQL Output Components

### sql_insert - Table Inserts

```yaml
output:
  sql_insert:
    driver: postgres
    dsn: postgres://user:pass@localhost:5432/mydb?sslmode=disable
    table: processed_events
    columns:
      - id
      - name
      - data
      - processed_at
    args_mapping: |
      root = [
        this.id,
        this.name,
        this.data.format_json(),
        now().ts_format("2006-01-02T15:04:05Z")
      ]
    # Handle conflicts (upsert)
    suffix: ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, processed_at = EXCLUDED.processed_at
```

### Batching for Performance

```yaml
output:
  sql_insert:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    table: events
    columns: [id, name, data]
    args_mapping: |
      root = [this.id, this.name, this.data.format_json()]
    batching:
      count: 100       # Batch 100 records
      period: 1s       # Or flush every second
      byte_size: 0     # No byte limit
    max_in_flight: 4   # Parallel batches
```

---

## SQL Processor Component

### sql_raw Processor - Query During Pipeline

Useful for enrichment and lookups:

```yaml
pipeline:
  processors:
    # Lookup reference data
    - branch:
        processors:
          - sql_raw:
              driver: postgres
              dsn: ${POSTGRES_DSN}
              query: SELECT name, category FROM products WHERE id = $1
              args_mapping: root = [this.product_id]
        result_map: |
          root.product = this.index(0)
```

### Multiple Queries in Transaction

```yaml
pipeline:
  processors:
    - sql_raw:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        queries:
          # First: Create record
          - query: |
              INSERT INTO audit_log (event_id, action, timestamp)
              VALUES ($1, $2, $3)
            args_mapping: |
              root = [this.id, "processed", now().ts_format("2006-01-02T15:04:05Z")]
            exec_only: true
          
          # Second: Update status
          - query: |
              UPDATE events SET status = $1 WHERE id = $2
            args_mapping: |
              root = ["completed", this.id]
            exec_only: true
```

---

## PostgreSQL-Specific Patterns

### DSN Format

```yaml
# Basic
dsn: postgres://user:password@host:5432/database?sslmode=disable

# With schema
dsn: postgres://user:password@host:5432/database?sslmode=disable&search_path=myschema

# SSL enabled (default)
dsn: postgres://user:password@host:5432/database?sslmode=require

# Environment variable interpolation
dsn: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=${POSTGRES_SSLMODE}
```

### Placeholder Syntax

PostgreSQL uses `$1`, `$2`, etc. for placeholders:

```yaml
query: |
  SELECT * FROM users 
  WHERE status = $1 
    AND created_at >= $2
    AND role IN ($3, $4)
args_mapping: |
  root = [
    this.status,
    this.start_date,
    "admin",
    "moderator"
  ]
```

### JSONB Operations

```yaml
# Insert with JSONB
output:
  sql_insert:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    table: events
    columns: [id, metadata]
    args_mapping: |
      root = [
        this.id,
        this.metadata.format_json()  # Serialize to JSON string
      ]

# Query JSONB fields
input:
  sql_raw:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    query: |
      SELECT id, metadata->>'name' as name
      FROM events
      WHERE metadata @> '{"type": "user"}'::jsonb
```

### UPSERT Pattern

```yaml
output:
  sql_insert:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    table: sync_state
    columns: [id, data, updated_at]
    args_mapping: |
      root = [
        this.id,
        this.data.format_json(),
        now().ts_format("2006-01-02T15:04:05Z")
      ]
    suffix: |
      ON CONFLICT (id) DO UPDATE SET 
        data = EXCLUDED.data,
        updated_at = EXCLUDED.updated_at
```

### Bulk Delete Pattern

Use processor for delete operations:

```yaml
pipeline:
  processors:
    - sql_raw:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        query: DELETE FROM events WHERE id = $1
        args_mapping: root = [this.id]
        exec_only: true
```

---

## Connection Management

### Connection Pool Settings

```yaml
input:
  sql_raw:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    query: SELECT * FROM events
    
    # Connection pool settings
    conn_max_open: 10      # Max open connections
    conn_max_idle: 5       # Max idle connections
    conn_max_idle_time: 5m # Idle timeout
    conn_max_life_time: 1h # Max connection lifetime
```

### Initialization Scripts

```yaml
output:
  sql_insert:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    table: events
    columns: [id, data]
    args_mapping: root = [this.id, this.data.format_json()]
    
    # Create table if not exists
    init_statement: |
      CREATE TABLE IF NOT EXISTS events (
        id VARCHAR(255) PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
    
    # Or use files
    init_files:
      - ./sql/init/*.sql
```

---

## CDC Source Pattern (PostgreSQL)

For capturing changes from PostgreSQL using polling:

```yaml
input:
  sql_raw:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    query: |
      SELECT id, data, operation, changed_at
      FROM cdc_outbox
      WHERE processed = false
      ORDER BY changed_at
      LIMIT 100
      FOR UPDATE SKIP LOCKED

pipeline:
  processors:
    # Process the change
    - mapping: |
        root = this.data.parse_json()
        root._cdc.operation = this.operation
        root._cdc.changed_at = this.changed_at
    
    # Mark as processed
    - sql_raw:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        query: UPDATE cdc_outbox SET processed = true WHERE id = $1
        args_mapping: root = [this._cdc.id]
        exec_only: true

output:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topic: cdc-events
```

---

## Data Type Mapping

### Bloblang to PostgreSQL

| Bloblang | PostgreSQL | Conversion |
|----------|------------|------------|
| `string` | `VARCHAR/TEXT` | Direct |
| `number` | `INTEGER` | `.int64()` |
| `number` | `BIGINT` | `.int64()` |
| `number` | `DECIMAL` | `.string()` (preserve precision) |
| `number` | `FLOAT` | `.float64()` |
| `bool` | `BOOLEAN` | Direct |
| `object` | `JSONB` | `.format_json()` |
| `array` | `JSONB` | `.format_json()` |
| `timestamp` | `TIMESTAMP` | `.ts_format("2006-01-02T15:04:05Z")` |
| `timestamp` | `TIMESTAMPTZ` | `.ts_format("2006-01-02T15:04:05Z07:00")` |
| `bytes` | `BYTEA` | Direct |

### Handling NULL Values

```yaml
args_mapping: |
  root = [
    this.id,
    this.name | null,           # NULL if missing
    this.count | 0,             # Default if missing
    this.data.format_json() | "{}"  # Empty JSON if missing
  ]
```

---

## Error Handling

### Retry on Failure

```yaml
output:
  retry:
    max_retries: 3
    backoff:
      initial_interval: 1s
      max_interval: 30s
    output:
      sql_insert:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        table: events
        columns: [id, data]
        args_mapping: root = [this.id, this.data.format_json()]
```

### Dead Letter Queue

```yaml
output:
  fallback:
    - sql_insert:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        table: events
        columns: [id, data]
        args_mapping: root = [this.id, this.data.format_json()]
    
    # On failure, write to DLQ
    - sql_insert:
        driver: postgres
        dsn: ${POSTGRES_DSN}
        table: events_dlq
        columns: [original_data, error_message, failed_at]
        args_mapping: |
          root = [
            this.format_json(),
            @fallback_error,
            now().ts_format("2006-01-02T15:04:05Z")
          ]
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [06-ERROR-HANDLING.md](06-ERROR-HANDLING.md) - Error handling patterns
- [07-PIPELINE-PATTERNS.md](07-PIPELINE-PATTERNS.md) - Complete pipeline examples
