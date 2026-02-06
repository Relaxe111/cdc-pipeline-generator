# Sink Server Groups

This document explains the `sink-groups.yaml` structure and how to manage sink destinations for CDC pipelines.

## Overview

Sink groups define **where** CDC data is written to. They exist alongside source groups (`source-groups.yaml`) but serve a different purpose:

| File | Purpose | Defines |
|------|---------|---------|
| `source-groups.yaml` | Source databases to extract CDC data from | Extraction patterns, source connections |
| `sink-groups.yaml` | Sink destinations to write CDC data to | Target connections, sink groups |

## Two Types of Sink Groups

### 1. Inherited Sink Groups (`sink_foo`)

**Purpose:** Write back to the same infrastructure as the source (or same database server, different database)

**When to use:**
- `db-shared` pattern only (doesn't make sense for `db-per-tenant`)
- Writing to a replica database on the same server
- Writing to a different schema on the same server
- Writing to a staging/test database on same infrastructure

**Characteristics:**
- Automatically inherits all servers from source group via `source_ref`
- Same connection config as source (host, port, credentials)
- Services auto-inherited (listed in `_inherited_services`)
- Targets configured per-service in `services/*.yaml`

**Example:**
```yaml
sink_foo:
  source_group: foo                 # inherits from source-groups.yaml → foo
  pattern: db-shared                # inherited
  type: postgres                    # inherited
  servers:
    default:
      source_ref: foo/default       # inherits connection config
    prod:
      source_ref: foo/prod
  _inherited_services:
    - directory
    - calendar
  sources: {}                       # configured in services/*.yaml
```

### 2. Standalone Sink Groups (`sink_analytics`)

**Purpose:** Write to external systems (analytics warehouse, HTTP endpoints, etc.)

**When to use:**
- Analytics/data warehouse (separate infrastructure)
- HTTP webhooks or APIs
- External monitoring systems
- Any destination NOT on source infrastructure

**Characteristics:**
- Own connection config (no `source_ref`)
- Links to source group via `source_group` (for Kafka topics)
- No inherited services
- Targets explicitly configured

**Example:**
```yaml
sink_analytics:
  source_group: foo                 # consumes from foo's Kafka topics
  pattern: db-shared
  type: postgres
  servers:
    default:
      type: postgres
      host: ${POSTGRES_ANALYTICS_HOST}
      port: ${POSTGRES_ANALYTICS_PORT}
      user: ${POSTGRES_ANALYTICS_USER}
      password: ${POSTGRES_ANALYTICS_PASSWORD}
  sources: {}                       # configured in services/*.yaml
```

## Structure

### Top-Level Fields

| Field | Required | Description |
|-------|----------|-------------|
| `source_group` | ✅ Yes | Which source group feeds this sink (for Kafka topics) |
| `pattern` | ✅ Yes | `db-shared` or `db-per-tenant` (usually matches source) |
| `type` | ✅ Yes | `postgres`, `mssql`, `http_client`, `http_server` |
| `kafka_topology` | No | Inherited from source group if not specified |
| `description` | No | Human-readable description |
| `servers` | ✅ Yes | Server connection configurations |
| `sources` | ✅ Yes | Service sink source mappings (usually empty, configured in services/*.yaml) |
| `_inherited_services` | No | List of services inherited from source (documentation only) |

### Server Configuration

#### Inherited Server (with `source_ref`)

```yaml
servers:
  default:
    source_ref: foo/default         # format: <group>/<server>
    # Optional overrides:
    user: ${DIFFERENT_USER}         # override inherited user
    password: ${DIFFERENT_PASSWORD} # override inherited password
```

**Resolution:**
- Copies all fields from `source-groups.yaml → foo → servers → default`
- Excludes `extraction_patterns` (not relevant for sinks)
- Any fields specified alongside `source_ref` override inherited values

#### Standalone Server

```yaml
servers:
  default:
    type: postgres
    host: ${POSTGRES_ANALYTICS_HOST}
    port: ${POSTGRES_ANALYTICS_PORT}
    user: ${POSTGRES_ANALYTICS_USER}
    password: ${POSTGRES_ANALYTICS_PASSWORD}
```

### Source Configuration

Sources are typically configured in `services/*.yaml`, not in `sink-groups.yaml`.

The sink group file acts as a **scaffold** — it declares available sink destinations, but individual services choose which to use.

**Example in `services/directory.yaml`:**
```yaml
# ...source configuration...

sinks:
  # Sink 1: Write back to source server (inherited)
  - sink_group: sink_foo
    server: default
    database: directory_replica_dev
    schema: cdc
    environments:
      dev:
        server: default
        database: directory_replica_dev
      prod:
        server: prod
        database: directory_replica

  # Sink 2: Write to analytics warehouse (standalone)
  - sink_group: sink_analytics
    server: default
    database: analytics_dev
    schema: staging
    environments:
      dev:
        server: default
        database: analytics_dev
      prod:
        server: prod
        database: analytics
```

## CLI Commands

### Create Inherited Sink Group

```bash
# Create sink group that inherits from source group 'foo'
cdc manage-sink-groups --create --source-group foo

# Only works for db-shared patterns
# Generates: sink_foo with all servers as source_ref
```

### Create Standalone Sink Group

```bash
# Create standalone analytics sink (auto-prefixes with 'sink_')
cdc manage-sink-groups --add-new-sink-group analytics --type postgres --for-source-group foo

# Creates: sink_analytics (empty sink group scaffold)
```

### List Sink Groups

```bash
cdc manage-sink-groups --list
```

### Show Sink Group Details

```bash
cdc manage-sink-groups --info sink_foo
cdc manage-sink-groups --info sink_analytics
```

### Validate Sink Groups

```bash
cdc manage-sink-groups --validate
```

## Source Reference Format

`source_ref` uses the format: `<source_group>/<server_name>`

**Examples:**
- `foo/default` → `source-groups.yaml → foo → servers → default`
- `foo/prod` → `source-groups.yaml → foo → servers → prod`
- `adopus/default` → `source-groups.yaml → adopus → servers → default`

## Pattern Compatibility

| Source Pattern | Inherited Sink? | Why |
|----------------|-----------------|-----|
| `db-shared` | ✅ Yes | Services map 1:1, structure mirrors cleanly |
| `db-per-tenant` | ❌ No | Customer-based structure doesn't map to sink structure |

**For `db-per-tenant`:**
- Only create standalone sink groups
- Service-level sink configuration handles per-customer routing

## HTTP Sinks (Future)

Support for HTTP sinks is planned:

```yaml
sink_webhooks:
  source_group: foo
  type: http_client
  servers:
    default:
      type: http_client
      base_url: ${WEBHOOK_BASE_URL}
      method: POST
      headers:
        Content-Type: application/json
        Authorization: Bearer ${WEBHOOK_TOKEN}
```

## Best Practices

1. **Use inherited sinks** for same-infrastructure destinations (replicas, staging DBs)
2. **Use standalone sinks** for external systems (analytics, webhooks)
3. **Keep `sources` empty** in `sink-groups.yaml` — configure in `services/*.yaml`
4. **Validate after changes** with `cdc manage-sink-groups --validate`
5. **Environment variables** should use same naming convention as source groups

## Examples

### Example 1: Replica Database (Inherited)

```yaml
sink_foo:
  source_group: foo
  servers:
    default:
      source_ref: foo/default       # same connection as source
```

**In `services/directory.yaml`:**
```yaml
sinks:
  - sink_group: sink_foo
    server: default
    database: directory_replica     # different database, same server
    schema: public
```

### Example 2: Analytics Warehouse (Standalone)

```yaml
sink_analytics:
  source_group: foo
  pattern: db-shared
  type: postgres
  servers:
    default:
      type: postgres
      host: analytics.example.com   # different server
      port: 5432
      user: ${ANALYTICS_USER}
      password: ${ANALYTICS_PASSWORD}
```

**In `services/directory.yaml`:**
```yaml
sinks:
  - sink_group: sink_analytics
    server: default
    database: cdc_directory
    schema: staging
```

## See Also

- [Server Group Documentation](../source-groups.yaml)
- [Service Configuration](services/README.md)
- [Pipeline Generation](generated/README.md)
