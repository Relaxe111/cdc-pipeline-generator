# Pipeline Templating System

This document describes the CDC pipeline templating architecture, including the separation of static Bloblang logic from dynamic YAML templates.

---

## Overview

The pipeline generation system uses a **two-phase approach**:

1. **Build Time** (Python Generator): Templates with `{{PLACEHOLDERS}}` are processed to generate concrete pipeline configurations
2. **Runtime** (Redpanda Connect): Generated pipelines load static `.blobl` files via `import`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        BUILD TIME (Python Generator)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  pipeline-templates/                    generated/                          │
│  ├── sink-pipeline.yaml    ──────────▶  ├── pipelines/                      │
│  ├── source-pipeline.yaml               │   ├── service-Table1-sink.yaml   │
│  └── bloblang/                          │   ├── service-Table1-source.yaml │
│      ├── common.blobl      ──(copy)──▶  │   └── ...                        │
│      ├── validation.blobl               └── bloblang/                       │
│      └── transforms.blobl                   ├── common.blobl                │
│                                             ├── validation.blobl            │
│                                             └── transforms.blobl            │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                        RUNTIME (Redpanda Connect)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Redpanda loads: generated/pipelines/service-Table1-sink.yaml              │
│       └── import "./bloblang/common.blobl"  ← Resolved at runtime          │
│       └── Executes pipeline with concrete values                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

### Template Directory (Source)

```
pipeline-templates/
├── sink-pipeline.yaml          # Sink template with {{PLACEHOLDERS}}
├── source-pipeline.yaml        # Source template with {{PLACEHOLDERS}}
└── bloblang/                   # Static Bloblang logic (no placeholders)
    ├── common.blobl            # Shared utilities (metadata, error handling)
    ├── validation.blobl        # Input validation maps
    ├── transforms.blobl        # Type conversion and transformation maps
    ├── cdc_operations.blobl    # CDC-specific logic (INSERT/UPDATE/DELETE)
    └── auth.blobl              # HMAC/JWT validation for webhooks
```

### Generated Directory (Output)

```
generated/
├── pipelines/                  # Generated YAML pipelines (per table/service)
│   ├── adopus-Actor-sink.yaml
│   ├── adopus-Actor-source.yaml
│   ├── adopus-Customer-sink.yaml
│   └── ...
└── bloblang/                   # Copied from templates (static, unchanged)
    ├── common.blobl
    ├── validation.blobl
    ├── transforms.blobl
    ├── cdc_operations.blobl
    └── auth.blobl
```

---

## Template Types

### 1. YAML Templates (Dynamic)

YAML templates contain `{{PLACEHOLDERS}}` that are replaced at build time:

```yaml
# pipeline-templates/sink-pipeline.yaml
input:
  kafka:
    addresses: ["{{KAFKA_BROKERS}}"]
    topics: ["{{TOPIC_PREFIX}}.{{SCHEMA}}.{{TABLE_NAME}}"]
    consumer_group: "{{CONSUMER_GROUP}}"

pipeline:
  processors:
    - mapping: |
        import "./bloblang/common.blobl"
        import "./bloblang/cdc_operations.blobl"
        
        # Dynamic values (generated)
        let table_name = "{{TABLE_NAME}}"
        let schema = "{{SCHEMA}}"
        let primary_key = "{{PRIMARY_KEY}}"
        
        # Static logic (from .blobl imports)
        root = this.apply("parse_cdc_envelope").apply("add_metadata")
        
        # Dynamic column mappings (generated per table)
        {{COLUMN_MAPPINGS}}

output:
  sql_insert:
    driver: postgres
    dsn: "{{POSTGRES_DSN}}"
    table: "{{SCHEMA}}.{{TABLE_NAME}}"
    columns: [{{COLUMNS}}]
    args_mapping: |
      root = [{{ARGS_MAPPING}}]
    suffix: "{{UPSERT_SUFFIX}}"
```

### 2. Bloblang Files (Static)

`.blobl` files contain reusable logic with **no placeholders**:

```blobl
# pipeline-templates/bloblang/common.blobl

# Add processing metadata to every message
map add_metadata {
    root = this
    root._meta.processed_at = now().ts_format("2006-01-02T15:04:05Z")
    root._meta.pipeline_id = env("PIPELINE_ID")
    root._meta.host = hostname()
}

# Safe null coalescing for optional fields
map safe_defaults {
    root = this
    root.created_at = this.created_at | now()
    root.updated_at = this.updated_at | now()
    root.version = this.version | 1
}

# Error context enrichment
map enrich_error {
    root = this
    root._error.message = error()
    root._error.source = error_source_label()
    root._error.timestamp = now().ts_format("2006-01-02T15:04:05Z")
}
```

```blobl
# pipeline-templates/bloblang/validation.blobl

# Validate required fields exist
map validate_required {
    let missing = []
    let missing = if this.id == null { $missing.append("id") } else { $missing }
    let missing = if this.op == null { $missing.append("op") } else { $missing }
    
    root = if $missing.length() > 0 {
        throw("Missing required fields: " + $missing.join(", "))
    } else {
        this
    }
}

# Validate field types
map validate_types {
    root = this
    root.id = this.id.string().catch(throw("id must be string-convertible"))
    root.timestamp = this.timestamp.number().catch(throw("timestamp must be numeric"))
}
```

```blobl
# pipeline-templates/bloblang/cdc_operations.blobl

# Parse Debezium/CDC envelope format
map parse_cdc_envelope {
    root = if this.op == "d" { this.before } else { this.after }
    root._cdc.operation = match this.op {
        "c" => "INSERT"
        "r" => "READ"
        "u" => "UPDATE"
        "d" => "DELETE"
        _ => "UNKNOWN"
    }
    root._cdc.source_ts = this.ts_ms
    root._cdc.transaction_id = this.transaction.id | null
}

# Handle different operations for sink
map route_by_operation {
    root = match this._cdc.operation {
        "DELETE" => this.apply("handle_delete")
        _ => this.apply("handle_upsert")
    }
}
```

```blobl
# pipeline-templates/bloblang/transforms.blobl

# Common type conversions
map convert_timestamps {
    root = this
    # MSSQL datetime to PostgreSQL timestamp
    root.created_at = this.created_at.ts_parse("2006-01-02T15:04:05").catch(null)
    root.updated_at = this.updated_at.ts_parse("2006-01-02T15:04:05").catch(null)
}

# Normalize boolean values from various formats
map normalize_booleans {
    root = this
    root.active = match this.active.type() {
        "bool" => this.active
        "number" => this.active != 0
        "string" => this.active.lowercase().contains("true") || this.active == "1"
        _ => false
    }
}

# Trim and normalize string fields
map normalize_strings {
    root = this
    root.name = (this.name | "").trim()
    root.email = (this.email | "").lowercase().trim()
    root.phone = (this.phone | "").re_replace_all("[^0-9+]", "")
}
```

---

## Placeholder Reference

### Standard Placeholders

| Placeholder | Description | Example Value |
|-------------|-------------|---------------|
| `{{TABLE_NAME}}` | Target table name | `Actor` |
| `{{SCHEMA}}` | Database schema | `dbo` |
| `{{PRIMARY_KEY}}` | Primary key column(s) | `actno` |
| `{{TOPIC_PREFIX}}` | Kafka topic prefix | `local.adopus` |
| `{{CONSUMER_GROUP}}` | Kafka consumer group | `adopus-sink` |
| `{{KAFKA_BROKERS}}` | Kafka broker addresses | `localhost:9092` |
| `{{POSTGRES_DSN}}` | PostgreSQL connection string | `postgres://...` |

### Generated Block Placeholders

| Placeholder | Description | Generated Content |
|-------------|-------------|-------------------|
| `{{COLUMNS}}` | Column list for SQL | `"id", "name", "email"` |
| `{{ARGS_MAPPING}}` | Bloblang args array | `this.id, this.name, this.email` |
| `{{COLUMN_MAPPINGS}}` | Field transformations | `root.id = this.actno` |
| `{{UPSERT_SUFFIX}}` | ON CONFLICT clause | `ON CONFLICT (id) DO UPDATE...` |

---

## Generation Flow

### Python Generator Responsibilities

```python
def generate_pipelines(service: Service) -> None:
    """Generate concrete pipelines from templates."""
    
    # 1. Copy static .blobl files (unchanged)
    copy_bloblang_files(
        source="pipeline-templates/bloblang/",
        dest="generated/bloblang/"
    )
    
    # 2. Load YAML templates
    sink_template = load_template("pipeline-templates/sink-pipeline.yaml")
    source_template = load_template("pipeline-templates/source-pipeline.yaml")
    
    # 3. Generate per-table pipelines
    for table in service.cdc_tables:
        context = build_template_context(service, table)
        
        # Replace placeholders
        sink_yaml = replace_placeholders(sink_template, context)
        source_yaml = replace_placeholders(source_template, context)
        
        # Write generated files
        write_file(f"generated/pipelines/{service.name}-{table.name}-sink.yaml", sink_yaml)
        write_file(f"generated/pipelines/{service.name}-{table.name}-source.yaml", source_yaml)
```

### Template Context

```python
def build_template_context(service: Service, table: Table) -> dict:
    """Build placeholder replacement context."""
    return {
        "TABLE_NAME": table.name,
        "SCHEMA": table.schema,
        "PRIMARY_KEY": table.primary_key,
        "TOPIC_PREFIX": service.topic_prefix,
        "CONSUMER_GROUP": f"{service.name}-sink",
        "KAFKA_BROKERS": service.kafka_brokers,
        "POSTGRES_DSN": service.postgres_dsn,
        "COLUMNS": generate_columns_list(table),
        "ARGS_MAPPING": generate_args_mapping(table),
        "COLUMN_MAPPINGS": generate_column_mappings(table),
        "UPSERT_SUFFIX": generate_upsert_clause(table),
    }
```

---

## Best Practices

### What Goes in `.blobl` Files

✅ **Put in `.blobl`:**
- Error handling patterns
- Validation logic
- Type conversions
- CDC envelope parsing
- Metadata enrichment
- HMAC/JWT authentication
- Reusable transformation maps

### What Stays in YAML Templates

✅ **Keep in YAML:**
- Table/schema names (`{{TABLE_NAME}}`)
- Connection strings (`{{POSTGRES_DSN}}`)
- Topic names (`{{TOPIC_PREFIX}}`)
- Column-specific mappings (`{{COLUMN_MAPPINGS}}`)
- Table-specific SQL (`{{UPSERT_SUFFIX}}`)

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Bloblang maps | `snake_case` | `parse_cdc_envelope` |
| Bloblang files | `snake_case.blobl` | `cdc_operations.blobl` |
| YAML templates | `kebab-case.yaml` | `sink-pipeline.yaml` |
| Generated files | `{service}-{table}-{type}.yaml` | `adopus-Actor-sink.yaml` |

### IDE Support

For best developer experience:
- Install the Benthos VS Code extension for `.blobl` syntax highlighting
- Keep complex Bloblang logic in `.blobl` files (better IDE support)
- Keep YAML templates simple (import + placeholders)

---

## Example: Complete Sink Template

```yaml
# pipeline-templates/sink-pipeline.yaml
input:
  kafka:
    addresses: ["{{KAFKA_BROKERS}}"]
    topics: ["{{TOPIC_PREFIX}}.{{SCHEMA}}.{{TABLE_NAME}}"]
    consumer_group: "{{CONSUMER_GROUP}}"

pipeline:
  processors:
    # Parse and validate
    - mapping: |
        import "./bloblang/common.blobl"
        import "./bloblang/validation.blobl"
        import "./bloblang/cdc_operations.blobl"
        import "./bloblang/transforms.blobl"
        
        # Validate input
        root = this.apply("validate_required")
        
        # Parse CDC envelope
        root = this.apply("parse_cdc_envelope")
        
        # Add metadata
        root = this.apply("add_metadata")
    
    # Table-specific transformations (generated)
    - mapping: |
        {{COLUMN_MAPPINGS}}
    
    # Error handling
    - catch:
        - mapping: |
            import "./bloblang/common.blobl"
            root = this.apply("enrich_error")

output:
  switch:
    cases:
      # Handle deletes
      - check: 'this._cdc.operation == "DELETE"'
        output:
          sql_raw:
            driver: postgres
            dsn: "{{POSTGRES_DSN}}"
            query: 'DELETE FROM "{{SCHEMA}}"."{{TABLE_NAME}}" WHERE "{{PRIMARY_KEY}}" = $1'
            args_mapping: "root = [this.{{PRIMARY_KEY}}]"
      
      # Handle inserts/updates (upsert)
      - output:
          sql_insert:
            driver: postgres
            dsn: "{{POSTGRES_DSN}}"
            table: '"{{SCHEMA}}"."{{TABLE_NAME}}"'
            columns: [{{COLUMNS}}]
            args_mapping: |
              root = [{{ARGS_MAPPING}}]
            suffix: "{{UPSERT_SUFFIX}}"
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Bloblang syntax basics
- [02-BLOBLANG-METHODS.md](02-BLOBLANG-METHODS.md) - Method reference
- [06-ERROR-HANDLING.md](06-ERROR-HANDLING.md) - Error handling patterns
- [07-PIPELINE-PATTERNS.md](07-PIPELINE-PATTERNS.md) - Complete pipeline examples
