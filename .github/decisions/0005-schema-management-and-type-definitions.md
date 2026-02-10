# 0005 - Schema Management CLI and Type Definitions

**Status:** Proposed
**Date:** 2026-02-10

## Context

Currently, managing custom sink table schemas requires manual YAML editing or using `--add-custom-sink-table` with inline `--column` specs. This has several problems:

1. **No reusable type definitions** — Column types are hardcoded in `_PG_TYPES` and `_MSSQL_TYPES` frozensets. There's no database-aware type catalog generated from actual server introspection.
2. **Custom tables are tightly coupled to sink operations** — Adding a custom table and adding it to a sink are done in one step (`--add-custom-sink-table`), but users often want to define table schemas independently first.
3. **No autocomplete for custom table references** — When adding a sink table, there's no way to tab-complete from pre-defined custom table schemas.
4. **No `manage-service-schema` command** — All schema operations go through `manage-service`, which is overloaded.
5. **Future: HTTP/GraphQL type sources** — We'll need to generate type definitions from Swagger/OpenAPI specs and GraphQL schemas, not just database introspection.

## Decision

### Phase 1: Type Definitions (auto-generated)

Generate `service-schemas/definitions/{pgsql|mssql}.yaml` automatically during `--inspect --save` and `--inspect-sink --save`.

```yaml
# service-schemas/definitions/pgsql.yaml
# Auto-generated from server introspection
# Source: asma/default (nonprod)
# Generated: 2026-02-10T12:00:00
types:
  # Standard PostgreSQL types
  text: { category: string, sql: text }
  varchar: { category: string, sql: "character varying" }
  integer: { category: numeric, sql: integer }
  bigint: { category: numeric, sql: bigint }
  boolean: { category: boolean, sql: boolean }
  uuid: { category: identifier, sql: uuid }
  timestamp with time zone: { category: temporal, sql: "timestamp with time zone" }
  timestamptz: { category: temporal, sql: "timestamp with time zone", alias: true }
  jsonb: { category: json, sql: jsonb }
  # ... all types discovered from pg_catalog.pg_type
  # USER-DEFINED types from the actual database
  citext: { category: string, sql: citext, user_defined: true }
```

**Generation trigger:** Runs automatically at server-group level during any `--inspect --save` or `--inspect-sink --save`. One file per DB engine type. Merges with existing definitions (additive).

**Used by:**
- Autocomplete for `--column` type specs
- Validation in `_parse_column_spec`
- Future: type mapping between MSSQL→PgSQL

### Phase 2: `cdc manage-service-schema` CLI

New top-level command for managing service schema definitions independently from service pipeline config.

```bash
# Create custom table schema
cdc manage-service-schema --service calendar \
  --add-custom-table public.my_events \
  --column id:uuid:pk \
  --column name:text:not_null \
  --column created_at:timestamptz:not_null:default_now

# List custom tables for a service
cdc manage-service-schema --service calendar --list-custom-tables

# Show table schema
cdc manage-service-schema --service calendar --show public.my_events

# Remove custom table
cdc manage-service-schema --service calendar --remove-custom-table public.my_events

# List all services with schemas
cdc manage-service-schema --list
```

**File output:** `service-schemas/{service}/custom-tables/{schema}.{table}.yaml`

**Schema format:** Identical to inspected schemas:
```yaml
database: null  # null for custom tables (no source DB)
schema: public
service: calendar
table: my_events
custom: true  # marks as user-defined, not inspected
columns:
- name: id
  type: uuid
  nullable: false
  primary_key: true
- name: name
  type: text
  nullable: false
  primary_key: false
- name: created_at
  type: timestamptz
  nullable: false
  primary_key: false
  default: now()
primary_key: id
```

### Phase 3: Refactored `--add-custom-sink-table` with `--from` reference

Change `--add-custom-sink-table` to support referencing pre-defined custom tables:

```bash
# Reference an existing custom table (autocomplete from custom-tables/)
cdc manage-service --service directory \
  --sink sink_asma.activities \
  --add-custom-sink-table public.my_events \
  --from public.customer_user

# --map-column: map source columns to sink columns
#   autocomplete LEFT side from source table columns
#   autocomplete RIGHT side from custom table columns
cdc manage-service --service directory \
  --sink sink_asma.activities \
  --add-custom-sink-table public.my_events \
  --from public.customer_user \
  --map-column customer_id:customer_id \
  --map-column brukerBrukerNavn:user_name
```

**Autocomplete chain:**
1. `--add-custom-sink-table` → list from `service-schemas/{target_service}/custom-tables/`
2. `--from` → list from source service tables (existing `list_tables_for_service`)
3. `--map-column` LEFT → columns from `--from` source table
4. `--map-column` RIGHT → columns from the referenced custom table

### Phase 4 (Future): HTTP/GraphQL Type Sources

Extend `service-schemas/definitions/` to support non-database sources:

```yaml
# service-schemas/definitions/openapi.yaml  (future)
# Auto-generated from Swagger/OpenAPI spec
# Source: https://api.example.com/swagger.json
types:
  string: { category: string }
  integer: { category: numeric }
  # ...

# service-schemas/definitions/graphql.yaml  (future)
# Auto-generated from GraphQL introspection
# Source: https://api.example.com/graphql
types:
  String: { category: string }
  Int: { category: numeric }
  # ...
```

**Not implemented now** — placeholder for when HTTP client/server sources are added. Generation will use either Swagger/OpenAPI JSON parsing or GraphQL introspection queries.

## Implementation Plan

### Module Structure

```
cdc_generator/
  cli/
    service_schema.py          # NEW: manage-service-schema CLI command
  validators/
    manage_service_schema/     # NEW: service schema management
      __init__.py
      custom_table_ops.py      # Create/remove/list custom tables
      type_definitions.py      # Generate/load type definitions
  helpers/
    autocompletions/
      service_schemas.py       # NEW: autocomplete for service-schemas
```

### Implementation Order

1. **type_definitions.py** — Type introspection queries + YAML generation
2. **Schema saver integration** — Call type def generation during `--save`
3. **custom_table_ops.py** — Custom table CRUD operations
4. **service_schema.py** — CLI command with arg parsing
5. **Autocomplete** — Service schema completions
6. **Refactor `--add-custom-sink-table`** — Add `--from` reference support
7. **`--map-column` autocomplete** — Source and target column completions

## Consequences

### Positive

- Type definitions become a shared, inspectable resource
- Custom tables are managed independently from sink config
- Autocomplete guides users through the full workflow
- Same YAML format for inspected and custom tables
- Foundation for HTTP/GraphQL type sources

### Negative

- New CLI command adds surface area
- Type definition files need to be regenerated when DB schema changes
- Two places to create custom tables (old inline `--column` still works)

### Notes

- Phase 4 (HTTP/GraphQL) is deferred — add to backlog
- Existing `--add-custom-sink-table` with `--column` continues to work (backward compatible)
- Type definition generation is additive (merge, don't replace)
- Custom table format matches inspected table format exactly
