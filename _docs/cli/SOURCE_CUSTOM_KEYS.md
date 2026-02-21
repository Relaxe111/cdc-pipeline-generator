# Source Custom Keys (SQL)

`source_custom_keys` let you compute per-database values during update and persist them under each `sources.<service>.<env>` entry.

## Supported execution type

- `sql` (current)

## Source groups (`manage-source-groups`)

Add or update a custom key definition:

```bash
cdc manage-source-groups \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM dbo.settings" \
  --custom-key-exec-type sql
```

Run update to execute custom SQL for every discovered database:

```bash
cdc manage-source-groups --update
```

## Sink groups (`manage-sink-groups`)

Add or update a custom key on a specific sink group:

```bash
cdc manage-sink-groups \
  --sink-group sink_analytics \
  --add-source-custom-key customer_id \
  --custom-key-value "SELECT customer_id FROM public.settings" \
  --custom-key-exec-type sql
```

Run update to execute custom SQL for every discovered sink database:

```bash
cdc manage-sink-groups --update --sink-group sink_analytics
```

## Persisted config shape

Definitions are saved as:

```yaml
source_custom_keys:
  customer_id:
    exec_type: sql
    value: SELECT customer_id FROM dbo.settings
```

## Generated `sources` shape

Resolved values are injected into each environment entry:

```yaml
sources:
  directory:
    schemas:
      - public
    nonprod:
      server: default
      database: directory_db
      table_count: 42
      customer_id: cust-001
```

## Warning behavior

- If a query fails or returns no scalar value for a specific database, update continues.
- A warning is printed with key, server, and database context.
