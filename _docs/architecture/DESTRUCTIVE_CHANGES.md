# Manual and Destructive Migration Changes

> Additive schema changes are auto-generated and applied automatically. Destructive or semantic changes must be handled manually first, then aligned in YAML and regenerated.

---

## Scope

This guide covers schema changes that can break data, code, or replication behavior and therefore require a manual migration step.

Typical destructive/manual actions:

- Drop column
- Rename column
- Change type incompatibly (for example `varchar` -> `integer`)
- Tighten nullability (`NULL` -> `NOT NULL`)
- Replace PK or uniqueness semantics
- Split/merge columns with transformation logic

---

## Rules

- Do not edit generated SQL files under `migrations/**` manually.
- Implement destructive operations in a separate manual migration SQL file.
- Run manual migration before running generator-managed apply.
- Keep service YAML as the source of truth after the manual migration is completed.
- Regenerate and apply managed migrations only after live DB shape matches YAML intent.
- Do not manually edit `services/_schemas/**` in normal workflow (inspect-managed/read-only).

### Recommended file location for manual SQL

Use a dedicated folder in the implementation repository:

- `migrations/manual/`

Recommended filename format:

- `YYYYMMDD_HHMM__short_description.sql`

Example:

- `migrations/manual/20260227_1130__rename_actor_old_name_to_new_name.sql`

---

## Why this is required

`cdc manage-migrations generate` intentionally emits additive DDL (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`) and never auto-generates drops/renames/type-shrinks.

`cdc manage-migrations apply` validates expected table shape before applying additive SQL. If existing columns conflict by type or nullability, apply fails fast so data-destructive operations are never executed implicitly.

---

## Standard Workflow

1. Decide the destructive change (rename/drop/type/nullability) and how data will be preserved.
2. Create a manual SQL file under `migrations/manual/`.
  - Example: `migrations/manual/20260227_1130__rename_actor_old_name_to_new_name.sql`
3. Put only the manual SQL for this change in that file.
4. Execute that SQL directly on target PostgreSQL (dev/stage/prod as needed).
5. Align generator inputs (do **not** edit `_schemas` manually):
  - If source schema changed (rename/type/nullability/add/drop in source DB):
    - run `cdc manage-services resources --service <service> --inspect`
    - this refreshes `services/_schemas/**` automatically
  - If you need extra sink-only columns:
    - edit `services/<service>.yaml` and add/update `column_templates` for the table
6. Re-check that expected generated columns now match your intended target shape.
7. Run:
  - `cdc manage-migrations generate`
  - `cdc manage-migrations diff`
  - `cdc manage-migrations apply --dry-run`
  - `cdc manage-migrations apply`
8. If `diff`/`apply` still reports mismatch, fix SQL or YAML and repeat.

---

## Which YAML should I update?

In this project, there are two different YAML responsibilities:

- **Inspect-managed schema YAML (read-only in normal flow):**
  - `services/_schemas/<service>/<schema>/<table>.yaml`
  - do not edit manually; refresh with:
    - `cdc manage-services resources --service <service> --inspect`

- **Service config YAML (manual edits allowed):**
  - `services/<service>.yaml`
  - use this for sink config changes like `column_templates`, transforms, ignore columns, etc.

So when source columns change, update by **inspect**, not hand-editing `_schemas`.
When you need sink-only modeled columns, edit `services/<service>.yaml` (`column_templates`).

### Exact commands (copy/paste)

If source schema changed and you need to refresh `_schemas`:

```bash
# 1) Refresh inspected table schemas for one service
cdc manage-services resources --service adopus --inspect

# Optional: inspect only one table
cdc manage-services resources --service adopus --inspect --table dbo.Actor

# 2) Re-generate migrations from refreshed schemas + service config
cdc manage-migrations generate

# 3) Check expected vs generated
cdc manage-migrations diff

# 4) Safety preview apply
cdc manage-migrations apply --dry-run

# 5) Real apply
cdc manage-migrations apply
```

If you need sink-only columns (without source schema change):

```bash
# 1) Edit service config (example)
#    services/adopus.yaml -> sinks.*.tables.<schema.table>.column_templates

# 2) Generate + validate + apply
cdc manage-migrations generate
cdc manage-migrations diff
cdc manage-migrations apply --dry-run
cdc manage-migrations apply
```

---

## Optional: service.yaml hints for better autodiscovery

`cdc manage-migrations generate` now supports optional per-table hints in `services/<service>.yaml`.
These hints improve auto-generated manual SQL for destructive changes.

Path:

- `sinks.<sink>.tables.<schema.table>.manual_migration_hints`

Example:

```yaml
sinks:
  sink_asma.directory:
    tables:
      adopus.Actor:
        from: dbo.Actor
        manual_migration_hints:
          renames:
            - from: old_name
              to: new_name
          type_changes:
            - column: score
              using: "NULLIF(trim(\"score\"), '')::integer"
          set_not_null:
            - column: country
              pre_sql: "UPDATE \"adopus\".\"Actor\" SET \"country\" = 'UNKNOWN' WHERE \"country\" IS NULL;"
```

What happens on generate:

- Additive changes are still auto-generated in table SQL (`ADD COLUMN IF NOT EXISTS`).
- Destructive/semantic changes still create `02-manual/<Table>/MANUAL_REQUIRED.sql`.
- If hints exist, hint-based SQL is inserted at the top of that manual file.

Do **not** edit generated SQL files in `migrations/<sink>/...`.

---

## Example A: Rename column

Goal: rename `old_name` -> `new_name` without data loss.

1) Create file:

`migrations/manual/20260227_1130__rename_actor_old_name_to_new_name.sql`

2) Add SQL in that file:

```sql
ALTER TABLE "my_schema"."Actor" RENAME COLUMN "old_name" TO "new_name";
```

3) Refresh schema from source (do not manually edit `_schemas`):

```bash
cdc manage-services resources --service adopus --inspect
```

4) Regenerate/apply:

```bash
cdc manage-migrations generate
cdc manage-migrations diff
cdc manage-migrations apply --dry-run
cdc manage-migrations apply
```

---

## Example B: Type conversion with cast

Goal: change `varchar` column to `integer`.

1) Create file:

`migrations/manual/20260227_1140__actor_score_varchar_to_integer.sql`

2) Add SQL in that file (validate cast safety first):

```sql
ALTER TABLE "my_schema"."Actor"
  ALTER COLUMN "score" TYPE integer
  USING NULLIF(trim("score"), '')::integer;
```

3) Refresh schema from source (do not manually edit `_schemas`):

```bash
cdc manage-services resources --service adopus --inspect
```

4) Regenerate/apply commands as in Example A.

---

## Example C: Tighten nullability

Goal: `NULL` -> `NOT NULL`.

1) Create file:

`migrations/manual/20260227_1150__actor_country_set_not_null.sql`

2) Add SQL in that file (backfill first, then constraint):

```sql
UPDATE "my_schema"."Actor"
SET "country" = 'UNKNOWN'
WHERE "country" IS NULL;

ALTER TABLE "my_schema"."Actor"
  ALTER COLUMN "country" SET NOT NULL;
```

3) Refresh schema from source (do not manually edit `_schemas`):

```bash
cdc manage-services resources --service adopus --inspect
```

4) Regenerate/apply commands as in Example A.

---

## Example D: Add sink-only column with `column_templates`

Goal: add `_customer_id` or another sink-modeled column without changing source table.

1) Edit service config:

- `services/adopus.yaml`

2) Add/update `column_templates` for the sink table (example shape):

```yaml
sinks:
  sink_asma.directory:
    tables:
      adopus.Actor:
        column_templates:
          - _customer_id
```

3) Generate/apply:

```bash
cdc manage-migrations generate
cdc manage-migrations diff
cdc manage-migrations apply --dry-run
cdc manage-migrations apply
```

---

## Failure mode and resolution

If apply fails with drift/type/nullability conflict:

- Read the reported table/column mismatch.
- Apply required manual SQL migration to make live schema compatible.
- Re-run dry-run apply.

Do not bypass this check by editing generated SQL.

---

## Preprod policy note

Current phase is preprod. Prefer clean replacement over backward-compatibility shims. Remove deprecated paths and obsolete schema constructs when touched.
