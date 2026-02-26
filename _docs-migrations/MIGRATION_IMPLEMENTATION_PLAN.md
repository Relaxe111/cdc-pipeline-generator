# Migration Implementation Plan

> **Working document.** Tracks implementation progress for `manage-migrations` command group.
>
> Reference audit: [MANAGE_MIGRATIONS_STATUS.md](MANAGE_MIGRATIONS_STATUS.md)
>
> Created: 2026-02-26

---

## Decisions Log

| Date | Decision | Context |
|------|----------|---------|
| 2026-02-26 | Output root: top-level `migrations/` (not `generated/pg-migrations/`) | Audit §3.3 |
| 2026-02-26 | Sub-layout: subdirectories by category (`infrastructure/`, `schemas/`, `tables/`, `staging/`) | Audit §3.3 |
| 2026-02-26 | `cdc_processing_log` → centralize in `cdc_management` schema (one table per database) | Audit §8 discussion |
| 2026-02-26 | `generated/table-definitions/` stays under `generated/` as intermediate artifact | Audit §3.3 || 2026-02-26 | Q1: Use Jinja2 templates for SQL generation (already a dependency) | Open Questions |
| 2026-02-26 | Q2: `migrations/` committed to git (reviewable artifacts, `DO NOT EDIT` header) | Open Questions |
| 2026-02-26 | Q3: `schema-docs` output stays in `generated/schemas/` (intermediate artifact) | Open Questions |
| 2026-02-26 | Q4: Remove stale `generated/pg-migrations/` immediately (preprod policy) | Open Questions |
| 2026-02-26 | Q5: Merge procedures always use batched processing | Open Questions |
| 2026-02-26 | Q6: GRANT user configurable via env var `CDC_DB_USER`, default `postgres` | Open Questions |
| 2026-02-26 | §11.5: No rollback/DOWN migrations — everything is new, no legacy to roll back | Recommendations |
| 2026-02-26 | §11.1: Rely on directory structure for migration ordering | Recommendations |
| 2026-02-26 | §11.2: Use `IF NOT EXISTS` everywhere; all table/column identifiers always double-quoted | Recommendations |
---

## Phase 0: CLI Cleanup

> Fix broken CLI routing before adding new functionality.
> Ref: Audit §2.1, §2.2

| # | Task | Status | Notes |
|---|------|--------|-------|
| 0.1 | Add `generate` subcommand to `MIGRATION_COMMANDS` dict | ⬜ | Core command that writes to `migrations/` |
| 0.2 | Remove or stub `enable-cdc`, `apply-replica`, `clean-cdc` from Click CLI | ⬜ | Currently crash at runtime — remove dead code (preprod policy) |
| 0.3 | Keep `schema-docs` in `manage-migrations` | ⬜ | It generates schema documentation, fits migration domain |
| 0.4 | Update Fish completions for `manage-migrations` subcommands | ⬜ | After 0.1–0.3 are done |

**Acceptance:** `cdc manage-migrations generate --help` works. Dead subcommands removed.

---

## Phase 1: Core Migration Generator

> Create the missing `migration_generator.py` module.
> Ref: Audit §1, §3, §4

### 1A: Table-Definition Generation

> Regenerate `generated/table-definitions/*.yaml` from MSSQL inspection + service config.
> Ref: Audit §3.2, §4.2

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1A.1 | Create `cdc_generator/core/migration_generator.py` skeleton | ⬜ | Entry point: `generate_migrations(service, env)` |
| 1A.2 | Read source tables from `services/{service}.yaml` → `source.tables` | ⬜ | Use existing `load_service_config()` |
| 1A.3 | Respect `ignore_columns` per table | ⬜ | Filter columns before DDL generation |
| 1A.4 | Connect to MSSQL reference DB, inspect each table | ⬜ | Reuse `schema_docs.py` introspection logic |
| 1A.5 | Generate `generated/table-definitions/{Table}.yaml` per source table | ⬜ | MSSQL→PG type mapping via `type_mapper.py` |
| 1A.6 | Support `--table` flag for single-table regeneration | ⬜ | Skip full introspection when only one table changed |

**Acceptance:** `cdc manage-migrations generate` produces 39 table-definition YAMLs (not 3).

### 1B: Schema Creation SQL

> Generate `migrations/schemas/create-schemas.sql` from config.
> Ref: Audit §9

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1B.1 | Read customer list from `source-groups.yaml` (db-per-tenant) | ⬜ | Use `_derive_customers_from_source_group()` |
| 1B.2 | Generate `CREATE SCHEMA IF NOT EXISTS` per customer | ⬜ | Idempotent |
| 1B.3 | Include `cdc_management` schema creation | ⬜ | Infrastructure schema |
| 1B.4 | Write to `migrations/schemas/create-schemas.sql` | ⬜ | Single file, auto-generated |

**Acceptance:** File lists all 26 customers + `cdc_management`. Adding a customer to source-groups and regenerating updates the file.

### 1C: Infrastructure SQL

> Generate `migrations/infrastructure/` from templates.
> Ref: Audit §8, merge control discussion

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1C.1 | Port `cdc-merge-control.sql` as template in generator | ⬜ | Store template in `cdc_generator/templates/migrations/` |
| 1C.2 | Move `cdc_processing_log` into `cdc_management` schema | ⬜ | Decision: one monitoring table per database |
| 1C.3 | Merge processing-log views into merge-control infrastructure | ⬜ | Single `infrastructure/cdc-management.sql` file |
| 1C.4 | Write to `migrations/infrastructure/cdc-management.sql` | ⬜ | Idempotent, run-once per database |

**Acceptance:** Single infrastructure SQL creates `cdc_management` schema with merge control + processing log + all views.

### 1D: Table DDL Generation

> Generate `migrations/tables/{Table}.sql` per CDC table.
> Ref: Audit §4, §7

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1D.1 | Read sink config: check `target_exists` flag | ⬜ | `target_exists: true` → skip DDL |
| 1D.2 | Read sink config: check `replicate_structure` flag | ⬜ | `replicate_structure: true` → use `structure_replicator.py` |
| 1D.3 | Wire `structure_replicator.py` into migration generator | ⬜ | Already generates correct DDL, just not connected |
| 1D.4 | Apply `column_templates` (e.g., add `customer_id` column) | ⬜ | Read from sink table config |
| 1D.5 | Add CDC metadata columns (`__sync_timestamp`, `__source`, etc.) | ⬜ | Define column set in one place |
| 1D.6 | Add PK constraint and sync timestamp index | ⬜ | Read PK from table-definition YAML |
| 1D.7 | Use `{{SCHEMA}}` placeholder for per-customer deployment | ⬜ | Consistent with existing pattern |
| 1D.8 | Write `migrations/tables/{Table}.sql` per table | ⬜ | One file per source table with `target_exists: false` |

**Acceptance:** 39 `adopus.*` tables generate DDL. 2 `public.*` tables (`target_exists: true`) are skipped.

### 1E: Staging Table + Merge Procedure Generation

> Generate `migrations/staging/{Table}-staging.sql` per CDC table.
> Ref: Audit §6

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1E.1 | Create staging table template (UNLOGGED, `LIKE` final table) | ⬜ | Port from existing `4-*-staging.sql` SQL |
| 1E.2 | Add Kafka metadata columns (`__kafka_offset`, etc.) | ⬜ | Required for offset tracking |
| 1E.3 | Generate merge stored procedure (`sp_merge_{table}`) | ⬜ | Batch UPSERT with `DISTINCT ON` dedup |
| 1E.4 | Add trigger `trg_mark_for_merge` on staging table | ⬜ | Wires into merge control system |
| 1E.5 | Include processing-log INSERT in merge procedure | ⬜ | Write to `cdc_management.cdc_processing_log` |
| 1E.6 | Write `migrations/staging/{Table}-staging.sql` per table | ⬜ | One file per table |

**Acceptance:** Every table in `migrations/tables/` has a corresponding `migrations/staging/` file. Merge procedures reference `cdc_management` schema.

---

## Phase 2: Service Config Integration

> Make migration generator fully config-aware.
> Ref: Audit §4, §7

| # | Task | Status | Notes |
|---|------|--------|-------|
| 2.1 | Parse sink config to determine which tables need migrations | ⬜ | `target_exists: false` → generate; `true` → skip |
| 2.2 | Support `from:` mapping (sink table name ≠ source table) | ⬜ | e.g., `adopus.Actor` from `dbo.Actor` |
| 2.3 | Support `columns:` rename mapping for `target_exists: true` | ⬜ | Generate validation SQL instead of DDL |
| 2.4 | Support `transforms:` / `bloblang_ref:` (document only) | ⬜ | Transforms don't affect DDL, just note in generated header |
| 2.5 | Handle multiple sinks per service | ⬜ | May generate migrations for different target databases |

**Acceptance:** Running `generate` against adopus service with its sink config produces correct, complete migration set.

---

## Phase 3: Schema Evolution

> Detect changes and generate incremental migrations.
> Ref: Audit §5

| # | Task | Status | Notes |
|---|------|--------|-------|
| 3.1 | Implement `diff` subcommand: compare source MSSQL vs generated table-defs | ⬜ | Detect new/removed/changed columns |
| 3.2 | Generate `ALTER TABLE ADD COLUMN` for new columns | ⬜ | Append to `migrations/tables/{Table}.sql` or separate `alter/` folder |
| 3.3 | Generate `ALTER TABLE ALTER COLUMN TYPE` for type changes | ⬜ | With safety checks (data loss risk) |
| 3.4 | Detect new tables added to service config | ⬜ | Generate full DDL for new tables |
| 3.5 | Detect removed tables (warn only, never auto-drop) | ⬜ | Safety: never generate DROP TABLE |
| 3.6 | Update staging table + merge procedure when columns change | ⬜ | Staging must stay in sync with final table |
| 3.7 | Add `--dry-run` flag: show planned changes without writing | ⬜ | Output to stdout |

**Acceptance:** After adding a column in MSSQL source, `cdc manage-migrations diff` shows the change. `cdc manage-migrations generate` updates the affected files.

---

## Phase 4: Migration State Tracking

> Track which migrations have been applied to which target.
> Ref: Audit §11.4

| # | Task | Status | Notes |
|---|------|--------|-------|
| 4.1 | Add `cdc_management.migration_history` table to infrastructure SQL | ⬜ | Columns: file, checksum, schema_name, applied_at |
| 4.2 | Add SHA256 checksums to generated SQL file headers | ⬜ | Detect manual edits |
| 4.3 | Implement `apply` subcommand: run SQL against target PG | ⬜ | Check history before applying |
| 4.4 | Record applied migrations in history table | ⬜ | Skip if already applied (idempotent) |
| 4.5 | Implement `status` subcommand: show applied vs pending | ⬜ | Per-schema status |

**Acceptance:** `cdc manage-migrations status --env nonprod` shows which migrations are pending per customer schema.

---

## Phase 5: Pattern Support (db-shared)

> Make migration generator work for asma pattern.
> Ref: Audit §10

| # | Task | Status | Notes |
|---|------|--------|-------|
| 5.1 | Branch on `pattern` in migration generator | ⬜ | db-per-tenant vs db-shared |
| 5.2 | db-shared: derive schema list from services (not customers) | ⬜ | e.g., `directory`, `chat`, `proxy` |
| 5.3 | db-shared: `{{SCHEMA}}` = service name | ⬜ | Different placeholder semantics |
| 5.4 | db-shared: `customer_id` is always required in every table | ⬜ | Verify `column_templates` includes `customer_id` |
| 5.5 | Test with asma service config | ⬜ | End-to-end validation |

**Acceptance:** `cdc manage-migrations generate` works for both adopus and asma implementations.

---

## Phase 6: Operational Tooling

> CLI commands for database operations.
> Ref: Audit §2.1 (dead subcommands)

| # | Task | Status | Notes |
|---|------|--------|-------|
| 6.1 | Implement `enable-cdc`: enable CDC tracking on MSSQL source tables | ⬜ | Runs `sys.sp_cdc_enable_table` per table |
| 6.2 | Implement `clean-cdc`: clean old CDC change tracking data | ⬜ | Purge old LSN entries |
| 6.3 | Implement `apply`: apply migrations to target PostgreSQL | ⬜ | With advisory locking for safety |
| 6.4 | Add `--check` flag: validate target schema matches generated DDL | ⬜ | Non-destructive validation |

**Acceptance:** Full lifecycle via CLI: inspect → generate → apply → validate.

---

## Implementation Order & Dependencies

```
Phase 0 (CLI cleanup)
  └──→ Phase 1A (table-definitions)
         └──→ Phase 1B (schemas) ──────────────────────┐
         └──→ Phase 1C (infrastructure) ───────────────┤
         └──→ Phase 1D (table DDL) ────────────────────┤
                └──→ Phase 1E (staging + merge) ───────┤
                                                       ▼
                                              Phase 2 (config integration)
                                                       │
                                    ┌──────────────────┤──────────────────┐
                                    ▼                  ▼                  ▼
                             Phase 3 (evolution)  Phase 5 (db-shared)  Phase 4 (tracking)
                                    │                                     │
                                    └────────────────┬────────────────────┘
                                                     ▼
                                              Phase 6 (operations)
```

**Phases 1B, 1C, 1D can run in parallel** after 1A is done.
**Phases 3, 4, 5 can run in parallel** after Phase 2 is done.

---

## Files To Create / Modify

### New files (generator)

| File | Phase | Purpose |
|------|-------|---------|
| `cdc_generator/core/migration_generator.py` | 1A | Main migration generation logic |
| `cdc_generator/templates/migrations/cdc-management.sql.j2` | 1C | Infrastructure SQL template |
| `cdc_generator/templates/migrations/table.sql.j2` | 1D | Table DDL template |
| `cdc_generator/templates/migrations/staging.sql.j2` | 1E | Staging + merge procedure template |
| `cdc_generator/templates/migrations/create-schemas.sql.j2` | 1B | Schema creation template |

### Modified files (generator)

| File | Phase | Change |
|------|-------|--------|
| `cdc_generator/cli/commands.py` | 0.1 | Add `generate` to `MIGRATION_COMMANDS` |
| `cdc_generator/cli/click_commands.py` | 0.2 | Remove dead subcommands, add `generate` |
| `cdc_generator/templates/init/cdc.fish` | 0.4 | Update Fish completions |
| `cdc_generator/validators/manage_server_group/scaffolding/create.py` | 1B | Create `migrations/` dirs in scaffold |
| `cdc_generator/validators/manage_server_group/scaffolding/templates.py` | 1B | Update structure docs |

### Output files (implementation repos)

| Path | Phase | Content |
|------|-------|---------|
| `migrations/infrastructure/cdc-management.sql` | 1C | Merge control + processing log + views |
| `migrations/schemas/create-schemas.sql` | 1B | Customer/service schema creation |
| `migrations/tables/{Table}.sql` | 1D | One per CDC table |
| `migrations/staging/{Table}-staging.sql` | 1E | One per CDC table |
| `generated/table-definitions/{Table}.yaml` | 1A | Intermediate artifact (39 files) |

---

## Open Questions

| # | Question | Status | Answer |
|---|----------|--------|--------|
| Q1 | Should we use Jinja2 templates or Python string formatting for SQL generation? | ✅ | Jinja2 — already a dependency, supports conditionals for pattern branching |
| Q2 | Should `migrations/` be gitignored (generated) or committed (reviewable)? | ✅ | Committed — reviewable deployment artifacts with `DO NOT EDIT` header |
| Q3 | Should `schema-docs` output move from `generated/schemas/` to `migrations/`? | ✅ | No — stays in `generated/schemas/` as intermediate artifact |
| Q4 | How to handle existing stale `generated/pg-migrations/`? Remove now or after Phase 1? | ✅ | Remove now (preprod policy) |
| Q5 | Should merge procedures use batched processing or single-pass for small tables? | ✅ | Batched always — existing merge procedures already do this |
| Q6 | GRANT statements: hardcode `postgres` user or make configurable? | ✅ | Configurable via `CDC_DB_USER` env var, default `postgres` |
