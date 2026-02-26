# Migration Implementation — Result Report

> **AI-friendly.** Structured for follow-up sessions.
>
> Date: 2026-02-26
> Session scope: Implement `cdc manage-migrations generate` (Phase 0 + Phase 1 + Phase 2)

---

## Summary

The `cdc manage-migrations generate` command is now **fully functional**.
It reads service/sink configuration, loads table-definitions from `generated/table-definitions/`,
and produces a complete set of idempotent PostgreSQL migration SQL files.

**Test suite:** 1358 passed, 0 failed (no regressions).

---

## What Was Implemented

### Files Created (5 new files, 1448 total lines)

| File | Lines | Purpose |
|------|-------|---------|
| `cdc_generator/core/migration_generator.py` | 957 | Core generation engine |
| `cdc_generator/cli/migration_generate.py` | 72 | CLI entry point (argparse) |
| `cdc_generator/templates/migrations/create-schemas.sql.j2` | 13 | Schema creation template |
| `cdc_generator/templates/migrations/cdc-management.sql.j2` | 271 | Full CDC infrastructure template |
| `cdc_generator/templates/migrations/staging.sql.j2` | 135 | Per-table staging + merge procedure template |

### Files Modified (2 files)

| File | Change |
|------|--------|
| `cdc_generator/cli/commands.py` | Added `generate` to `MIGRATION_COMMANDS` dict |
| `cdc_generator/cli/click_commands.py` | Added `generate` Click subcommand + updated help text |

### Files Removed (1 file)

| File | Reason |
|------|--------|
| `cdc_generator/templates/migrations/table.sql.j2` | Dead code — DDL is built in Python (`_build_create_table_sql()`) for proper formatting |

---

## Design Decisions Made During Implementation

| Decision | Rationale |
|----------|-----------|
| Table DDL built in Python, not Jinja2 | Jinja2 `trim_blocks` collapsed multi-line column defs; Python gives full formatting control |
| UPDATE SET clause built in Python | Same whitespace issue — pre-formatted string passed as `{{ update_set_sql }}` |
| `RenderContext` dataclass | Groups 6 shared rendering params to keep function signatures under 7 args |
| `target_exists: true` tables → skipped entirely | These tables already exist in PostgreSQL; no DDL needed |
| CDC metadata columns always appended | `__sync_timestamp`, `__source`, `__deleted`, `__operation`, `__lsn`, `__seq_val` |
| Column templates resolved via existing `resolve_column_templates()` | Reuses `column_template_operations.py` — adds `_customer_id UUID NOT NULL` etc. |
| Schema placeholder: `{{SCHEMA}}` (db-per-tenant) or literal (db-shared) | Consistent with pipeline template convention |
| DB user from `CDC_DB_USER` env var, default `postgres` | All GRANT statements use this |

---

## Verified Behavior

| Feature | Status | Evidence |
|---------|--------|----------|
| Dry-run mode (`--dry-run`) | ✅ | Lists 41 tables, 13 schemas without writing files |
| Full generation (no filter) | ✅ | 5 files for 1 table (Actor) + 13 schemas |
| Table filter (`--table Actor`) | ✅ | Generates only Actor + substring matches |
| `ignore_columns` filtering | ✅ | `Døv` and `Fødeland` excluded from Actor DDL |
| `column_templates` injection | ✅ | `_customer_id UUID NOT NULL` added to Actor |
| CDC metadata columns | ✅ | 6 metadata columns appended to every table |
| Primary key in CREATE TABLE | ✅ | `PRIMARY KEY ("actno")` in Actor DDL |
| Sync timestamp index | ✅ | `CREATE INDEX IF NOT EXISTS` after each table |
| Multi-line SQL formatting | ✅ | Column defs and UPDATE SET properly formatted |
| `target_exists: true` tables skipped | ✅ | `customer_user`, `adopus_AdgangLinjer` not generated |
| Missing table-definition warnings | ✅ | 36 warnings for tables without YAML definitions |
| No-PK table warnings | ✅ | 2 warnings for tables without primary keys |
| Infrastructure: schema creation | ✅ | 13 customer schemas + `cdc_management` |
| Infrastructure: cdc-management | ✅ | merge_control, error_log, processing_log, migration_history, 4 views, permissions |
| Staging: UNLOGGED table | ✅ | `stg_<Table>` with Kafka metadata columns |
| Staging: merge procedure | ✅ | Batched 10k UPSERT with `DISTINCT ON` dedup |
| Staging: trigger + mark_for_merge | ✅ | Auto-marks table for merge on INSERT |
| Manifest YAML | ✅ | Lists all generated files + schemas |
| Existing test suite | ✅ | 1358 passed, 0 failed |

---

## Generated Output Structure

```
generated/pg-migrations/
├── 00-infrastructure/
│   ├── 01-create-schemas.sql      # 13 customer schemas + cdc_management
│   └── 02-cdc-management.sql      # merge control, monitoring, permissions
├── 01-tables/
│   ├── Actor.sql                  # CREATE TABLE + index
│   └── Actor-staging.sql          # staging table + merge proc + trigger
└── manifest.yaml                  # file inventory
```

Only 1 of 39 `adopus.*` tables generated (Actor) because only 3 table-definitions exist
and 2 of those (Fraver, Test) are source-only tables not in the sink config.

---

## What Still Needs To Be Done

### Blockers for Full Table Coverage

| # | Task | Priority | Detail |
|---|------|----------|--------|
| B1 | Generate remaining table-definitions | **P0** | Run `cdc manage-services config --service adopus --inspect` against MSSQL to create `generated/table-definitions/*.yaml` for all 39 tables. Currently only 3 exist (Actor, Fraver, Test). |
| B2 | Verify full 39-table generation | **P0** | After B1, run `cdc manage-migrations generate` and verify all 39 `adopus.*` sink tables produce correct DDL. |

### Phase 0: CLI Cleanup (Partially Done)

| # | Task | Status | Detail |
|---|------|--------|--------|
| 0.1 | `generate` subcommand | ✅ Done | Wired in `MIGRATION_COMMANDS` + Click |
| 0.2 | Remove dead `enable-cdc`, `apply-replica`, `clean-cdc` | ⬜ Not done | Still crash at runtime — remove or stub (preprod policy) |
| 0.3 | Keep `schema-docs` in `manage-migrations` | ✅ Already works | No change needed |
| 0.4 | Update Fish completions | ⬜ Not done | `cdc_generator/templates/init/cdc.fish` needs `generate` added |

### Phase 1: Remaining Items

| # | Task | Status | Detail |
|---|------|--------|--------|
| 1A.4 | MSSQL introspection in migration generator | ⬜ | Currently reads pre-generated YAML only. Live introspection would bypass the YAML step. Not critical — YAML workflow works. |
| 1D.3 | `replicate_structure` via `structure_replicator.py` | ⬜ | Code checks the flag but doesn't call `structure_replicator.py`. Tables with `replicate_structure: true` are generated from table-definitions instead. May need integration if behavior differs. |

### Phase 2: Service Config Integration (Mostly Done)

| # | Task | Status | Detail |
|---|------|--------|--------|
| 2.1 | Parse sink config for `target_exists` | ✅ Done | Skips `target_exists: true` tables |
| 2.2 | Support `from:` mapping | ✅ Done | Resolves source table from `from:` field |
| 2.3 | `columns:` rename mapping validation | ⬜ Not done | Low priority — `target_exists` tables are skipped entirely |
| 2.4 | Document `transforms:` in header | ⬜ Not done | Cosmetic only |
| 2.5 | Multiple sinks per service | ⬜ Partial | Currently processes first sink group. Multiple sink groups need testing. |

### Phase 3-6: Future Work (Not Started)

| Phase | Scope | Priority | Notes |
|-------|-------|----------|-------|
| Phase 3 | Schema evolution (`diff` command) | P2 | Detect MSSQL changes → generate `ALTER TABLE` |
| Phase 4 | Migration state tracking (`apply`, `status`) | P2 | `migration_history` table already exists in `cdc-management.sql.j2` |
| Phase 5 | `db-shared` pattern support | P2 | `_resolve_schema_placeholder()` already handles it; needs testing with asma |
| Phase 6 | Operational tooling (`enable-cdc`, `apply`, `clean-cdc`) | P3 | Requires live DB connections |

---

## Known Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| Only works with pre-generated `table-definitions/*.yaml` | Must run inspect first | Run `cdc manage-services config --inspect` before generate |
| Output dir hardcoded to `generated/pg-migrations/` | Plan says `migrations/` | Override with `--output-dir` flag; rename later |
| `table.sql.j2` template removed — DDL built in Python | Template pattern inconsistency (staging uses Jinja2, table DDL uses Python) | Move staging to Python too, or move DDL back to Jinja2 with custom whitespace handling |
| Lambda warnings from ruff on `list[str]` field factories | Cosmetic lint noise | Suppress with `# noqa` or accept the warning (needed for pyright strict) |
| Stale old migration files may exist in `generated/pg-migrations/` | Previous runs leave orphan files | Clean manually or add `--clean` flag |

---

## Key File References

| File | Role |
|------|------|
| `cdc_generator/core/migration_generator.py` | Generator engine — start here |
| `cdc_generator/cli/migration_generate.py` | CLI entry point |
| `cdc_generator/templates/migrations/*.j2` | SQL templates (2 infra + 1 staging) |
| `cdc_generator/cli/commands.py` | `MIGRATION_COMMANDS` routing dict |
| `cdc_generator/cli/click_commands.py` | Click subcommand definitions |
| `cdc_generator/core/column_template_operations.py` | Column template resolution |
| `cdc_generator/core/type_mapper.py` | MSSQL → PG type mapping |
| `cdc_generator/core/structure_replicator.py` | Existing DDL generator (reference) |
| `_docs-migrations/MIGRATION_IMPLEMENTATION_PLAN.md` | Full phase plan with all decisions |

---

## Quick Commands

```bash
# Generate migrations (from implementation repo)
cdc manage-migrations generate

# Generate for specific table
cdc manage-migrations generate --table Actor

# Dry run (preview only)
cdc manage-migrations generate --dry-run

# Custom output directory
cdc manage-migrations generate --output-dir ./my-migrations

# Direct Python invocation
python -m cdc_generator.cli.migration_generate --service adopus --table Actor
```
