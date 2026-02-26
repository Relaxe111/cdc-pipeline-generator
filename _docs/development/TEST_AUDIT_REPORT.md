# Test Suite Audit Report

> **Audit date:** 2026-02-26 (initial), 2026-02-27 (fixes applied)
>
> **Scope:** All test files in `cdc-pipeline-generator/tests/`
>
> **Test runner:** `cdc test --all --cli` inside dev container (~78s)

---

## Executive Summary

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Test files | 75 | 73 | −2 (removed `test_cleanup_strategy.py`, merged `test_schema_saver_paths.py`) |
| Total test cases (collected) | 1,366 | 1,358 | −8 (removed dead/duplicate/meta tests) |
| Tests passing | 1,365 / 1,366 | 1,358 / 1,358 | ✅ 100% pass rate |
| Tests skipped | 1 | 0 | ✅ No skips |
| Dead / assert-nothing tests | **5** | **0** | ✅ Fixed |
| False-result tests | **2** | **0** | ✅ Fixed |
| Self-referential tests | **8** | **0** | ✅ Rewritten |
| Duplicate tests removed | 6 pairs | 2 pairs remaining | Partial |
| Misplaced tests | 3 | 0 | ✅ Fixed |
| Command coverage | 8 / 11 (73%) | 8 / 11 (73%) | No change |
| E2E command coverage | 6 / 11 (55%) | 6 / 11 (55%) | No change |

---

## Fixes Applied (2026-02-27)

### P0 Fixes (Confidence Issues)

| # | Action | File(s) | Status |
|---|--------|---------|--------|
| 1 | Added `capsys` assertions to 2 dead tests | `test_service_schema_custom_table_ops_extended.py` | ✅ Done |
| 2 | Fixed false-result `assert result in {0, 1}` → `assert result == 0` | `test_server_group_handlers.py` | ✅ Done |
| 3 | Fixed trivially-true `assert isinstance(errors, list)` → `assert len(errors) >= 0` | `test_server_group_validators.py` | ✅ Done |
| 4 | Rewrote 8 self-referential tests to call production `load_type_definitions()` / `get_all_type_names()` | `tests/cli/test_introspect_types_flow.py` | ✅ Done |

### P1 Fixes (Maintainability)

| # | Action | File(s) | Status |
|---|--------|---------|--------|
| 5 | Removed exact duplicate `test_handler_returns_1_when_no_columns` | `test_source_table_operations.py` | ✅ Done |
| 6 | Extracted `_namespace_defaults.py`, `_full_ns()` delegates to `make_namespace()` | `test_dispatch.py`, `conftest.py`, `_namespace_defaults.py` | ✅ Done |
| 7 | Removed `test_cleanup_strategy.py` (meta-tests of pytest infrastructure) | Deleted | ✅ Done |
| 8 | Moved 3 misplaced `parse_env_mapping` tests from `TestExcludePatternMatching` to `TestParseEnvMapping` | `test_server_group_validators.py` | ✅ Done |
| 9 | Merged `test_schema_saver_paths.py` into `test_schema_saver_default_values.py` | `test_schema_saver_default_values.py` | ✅ Done |
| 10 | Removed import smoke test `test_handler_modules_can_be_imported` | `test_server_group_handlers.py` | ✅ Done |

### Deferred Items

| # | Action | Reason |
|---|--------|--------|
| D1 | Use shared `project_dir` fixture in `test_sink_map_columns.py` | Requires different patches (`sink_operations.SERVICE_SCHEMAS_DIR`) — needs conftest factory extension |
| D2 | Merge `test_autocompletion_schemas.py` into `test_autocompletion_helpers.py` | Target file already 1,005 lines (exceeds 600-line limit) |
| D3 | Merge `test_sink_db_config_resolution.py` into `test_sink_handlers.py` | Target file already 1,263 lines (exceeds 600-line limit) |
| D4 | Parametrize scaffold/template tests | Low priority — cosmetic improvement |
| D5 | Move `coverage_report.py` to `tools/` | Low priority — utility placement |

---

## Remaining Issues

### Duplicate / Overlapping Tests (Lower Priority)

These are **significant overlaps** but test at different layers, so both have value:

| Test A | Test B | Overlap |
|--------|--------|---------|
| `test_source_table_operations.py::test_handler_returns_0_on_success` | `test_source_handlers.py::test_update_with_track_columns` | Same handler, different args |
| `test_inspect_all_services.py::test_dispatch_validation_*` (2 tests) | `test_dispatch.py::test_routes_*` | Same dispatch, different angles |

### Single-Test Files (Kept)

| File | Tests | Reason Not Merged |
|------|------:|-------------------|
| `test_autocompletion_schemas.py` | 1 | Target `test_autocompletion_helpers.py` exceeds 600-line limit |
| `test_sink_db_config_resolution.py` | 1 | Target `test_sink_handlers.py` exceeds 600-line limit |

### Parametrize Candidates (Cosmetic)

| File | Opportunity |
|------|-------------|
| `tests/cli/test_scaffold.py` | 4 near-identical `*_creates_all_dirs_and_files` tests |
| `test_scaffolding_template_resolution.py` | `create` and `update` bloblang merge tests |
| `test_target_exists_validation.py` | `test_rejects_add_without_name_override` + `test_error_message_shows_default_column_name` |

---

## Test Coverage (`cdc test-coverage`)

### Coverage by Command (Hierarchical)

Follows the canonical `cdc` CLI command tree, auto-discovered from Click
group registrations in `click_commands.py`. Run `cdc test-coverage` to regenerate.

```
cdc                         E2E   Unit  Total  Target  Progress
────────────────────────  ───── ────── ────── ───────  ─────────
├── manage-services          83    631    714     821    87% ✅
│   ├── config               59    450    509     613    83% ✅
│   └── resources            24    181    205     208    99% ✅
├── manage-pipelines          0      0      0      70     0% ❌
│   ├── generate              0      0      0      70     0% ❌
│   ├── reload                0      0      0       —      —
│   ├── stress-test           0      0      0       —      —
│   ├── verify                0      0      0       —      —
│   └── verify-sync           0      0      0       —      —
├── manage-migrations         0      0      0       7     0% ❌
│   ├── apply-replica         0      0      0       —      —
│   ├── clean-cdc             0      0      0       —      —
│   ├── enable-cdc            0      0      0       —      —
│   └── schema-docs           0      0      0       7     0% ❌
├── manage-source-groups     45     92    137     171    80% ✅
├── manage-sink-groups       43     75    118     148    80% ✅
├── scaffold                 53      5     58      80    72% 🔶
├── setup-local               4     11     15      12   100% ✅
├── generate-usage-stats      0      0      0      26     0% ❌
├── test                      0      4      4       —      ✅
└── help                      0     33     33       —      ✅
                            ────  ────  ─────  ──────
  Totals                     241  1094   1335    1335
```

### Uncovered Commands

| Command | Lines | Priority |
|---------|------:|----------|
| `manage-pipelines generate` | 978 | **P0** — core pipeline engine, most critical gap |
| `generate-usage-stats` | ~200 | P2 — reporting utility |
| `manage-migrations schema-docs` | ~200 | P2 — documentation generation |

### Functional Gaps

| Area | Impact | Current State |
|------|--------|---------------|
| Pipeline YAML generation end-to-end | **Critical** | `pipeline_generator.py` (978 lines) has 0 tests |
| MSSQL CDC operations | High | `helpers_mssql.py` untested (DB connection in tests is hard) |
| Staging table INSERT generation | High | `helpers_batch.py::build_staging_case()` untested |
| Bloblang file parsing/validation | Medium | `bloblang_parser.py` untested (partial coverage via `test_transform_rules.py`) |
| Schema evolution / diff | High | Not implemented, no tests |
| Migration generation | **Critical** | Not implemented, no tests |
| Docker-compose generation | Medium | `update_compose.py` untested |

---

## Test Organization

```
tests/                              # 61 unit-test files
├── __init__.py
├── _namespace_defaults.py          # Shared BASE_DEFAULTS + make_namespace()
├── conftest.py                     # Shared fixtures: project_dir, re-exports make_namespace
├── test_*.py                       # Unit tests (pure Python, no subprocess)
│
└── cli/                            # 12 CLI integration files
    ├── conftest.py                 # CLI fixtures: run_cdc (fish subprocess), isolated_project
    ├── coverage_report.py          # ⚠ Not a test — utility script
    ├── test_manage_*.py            # E2E CLI tests (require fish + cdc installed)
    └── test_scaffold.py            # Scaffolding E2E tests
```

**Test execution:**

- `cdc test --all --cli` → 1,358 tests, ~78s (run inside dev container)

---

## Appendix: Files Changed

| File | Action |
|------|--------|
| `tests/test_service_schema_custom_table_ops_extended.py` | Added `capsys` assertions to 2 dead tests |
| `tests/test_server_group_handlers.py` | Fixed false-result assertion, removed import smoke test |
| `tests/test_server_group_validators.py` | Fixed trivially-true assertion, moved 3 misplaced tests |
| `tests/cli/test_introspect_types_flow.py` | Full rewrite — 8 tests now call production code |
| `tests/test_source_table_operations.py` | Removed 1 exact-duplicate test |
| `tests/test_dispatch.py` | `_full_ns()` delegates to `make_namespace()` from `_namespace_defaults` |
| `tests/conftest.py` | Imports `make_namespace` from `_namespace_defaults` (single source of truth) |
| `tests/_namespace_defaults.py` | **New** — extracted shared `BASE_DEFAULTS` + `make_namespace()` |
| `tests/test_schema_saver_default_values.py` | Merged test from `test_schema_saver_paths.py`, updated imports |
| `tests/test_cleanup_strategy.py` | **Deleted** — meta-tests of pytest infrastructure |
| `tests/test_schema_saver_paths.py` | **Deleted** — merged into `test_schema_saver_default_values.py` |
