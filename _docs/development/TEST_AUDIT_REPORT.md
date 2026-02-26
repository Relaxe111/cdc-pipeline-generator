# Test Suite Audit Report

> **Audit date:** 2026-02-26
>
> **Scope:** All test files in `cdc-pipeline-generator/tests/`
>
> **Test runner:** `cdc test --all --cli` inside dev container (79.46s)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Test files | 75 (63 unit + 12 CLI) |
| Total test cases (collected) | 1,366 |
| Total test cases (tracked by `cdc test-coverage`) | 1,343 (242 CLI e2e + 1,101 unit) |
| Tests passing | 1,365 / 1,366 ✅ |
| Tests skipped | 1 (conditional: `test_cleanup_strategy.py`) |
| Tests failing | 0 |
| Command coverage | **8 / 11 commands (73%)** |
| E2E command coverage | **6 / 11 commands (55%)** |
| Test progress vs targets | **1,343 / 1,335 (100%+)** |
| Dead / assert-nothing tests | **5** |
| False-result tests | **2** |
| Duplicate / overlapping tests | **6 pairs** |
| Self-referential tests | **8** (test their own mock data, not production code) |
| Untested commands | **3** (`generate-usage-stats`, `manage-pipelines-generate`, `manage-migrations-schema-docs`) |

---

## 1. Dead Tests (Assert Nothing)

Tests that execute code but have **no assertions**, so they always pass regardless of behavior.

| File | Test | Issue |
|------|------|-------|
| [test_service_schema_custom_table_ops_extended.py](../../tests/test_service_schema_custom_table_ops_extended.py) | `test_try_type_definitions_check_with_no_types_warns` | Calls function, no assert/raises |
| [test_service_schema_custom_table_ops_extended.py](../../tests/test_service_schema_custom_table_ops_extended.py) | `test_try_type_definitions_check_with_missing_type_warns` | Calls function, no assert/raises |
| [test_cleanup_strategy.py](../../tests/test_cleanup_strategy.py) | `test_test_workspace_directory_for_manual_testing` | Conditional: if `.test-workspace/` absent, no assertions execute |

**Fix:** Add assertions (e.g., `capsys` output check, mock.assert_called) or remove.

---

## 2. False-Result Tests

Tests with assertions that **can never fail**, giving false confidence.

| File | Test | Issue |
|------|------|-------|
| [test_server_group_handlers.py](../../tests/test_server_group_handlers.py) | `test_remove_server_in_use_by_sources_returns_error` | `assert result in {0, 1}` — always true for any int return code |
| [test_server_group_validators.py](../../tests/test_server_group_validators.py) | `test_missing_kafka_bootstrap_servers_returns_error` | `assert isinstance(errors, list)` — trivially true for the return type |

**Fix:** Assert specific expected return codes / error contents.

---

## 3. Trivial / Meta Tests

Tests that test pytest infrastructure or have questionable value.

| File | Test | Issue |
|------|------|-------|
| [test_cleanup_strategy.py](../../tests/test_cleanup_strategy.py) | `test_tmp_path_provides_isolation` | Tests pytest's own `tmp_path` — meta-test of framework |
| [test_cleanup_strategy.py](../../tests/test_cleanup_strategy.py) | `test_no_artifacts_in_generator_root` | Uses `pytest.skip()` when artifacts exist — conditionally no-op |
| [test_server_group_handlers.py](../../tests/test_server_group_handlers.py) | `test_handler_modules_can_be_imported` | Import smoke test — adds no value alongside existing tests |

**Recommendation:** `test_cleanup_strategy.py` documents testing strategy but isn't a real test file. Consider moving to docs or removing.

---

## 4. Self-Referential Tests (Test Own Mock Data)

Tests that create mock data, write it, read it back, and assert the mock values they just wrote. They exercise YAML round-tripping, not production code.

| File | Tests Affected | Issue |
|------|---------------|-------|
| [tests/cli/test_introspect_types_flow.py](../../tests/cli/test_introspect_types_flow.py) | 8 of 9 tests | Create mock YAML → read back → assert hardcoded values. Only `test_missing_definitions_file_returns_none` tests real behavior. |

**Examples of self-referential tests:**
- `test_pgsql_definitions_contain_postgres_types` — writes `{"numeric": {"types": {"bigint": ...}}}` then asserts `"bigint" in result`
- `test_uuid_type_suggests_uuid_defaults` — writes `{"uuid": {"default": "gen_random_uuid()"}}` then asserts `"gen_random_uuid()"` is present

**Fix:** Rewrite to actually invoke `--db-definitions` or completion queries against real code paths.

---

## 5. Duplicate / Overlapping Tests

### 5.1 Exact or Near-Exact Duplicates

| Test A | Test B | Overlap |
|--------|--------|---------|
| `test_source_table_operations.py::TestHandleUpdateSourceTable::test_handler_returns_1_when_no_columns` | `test_source_handlers.py::TestHandleUpdateSourceTable::test_no_columns_returns_1` | Same handler, same scenario, same assertion |

### 5.2 Significant Overlaps (Different Layer, Same Behavior)

| Test A | Test B | Overlap |
|--------|--------|---------|
| `test_source_table_operations.py::test_handler_returns_0_on_success` | `test_source_handlers.py::test_update_with_track_columns` | Same handler flow, different args |
| `test_service_parser.py::TestMainServiceAssignment` | `test_dispatch_routing.py::TestMainPositionalService` | Both test `main()` service assignment priority |
| `test_inspect_all_services.py::test_dispatch_validation_validates_config_without_service` | `test_dispatch.py::test_routes_validate_config` | Both test `_dispatch_validation` with `validate_config=True` |
| `test_inspect_all_services.py::test_dispatch_validation_returns_none_for_unhandled_commands` | `test_dispatch.py::test_returns_none_no_service` | Same dispatch, different angle |
| `test_source_table.py` (CLI) | `test_manage_services_config.py` (CLI) | Both test `--source-table --track-columns` flow |

### 5.3 Near-Duplicate Fixtures

| Location A | Location B | Issue |
|-----------|-----------|-------|
| `tests/conftest.py::make_namespace()` | `tests/test_dispatch.py::_full_ns()` | Near-duplicate; only `skip_validation` default differs |
| `tests/conftest.py::project_dir` | `tests/test_sink_map_columns.py::project_dir` | Redefined locally instead of using shared fixture |

**Recommendation:** Remove exact duplicates. Consolidate `_full_ns()` into conftest. Use shared `project_dir` fixture.

---

## 6. Structural Issues

### 6.1 Misplaced Tests

| File | Issue |
|------|-------|
| [test_server_group_validators.py](../../tests/test_server_group_validators.py) | 3 tests in `TestExcludePatternMatching` actually test `parse_env_mapping` — wrong class |

### 6.2 Misplaced Files

| File | Issue | Recommendation |
|------|-------|---------------|
| [tests/cli/coverage_report.py](../../tests/cli/coverage_report.py) | Utility script, not a test file — `0 tests` | Move to `tools/` or `scripts/` |
| [test_cleanup_strategy.py](../../tests/test_cleanup_strategy.py) | Documents testing strategy, not real tests | Move content to docs |

### 6.3 Single-Test Files

Files with only 1 test — consider merging into related test files:

| File | Tests | Merge candidate |
|------|------:|----------------|
| [test_schema_saver_paths.py](../../tests/test_schema_saver_paths.py) | 1 | → `test_schema_saver_default_values.py` or `test_schema_saver_tracked_tables.py` |
| [test_autocompletion_schemas.py](../../tests/test_autocompletion_schemas.py) | 1 | → `test_autocompletion_helpers.py` |
| [test_sink_db_config_resolution.py](../../tests/test_sink_db_config_resolution.py) | 1 | → `test_sink_handlers.py` |

### 6.4 Parametrize Candidates

| File | Issue |
|------|-------|
| [tests/cli/test_scaffold.py](../../tests/cli/test_scaffold.py) | 4 near-identical `*_creates_all_dirs_and_files` tests — parametrize over pattern × source-type |
| [test_scaffolding_template_resolution.py](../../tests/test_scaffolding_template_resolution.py) | `create` and `update` bloblang merge tests are identical except one function call |
| [test_target_exists_validation.py](../../tests/test_target_exists_validation.py) | `test_rejects_add_without_name_override` and `test_error_message_shows_default_column_name` — same setup, merge |

---

## 7. Test Coverage (`cdc test-coverage`)

Coverage is measured by **command surface** using the built-in `cdc test-coverage` tool,
which maps every test to its corresponding `cdc` subcommand.

### 7.1 Coverage by Command

| Command | E2E | Unit | Total | Target | Progress |
|---------|----:|-----:|------:|-------:|---------:|
| `cdc scaffold` | 53 | 5 | 58 | 80 | 72% 🔶 |
| `cdc manage-source-groups` | 45 | 93 | 138 | 171 | 81% ✅ |
| `cdc manage-sink-groups` | 43 | 75 | 118 | 148 | 80% ✅ |
| `cdc setup-local` | 4 | 11 | 15 | 12 | 100% ✅ |
| `cdc generate-usage-stats` | 0 | 0 | 0 | 26 | 0% ❌ |
| `cdc test` | 0 | 4 | 4 | — | ✅ |
| `cdc help` | 0 | 33 | 33 | — | ✅ |
| `cdc manage-pipelines generate` | 0 | 0 | 0 | 70 | 0% ❌ |
| `cdc manage-services config` | 59 | 451 | 510 | 613 | 83% ✅ |
| `cdc manage-services resources` | 24 | 181 | 205 | 208 | 99% ✅ |
| `cdc manage-migrations schema-docs` | 0 | 0 | 0 | 7 | 0% ❌ |

**Summary:** 8/11 commands covered (73%), 6/11 with E2E tests (55%).

### 7.2 Uncovered Commands

| Command | Lines | Priority |
|---------|------:|----------|
| `manage-pipelines generate` | 978 | **P0** — core pipeline engine, most critical gap |
| `generate-usage-stats` | ~200 | P2 — reporting utility |
| `manage-migrations schema-docs` | ~200 | P2 — documentation generation |

### 7.3 Functional Gaps

Beyond module-level gaps, these **functional areas** lack test coverage:

| Area | Impact | Current State |
|------|--------|---------------|
| Pipeline YAML generation end-to-end | **Critical** | `pipeline_generator.py` (978 lines) has 0 tests |
| MSSQL CDC operations | High | `helpers_mssql.py` untested (DB connection in tests is hard) |
| Staging table INSERT generation | High | `helpers_batch.py::build_staging_case()` untested |
| Bloblang file parsing/validation | Medium | `bloblang_parser.py` untested (partial coverage via `test_transform_rules.py`) |
| Schema evolution / diff | High | Not implemented (see migration plan), no tests |
| Migration generation | **Critical** | Not implemented (see [MIGRATION_IMPLEMENTATION_PLAN.md](../_docs-migrations/MIGRATION_IMPLEMENTATION_PLAN.md)) |
| Docker-compose generation | Medium | `update_compose.py` untested |
| Service creation flow | Medium | `service_creator.py` untested (CLI tests cover partially) |

### 7.4 Coverage Note

Many source modules that lack *direct* unit tests are still exercised
indirectly through CLI e2e and integration tests. The `cdc test-coverage`
tool tracks coverage by **command surface** (how many `cdc` subcommands
have tests), which gives 73% command coverage. Individual module-level
coverage would require `pytest --cov` instrumentation.

---

## 8. Improvement Recommendations

### P0 — Fix Now (Confidence Issues)

| # | Action | Files |
|---|--------|-------|
| 1 | Add assertions to 2 dead tests in `test_service_schema_custom_table_ops_extended.py` | 1 file |
| 2 | Fix `test_remove_server_in_use_by_sources_returns_error` — assert `result == 1` | 1 file |
| 3 | Fix `test_missing_kafka_bootstrap_servers_returns_error` — assert `len(errors) > 0` | 1 file |
| 4 | Remove or rewrite 8 self-referential tests in `test_introspect_types_flow.py` | 1 file |

### P1 — Clean Up (Maintainability)

| # | Action | Files |
|---|--------|-------|
| 5 | Remove exact duplicate `test_handler_returns_1_when_no_columns` | `test_source_table_operations.py` |
| 6 | Consolidate `_full_ns()` into `conftest.py::make_namespace()` | `test_dispatch.py` |
| 7 | Use shared `project_dir` fixture | `test_sink_map_columns.py` |
| 8 | Move `coverage_report.py` to `tools/` | `tests/cli/` |
| 9 | Remove `test_cleanup_strategy.py` (meta-tests of pytest) | 1 file |
| 10 | Merge single-test files into related files | 3 files |
| 11 | Fix misplaced tests in `TestExcludePatternMatching` | `test_server_group_validators.py` |

### P2 — Coverage Expansion (Priority Order)

| # | Module | Lines | Recommended Tests |
|---|--------|------:|-------------------|
| 1 | `manage-pipelines generate` | 978 | Unit tests for template substitution, field generation, per-table pipeline output |
| 2 | `manage-services resources` | ~500 | inspect, custom-tables, column-templates, transforms |
| 3 | `generate-usage-stats` | ~200 | Stats generation and output |
| 4 | `manage-migrations schema-docs` | ~200 | Schema doc generation |

---

## 9. Test Organization Summary

```
tests/                         # 63 unit-test files
├── conftest.py                # Shared fixtures: make_namespace, project_dir
├── test_*.py                  # Unit tests (pure Python, no subprocess)
│
└── cli/                       # 12 CLI integration files
    ├── conftest.py            # CLI fixtures: run_cdc (fish subprocess), run_cdc_completion
    ├── coverage_report.py     # ⚠ Not a test — utility script
    ├── test_manage_*.py       # E2E CLI tests (require fish + cdc installed)
    └── test_scaffold.py       # Scaffolding E2E tests
```

**Test execution:**
- `cdc test --all --cli` → 1,366 tests, ~79s (run inside dev container)

---

## Appendix: All Test Files by Module Area

| Area | Files | Tests |
|------|------:|------:|
| Column templates & operations | 4 | 78 |
| Service schema operations | 5 | 54 |
| CLI dispatch & routing | 4 | 93 |
| Source table management | 4 | 55 |
| Sink table & column mapping | 5 | 123 |
| Sink groups | 5 | 96 |
| Server groups | 4 | 94 |
| Type mapping & compatibility | 3 | 66 |
| Source ref resolution | 1 | 38 |
| Template validation | 3 | 85 |
| Transform rules & validation | 2 | 26 |
| Schema saving | 3 | 7 |
| Autocompletion | 5 | 50 |
| Fish completions | 1 | 92 |
| Configuration & migration | 3 | 8 |
| Scaffolding | 2 | 37 |
| Setup & infrastructure | 3 | 16 |
| CLI integration (subprocess) | 10 | 179 |
| Utilities & misc | 3 | 12 |
| **Total** | **75** | **1,366** |
