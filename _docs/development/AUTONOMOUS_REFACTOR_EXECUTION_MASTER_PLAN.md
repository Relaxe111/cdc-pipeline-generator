# Autonomous Refactor Execution Master Plan

**Date:** 2026-03-04  
**Scope:** `cdc-pipeline-generator` (generator repo only)  
**Mode:** Autonomous + iterative execution until agreed issues are resolved

---

## 1) Objective

Provide a single operational plan that allows continuous autonomous refactoring with:

1. Explicit backlog of agreed issues,
2. Strict execution loop (small slices + immediate validation),
3. Phase-level and overall Definition of Done,
4. Clear blocker policy (when to stop vs continue).

This plan supersedes ad-hoc execution and is intended to be followed end-to-end without extra prompts.

---

## 2) Agreed Issues to Resolve

Primary agreed issue: oversized/high-churn modules that violate size/responsibility standards and slow safe iteration.

Current hotspots (from latest size scan, files > 600 lines in `cdc_generator/`):

- `cdc_generator/cli/sink_group.py` (2599)
- `cdc_generator/cli/completions.py` (1908)
- `cdc_generator/cli/click_commands.py` (1511)
- `cdc_generator/cli/service_handlers_sink.py` (766)
- `cdc_generator/helpers/helpers_sink_groups.py` (998)
- `cdc_generator/cli/service.py` (958)
- `cdc_generator/validators/manage_service_schema/source_type_overrides.py` (918)
- `cdc_generator/validators/manage_service/db_inspector_common.py` (864)
- `cdc_generator/validators/manage_server_group/scaffolding/templates.py` (807)
- `cdc_generator/helpers/autocompletions/sinks.py` (797)
- `cdc_generator/validators/template_validator.py` (776)
- `cdc_generator/validators/manage_server_group/db_inspector.py` (735)
- `cdc_generator/core/migration_apply.py` (731)
- `cdc_generator/cli/click_commands.py` (1528)
- `cdc_generator/cli/completions.py` (1908)
- `cdc_generator/cli/sink_group.py` (2599)

Current method/compliance audit snapshot:

- Function-size violations (`>100` lines): `45` functions in `cdc_generator/`.
  - largest: `cli/source_group.py::main` (428), `cli/sink_group.py::main` (427), `cli/service.py::_build_parser` (282), `validators/manage_server_group/scaffolding/templates.py::get_docker_compose_template` (243).
- `type: ignore` occurrences in `cdc_generator/`: `49` total.
  - top offenders: `validators/manage_server_group/config.py` (15), `cli/smart_command.py` (4), `cli/sink_group.py` (4), `validators/manage_server_group/yaml_writer.py` (4).
- `Any` token occurrences in `cdc_generator/`: `773` total (broad typing-hardening backlog).

Secondary agreed issue: touched-path type/structure hardening and removal of avoidable compatibility debt in preprod scope.

---

## 3) Constraints and Invariants

- Keep behavior stable unless a phase explicitly deprecates legacy behavior.
- Preserve CLI entrypoints and command UX contracts during extraction.
- Prefer facade + extracted modules per responsibility.
- `__init__.py` policy: import/export-only modules; no function definitions in `__init__.py`.
- No broad suppression (`type: ignore`, broad `noqa`) in newly touched code.
- Do not refactor unrelated modules in the same slice.
- Run targeted tests after every cohesive slice.

Preprod policy in effect:

- Remove obsolete compatibility layers when touched, when safe and test-covered.

---

## 4) Autonomous Execution Loop (Mandatory)

For each slice, execute exactly this loop:

1. Pick the next highest-priority seam from this plan.
2. Extract one cohesive responsibility into a new/adjacent helper module.
3. Keep existing public API by delegating from facade module.
4. Run lint on touched files.
5. Run targeted tests for touched area.
6. If green: continue to next slice automatically.
7. If red: fix only regressions introduced by current slice; re-run.
8. Write a short changelog note (what changed, tests run, remaining risk).

Do not pause for confirmation between slices unless blocked by policy in Section 9.

---

## 5) Phase Backlog and Order

## Phase 1 — Complete `sink_operations` decomposition (completed)

Target file: `cdc_generator/validators/manage_service/sink_operations.py`

Sub-phases:

1. Extract listing/formatting (started).
2. Extract table add/update validation helpers.
3. Extract mapping context + mapping validation pipeline.
4. Extract sink validation/reporting helpers.
5. Keep `sink_operations.py` as thin facade/orchestrator.

Exit target:

- `sink_operations.py` <= 600 lines,
- behavior preserved,
- CLI sink tests green.

---

## Phase 2 — Convert `core/migration_generator/__init__.py` to import/export-only (completed)

Target file: `cdc_generator/core/migration_generator/__init__.py`

Sub-phases:

1. Move orchestration and wrappers out of `__init__.py` into dedicated modules.
2. Keep compatibility at package surface via re-export imports only.
3. Ensure no function bodies remain in `__init__.py`.

Exit target:

- `__init__.py` contains imports/exports only (no functions),
- package API stable,
- migration E2E and related tests green.

---

## Phase 3 — Decompose CLI mega-modules (in progress)

Targets:

- `cdc_generator/cli/sink_group.py`
- `cdc_generator/cli/completions.py`
- `cdc_generator/cli/click_commands.py`
- `cdc_generator/cli/service_handlers_sink_custom.py`

Sub-phases (per file):

1. Extract pure parsing/normalization helpers.
2. Extract output-formatting/help-text builders.
3. Extract command handlers by domain into submodules.
4. Preserve command registration surface in facade file.

Method hotspots (first split wave, >100 lines):

- `cli/source_group.py::main` (428)
- `cli/sink_group.py::main` (427)
- `cli/service.py::_build_parser` (282)
- `cli/click_commands.py::manage_services_resources_cmd` (110)
- `cli/completions.py::complete_map_column` (103)

Exit target:

- each facade <= 600 lines,
- largest per-file methods reduced to <= 100 lines in touched files,
- command UX unchanged,
- fish completion tests + CLI tests green.

---

## Phase 4 — Final hardening and cleanup

Sub-phases:

1. Remove temporary wrappers introduced only for transitional moves.
2. Normalize imports and docstrings in new modules.
3. Run broader impacted test matrix.
4. Update docs with final module map.

Exit target:

- no new lint/type debt,
- tests green,
- docs reflect final structure.

---

## Phase 5 — Type-safety debt reduction (`type: ignore` + high-`Any` paths)

Targets:

- `cdc_generator/validators/manage_server_group/config.py`
- `cdc_generator/cli/smart_command.py`
- `cdc_generator/validators/manage_server_group/yaml_writer.py`
- `cdc_generator/helpers/helpers_completions.py`

Sub-phases:

1. Replace non-import `type: ignore` usages with explicit casts/runtime guards.
2. Migrate YAML reads to typed wrappers where possible.
3. Reduce `Any`-heavy signatures to TypedDict/dataclass contracts in touched modules.

Exit target:

- non-import `type: ignore` occurrences reduced substantially in targeted modules,
- touched paths lint/type clean,
- behavior unchanged.

---

## 6) Test Matrix by Phase

Run minimally required tests after each slice:

- Sink operations / manage-service changes:
  - `tests/cli/test_manage_services_config.py`
- Pipeline generation related changes:
  - `tests/test_pipeline_generation.py`
- Migration generation changes:
  - `tests/test_migration_e2e.py`
- Completions / CLI wiring changes:
  - `tests/test_fish_completions.py`

When a slice touches multiple domains, run the union of affected suites.

---

## 7) Slice Size Rules

Per slice:

- Move 1 logical seam only.
- Prefer 1–3 files edited, plus at most 1–2 new helper modules.
- Keep patch reviewable and reversible.
- Avoid broad renames unless unavoidable.

---

## 8) Progress and Changelog Contract

After each successful slice, append/update a short status summary with:

1. `Changed`: modules extracted or rewired,
2. `Validated`: lint + tests run,
3. `Delta`: key line-count improvement,
4. `Next`: exact next seam.

Status model:

- `pending` / `in-progress` / `completed` / `blocked`.

---

## 9) Blocker Policy (Only Reasons to Stop)

Stop autonomous execution only if:

1. Required runtime dependency is missing and cannot be installed in current environment,
2. Merge/rebase conflict makes safe continuation ambiguous,
3. Required behavior decision conflicts with accepted architecture policy and cannot be inferred.

Not blockers:

- normal lint failures,
- failing tests caused by current slice,
- import-order/style issues,
- manageable typing errors in touched files.

These must be fixed and execution continues.

---

## 10) Overall Definition of Done

This plan is complete when all are true:

1. All agreed hotspot files are reduced below 600 lines (or formally split with thin facades),
2. No regression in impacted CLI/generation behavior,
3. Touched paths are lint-clean and type-safe without suppression debt,
4. Iterative changelog exists for each phase,
5. Final test matrix for impacted suites passes,
6. All touched `__init__.py` modules follow import/export-only rule.

---

## 11) Immediate Next Action

Continue with **Phase 3**, next seam:

- Continue on `cdc_generator/cli/service_handlers_sink_custom.py` (smallest-risk CLI hotspot),
- extract custom-table mutation/list helpers next,
- run `ruff` + CLI/completion impacted tests,
- publish short slice changelog and continue automatically.

---

## 12) Execution Log

### 2026-03-04 — Iteration 1.1 (Phase 1 ongoing)

Changed:

- Extracted mapping pipeline internals from `sink_operations.py` to:
  - `cdc_generator/validators/manage_service/sink_mapping.py`
    - context resolution,
    - schema loading,
    - mapping validation,
    - mapping apply + required-column warning.
- Extracted sink validation rule helper to:
  - `cdc_generator/validators/manage_service/sink_validation.py`
- Delegated sink list rendering to existing helper modules:
  - `cdc_generator/validators/manage_service/sink_listing.py`
  - `cdc_generator/validators/manage_service/sink_display.py`

Validated:

- `ruff check` on all touched sink modules: pass.
- `pytest` impacted suites: `31 passed, 30 skipped`.

Delta:

- `cdc_generator/validators/manage_service/sink_operations.py`
  - `2749 -> 2249` lines in this iteration.

Next:

- Continue Phase 1 by extracting add/update schema compatibility + table mutation helpers from `sink_operations.py` into dedicated modules, preserving facade API.

### 2026-03-04 — Iteration 1.2 (Phase 1 ongoing)

Changed:

- Extracted add-table compatibility pipeline from sink facade into:
  - `cdc_generator/validators/manage_service/sink_add_table_compatibility.py`
- Extracted top-level sink add/remove operations into:
  - `cdc_generator/validators/manage_service/sink_service_ops.py`
- Kept `sink_operations.py` as a thinner facade delegating to extracted modules.

Validated:

- `ruff check` on touched sink modules: pass.
- `pytest` impacted suites: `31 passed, 30 skipped`.

Delta:

- `cdc_generator/validators/manage_service/sink_operations.py`
  - `1324 -> 871` lines in this iteration.

Next:

- Continue Phase 1 by extracting table mutation/file-write helpers (`_validate_table_add`, `_save_custom_table_structure`, `_remove_custom_table_file`, schema-update validators) into a dedicated mutation module.

### 2026-03-04 — Iteration 1.3 (Phase 1 ongoing)

Changed:

- Extracted table mutation helpers from sink facade into:
  - `cdc_generator/validators/manage_service/sink_table_mutations.py`
    - table-add input validation,
    - custom-table reference write/remove,
    - table remove,
    - schema-update validation + update flow.
- Extracted type-compatibility runtime logic into:
  - `cdc_generator/validators/manage_service/sink_type_compatibility.py`
- Extracted list/validate flows into:
  - `cdc_generator/validators/manage_service/sink_list_validation.py`
- Kept `sink_operations.py` as delegating facade for public API.

Validated:

- `ruff check` on touched sink modules: pass.
- `pytest` impacted suites: `31 passed, 30 skipped`.

Delta:

- `cdc_generator/validators/manage_service/sink_operations.py`
  - `871 -> 615` lines in this iteration.

Next:

- Continue Phase 1 with a final seam: extract `add_sink_table` orchestration into a dedicated helper/orchestrator module to bring facade below `<= 600` while preserving public API and tests.

### 2026-03-04 — Iteration 1.4 (Phase 1 checkpoint)

Changed:

- Finalized sink facade reduction while preserving test monkeypatch compatibility points.
- Kept `add_sink_table` orchestration in `sink_operations.py` (facade module) but retained extracted helpers for:
  - table mutations (`sink_table_mutations.py`),
  - type compatibility (`sink_type_compatibility.py`),
  - list/validate flows (`sink_list_validation.py`).
- Added callback injection points in extracted helpers where needed to keep facade-level monkeypatch behavior stable.
- Updated add-table compatibility logic to align with sink-from behavior:
  - defaulted sink columns are not treated as required,
  - implicit permissive UUID/numeric-to-text identity bypass removed.

Validated:

- `ruff check` on all touched sink modules: pass.
- `pytest` impacted suites (expanded): `57 passed, 30 skipped`.

Delta:

- `cdc_generator/validators/manage_service/sink_operations.py`
  - `615 -> 573` lines in this iteration.
  - Phase 1 line-size exit target (`<= 600`) met.

Next:

- Continue with next agreed hotspot from backlog (Phase 2 or Phase 3), starting with smallest-risk seam extraction and same lint+targeted-test loop.

### 2026-03-04 — Iteration 2.1 (Phase 2 intermediate)

Changed:

- Replaced the monolithic `core/migration_generator/__init__.py` implementation with a thin facade that delegates to extracted modules (`columns`, `service_parsing`, `table_processing`, `rendering`, `manual_migrations`, `file_writers`).
- Preserved package-level compatibility exports and compatibility patch-points used by existing tests:
  - `build_columns_from_table_def`, `build_full_column_list`, `load_table_definitions` wrappers,
  - module-level symbols `resolve_column_templates`, `resolve_transforms`, `get_service_schema_read_dirs` remain patchable from the facade namespace.
- Kept public orchestration flow (`generate_migrations`, per-sink generation) behaviorally stable while removing duplicated local helper implementations.

Validated:

- `ruff check cdc_generator/core/migration_generator/__init__.py`: pass.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_migration_generator.py -q`: `53 passed`.

Delta:

- `cdc_generator/core/migration_generator/__init__.py`
  - `1498 -> 415` lines in this iteration.
  - Line-size target was met, but this checkpoint is now superseded by the stricter import/export-only `__init__.py` policy.

Next:

- Continue Phase 2 until `core/migration_generator/__init__.py` has no function definitions and keeps stable exports.

### 2026-03-04 — Iteration 2.2 (Phase 2 checkpoint)

Changed:

- Moved all executable logic out of `core/migration_generator/__init__.py` into:
  - `cdc_generator/core/migration_generator/runtime.py`
- Converted `core/migration_generator/__init__.py` to import/export-only policy:
  - no function definitions,
  - stable re-exports for public API,
  - kept package-level monkeypatch targets (`resolve_column_templates`, `resolve_transforms`, `get_project_root`, `load_service_config`, `get_service_schema_read_dirs`, `load_yaml_file`) available.
- Preserved compatibility behavior by resolving patchable symbols through package namespace inside runtime entrypoints.

Validated:

- `ruff check cdc_generator/core/migration_generator/__init__.py cdc_generator/core/migration_generator/runtime.py`: pass.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_migration_generator.py -q`: `53 passed`.
- `grep '^def\\s' cdc_generator/core/migration_generator/__init__.py`: no matches.

Delta:

- `cdc_generator/core/migration_generator/__init__.py`
  - `415 -> 81` lines in this iteration.
- `cdc_generator/core/migration_generator/runtime.py`
  - new runtime implementation module (`379` lines).
- Phase 2 exit target met under strict policy (`__init__.py` import/export-only, no functions).

Next:

- Proceed to Phase 3 hotspot decomposition (`cli/*` mega-modules), starting with smallest-risk seam extraction.

### 2026-03-04 — Iteration 3.1 (Phase 3 in progress)

Changed:

- Extracted pure custom-table parsing/normalization helpers from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_parsing.py`
- Rewired handler module to delegate to extracted helpers while preserving existing function names used by tests/importers.
- Tightened typing in touched handler paths to satisfy strict Pylance checks in modified file.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_parsing.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `1030 -> 843` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_parsing.py`
  - new helper module (`208` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting schema-file IO/update helpers (`_load_columns_map_from_schema`, `_update_schema_file_add_column`, `_update_schema_file_remove_column`) into a dedicated module.

### 2026-03-04 — Iteration 3.2 (Phase 3 in progress)

Changed:

- Extracted schema-file IO/update helpers from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_schema_files.py`
- Kept facade compatibility in handler module by preserving existing internal function names as delegating wrappers:
  - `_load_columns_map_from_schema`
  - `_update_schema_file_add_column`
  - `_update_schema_file_remove_column`
- Preserved test monkeypatch behavior by injecting handler-level `SERVICE_SCHEMAS_DIR` and `yaml` into extracted helper calls.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_schema_files.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `843 -> 760` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_schema_files.py`
  - new helper module (`148` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting output-formatting/help-text builders to further reduce facade size while preserving CLI behavior.

### 2026-03-04 — Iteration 3.3 (Phase 3 in progress)

Changed:

- Extracted output/help-text builders from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_output.py`
- Rewired facade module to consume pure message-builder helpers for:
  - existing-table guidance,
  - source table/schema-not-found guidance,
  - custom-table modify restrictions,
  - custom-table creation success output,
  - missing-column guidance.
- Preserved behavior by keeping all printing in the facade and extracting only message construction.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_output.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `760 -> 779` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_output.py`
  - new helper module (`111` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting source-table load/validation helpers (`_load_columns_from_source_table` + related guidance) into a dedicated module while preserving CLI behavior.

### 2026-03-04 — Iteration 3.4 (Phase 3 in progress)

Changed:

- Extracted source-table load/validation flow from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_source_loader.py`
- Kept facade compatibility by preserving `_load_columns_from_source_table` in the handler as a delegating wrapper.
- Added dependency injection container (`SourceLoaderDeps`) to keep extracted module testable and avoid lint violations from excessive function arguments.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_source_loader.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `779 -> 721` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_source_loader.py`
  - new helper module (`108` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting sink/table config navigation helpers (`_get_sinks_dict`, `_get_sink_tables`, `_resolve_sink_config`, `_extract_target_service`) into a dedicated module while preserving facade behavior.

### 2026-03-04 — Iteration 3.5 (Phase 3 in progress)

Changed:

- Extracted sink/table config navigation helpers from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_config_nav.py`
- Kept facade compatibility by preserving existing internal helper names as delegating wrappers:
  - `_get_sinks_dict`
  - `_get_sink_tables`
  - `_resolve_sink_config`
  - `_extract_target_service`
- Preserved user-facing behavior by keeping validation/error messaging in facade wrappers and extracting pure navigation logic only.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_config_nav.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `721 -> 723` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_config_nav.py`
  - new helper module (`45` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting custom-table mutation/list helpers (`_load_custom_table`, `_get_tables_dict_from_config`, `list_custom_table_columns`, `list_custom_tables_for_sink`) into a dedicated module while preserving facade behavior.

### 2026-03-04 — Iteration 3.6 (Phase 3 in progress)

Changed:

- Extracted custom-table mutation/list helpers from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_table_access.py`
- Kept facade compatibility by preserving existing function names as delegates:
  - `_load_custom_table`
  - `list_custom_table_columns`
  - `list_custom_tables_for_sink`
- Stabilized dependency initialization order by creating table-access dependencies lazily.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_table_access.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `723 -> 632` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_table_access.py`
  - new helper module (`182` lines).

Next:

- Continue Phase 3 on `service_handlers_sink_custom.py` by extracting custom-table column mutation handlers (`add_column_to_custom_table`, `remove_column_from_custom_table`) into a dedicated module to bring facade under `<= 600`.

### 2026-03-04 — Iteration 3.7 (Phase 3 in progress)

Changed:

- Extracted custom-table column mutation handlers from:
  - `cdc_generator/cli/service_handlers_sink_custom.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_custom_column_mutations.py`
- Kept facade compatibility by preserving public handler function names as delegates:
  - `add_column_to_custom_table`
  - `remove_column_from_custom_table`
- Introduced lazy dependency builder for column-mutation flow to preserve wiring stability and behavior.

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink_custom.py cdc_generator/cli/service_handlers_sink_custom_column_mutations.py`: pass.
- `get_errors` on both touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_custom_table_parsing.py tests/test_sink_handlers.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink_custom.py`
  - `632 -> 592` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_custom_column_mutations.py`
  - new helper module (`122` lines).
- Phase 3 local target met for this facade (`<= 600`).

Next:

- Continue Phase 3 with next CLI hotspot: extract helpers from `cdc_generator/cli/service_handlers_sink.py` (`766`) to reduce facade size while preserving CLI behavior.

### 2026-03-04 — Iteration 3.8 (Phase 3 in progress)

Changed:

- Extracted add-table helper cluster from:
  - `cdc_generator/cli/service_handlers_sink.py`
  into:
  - `cdc_generator/cli/service_handlers_sink_add_table_helpers.py`
- Preserved facade-level compatibility for patch-sensitive flows by keeping these functions in facade and delegating only stable helper logic:
  - `_apply_transform_after_add`
  - `_handle_from_all_add_table`
- Restored expected module patch-points used by tests (`add_transform_to_table`, `remove_sink_table`).

Validated:

- `ruff check cdc_generator/cli/service_handlers_sink.py cdc_generator/cli/service_handlers_sink_add_table_helpers.py`: pass.
- `get_errors` on touched sink/custom handler files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_sink_handlers.py tests/test_custom_table_parsing.py -q`: `77 passed`.

Delta:

- `cdc_generator/cli/service_handlers_sink.py`
  - `766 -> 529` lines in this iteration.
- `cdc_generator/cli/service_handlers_sink_add_table_helpers.py`
  - new helper module (`434` lines).
- `cdc_generator/cli/service_handlers_sink_custom.py`
  - remains under threshold (`596` lines).

Next:

- Continue Phase 3 with next smallest CLI hotspot: reduce `cdc_generator/cli/commands.py` (`619`) below `<= 600` with a minimal helper extraction.

### 2026-03-04 — Iteration 3.9 (Phase 3 in progress)

Changed:

- Extracted CLI help rendering from:
  - `cdc_generator/cli/commands.py`
  into:
  - `cdc_generator/cli/commands_help.py`
- Kept facade-level `print_help` API as a delegating wrapper.

Validated:

- `ruff check cdc_generator/cli/commands.py cdc_generator/cli/commands_help.py`: pass.
- `get_errors` on touched commands files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_fish_completions.py tests/test_pipeline_generation.py tests/test_sink_handlers.py -q`: pass.

Delta:

- `cdc_generator/cli/commands.py`
  - `618 -> 578` lines in this iteration.
- `cdc_generator/cli/commands_help.py`
  - new helper module.

Next:

- Continue Phase 3 with `cdc_generator/cli/source_group.py` extraction to bring facade under `<= 600`.

### 2026-03-04 — Iteration 3.10 (Phase 3 in progress)

Changed:

- Extracted special action handlers from:
  - `cdc_generator/cli/source_group.py`
  into:
  - `cdc_generator/cli/source_group_special_handlers.py`
- Kept facade compatibility by preserving function names as delegating wrappers:
  - `_handle_introspect_types`
  - `_handle_db_definitions`
  - `_handle_add_source_custom_key`

Validated:

- `ruff check cdc_generator/cli/source_group.py cdc_generator/cli/source_group_special_handlers.py`: pass.
- `get_errors` on touched source-group files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_server_group_dispatch.py tests/test_fish_completions.py -q`: `116 passed`.

Delta:

- `cdc_generator/cli/source_group.py`
  - `724 -> 539` lines in this iteration.
- `cdc_generator/cli/source_group_special_handlers.py`
  - new helper module (`213` lines).

Next:

- Continue Phase 3 with next remaining mega-hotspot: `cdc_generator/cli/click_commands.py` seam extraction.

### 2026-03-04 — Iteration 3.11 (Phase 3 in progress)

Changed:

- Reduced passthrough duplication in:
  - `cdc_generator/cli/click_commands.py`
- Added shared dispatch helpers for repeated command passthrough patterns:
  - `_dispatch_grouped_passthrough`
  - `_dispatch_command_passthrough`
- Rewired repeated manage-pipelines/manage-migrations/test passthrough handlers to use these helpers.

Validated:

- `ruff check cdc_generator/cli/click_commands.py --select I001`: pass.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_server_group_dispatch.py tests/test_sink_handlers.py tests/test_fish_completions.py tests/test_pipeline_generation.py -q`: `179 passed`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1527 -> 1511` lines in this iteration.

Notes:

- `click_commands.py` has pre-existing Ruff complexity findings (`PLR0911`, `PLR0913`) in `manage_services_resources_cmd`; these were not introduced by this iteration and remain as next refactor target.

Next:

- Continue Phase 3 by decomposing `manage_services_resources_cmd` from `cdc_generator/cli/click_commands.py` into dedicated helper(s) to reduce complexity and unblock full-file Ruff checks.

### 2026-03-04 — Iteration 3.12 (Phase 3 in progress)

Changed:

- Extracted resources runtime orchestration from:
  - `cdc_generator/cli/click_commands.py`
  into:
  - `cdc_generator/cli/click_commands_resources.py`
- Kept CLI declaration surface in facade and delegated runtime path via:
  - `execute_manage_services_resources(ctx, options_from_kwargs(kwargs))`
- Completed style/typing cleanup for extracted resources helper and removed dead return in facade handler.

Validated:

- `ruff check cdc_generator/cli/click_commands.py cdc_generator/cli/click_commands_resources.py`: pass.
- `get_errors` on touched click files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/test_fish_completions.py tests/test_pipeline_generation.py tests/test_sink_handlers.py tests/test_server_group_dispatch.py tests/test_custom_table_parsing.py -q`: `196 passed`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1511 -> 1413` lines in this iteration.
- `cdc_generator/cli/click_commands_resources.py`
  - new helper module (`177` lines).

Next:

- Continue Phase 3 with next `click_commands.py` runtime seam (`source-overrides` command cluster).

### 2026-03-04 — Iteration 3.13 (Phase 3 in progress)

Changed:

- Extracted source-overrides runtime logic from:
  - `cdc_generator/cli/click_commands.py`
  into:
  - `cdc_generator/cli/click_commands_source_overrides.py`
- Rewired these command handlers to delegate runtime behavior while preserving Click command declarations:
  - `manage_services_source_overrides_cmd`
  - `manage_services_source_overrides_set_cmd`
  - `manage_services_source_overrides_remove_cmd`
  - `manage_services_source_overrides_list_cmd`
- Cleaned extracted helper import/type issues and preserved command UX/error messaging.
- Resolved diagnostics debt in sink facade extraction by exporting/importing public helper aliases:
  - `cdc_generator/cli/service_handlers_sink_add_table_helpers.py`
  - `cdc_generator/cli/service_handlers_sink.py`

Validated:

- `ruff check cdc_generator/cli/click_commands.py cdc_generator/cli/click_commands_resources.py cdc_generator/cli/click_commands_source_overrides.py cdc_generator/cli/service_handlers_sink.py cdc_generator/cli/service_handlers_sink_add_table_helpers.py`: pass.
- `get_errors` on touched click/sink files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/cli/test_manage_services_schema.py tests/test_fish_completions.py tests/test_sink_handlers.py tests/test_server_group_dispatch.py tests/test_pipeline_generation.py tests/test_custom_table_parsing.py -q`: `206 passed, 14 skipped`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1413 -> 1354` lines in this iteration.
- `cdc_generator/cli/click_commands_source_overrides.py`
  - new helper module (`146` lines).

Next:

- Continue Phase 3 with next `click_commands.py` extraction seam (`manage_services_schema_custom_tables_cmd` + `manage_services_schema_transforms_cmd`) to keep reducing facade size and complexity.

### 2026-03-04 — Iteration 3.14 (Phase 3 in progress)

Changed:

- Extracted schema resource command runtime logic from:
  - `cdc_generator/cli/click_commands.py`
  into:
  - `cdc_generator/cli/click_commands_schema_resources.py`
- Delegated these facade handlers to extracted helper functions while preserving Click option declarations:
  - `manage_services_schema_custom_tables_cmd`
  - `manage_services_schema_transforms_cmd`

Validated:

- `ruff check cdc_generator/cli/click_commands.py cdc_generator/cli/click_commands_schema_resources.py`: pass.
- `get_errors` on touched click files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/cli/test_manage_services_schema.py tests/test_fish_completions.py tests/test_pipeline_generation.py -q`: `111 passed, 14 skipped`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1354 -> 1324` lines in this iteration.
- `cdc_generator/cli/click_commands_schema_resources.py`
  - new helper module (`74` lines).

Next:

- Continue Phase 3 by extracting the next cohesive runtime block from `cdc_generator/cli/click_commands.py` (column-templates / resources-inspect cluster).

### 2026-03-04 — Iteration 3.15 (Phase 3 in progress)

Changed:

- Extracted resources inspect runtime logic from:
  - `cdc_generator/cli/click_commands.py`
  into:
  - `cdc_generator/cli/click_commands_resources_inspect.py`
- Delegated `manage_services_resources_inspect_cmd` to helper executor and removed now-unused facade imports.

Validated:

- `ruff check cdc_generator/cli/click_commands.py cdc_generator/cli/click_commands_resources_inspect.py`: pass.
- `get_errors` on touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/cli/test_manage_services_schema.py tests/test_fish_completions.py tests/test_pipeline_generation.py tests/test_server_group_dispatch.py -q`: `129 passed, 14 skipped`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1324 -> 1300` lines in this iteration.
- `cdc_generator/cli/click_commands_resources_inspect.py`
  - new helper module (`56` lines).

Next:

- Continue Phase 3 with the next `click_commands.py` extraction seam (`manage_column_templates_cmd` runtime block) and continue toward facade-size convergence.

### 2026-03-04 — Iteration 3.16 (Phase 3 in progress)

Changed:

- Extracted column-templates runtime orchestration from:
  - `cdc_generator/cli/click_commands.py`
  into:
  - `cdc_generator/cli/click_commands_column_templates.py`
- Delegated `manage_column_templates_cmd` to helper executor and kept Click option declarations in facade.

Validated:

- `ruff check cdc_generator/cli/click_commands.py cdc_generator/cli/click_commands_column_templates.py`: pass.
- `get_errors` on touched files: no errors.
- `/Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator/.venv/bin/python -m pytest tests/cli/test_manage_services_schema.py tests/test_fish_completions.py tests/test_pipeline_generation.py tests/test_server_group_dispatch.py -q`: `129 passed, 14 skipped`.

Delta:

- `cdc_generator/cli/click_commands.py`
  - `1300 -> 1279` lines in this iteration.
- `cdc_generator/cli/click_commands_column_templates.py`
  - new helper module (`34` lines).

Next:

- Continue Phase 3 by extracting the next coherent runtime cluster from `cdc_generator/cli/click_commands.py` (remaining grouped command adapters) and proceed until facade-size target is reached.
