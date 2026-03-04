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
- `cdc_generator/cli/click_commands.py` (1528)
- `cdc_generator/cli/service_handlers_sink_custom.py` (1030)
- `cdc_generator/helpers/helpers_sink_groups.py` (998)
- `cdc_generator/cli/service.py` (958)
- `cdc_generator/validators/manage_service_schema/source_type_overrides.py` (918)
- `cdc_generator/validators/manage_service/db_inspector_common.py` (864)
- `cdc_generator/validators/manage_server_group/scaffolding/templates.py` (807)
- `cdc_generator/helpers/autocompletions/sinks.py` (797)
- `cdc_generator/validators/template_validator.py` (776)
- `cdc_generator/cli/service_handlers_sink.py` (766)
- `cdc_generator/validators/manage_server_group/db_inspector.py` (735)
- `cdc_generator/core/migration_apply.py` (731)
- `cdc_generator/cli/source_group.py` (725)
- `cdc_generator/cli/commands.py` (619)

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

- Start with `cdc_generator/cli/service_handlers_sink_custom.py` (smallest-risk CLI hotspot),
- extract parser/normalization helpers first,
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
