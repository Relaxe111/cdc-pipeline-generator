# Sink Env-Aware Routing + Unique Template Validation — Implementation Report

**Date:** 2026-03-04  
**Workspace:** `cdc-pipeline-generator` (primary), with schema parity updates in `adopus-cdc-pipeline`  
**Status:** ✅ Implemented and validated

---

## 1) What was implemented

This work implements the full plan for:

1. **Env-aware sink routing via `target_sink_env`**
2. **`column_templates.unique` support and preflight validation**
3. **Fail-fast vs warning-only routing preflight policy**
4. **Generator resilience: skip invalid routes/customers but continue valid generation**
5. **Transform execution-stage behavior preservation (`source` / `sink`)**
6. **Tests + docs/examples updates**

---

## 2) Final behavior contract (AI-friendly reference)

### 2.1 `target_sink_env` routing

- `target_sink_env` is optional in source env entries.
- It is used when:
  - source group is **not** environment-aware, and
  - sink group **is** environment-aware.
- If provided, it has strict precedence and must match an existing sink env key.

### 2.2 Preflight policy in `manage-services config`

- **Error (fast-fail):** sink topology is available and required `target_sink_env` is missing/invalid.
- **Warning-only:** sink topology is unavailable/not initialized (bootstrap-safe).

### 2.3 `column_templates.unique`

- New template flag: `unique: true`.
- Validation scope:
  - **Per sink env** when sink is environment-aware.
  - **Per source-group** when sink is not environment-aware.
- Invalid values:
  - resolved empty/null/none are rejected.
- Collision errors include:
  - template key,
  - value,
  - scope,
  - conflicting routes.

### 2.4 Generation safety net

- Consolidated sink generation now skips invalid customer routing or unresolved route-level enrichment errors.
- Valid customers/routes still generate.
- Skip counts are summarized (`skipped_customers`, `skipped_routes`).

### 2.5 Transform stage compatibility

- `execution_stage` support maintained:
  - `source` transforms execute at source stage.
  - `sink` transforms execute at sink stage.
- Default remains `source` if unspecified.

---

## 3) Main code changes

## 3.1 New shared routing module

- `cdc_generator/core/sink_env_routing.py` (new)
  - `list_sink_source_env_keys(...)`
  - `resolve_sink_env_key(..., target_sink_env=None)`
  - `get_sink_target_env_keys(project_root, sink_key)`

## 3.2 Generator integration

- `cdc_generator/core/pipeline_generator.py`
  - Uses shared sink env resolver.
  - `resolve_postgres_url_from_sink_groups(..., target_sink_env=None)`
  - `_resolve_consolidated_postgres_url(...)` passes customer env `target_sink_env`.
  - Consolidated sink generation now skips invalid customers/routes and prints summaries.
  - Source/sink transform stage behavior aligned.

## 3.3 Service config derivation

- `cdc_generator/helpers/service_config.py`
  - Derived customer environments now propagate `target_sink_env` from source-group env config.

## 3.4 Manage-service preflight validation

- `cdc_generator/validators/manage_service/validation.py`
  - Added `validate_service_sink_preflight(service, config=None)`.
  - Added routing preflight collector.
  - Added unique-template collision/empty-value collector.
  - Integrated preflight into `validate_service_config(...)`.

## 3.5 Sink mutation flows enforce preflight

- `cdc_generator/validators/manage_service/sink_operations.py`
  - `add_sink_to_service(...)` and `add_sink_table(...)` now run preflight before save.
  - Roll back staged mutation on preflight errors.
- `cdc_generator/validators/manage_service/sink_template_ops.py`
  - `add_column_template_to_table(...)` runs preflight and rolls back on errors.

## 3.6 Template model support

- `cdc_generator/core/column_templates.py`
  - `ColumnTemplate` now includes `unique: bool = False`.
  - Parser reads and validates `unique`.

## 3.7 Type/schema support

- `cdc_generator/core/sink_types.py`
  - `SinkSourceEnvironment` includes optional `target_sink_env`.
- `cdc_generator/validators/manage_server_group/types.py`
  - `DatabaseEntry` includes optional `target_sink_env`.
- JSON schemas updated in both repos:
  - `.vscode/schemas/source-groups.schema.json` (`target_sink_env`)
  - `.vscode/schemas/column-templates.schema.json` (`unique`, related fields)

---

## 4) Tests added/updated

### 4.1 Routing preflight tests

- `tests/cli/test_manage_services_config.py`
  - Fails when `target_sink_env` is missing while required.
  - Warning-only when sink topology is unavailable.

### 4.2 Unique + transform stage tests

- `tests/test_column_template_operations.py`
  - Default transform stage is `source`.
  - Explicit `sink` stage is preserved.
  - Unique collision fails in same sink-env scope.
  - Same value across different sink envs is allowed.

### 4.3 Template parsing test

- `tests/test_column_templates.py`
  - Parses `unique: true` correctly.

### 4.4 Generator route-skip test

- `tests/test_pipeline_generation.py`
  - Consolidated sink generation skips invalid `target_sink_env` route and avoids emitting sink output when no valid routes remain.

---

## 5) Verification executed

Executed commands (requested in plan):

```bash
pytest tests/cli/test_manage_services_config.py tests/test_column_template_operations.py tests/test_pipeline_generation.py -q
pytest tests/test_source_ref_resolver.py -q
```

Observed results:

- `59 passed, 30 skipped`
- `40 passed`

---

## 6) Docs/examples updated

- `cdc_generator/templates/init/service-schemas/column-templates.yaml`
  - Added `unique` semantics and example usage.
- `_docs/cli/SOURCE_CUSTOM_KEYS.md`
  - Added `target_sink_env` example.
  - Added preflight fail-fast/warning-only behavior notes.
  - Added `unique` scope/collision behavior notes.

---

## 7) Notes for future AI/dev work

Use this checklist before changing related logic:

1. Keep sink-env resolution in `core/sink_env_routing.py` as single source of truth.
2. Keep `manage-services config` preflight semantics:
   - strict when topology exists,
   - warning-only during bootstrap.
3. Preserve `unique` scope rules exactly (sink env-aware vs non-env-aware).
4. In generation, prefer partial progress over global failure (skip invalid routes/customers).
5. When touching transforms, preserve execution stage contract (`source` default, `sink` explicit).
6. Maintain schema parity in both repos when config keys are expanded.

---

## 8) Suggested next maintenance tasks (optional)

- Add a focused unit test file for `core/sink_env_routing.py` helper behavior matrix.
- Add one integration test asserting skip-summary counts in generator output text.
- If desired, split preflight collectors into dedicated module(s) for easier extension.

---

## 9) Canonical reference path

This report is intentionally stable for future prompts and AI context reuse:

- `_docs/development/SINK_ENV_AWARE_UNIQUE_ROUTING_IMPLEMENTATION_REPORT.md`
