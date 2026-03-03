# Pipeline Management Rollout Summary (Completed)

**Status:** Completed  
**Original Plan Date:** 2025-07-01  
**Closure Date:** 2026-03-02  
**Scope:** `cdc manage-pipelines` evolution + Bento migration alignment across generator and implementation docs

---

## 1) Outcome

The planned rollout is complete. The original implementation checklist has been fully delivered and this document now serves as a closure record (not an active plan).

### Delivered capabilities

- Canonical pipeline layout under `pipelines/templates/` and `pipelines/generated/`
- `manage-pipelines` command family delivered and wired
  - `generate`, `list`, `verify`, `diff`, `health`, `prune`
- Stub command cleanup completed (preprod cleanup policy)
- Bento migration completed for source/sink template runtime usage
- Documentation and reference cleanup completed for legacy pipeline path naming

---

## 2) Final State

### Canonical structure

```text
pipelines/
├── templates/
│   ├── source-pipeline.yaml
│   └── sink-pipeline.yaml
└── generated/
    ├── sources/{env}/{customer}/source-pipeline.yaml
    └── sinks/{env}/sink-pipeline.yaml
```

### Runtime posture

- Bento is the active runtime target for generated pipeline templates.
- No dual-runtime management layer is maintained in this rollout.

---

## 3) Validation Evidence

### Functional verification

- Pipeline generation and verification flows executed successfully in workspace context.
- Focused test suite passed:
  - `tests/test_fish_completions.py`
  - `tests/test_pipeline_generation.py`

### Bloblang compatibility audit

- Full audit executed against all `.blobl` files using Bento lint in containerized mode.
- Result: **17 passed, 0 failed**.

### Documentation cleanup verification

- Legacy doc patterns were removed from active markdown content for:
  - `Redpanda Connect` / `redpanda-connect`
  - `pipeline-templates/`
  - `generated/pipelines`

---

## 4) Decisions Snapshot

The rollout followed the previously locked decisions:

- Pattern-driven behavior only (`db-per-tenant` / `db-shared`), no service-name branching
- Preprod cleanup policy applied (remove legacy/stub paths instead of compatibility shims)
- `verify` consolidated with mode flags (`--full`, `--sink`)
- Operational commands (`diff`, `health`, `prune`) included as mandatory scope

---

## 5) Reference Documents

- `_docs/architecture/BENTO_MIGRATION_DECISION_PLAN.md`
- `_docs/bento-bloblang/README.md`
- `cdc_generator/cli/commands.py`
- `cdc_generator/cli/click_commands.py`
- `cdc_generator/core/pipeline_generator.py`
- `tests/test_pipeline_generation.py`

---

## 6) Lifecycle

This file is intentionally retained as a concise completion record.

- Do not reopen this as an active checklist.
- For future changes, create a new scoped plan document with a fresh status/date.
