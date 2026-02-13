# Gap Analysis: Command Grouping for Pipelines and Migrations

## Goal

Provide full operational management through two canonical command groups:

- `cdc manage-pipelines ...`
- `cdc manage-migrations ...`

This document records:
1. Where existing commands were grouped
2. What command gaps still exist
3. Test strategy scope (including `--fast-pipelines` / `--full-pipelines`)

---

## 1) Canonical command grouping (implemented)

### `manage-pipelines`

- `generate`
- `reload`
- `verify`
- `verify-sync`
- `stress-test`

### `manage-migrations`

- `enable-cdc`
- `apply-replica`
- `clean-cdc`
- `schema-docs`

### Legacy commands removed

The following flat top-level commands were removed in favor of canonical grouping:

- `generate`
- `reload-pipelines`
- `verify`
- `verify-sync`
- `stress-test`
- `enable`
- `migrate-replica`
- `clean`
- `schema-docs`

---

## 2) Gap analysis (missing commands for full management)

## P0 (must-have)

### Pipelines
- `manage-pipelines validate`  
  Validate generated pipeline artifacts (syntax/loadability) right after generation.
- `manage-pipelines list`  
  List generated pipeline units per service/customer/environment.

### Migrations
- `manage-migrations status`  
  Current migration state per target DB.
- `manage-migrations plan`  
  Dry-run summary of what would be applied.

## P1 (high value)

### Pipelines
- `manage-pipelines diff`  
  Show config-to-generated deltas.
- `manage-pipelines health`  
  Runtime checks for connect/mq/pipeline endpoints.

### Migrations
- `manage-migrations history`  
  Applied migration history with ordering/checksums.
- `manage-migrations drift-check`  
  Detect drift between expected and actual schema state.

## P2 (nice-to-have)

### Pipelines
- `manage-pipelines prune`  
  Remove obsolete generated artifacts.

### Migrations
- `manage-migrations rollback`  
  Controlled rollback for known-safe migration steps.

---

## 3) Test scope decision (requested consideration #3)

Question: Should pipeline test modes be included now?

- `cdc test --fast-pipelines`
- `cdc test --full-pipelines`

### Recommendation

Implement in two steps:

1. **Now (current phase)**
   - Keep command grouping migration focused on CLI IA changes.
   - Add documentation and contracts for test modes.

2. **Next phase**
   - Implement both flags in `cdc test`.
   - Wire post-generation execution path so both modes can run immediately after `cdc generate`.

### Why

- Keeps structural CLI migration low-risk.
- Avoids mixing IA refactor with test harness runtime complexity.
- Preserves clear regression surface for this phase.

Related document: `_docs/development/PIPELINE_TEST_MODES.md`.

---

## 4) Validation checklist

Before finalizing each follow-up increment:

- Run Ruff checks and fix all introduced warnings in touched files
- Run strict type checks
- Run full test suite
- Verify shell completions for:
  - `manage-pipelines` group + subcommands
  - `manage-migrations` group + subcommands
- Verify help output and command discoverability

---

## 5) Implementation status (started)

Initial migration groundwork is now implemented:

- Added centralized path resolver with preferred+legacy support:
  - `services/_schemas` (preferred write path)
  - `service-schemas` (legacy read compatibility)
- Updated service-schema CRUD and schema-related autocompletions to use compatibility resolution.
- Updated service removal to clean both preferred and legacy schema locations.
- Updated CLI/help text for `manage-service-schema` to reference `services/_schemas`.

Current scope is intentionally incremental (safe start):

- ✅ New writes for custom-table schema management go to `services/_schemas`.
- ✅ Reads support both new and legacy locations.
- ✅ Full test suite passes after changes.
- ⏳ Remaining modules that still assume `service-schemas/` will be migrated in follow-up phases.
