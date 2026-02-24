# Pattern + CLI Audit Refactor Summary

**Status:** Completed  
**Date:** 2026-02-24

## Purpose

This document is the final consolidated summary of the completed pattern/CLI audit and refactor work that was previously tracked in `_docs-temporary-audit-refactor/`.

It intentionally avoids duplicating large investigation artifacts and instead records durable outcomes and references canonical documentation.

## Outcomes (Completed)

- Canonical CLI grouped flow is reinforced:
  - `manage-services config`
  - `manage-services resources`
- Approved short aliases are standardized and active:
  - `ms`, `msc`, `msr`, `msog`, `msig`, `mp`, `mm`
- Sink inspect UX and completion behavior were normalized:
  - `--inspect-sink` supports optional sink key completion
  - single-service contexts auto-resolve to sink suggestions from `services/*.yaml`
- Sink inspection/save resolution now uses sink-group source data correctly for target-service database/schema lookup.
- Schema save path is canonicalized for writes:
  - `services/_schemas/<service>/<schema>/...`
  - legacy `service-schemas/...` write path removed
- Generated schema YAML includes DB default metadata:
  - `default_value` is now persisted per column (PostgreSQL + MSSQL)

## Decision Notes (Durable)

### 1) Preprod cleanup mode is active (temporary policy)

During preprod, we remove obsolete aliases/paths/docs/code immediately when touched and do not keep compatibility shims by default.

This policy is documented in:
- `.github/copilot-instructions.md` (workspace)
- `cdc-pipeline-generator/.github/copilot-instructions.md`
- `adopus-cdc-pipeline/.github/copilot-instructions.md`

### 2) Pattern behavior remains configuration-driven

Pattern differences (`db-per-tenant` vs `db-shared`) are preserved only where architecture requires them, while command semantics and UX are unified where intent is equivalent.

## Canonical References

Use these as the primary sources going forward:

- CLI grouping and rollout context:
  - `_docs/development/GAP_ANALYSIS_COMMAND_GROUPING.md`
- Runtime and pipeline architecture context:
  - `_docs/architecture/`
- Active ADR index and decision records:
  - `.github/decisions/README.md`

## What Was Archived and Removed

The temporary working folder `_docs-temporary-audit-refactor/` has been retired after consolidation into this summary and ADR updates.

## Follow-up Guidance

- Do not recreate temporary audit artifacts for this completed initiative.
- If new CLI/pattern scope appears, open a new scoped document under `_docs/` and add an ADR only for durable, cross-cutting decisions.
