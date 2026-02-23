# Decision Plan — Migration from Redpanda Connect to Bento

> **Status:** Proposed
> **Scope:** Generator-driven source/sink pipeline migration
> **Last updated:** February 2026

---

## Goal and Scope

Migrate source and sink pipeline runtimes from Redpanda Connect to Bento with minimal behavior changes, while keeping generation fully file-driven through the generator.

**In scope:**
- Source pipeline generation (MSSQL/PG CDC)
- Sink pipeline generation (PostgreSQL write/merge paths)
- Runtime selection in generator output
- Validation/test workflow in CI (`lint` + `test`)

**Out of scope (for initial migration):**
- Re-architecting transformations
- Introducing Bytewax/Materialize-specific logic
- Changing topic contracts or event schemas

---

## Decision Gates (Go/No-Go)

Proceed to production migration only if all gates pass:

| Gate | Pass Criteria |
|------|---------------|
| **Config compatibility** | Existing generated YAML runs on Bento with no functional drift for selected pilot services |
| **CDC parity** | `mssql_cdc` and `postgres_cdc` outputs match baseline topic shape and metadata expectations |
| **Sink parity** | Sink behavior (insert/update/delete handling, merge semantics, DLQ/error flow) matches current production behavior |
| **Operational parity** | Equivalent observability, alerting, restart/recovery behavior validated |
| **Performance guardrail** | Throughput/latency is at least baseline ± agreed tolerance |

If any gate fails, keep Redpanda Connect as default runtime and continue with targeted fixes before re-evaluating.

---

## Execution Phases (Generator-Driven)

### Phase A — Generator Runtime Abstraction

Add runtime selection to generator inputs/templates so generation can target either runtime without duplicating service definitions.

Recommended model:
- `runtime: redpanda_connect | bento` at source-group and sink-group level
- default remains `redpanda_connect` for backward compatibility
- no service-name-specific logic; use existing pattern-agnostic config model

Deliverables:
- Template/runtime switch support for source and sink generation
- Backward-compatible defaults
- Snapshot tests for both runtime outputs

### Phase B — Source Pipeline Parity

Generate Bento versions of source pipelines first:
- MSSQL: `mssql_cdc`
- PostgreSQL: `postgres_cdc`
- Optional HTTP ingress patterns: `http_client` / `http_server`

Validation:
- Run `bento lint` and `bento test` on generated source configs
- Compare emitted topic payload shape and key fields with current Redpanda Connect baseline

### Phase C — Sink Pipeline Parity

Generate Bento sink pipelines with the same transformation and routing intent as current generation.

Validation:
- Replay representative CDC samples through both runtimes
- Compare target DB outcomes and error/DLQ behavior
- Confirm conditional drops, enrichment paths, and computed fields match

### Phase D — Dual-Run Canary

Run Bento in parallel for selected services while Redpanda Connect remains authoritative:
- Mirror traffic to Bento-generated pipelines
- Compare outputs by deterministic checks (message counts, key-level checksums, sampled row diffs)
- Promote service-by-service once parity threshold is met

### Phase E — Controlled Cutover

Switch runtime per source/sink group from `redpanda_connect` to `bento` in generator input, regenerate, and deploy in waves:
- Wave 1: low-risk services
- Wave 2: medium complexity
- Wave 3: high-volume/high-criticality

Each wave requires explicit gate sign-off before continuing.

---

## Rollback Strategy

Rollback must be config-only and fast:
- Keep runtime switch reversible in generator input files
- Preserve previous generated artifacts per release tag
- If incident occurs, revert runtime to `redpanda_connect`, regenerate, redeploy
- Document maximum acceptable rollback time (target: minutes, not hours)

---

## Definition of Done

Migration is complete when:
- All targeted source/sink groups generate and run on Bento
- CI enforces Bento lint/test on generated artifacts
- Production parity metrics are stable across agreed observation window
- Runbooks and on-call docs reference Bento commands (`bento lint`, `bento test`, runtime debug flow)
- Redpanda Connect remains optional fallback, not primary runtime
