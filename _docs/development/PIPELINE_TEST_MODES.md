# Pipeline Test Modes (AI-friendly)

## Why this exists

After `cdc generate`, we want a predictable way to validate generated pipelines at two levels:

1. **Fast confidence** (quick, deterministic, local)
2. **Full confidence** (realistic, heavier integration)

This document defines the meaning of:
- `cdc test --fast-pipelines`
- `cdc test --full-pipelines`

---

## Mode 1: `--fast-pipelines`

### Purpose
Run a **simple isolated scenario** right after generation to catch template and wiring regressions quickly.

### Scope
- Uses mock/controlled input (for example HTTP/file fixtures)
- Exercises generated source + sink pipeline logic in a lightweight way
- Verifies expected transformed output artifacts
- Avoids heavy CDC infra dependencies when possible

### What it should validate
- Pipeline file generation is structurally valid
- Bloblang transformations execute for representative rows/events
- Routing/topic/file outputs match expected values
- Basic schema mapping assumptions still hold

### What it should *not* try to validate
- Full end-to-end CDC behavior under production-like load
- DB-specific CDC edge cases (LSN progression, connector internals)
- Long-running reliability scenarios

### Expected characteristics
- Fast runtime (target: minutes, not tens of minutes)
- Deterministic outputs
- Suitable for frequent local use and PR gating

---

## Mode 2: `--full-pipelines`

### Purpose
Run a **full integration pipeline validation** for high confidence.

### Scope
- Includes realistic infra components (connect/mq + source/sink dependencies)
- Executes generated pipelines through broader end-to-end paths
- Validates integration contracts between components

### What it should validate
- End-to-end data movement through generated pipelines
- Integration correctness across source, connect/mq, and sink boundaries
- Runtime behavior that cannot be proven in fast mode

### Expected characteristics
- Slower runtime
- More dependencies and setup
- Best for pre-merge gates, nightly runs, and release readiness

---

## Suggested implementation plan

### Phase 1 — Command surface
- Add `--fast-pipelines` and `--full-pipelines` flags to `cdc test`
- Enforce mutually exclusive behavior (exactly one mode per run)
- Provide clear CLI help text and examples

### Phase 2 — Fast mode first (must-have)
- Implement one canonical isolated test flow:
  - mock source/http input
  - generated source pipeline processing
  - connect/mq handoff (or mocked equivalent)
  - sink file output
  - golden-output assertion
- Keep fixtures minimal and deterministic

### Phase 3 — Full mode
- Reuse fast mode fixtures where possible
- Add infra-dependent integration flow(s)
- Add stronger assertions for cross-component behavior

### Phase 4 — Developer workflow
- Ensure both modes are runnable immediately after `cdc generate`
- Add docs/examples to reduce ambiguity for AI and humans
- Add CI matrix guidance:
  - PR: `--fast-pipelines`
  - nightly/release: `--full-pipelines`

---

## Suggested pass/fail contract

A mode should be considered successful only when:

1. Generated pipelines load and run for the scenario
2. Output matches expected artifacts exactly (or by defined stable rules)
3. Validation reports include actionable failure context (which step failed)

---

## Open decisions to settle during implementation

- Should `cdc test` default to one mode when no flag is provided?
- Should `--full-pipelines` automatically include fast checks first?
- Which minimal dataset becomes the canonical golden fixture?
- Which environments can run full mode reliably (local/dev CI/nightly)?
