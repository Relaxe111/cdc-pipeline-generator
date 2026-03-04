# Sink Env-Aware Routing Refactor Gap Analysis + Phased Autonomous Plan

**Date:** 2026-03-04  
**Scope:** Generator code changed for `target_sink_env` + `column_templates.unique` rollout  
**Goal:** Align implementation with project instructions and coding standards for long-term maintainability

---

## 1. Standards Baseline Used

This analysis is aligned against:

- `.github/copilot-instructions.md` (router + invariants)
- `.github/copilot-instructions-coding-guidelines.md`
- `.github/copilot-instructions-type-safety.md`
- `.github/copilot-instructions-architecture.md`
- `_docs/development/CODING_STANDARDS.md`

### Key hard constraints extracted

1. File size target `200-400`, max `600` lines.
2. Function size target `10-50`, max `100` lines.
3. No suppression-style fixes (`type: ignore`, broad `noqa`) in project code (except external import-untyped boundary).
4. Prefer typed contracts over raw YAML dicts (`TypedDict` / dataclass + runtime validation).
5. Keep logic modular by feature responsibility.
6. Keep generator pattern-agnostic and avoid service hardcoding.

---

## 2. Audited Files (changed in this feature)

Primary implementation files:

- `cdc_generator/core/sink_env_routing.py`
- `cdc_generator/core/pipeline_generator.py`
- `cdc_generator/validators/manage_service/validation.py`
- `cdc_generator/validators/manage_service/sink_operations.py`
- `cdc_generator/validators/manage_service/sink_template_ops.py`
- `cdc_generator/core/column_templates.py`
- `cdc_generator/core/column_template_operations.py`
- `cdc_generator/helpers/service_config.py`

Supporting schema/docs/tests changed and already validated separately.

---

## 3. Gap Analysis (Instruction vs Current State)

## 3.1 File-size and module-boundary gaps (Critical)

Measured line counts:

- `cdc_generator/core/pipeline_generator.py`: **1535** (max 600)
- `cdc_generator/validators/manage_service/sink_operations.py`: **2747** (max 600)
- `cdc_generator/validators/manage_service/sink_template_ops.py`: **558** (near max, high churn risk)
- `cdc_generator/core/column_template_operations.py`: **531** (near max)

**Gap:** Core generation and sink config logic are concentrated in oversized modules, violating architecture and maintainability standards.

**Impact:** High regression risk, slow AI navigation, difficult review/testing isolation.

## 3.2 Function-size and responsibility gaps (High)

High-complexity functions in changed path:

- `generate_consolidated_sink(...)` in `core/pipeline_generator.py`
- `build_source_table_inputs(...)` in `core/pipeline_generator.py`
- `_collect_unique_template_issues(...)` in `validators/manage_service/validation.py`

**Gap:** Mixed responsibilities (routing resolution, preflight behavior, formatting, generation, error reporting) in single functions.

**Impact:** Low composability and hard-to-target unit tests.

## 3.3 Type-safety gaps in touched files (Critical)

### Directly observed in touched path

- `helpers/service_config.py` contains multiple `type: ignore[...]` usages.
- `helpers/service_config.py` passes around broad `dict[str, Any]` with deep casts and limited centralized typed validation.
- `core/pipeline_generator.py` and `validators/manage_service/validation.py` rely heavily on ad-hoc `dict[str, Any]` casting across YAML reads.

**Gap:** Violates strict type-safety policy and weakens schema drift detection.

**Impact:** Silent runtime type mismatches, harder static analysis, brittle refactors.

## 3.4 Architecture/pattern-agnostic gaps (High)

- `load_customer_config(...)` and `get_all_customers(...)` in `helpers/service_config.py` hardcode service fallback to `"adopus"`.
- Legacy fallback paths are still active in hot path (`2-customers/` compatibility and mixed normalization logic).

**Gap:** Conflicts with pattern-agnostic architecture and preprod cleanup policy (remove obsolete compatibility by default when touched).

**Impact:** Hidden coupling to implementation name and ambiguous runtime behavior.

## 3.5 Validation architecture gaps (Medium)

- `validate_service_config(...)` currently mixes: user-facing printing, schema checks, hierarchical checks, and preflight aggregation.
- New preflight logic is valuable but still tightly coupled to CLI-style messaging and generic config dictionaries.

**Gap:** Validation engine is not cleanly separated into typed, reusable subcomponents.

**Impact:** Harder to reuse preflight checks in non-CLI contexts and harder to test independently.

## 3.6 Positive alignment already achieved (Keep)

- Shared sink env routing helper extracted to `core/sink_env_routing.py` ✅
- Routing fail-fast/warn-only semantics implemented ✅
- `column_templates.unique` contract + scoped collision behavior implemented ✅
- Generator route/customer skip safety net implemented ✅
- Tests and docs added for new behavior ✅

---

## 4. Refactor Strategy (Phased, autonomous)

## Phase 0 — Safety rails and baseline snapshot

**Objective:** Freeze behavior before structural changes.

**Tasks:**

1. Capture current passing baseline for targeted suites:
   - `tests/cli/test_manage_services_config.py`
   - `tests/test_column_template_operations.py`
   - `tests/test_pipeline_generation.py`
   - `tests/test_source_ref_resolver.py`
2. Add/adjust smoke tests to pin current routing/unique behavior if coverage gaps found.
3. Create temporary migration notes documenting moved symbols and compatibility wrappers.

**Exit criteria:** All baseline tests pass; no behavior deltas.

---

## Phase 1 — Typed contracts for sink-routing + preflight inputs (no behavior change)

**Objective:** Reduce `Any` surface around newly added routing/unique logic.

**Tasks:**

1. Introduce typed config contracts for preflight and generation boundaries:
   - `SourceEnvConfig` (includes optional `target_sink_env`)
   - `SourceEntryConfig`
   - `SinkRouteRef`
   - `SinkTopologyInfo`
2. Add typed loader helpers that validate and return typed structures at one boundary.
3. Refactor `validate_service_sink_preflight(...)` internals to consume typed contracts.
4. Remove newly avoidable casts in preflight path.

**Primary files:**

- `validators/manage_service/validation.py`
- `core/sink_env_routing.py`
- `helpers/service_config.py` (typed extraction helpers only)

**Exit criteria:**

- No behavior change.
- Type checker issues reduced in touched files.
- Tests unchanged and passing.

---

## Phase 2 — Decompose validation module by responsibility

**Objective:** Split rule evaluation from I/O/printing and reduce function complexity.

**Tasks:**

1. Extract preflight collectors into dedicated module package:
   - `validators/manage_service/preflight/sink_routing_rules.py`
   - `validators/manage_service/preflight/unique_template_rules.py`
   - `validators/manage_service/preflight/types.py`
2. Keep `validation.py` as orchestrator + reporting facade.
3. Convert large collectors into small pure evaluators (<100 lines each).
4. Preserve exact error/warning semantics and message contracts where tests depend.

**Exit criteria:**

- `validation.py` reduced significantly (target <300 lines).
- Each evaluator unit-testable in isolation.
- Existing CLI validation tests pass.

---

## Phase 3 — Pipeline generation split (highest ROI)

**Objective:** Break `pipeline_generator.py` into composable modules while keeping CLI contract stable.

**Target package layout:**

- `core/pipeline_generation/__init__.py`
- `core/pipeline_generation/source_generation.py`
- `core/pipeline_generation/sink_generation.py`
- `core/pipeline_generation/template_resolution.py`
- `core/pipeline_generation/table_metadata.py`
- `core/pipeline_generation/runtime_processors.py`
- `core/pipeline_generator.py` (thin CLI/orchestrator facade)

**Tasks:**

1. Move pure helpers (`_resolve_template_expr`, processor builders, table metadata readers) into focused modules.
2. Keep public entrypoints stable:
   - `generate_customer_pipelines(...)`
   - `generate_consolidated_sink(...)`
3. Replace repeated dict/cast access with typed helper accessors.
4. Ensure all moved functions have docstrings and typed signatures.

**Exit criteria:**

- `pipeline_generator.py` <= 400 lines.
- No generation behavior change.
- Pipeline generation tests pass.

---

## Phase 4 — Service-config hardening and preprod cleanup

**Objective:** Remove touched-path compatibility debt and hardcoded service coupling.

**Tasks:**

1. Remove hardcoded `"adopus"` defaults from loader paths in generator scope.
2. Isolate or remove legacy `2-customers` fallback in active generation path per preprod policy.
3. Eliminate `type: ignore` debt in `helpers/service_config.py` by:
   - introducing typed intermediate structures,
   - performing runtime structure guards,
   - returning explicit typed dictionaries.
4. Add migration note for deprecated paths/assumptions.

**Exit criteria:**

- No `type: ignore` remains in touched service-config path.
- Pattern-agnostic service resolution is explicit.
- Existing behavior preserved for supported current structure.

---

## Phase 5 — sink_operations/sink_template_ops modularization (stability pass)

**Objective:** Reduce long-term maintenance risk in sink command handlers.

**Tasks:**

1. Split `sink_operations.py` into submodules by concern:
   - parsing/normalization
   - schema/type compatibility
   - mutations (add/remove sink/table)
   - preflight integration
2. Keep CLI signatures and imports backward compatible via facade exports.
3. Do same for `sink_template_ops.py` if phase budget allows.

**Exit criteria:**

- `sink_operations.py` facade <= 400 lines.
- New modules each <= 400 lines.
- CLI sink tests pass.

---

## Phase 6 — Final quality gate and docs sync

**Objective:** Close with enforceable quality baseline.

**Tasks:**

1. Run full targeted test matrix and additional impacted suites.
2. Update architecture docs with new module map.
3. Update AI handoff docs with current canonical extension points.
4. Remove temporary wrappers introduced in earlier phases.

**Exit criteria:**

- All tests green for impacted areas.
- No new schema/type regressions.
- Docs reflect final structure.

---

## 5. Autonomous Execution Protocol (for AI agent)

This section is intentionally instruction-like so an AI agent can execute without user interaction.

### 5.1 Default autonomy policy

- Execute phases sequentially (`0 -> 6`) without asking for confirmation between phases.
- After each phase:
  - run phase-specific tests,
  - produce a short changelog,
  - continue automatically.
- Only stop for genuine blockers:
  - missing runtime dependency,
  - unresolved merge conflict,
  - conflicting architectural decision not inferable from accepted ADRs.

### 5.2 Change boundaries

- Touch only generator repo code/docs/tests related to this plan.
- Avoid unrelated generated artifacts and unrelated feature changes.
- Preserve external CLI behavior unless explicitly phased to deprecate legacy path.

### 5.3 Mandatory per-phase checklist

1. Update plan status (phase in progress/completed).
2. Implement smallest cohesive slice.
3. Run targeted tests first, then broader impacted tests.
4. Fix only regressions caused by the current phase.
5. Commit-ready summary:
   - what changed,
   - why,
   - tests run,
   - residual risks.

### 5.4 Quality gates

A phase is complete only if:

- tests for touched area pass,
- no new lint/type debt introduced,
- moved code has docstrings and typed signatures,
- file/function size trend improves (or documented exception with follow-up task).

---

## 6. Recommended implementation order by ROI

1. **Phase 1 + 2** (typed preflight + validation split) — immediate maintainability for new logic.
2. **Phase 3** (pipeline generator decomposition) — biggest architecture win.
3. **Phase 4** (service-config cleanup) — aligns with preprod policy + type safety.
4. **Phase 5** (sink ops modularization) — larger but high debt payoff.
5. **Phase 6** finalization.

---

## 7. Risks and mitigations

### Risk A: Behavior drift during large file split

**Mitigation:** move code in thin slices with unchanged tests + stable facade exports.

### Risk B: Message text coupling in tests

**Mitigation:** keep existing error/warning wording stable until dedicated message-contract refactor.

### Risk C: Legacy path removal surprise

**Mitigation:** phase 4 includes explicit migration note and deprecation mapping.

---

## 8. Definition of Done (overall)

- All new sink env-aware and unique-validation behavior preserved.
- Touched modules aligned with size and responsibility standards.
- Type-safety policy respected in touched paths (no suppression debt).
- Generator paths are pattern-agnostic and preprod-clean.
- AI can continue autonomous iterations using this plan without user steering.

---

## 9. Canonical reference

Use this document as the execution source for future refactor iterations:

- `_docs/development/SINK_ENV_AWARE_REFACTOR_GAP_ANALYSIS_AND_PHASED_PLAN.md`
