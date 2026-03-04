# Repo-Wide Python Size + Coding Guidelines Gap Analysis and Phased Plan

**Date:** 2026-03-04  
**Scope:** All Python files in `cdc-pipeline-generator`  
**Goal:** Create a repo-wide, execution-ready refactor plan aligned with coding guidelines for file/function size and modularity

---

## 1) Baseline and Method

### 1.1 Standards used

This analysis uses the current project standards:

- `.github/copilot-instructions-coding-guidelines.md`
- `_docs/development/CODING_STANDARDS.md`

Hard thresholds applied:

- **File size:** ideal `200-400`, max `600`
- **Function/method size:** ideal `10-50`, max `100`

### 1.2 How metrics were collected

Tools used:

1. `search_subagent` to locate authoritative thresholds in docs.
2. AST-based scanner script to inspect every `*.py` file and every function/method:
   - script: `_docs/development/_stats/repo_py_audit_script.py`
   - output: `_docs/development/_stats/repo_py_size_audit.json`

Coverage:

- **All Python files** under repository root, excluding: `.git`, `.venv`, `__pycache__`, `.mypy_cache`, `.pytest_cache`, `cdc_pipeline_generator.egg-info`.

---

## 2) Repo-Wide Findings

## 2.1 High-level numbers

- Total Python files scanned: **252**
- Files over 600 lines: **37**
- Functions/methods over 100 lines: **51**

Distribution of over-limit files:

- `cdc_generator`: **19** files
- `tests`: **18** files

Severity distribution (files >600):

- `>2000`: **2**
- `1501-2000`: **4**
- `1201-1500`: **3**
- `1001-1200`: **6**
- `801-1000`: **9**
- `601-800`: **13**

Severity distribution (functions >100):

- `>300`: **2**
- `201-300`: **3**
- `151-200`: **6**
- `121-150`: **18**
- `101-120`: **22**

## 2.2 Top file-size offenders

1. `2748` — `cdc_generator/validators/manage_service/sink_operations.py`
2. `2598` — `cdc_generator/cli/sink_group.py`
3. `1907` — `cdc_generator/cli/completions.py`
4. `1873` — `cdc_generator/core/migration_generator.py`
5. `1619` — `cdc_generator/core/pipeline_generator.py`
6. `1527` — `cdc_generator/cli/click_commands.py`
7. `1393` — `tests/cli/test_manage_services_config.py`
8. `1263` — `tests/test_sink_handlers.py`
9. `1227` — `tests/cli/coverage_report.py`
10. `1156` — `tests/test_fish_completions.py`

## 2.3 Top function/method-size offenders

1. `428` — `cdc_generator/cli/source_group.py::main`
2. `427` — `cdc_generator/cli/sink_group.py::main`
3. `282` — `cdc_generator/cli/service.py::_build_parser`
4. `243` — `cdc_generator/validators/manage_server_group/scaffolding/templates.py::get_docker_compose_template`
5. `225` — `cdc_generator/validators/manage_service/service_creator.py::create_service`
6. `189` — `cdc_generator/validators/manage_service/db_inspector_common.py::get_service_db_config`
7. `175` — `cdc_generator/cli/scaffold_command.py::main`
8. `164` — `cdc_generator/validators/manage_server_group/handlers_update.py::handle_update`
9. `162` — `cdc_generator/validators/manage_server_group/db_inspector.py::list_postgres_databases`
10. `162` — `cdc_generator/validators/manage_service/sink_operations.py::add_sink_table`

---

## 3) Gap Analysis vs Guidelines

## 3.1 File-size guideline gaps

- The codebase has concentrated hotspots in **CLI orchestration**, **sink/service validators**, and **core generation**.
- A significant portion of oversized files are in `tests`, which increases maintenance and slows focused AI navigation.

## 3.2 Function-size guideline gaps

- Oversized functions cluster in:
  - CLI command routers (`main`/parser builders)
  - service/sink mutation workflows
  - scaffolding/template builders
  - db inspection handlers

## 3.3 Architectural impact

- Large orchestration files mix parsing, validation, execution, and output formatting in single modules.
- Long functions combine control-flow + business rules + I/O concerns, making refactors high-risk and test targeting harder.

---

## 4) Prioritized Refactor Buckets

## Bucket A — Runtime-critical production code (highest priority)

Focus files:

- `cdc_generator/validators/manage_service/sink_operations.py`
- `cdc_generator/core/migration_generator.py`
- `cdc_generator/core/pipeline_generator.py`
- `cdc_generator/validators/manage_service/service_creator.py`
- `cdc_generator/validators/manage_service/db_inspector_common.py`

Why first:

- Highest operational impact and regression risk.
- Largest files/functions in active generation and config mutation paths.

## Bucket B — CLI orchestration/command composition

Focus files:

- `cdc_generator/cli/sink_group.py`
- `cdc_generator/cli/source_group.py`
- `cdc_generator/cli/completions.py`
- `cdc_generator/cli/click_commands.py`
- `cdc_generator/cli/service.py`
- `cdc_generator/cli/scaffold_command.py`

Why second:

- Biggest function-size violations (`main` and parser builders).
- Great ROI from command-handler decomposition and shared builder helpers.

## Bucket C — Server-group validators and scaffolding modules

Focus files:

- `cdc_generator/validators/manage_server_group/handlers_update.py`
- `cdc_generator/validators/manage_server_group/db_inspector.py`
- `cdc_generator/validators/manage_server_group/yaml_writer.py`
- `cdc_generator/validators/manage_server_group/scaffolding/templates.py`

Why third:

- Multiple >100-line functions and mixed concerns.
- Supports cleaner CLI split and lowers future coupling.

## Bucket D — Large test modules

Focus files (examples):

- `tests/cli/test_manage_services_config.py`
- `tests/test_sink_handlers.py`
- `tests/cli/coverage_report.py`
- `tests/test_fish_completions.py`

Why fourth:

- 18 oversized test files; not production runtime risk, but major maintainability burden.
- Split by scenario/domain while preserving behavior and naming discoverability.

---

## 5) Phased Autonomous Plan (Repo-Wide)

## Phase 0 — Safety gates and tracking

Tasks:

1. Keep `_docs/development/_stats/repo_py_size_audit.json` as baseline.
2. Add lightweight progress tracker section in this doc (per phase done/not done).
3. Lock regression command set:
   - `cdc test --all --cli`
   - targeted suites for each touched area.

Exit criteria:

- Baseline captured and reproducible.
- No structural refactor started without pass/fail gate command per phase.

---

## Phase 1 — Break top runtime hotspots (>1500 lines)

Targets:

- `sink_operations.py`
- `migration_generator.py`
- `pipeline_generator.py`

Tasks:

1. Split by feature boundaries into submodules (mutations, validation bridges, rendering, persistence).
2. Keep facades stable in original entry modules.
3. Move only cohesive slices per PR-sized unit.
4. Enforce function-size limit in moved code.

Exit criteria:

- Each target reduced to `<=900` first pass.
- No behavior regressions in `cdc test --all --cli`.

---

## Phase 2 — CLI main/parser decomposition

Targets:

- `cli/sink_group.py`, `cli/source_group.py`, `cli/service.py`, `cli/click_commands.py`, `cli/completions.py`, `cli/scaffold_command.py`

Tasks:

1. Extract parser builders into dedicated small builders per command group.
2. Extract execution handlers from `main` to pure command functions.
3. Centralize shared option definitions.

Exit criteria:

- No `main` function >100 lines.
- Each CLI module trends toward `<=800` in this phase.

---

## Phase 3 — Validator/scaffolding normalization

Targets:

- `manage_server_group` large handlers and template generators.

Tasks:

1. Separate DB read, normalization, and write paths.
2. Move template assembly into focused template modules.
3. Keep YAML write semantics unchanged; add guard tests around output parity.

Exit criteria:

- Target functions under 100 lines or explicitly delegated.
- Large modules reduced with stable public imports.

---

## Phase 4 — Complete file-size conformance for production code

Scope:

- All non-test files over 600 lines.

Tasks:

1. Finish second-pass extraction for modules still >600 after phases 1–3.
2. Add internal package-level `__init__` facades where needed.
3. Resolve any temporary compatibility shims introduced earlier.

Exit criteria:

- **0 production files** over 600 lines.
- **0 production functions/methods** over 100 lines.

---

## Phase 5 — Test suite modularization

Scope:

- All oversized test files.

Tasks:

1. Split tests by behavior domains (routing, validation, CLI parsing, sink ops).
2. Keep fixture reuse in `conftest.py`; avoid logic duplication.
3. Preserve node IDs where possible for CI familiarity.

Exit criteria:

- Test modules generally <=600 lines (exceptions documented).
- No test behavior drift.

---

## Phase 6 — Institutionalize guardrails

Tasks:

1. Keep audit script in repo and run in CI (report mode).
2. Add non-blocking warning gate initially, then migrate to blocking for production paths.
3. Re-run audit and publish delta trend in `_docs/development/_stats/`.

Exit criteria:

- Continuous visibility on file/function-size drift.
- New violations prevented from re-accumulating.

---

## 6) Execution Rules for Autonomous Refactor

1. Refactor in smallest cohesive slices.
2. Preserve public APIs and CLI behavior unless phase explicitly changes them.
3. After each slice:
   - run targeted tests,
   - run `cdc test --all --cli` periodically,
   - update audit JSON and delta counts.
4. Do not mix unrelated feature work with structural refactors.
5. Prefer extraction + thin facade over in-place rewrites.

---

## 7) Immediate Next Sprint Recommendation

Best ROI start order:

1. `cdc_generator/validators/manage_service/sink_operations.py`
2. `cdc_generator/core/pipeline_generator.py`
3. `cdc_generator/core/migration_generator.py`
4. `cdc_generator/cli/sink_group.py` and `cdc_generator/cli/source_group.py`

Reason:

- This addresses the largest production hotspots and the two largest oversized functions first, reducing risk and review complexity quickly.

---

## 8) Canonical Artifacts

- Repo-wide metrics JSON: `_docs/development/_stats/repo_py_size_audit.json`
- Metric script: `_docs/development/_stats/repo_py_audit_script.py`
- This plan: `_docs/development/REPO_WIDE_PY_SIZE_AND_CODING_GUIDELINES_GAP_ANALYSIS_AND_PHASED_PLAN.md`
