# Pipeline Management Implementation Plan

**Status:** Proposed  
**Date:** 2025-07-01  
**Context:** Comprehensive plan for `cdc manage-pipelines` evolution and Bento migration integration

### Decisions (Locked)

| # | Question | Decision |
|---|----------|----------|
| 1 | Canonical structure scope | **A: Both repos together** (generator + adopus in one effort) |
| 2 | What happens to `generated/` in adopus | **Already done** — schemas → `services/_schemas/`, pg-migrations → `migrations/`. Only `generated-legacy/pipelines/` remains → `pipelines/generated/` |
| 3 | When to start Bento investigation | **B: After Phase 0 completes** |
| 4 | Remove `multi-tenant`/`per-server` path segments | **A: Remove entirely** |
| 5 | Remove stub commands | **A: Remove entirely** (preprod cleanup) |
| 6 | Test data strategy | **C: Both** (synthetic for fast, real for full) |
| 7 | Dual-runtime support | **No** — direct migration to Bento (no production deployments yet) |
| 8 | verify/test merge | **Yes** — single `verify` command with `--full` and `--sink` flags |
| 9 | P2 priority | **Promoted to P1** — diff/health/prune are operational necessities |

---

## 0. Canonical Directory Structure

### 0.1 New Unified Pipeline Structure

To align with other `manage-*` commands (`manage-services` → `services/`, `manage-source-groups` → `source-groups.yaml`), all pipeline-related resources will be consolidated under a single `pipelines/` directory:

```
pipelines/
├── templates/                    # Pipeline templates (replacing root-level pipeline-templates/)
│   ├── source-pipeline.yaml     # Source template
│   └── sink-pipeline.yaml       # Sink template
│
└── generated/                   # All generated pipeline artifacts
    ├── sources/                # Generated source pipelines
    │   └── {env}/
    │       └── {customer}/
    │           └── source-pipeline.yaml
    └── sinks/                  # Generated sink pipelines
        └── {env}/
            └── sink-pipeline.yaml
```

**No runtime nesting** — Decision 7: direct Bento migration (no dual-runtime support needed since there are no production deployments).

### 0.2 `generated/` Status (Already Refactored)

Non-pipeline artifacts have **already been refactored** out of `generated/`:

| Artifact | Old Location | New Location | Managed By |
|----------|-------------|-------------|------------|
| Table definitions | `generated/table-definitions/` | `services/_schemas/_definitions/` | `manage-services resources` |
| Schemas | `generated/schemas/` | `services/_schemas/` | `manage-services resources` |
| PG migrations | `generated/pg-migrations/` | `migrations/` | `manage-migrations` |
| Pipelines | `generated/pipelines/` | **not yet moved** | `manage-pipelines` (this plan) |

**Current adopus state:**
- `generated/` directory no longer exists
- `generated-legacy/` contains old artifacts (can be deleted after pipeline migration)
- `pipeline-templates/` still at root — needs moving to `pipelines/templates/`

**Stale code references in generator (separate cleanup, out of scope for this plan):**

| File | Stale Reference | Correct Path |
|------|----------------|---------------|
| `pipeline_generator.py` L119-120 | `GENERATED_ROOT / 'table-definitions'` | `services/_schemas/_definitions/` |
| `schema_docs.py` L155 | `Path("generated/schemas")` | `services/_schemas/` |
| `migration_generator.py` L328 | doc ref `generated/table-definitions` | `services/_schemas/_definitions/` |
| `scaffolding/create.py` L254-255 | `"generated/schemas"`, `"generated/pg-migrations"` | `"services/_schemas"`, `"migrations"` |
| `scaffolding/vscode_settings.py` L84-85, 109-111 | `"generated/schemas/*"`, `"generated/table-definitions/*"` | `"services/_schemas/*"` |

> **Note:** These stale references should be cleaned up separately or may already be dead code paths.

### 0.3 Impact Assessment — What Must Change

#### Code files (6 files, pipeline-specific changes):

| File | Lines | What Changes |
|------|-------|--------------|
| `cdc_generator/core/pipeline_generator.py` | L101, 106-107 | `TEMPLATES_DIR`, `GENERATED_DIR` constants |
| `cdc_generator/helpers/helpers_sink_groups.py` | L139 | Remove `"multi-tenant"` path segment (Decision 4) |
| `cdc_generator/validators/manage_server_group/handlers_group.py` | L36, 41 | Scaffolded directory names |
| `cdc_generator/validators/manage_server_group/scaffolding/create.py` | L252, 373-381 | Dir creation + file creation paths |
| `cdc_generator/validators/manage_server_group/scaffolding/templates.py` | L228, 244, 486, 532, 546, 771 | Docker-compose, README, gitignore template strings |
| `cdc_generator/validators/manage_server_group/scaffolding/vscode_settings.py` | L83, 107-108 | Workspace config + file exclusions |

#### Stale `generated/` references (separate cleanup, out of scope):

| File | Lines | Stale Reference |
|------|-------|----------------|
| `pipeline_generator.py` | L119-120 | `GENERATED_ROOT / 'table-definitions'` → now `services/_schemas/_definitions/` |
| `migration_generator.py` | L328 | doc ref `generated/table-definitions` |
| `schema_docs.py` | L155 | `Path("generated/schemas")` → now `services/_schemas/` |
| `scaffolding/create.py` | L254-255 | `"generated/schemas"`, `"generated/pg-migrations"` |
| `scaffolding/vscode_settings.py` | L84-85, 109-111 | `"generated/schemas/*"`, `"generated/table-definitions/*"` |

#### Test files (1 file):

| File | Lines | What Changes |
|------|-------|--------------|
| `tests/cli/test_scaffold.py` | L91, 115-116, 540, 603-630, 988 | Expected directory/file paths in assertions |

#### Config files (4 files):

| File | What Changes |
|------|--------------|
| `cdc-pipeline-generator/.gitignore` | `generated/pipelines/*` → `pipelines/generated/**` |
| `adopus-cdc-pipeline/.gitignore` | Same |
| `adopus-cdc-pipeline/docker-compose.yml` | Volume mount paths (commented-out, L168/184) |
| Both `.vscode/settings.json` | File exclusion patterns |

#### Documentation (~25 files, ~130 references):

Markdown docs across both repos reference `pipeline-templates/` (~78 occurrences) and `generated/pipelines/` (~56 occurrences). Non-functional but must be updated for accuracy.

#### Kubernetes (`_infra/`):

No filesystem path references to `pipeline-templates` or `generated/pipelines`. Only namespace naming (`cdc-pipeline`). **No K8s config changes needed for directory restructure.**

#### Bloblang files:

Bloblang files live in `services/_bloblang/` — completely separate from pipeline structure. **No changes needed for bloblang paths.**

---

## 1. Current State Audit

### 1.1 What Currently Exists

#### Commands Implemented

| Command | Status | Implementation | What It Does |
|---------|--------|----------------|--------------|
| `generate` | ✅ Implemented | `pipeline_generator.py` (979 lines) | Generates Redpanda Connect YAML pipelines from templates |
| `reload` | ⚠️ Stub | CLI definition only, no handler in `PIPELINE_COMMANDS` | Intended to regenerate + reload pipelines |
| `verify` | ⚠️ Stub | CLI definition only, no handler in `PIPELINE_COMMANDS` | Intended to verify pipeline connections |
| `verify-sync` | ⚠️ Stub | CLI definition only, no handler in `PIPELINE_COMMANDS` | Intended to verify CDC synchronization |
| `stress-test` | ⚠️ Stub | CLI definition only, no handler in `PIPELINE_COMMANDS` | Intended for CDC stress testing |

**Alias:** `mp` → `manage-pipelines`

#### Generation System Architecture

```
pipeline_generator.py (979 lines)
├── generate_customer_pipelines()     # Per-customer source pipelines
├── generate_consolidated_sink()      # Per-environment consolidated sinks
├── build_source_table_inputs()       # CDC polling with LSN tracking
├── build_table_routing_map()         # Bloblang routing cases
├── build_sink_topics()               # Kafka topic lists
└── substitute_variables()            # Template {{VAR}} replacement
```

**Key features:**
- ✅ Validates service configs before generation
- ✅ Incremental generation (only writes if content changed, ignoring timestamps)
- ✅ Preserves environment variables (`${VAR}` format)
- ✅ Multi-table CDC with LSN caching
- ✅ Composite primary key support
- ⚠️ Hard-coded for Redpanda Connect only
- ⚠️ Uses legacy `2-customers/{customer}.yaml` in generated headers

### 1.2 Gaps (P0 → P2)

#### P0 (Blocking for Phase 0)

1. **No validation after generation** — `manage-pipelines validate` missing
2. **No visibility into generated artifacts** — `manage-pipelines list` missing
3. **4 stub commands with no handlers** — users see commands that fail silently

#### P1 (High Priority)

1. **No config drift detection** — `manage-pipelines diff` missing
2. **No health checks** — `manage-pipelines health` missing
3. **No cleanup** — `manage-pipelines prune` missing
4. **Test modes not implemented** — `--fast-pipelines` / `--full-pipelines` documented but not wired (promoted from P2)
5. **Generation is all-or-nothing** — `--force` flag exists but unused (promoted from P2)

### 1.3 Test Infrastructure Status

| Item | Status | Notes |
|------|--------|-------|
| `TEST_SETUP.md` | ✅ Found | Complete test DB setup (avansas, 1000 records, Actor + AdgangLinjer) |
| `TEST_CLEANUP.md` | ❌ Not found | Does not exist |
| `TEST_AUDIT_REPORT.md` | ❌ Not found | Does not exist |
| `PIPELINE_TEST_MODES.md` | ✅ Found | Detailed spec for `--fast-pipelines` / `--full-pipelines`, not implemented |
| Pipeline generation tests | ❌ Missing | No test file for `pipeline_generator.py` |
| `test_scaffold.py` | ✅ Exists | Tests scaffolding — will need path updates |

---

## 2. Bento Migration (Direct Switch)

### 2.1 Simplified Approach (Decision 7)

The original `BENTO_MIGRATION_DECISION_PLAN.md` proposed 5 phases (A–E) with dual-runtime support and canary deployments. Since there are **no production deployments yet**, we skip all that complexity and do a direct migration:

| Step | What | When |
|------|------|------|
| 1 | Port source template to Bento format | Phase 1 |
| 2 | Port sink template to Bento format | Phase 1 |
| 3 | Bloblang compatibility audit | Phase 1 |
| 4 | Switch binary (redpanda-connect → bento) | Phase 1 |
| 5 | Remove Redpanda Connect references | Phase 1 cleanup |

**No runtime abstraction layer.** No `set-runtime`/`list-runtimes` commands. No dual-runtime directories.

**Rollback strategy:** Git revert to Redpanda Connect templates if Bento has blocking issues.

### 2.2 Impact on Directory Structure

No `{runtime}/` nesting in templates or generated paths. Templates live flat under `pipelines/templates/`. If Bento templates differ structurally from Redpanda Connect, we just replace the files.

---

## 3. Phased Implementation Plan

### Phase 0: Foundation (Weeks 1-2)

**Goal:** Fix generate, establish canonical structure, add verify command

#### Deliverables:

**-1. Fix `generate` command (prerequisite)**
- Fix `GENERATED_DIR` — currently points to dead path `generated/pipelines/multi-tenant/`
- Fix `load_generated_table_definitions()` — reads from stale `generated/table-definitions/` (now `services/_schemas/_definitions/`)
- Fix generated header — still references `2-customers/{customer}.yaml`
- Remove `CUSTOMERS_DIR` legacy constant
- Verify generation produces correct source + sink output

**0. Canonical directory restructure**
- Create `pipelines/templates/` and move source + sink templates
- Create `pipelines/generated/` with `.gitkeep`
- Update `pipeline_generator.py` constants:
  ```python
  TEMPLATES_DIR = PROJECT_ROOT / "pipelines" / "templates"
  GENERATED_DIR = PROJECT_ROOT / "pipelines" / "generated"
  ```
- Update scaffolding code (`create.py`, `templates.py`, `vscode_settings.py`, `handlers_group.py`)
- Update `helpers_sink_groups.py` — remove `multi-tenant`/`per-server` path segments
- Update `.gitignore` files in both repos
- Update `test_scaffold.py` assertions
- Apply same restructure to adopus-cdc-pipeline (Decision 1: A)

**1. Implement `cdc manage-pipelines verify`**
- **Default (light):** YAML syntax check + structure validation + no unsubstituted `{{PLACEHOLDER}}` + required top-level keys
- **`--full`:** Generate all pipelines + validate outputs (if full doesn’t take much longer than light, make full the default)
- **`--sink`:** Verify sink connections against PostgreSQL
- Add to `PIPELINE_COMMANDS` dict in `commands.py`

**2. Implement `cdc manage-pipelines list`**
- Scan `pipelines/generated/` directory
- Group by env/customer
- Show file count, last modified, staleness indicator
- Add to `PIPELINE_COMMANDS` dict in `commands.py`

**3. Remove stub commands**
- Remove `reload`, `verify` (old stub), `verify-sync`, `stress-test` from `click_commands.py`
- Add new `verify` command with `--full` / `--sink` flags

**4. Create test infrastructure**
- Add `tests/test_pipeline_generation.py`
- Create synthetic fixture (1 customer, 1 table) for fast tests
- Create real avansas fixture for full tests

#### Success Criteria:
- ✅ `generate` produces correct output to canonical paths
- ✅ `verify` catches syntax errors and structure issues
- ✅ `verify --sink` validates sink connectivity
- ✅ `list` shows accurate inventory
- ✅ All stale paths and legacy constants removed
- ✅ All existing tests pass (scaffold tests updated)

---

### Phase 1: Bento Migration + Operational Commands (Weeks 3-5)

**Goal:** Switch to Bento, add diff/health/prune

#### Deliverables:

**Bento migration (direct switch — Decision 7):**
1. Install Bento binary in dev container
2. Port source template (Redpanda Connect → Bento format)
3. Port sink template
4. Bloblang compatibility audit (test all mappings against Bento)
5. Update generator to use `bento` binary references where needed
6. Remove all `redpanda-connect` references

**Operational commands (promoted from P2 — Decision 9):**
1. **`diff`** — compare configs to generated pipelines, detect drift
2. **`health`** — check Bento instance reachability
3. **`prune`** — identify and remove orphaned pipeline files (dry-run default)
4. **Enhanced `list`** — add `--status` flag

#### Success Criteria:
- ✅ All pipelines generate with Bento-compatible templates
- ✅ Bloblang mappings verified against Bento binary
- ✅ `diff` detects config drift
- ✅ `health` checks Bento reachability
- ✅ `prune` cleans orphaned files safely

---

## 4. Testing Strategy

### 4.1 Verify Modes (replaces separate test/verify)

| Mode | Purpose | Duration | When |
|------|---------|----------|------|
| `cdc mp verify` | YAML syntax + structure + placeholder check | < 1 min | Every PR, local dev |
| `cdc mp verify --full` | Full generation + validate all outputs | < 5 min | Pre-merge |
| `cdc mp verify --sink` | Verify sink PostgreSQL connectivity | < 2 min | Before deployment |

If `--full` doesn’t take much longer than default, make full validation the default.

### 4.2 Test Fixtures

**Location:** `tests/fixtures/`

**Needed:**
- `test-source-groups.yaml` — minimal source group config
- `test-service.yaml` — test service with 1 customer, 1 table (synthetic)
- `test-service-full.yaml` — real avansas config for full tests
- `expected-source-pipeline.yaml` — golden output for source
- `expected-sink-pipeline.yaml` — golden output for sink

---

## 5. Readiness Gap Analysis

### 5.1 What We Have (Ready to Implement)

| Item | Status | Notes |
|------|--------|-------|
| Working `generate` command | ⚠️ Broken paths | 979-line impl, but output path (`generated/pipelines/multi-tenant/`) and table-defs path (`generated/table-definitions/`) are stale |
| Template files (both repos) | ✅ Ready | `source-pipeline.yaml` + `sink-pipeline.yaml` exist |
| Scaffolding system | ✅ Ready | Creates dirs + templates for new implementations |
| CLI architecture | ✅ Ready | Click groups, passthrough pattern, `PIPELINE_COMMANDS` dict |
| Test database setup | ✅ Ready | `TEST_SETUP.md` with avansas (1000 records) |
| Pipeline test spec | ✅ Ready | `PIPELINE_TEST_MODES.md` with clear requirements |
| Bento migration plan | ✅ Ready | `BENTO_MIGRATION_DECISION_PLAN.md` with 5 phases |
| Gap analysis doc | ✅ Ready | `GAP_ANALYSIS_COMMAND_GROUPING.md` with P0-P2 tiers |
| Full path audit | ✅ Done | 6 pipeline code files + 5 stale refs, 1 test file, 4 config files, ~25 doc files |
| Existing tests | ✅ Ready | `test_scaffold.py` covers scaffolding paths |

### 5.2 What We Know But Need to Build

| Item | Required For | Effort | Blocked By |
|------|-------------|--------|-----------|
| `validate` command handler | Phase 0 | Small | Nothing — merged into `verify` command |
| `list` command handler | Phase 0 | Small | Nothing — can start |
| Fix `generate` stale paths | Phase 0 | Small | Nothing — **prerequisite, start here** |
| Path constant updates in `pipeline_generator.py` | Phase 0 | Small | Nothing — decisions resolved |
| Scaffolding path updates (4 files) | Phase 0 | Medium | Nothing — decisions resolved |
| `test_pipeline_generation.py` test file | Phase 0 | Medium | Nothing — can start |
| `verify` command (light + full + sink modes) | Phase 0 | Medium | Nothing — can start |
| Adopus repo pipeline moves + `generated-legacy/` cleanup | Phase 0 | Small | Nothing — Decision 1 resolved |
| Bento template ports | Phase 1 | Large | Phase 0 + Bloblang audit |
| Operational commands (diff, health, prune) | Phase 1 | Medium | Phase 0 must complete first |

### 5.3 What We Don't Know (Knowledge Gaps)

| Gap | Impact | How to Resolve |
|-----|--------|----------------|
| **Bento YAML schema** — is it structurally identical to Redpanda Connect? | Phase 1 template porting | Install Bento after Phase 0, compare docs (Decision 3: B) |
| **Bloblang compatibility** — do all functions/syntax work in Bento? | Phase 1 template correctness | Run test mappings against Bento binary |
| **Bento binary availability** — is `bento` CLI available in dev container? | Phase 1 | Check Docker image, install if needed |
| **Other implementations** — does asma-cdc-pipeline also need restructure? | Future scope | Check if asma uses same pipeline structure |
| **Generate correctness** — does `generate` still produce valid output? | Phase 0 prerequisite | Fix stale paths, run and verify output |

### 5.4 Conclusion: Ready to Start?

**Yes.** All 6 decisions are resolved. Phase 0 can start immediately with no blockers.

Summary of decisions:
- **Both repos together** (generator + adopus in one effort)
- **`generated/` already eliminated** (schemas → `services/_schemas/`, pg-migrations → `migrations/`, only pipelines remain)
- **Remove `multi-tenant`/`per-server` path segments** (flat paths)
- **Remove 4 stub commands** (preprod cleanup)
- **Bento investigation after Phase 0** (focused scope)
- **Both test data** strategies (synthetic + real)
- **Direct Bento migration** (no dual-runtime, no canary)
- **Merge verify + test** into single `verify` command with modes
- **P2 promoted to P1** (diff/health/prune are operational necessities)

---

## 6. Decisions (All Resolved)

See the Decisions table at the top of this document for the summary.

All 6 decisions have been locked. No further human input is needed to begin Phase 0.

---

## 7. Implementation Checklist

### Phase 0 (Weeks 1-2)

#### Fix `generate` command (prerequisite)
- [x] Fix `GENERATED_DIR` — points to dead `generated/pipelines/multi-tenant/`
- [x] Fix `load_generated_table_definitions()` — reads from stale `generated/table-definitions/`
- [x] Fix generated header — references legacy `2-customers/{customer}.yaml`
- [x] Remove `CUSTOMERS_DIR` legacy constant
- [x] Verify source + sink output is correct

#### Pipeline directory restructure
- [x] Create `pipelines/templates/` in generator
- [x] Move source + sink templates to `pipelines/templates/`
- [x] Create `pipelines/generated/` with `.gitkeep`
- [x] Update `pipeline_generator.py` path constants (L101, 106-107)
- [x] Update `helpers_sink_groups.py` — remove `multi-tenant`/`per-server` returns (L139)

#### `generated-legacy/` cleanup
- [ ] Verify `generated-legacy/` contents are superseded
- [ ] Move `generated-legacy/pipelines/` → `pipelines/generated/` (or regenerate)
- [ ] Delete `generated-legacy/` directory

#### Scaffolding updates (all 4 files)
- [x] Update `create.py` — dir list, file creation paths
- [x] Update `templates.py` — docker-compose, README, gitignore strings
- [x] Update `vscode_settings.py` — file exclusions, workspace config
- [x] Update `handlers_group.py` — scaffolded dirs

#### Config files
- [x] Update `.gitignore` (generator): pipeline paths
- [x] Update `.gitignore` (adopus): pipeline paths
- [x] Update `docker-compose.yml` (adopus): volume mount paths

#### Apply to adopus-cdc-pipeline (Decision 1: A)
- [x] Move `pipeline-templates/` → `pipelines/templates/`
- [ ] Move `generated-legacy/pipelines/` → `pipelines/generated/` (or regenerate)
- [ ] Delete `generated-legacy/` after verification
- [x] Update `test_scaffold.py` (10+ assertions)

#### New commands
- [x] Implement `verify` command (light mode: YAML syntax + structure + placeholder check)
- [x] Implement `verify --full` (generate all + validate outputs)
- [x] Implement `verify --sink` (verify sink PostgreSQL connectivity)
- [x] Implement `list` command
- [x] Add to `PIPELINE_COMMANDS` dict in `commands.py`
- [x] Add CLI definitions in `click_commands.py`

#### Stub cleanup (Decision 5: A)
- [x] Remove `reload` command definition
- [x] Remove old `verify` stub
- [x] Remove `verify-sync` command definition
- [x] Remove `stress-test` command definition

#### Testing (Decision 6: C)
- [x] Create `tests/test_pipeline_generation.py`
- [x] Create `tests/fixtures/` with synthetic test config (1 customer, 1 table)
- [x] Create `tests/fixtures/` with real avansas test config
- [x] Verify all existing tests pass

#### Documentation
- [x] Update Copilot instructions (both repos)
- [x] Update `_docs/bento-bloblang/08-PIPELINE-TEMPLATING.md`
- [x] Update README files

### Phase 1 (Weeks 3-5)

#### Bento migration (direct switch)
- [x] Install Bento binary in dev container
- [x] Port source template to Bento format
- [x] Port sink template to Bento format
- [ ] Bloblang compatibility audit (test all mappings)
- [x] Update generator for Bento binary references
- [ ] Remove all Redpanda Connect references
- [x] Update `BENTO_MIGRATION_DECISION_PLAN.md` status

#### Operational commands (promoted from P2)
- [x] Implement `diff` command
- [x] Implement `health` command
- [x] Implement `prune` command
- [x] Enhance `list` with `--status` flag
- [ ] Update all documentation (~25 files, ~130 references)

---

## 8. Command Reference

### Currently Implemented

| Command | Description |
|---------|-------------|
| `cdc manage-pipelines generate [--all] [--force]` | Generate pipelines from templates |
| `cdc manage-pipelines list [--status]` | List generated pipelines and readiness/freshness summary |
| `cdc manage-pipelines verify [--full] [--sink]` | Verify generated pipelines (syntax/structure/connectivity) |
| `cdc manage-pipelines diff` | Show pipeline drift between templates and generated outputs |
| `cdc manage-pipelines health [--url <endpoint>]` | Check pipeline runtime health endpoints |
| `cdc manage-pipelines prune [--confirm]` | Prune stale generated artifacts |

### Planned by Phase

| Phase | Command | Priority |
|-------|---------|----------|
| 0 | `verify` (light: YAML + structure) | P0 |
| 0 | `verify --full` (generate + validate all) | P0 |
| 0 | `verify --sink` (sink connectivity) | P0 |
| 0 | `list` | P0 |
| 1 | `diff` | P1 |
| 1 | `health` | P1 |
| 1 | `prune [--confirm]` | P1 |

### Commands to Remove (Preprod Cleanup)

| Command | Re-add When |
|---------|-------------|
| `reload` | When Bento supports live reload |
| old `verify` stub | Replaced by new `verify` |
| `verify-sync` | Folded into `verify --sink` |
| `stress-test` | Separate effort |

---

## 9. References

| Document | Purpose |
|----------|---------|
| `BENTO_MIGRATION_DECISION_PLAN.md` | Bento migration strategy |
| `PIPELINE_TEST_MODES.md` | Test mode spec |
| `GAP_ANALYSIS_COMMAND_GROUPING.md` | Command gap analysis |
| `TEST_SETUP.md` | Test database setup |

| Key File | Purpose |
|----------|---------|
| `cdc_generator/core/pipeline_generator.py` | Main generation logic (979 lines) |
| `cdc_generator/cli/commands.py` | Command routing + `PIPELINE_COMMANDS` dict |
| `cdc_generator/cli/click_commands.py` | CLI definitions (Click decorators) |
| `cdc_generator/helpers/helpers_sink_groups.py` | Pattern → path segment mapping |
| `cdc_generator/validators/manage_server_group/scaffolding/create.py` | New project scaffolding |
| `cdc_generator/validators/manage_server_group/scaffolding/templates.py` | Scaffolding template strings |
| `cdc_generator/validators/manage_server_group/scaffolding/vscode_settings.py` | VS Code workspace config |

---

**END OF PLAN**