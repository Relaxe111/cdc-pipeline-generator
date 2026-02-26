# CDC Pipeline Generator - Copilot Instructions

> Router file: keep this minimal and load detailed guides on demand.

---

## 🎯 Project Purpose

**Abstract, reusable library** for generating Redpanda Connect CDC pipelines.

**CRITICAL:** All scripts and logic live here. Implementations (adopus/asma) contain ONLY YAML files and generated artifacts.

| pattern | Architecture | Example |
|-------------------|--------------|---------|
| `db-per-tenant` | One server → N pipelines (1 per customer) | adopus-cdc-pipeline |
| `db-shared` | One server → 1 pipeline (all customers) | asma-cdc-pipeline |

---

## 🏛️ Always-On Invariants

- Use `pattern` for behavior; never branch on service names.
- Treat generator as the only script/code location; implementations stay YAML/artifacts only.
- Route detailed constraints to scoped guides below; do not duplicate policy text here.

### Preprod Phase Policy (Temporary)

- Current phase is **preprod** (no production consumers yet).
- **Do not preserve legacy/backward compatibility by default.**
- Remove deprecated aliases, legacy paths, and obsolete code immediately when touched.
- Prefer clean replacement over compatibility shims.
- This policy is temporary and must be removed when production rollout starts.

---

## 📖 Detailed Instructions

**Load these when working on specific tasks:**

| Guide | When to Use |
|-------|-------------|
| [Coding Guidelines](.github/copilot-instructions-coding-guidelines.md) | Code style, naming, organization, file size limits |
| [Type Safety Rules](.github/copilot-instructions-type-safety.md) | Fixing type errors, adding type hints |
| [Architecture](.github/copilot-instructions-architecture.md) | Understanding patterns, service structure |
| [Development Workflow](.github/copilot-instructions-dev-workflow.md) | Dev container, testing, common tasks |
| [Bento Migration Plan](_docs/architecture/BENTO_MIGRATION_DECISION_PLAN.md) | Runtime switch planning (Redpanda Connect → Bento), decision gates, phased rollout |
| [Redpanda Connect](_docs/redpanda-connect/README.md) | Pipeline templates, Bloblang syntax |
| [Decision Navigation (ADR)](.github/decisions/README.md) | Entry point for past architectural decisions; load targeted ADRs only |

---

## 🧭 Decision Navigation (ADR Path)

Use this path when you need historical rationale or trade-off context:

1. Open `.github/decisions/README.md` (index)
2. Load only the single relevant `000X-*.md` ADR with status `Accepted` or `Proposed`
3. Load additional ADRs only for cross-cutting changes

ADR lifecycle policy (anti-stale):
- ADRs marked `Deprecated` or `Superseded` are not default guidance.
- If an ADR is superseded, follow the replacement ADR (`Superseded by: 00XX-...`).
- Deprecated ADRs must include a one-line replacement reference or explicit `No replacement` note.
- Do not reintroduce deprecated approaches unless the task explicitly requires legacy migration analysis.

| ADR | Topic | Load when task is about... |
|-----|-------|----------------------------|
| `0001-split-copilot-instructions.md` | Instruction split and token efficiency | instruction organization, routing, context size |
| `0002-strict-type-checking.md` | Strict typing toolchain | pyright/mypy/ruff strictness and enforcement |
| `0003-shared-data-structures.md` | Shared typed config models | TypedDict/dataclass contracts for YAML config |
| `0004-runtime-bloblang-validation.md` | Runtime Bloblang validation | validating mappings against sample data/runtime checks |
| `0005-schema-management-and-type-definitions.md` | Schema CLI and type definitions | schema management command design and type catalogs |
| `0006-pattern-cli-audit-and-preprod-compat-policy.md` | Pattern/CLI audit consolidation + preprod cleanup policy | whether to remove legacy paths/aliases now, and how to treat pattern differences vs UX unification |

When any ADR becomes obsolete, keep only a minimal tombstone entry (status + superseded/replacement link).

---

## 🔄 Context Triggers

**Auto-load files based on task type:**

| Task | Files to Load |
|------|--------------|
| Service YAML changes | `services/*.yaml` + `source-groups.yaml` + `validators/manage_service/` |
| Pipeline generation | `pipeline-templates/*.yaml` + `cdc_generator/core/pipeline_generator.py` |
| Server group changes | `source-groups.yaml` + `validators/manage_server_group/` |
| CLI command work | `cdc_generator/cli/commands.py` + `cdc_generator/cli/*.py` |
| Type/lint fixes | `pyrightconfig.json` + `pyproject.toml` + [type-safety](.github/copilot-instructions-type-safety.md) |
| Adding helpers | `cdc_generator/helpers/*.py` (check existing before creating new) |
| Schema validation | `cdc_generator/validators/manage_service/schema_generator/` |
| DB inspection | `cdc_generator/helpers/helpers_mssql.py` + `cdc_generator/validators/*/db_inspector.py` |
| Bloblang/templates | `pipeline-templates/*.yaml` + [Redpanda docs](_docs/redpanda-connect/README.md) |
| Bento migration / runtime switch | `_docs/architecture/BENTO_MIGRATION_DECISION_PLAN.md` + `cdc_generator/core/pipeline_generator.py` + `pipeline-templates/*.yaml` |
| Architecture decisions / ADR rationale | `.github/decisions/README.md` + relevant `000X-*.md` + [architecture](.github/copilot-instructions-architecture.md) |

---

## Router Policy

- Keep this file as a dispatcher only.
- Put detailed, domain-specific policy in `copilot-instructions-*` guides.
- Add new trigger rows instead of adding long narrative sections here.
