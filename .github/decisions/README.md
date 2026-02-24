# Architecture Decision Records

This directory contains lightweight Architecture Decision Records (ADRs) documenting significant decisions made in the CDC pipeline generator project.

## Quality Bar (Concise + Practical)

Each ADR should stay short and implementation-useful:
- state the concrete problem
- record the decision in actionable terms
- define scope and when to load/use the ADR
- summarize impact in a few bullets
- avoid long examples and speculative implementation dumps

## Status Lifecycle (Keep Context Fresh)

- `Accepted`: active guidance
- `Proposed`: candidate guidance (validate before broad adoption)
- `Deprecated`: no longer recommended; keep only short rationale + replacement pointer
- `Superseded`: replaced by newer ADR; include `Superseded by: 00XX-...`

Default load policy:
1. Prefer `Accepted`
2. Use `Proposed` only when actively evaluating that direction
3. Do not load `Deprecated`/`Superseded` unless doing migration or historical analysis

## Format

Each decision is a short markdown file:

```
NNNN-short-title.md
```

## Template

Use [0000-template.md](0000-template.md) as a starting point.

Load pattern:
1. Start with this README
2. Open only the single relevant ADR (`000X-*.md`)
3. Open additional ADRs only if cross-cutting

## Decisions

| # | Decision | Status | Date |
|---|----------|--------|------|
| 0001 | [Split copilot instructions for token efficiency](0001-split-copilot-instructions.md) | Accepted | 2026-02-06 |
| 0002 | [Strict Python type checking with pyrightconfig](0002-strict-type-checking.md) | Accepted | 2026-02-06 |
| 0003 | [Shared data structures for config objects](0003-shared-data-structures.md) | Proposed | 2026-02-06 |
| 0004 | [Runtime Bloblang validation with sample data](0004-runtime-bloblang-validation.md) | Proposed | 2026-02-08 |
| 0005 | [Schema management CLI and type definitions](0005-schema-management-and-type-definitions.md) | Proposed | 2026-02-10 |
| 0006 | [Pattern/CLI audit consolidation and preprod compatibility policy](0006-pattern-cli-audit-and-preprod-compat-policy.md) | Accepted | 2026-02-24 |
