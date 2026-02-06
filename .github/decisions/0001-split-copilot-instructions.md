# 0001 - Split Copilot Instructions for Token Efficiency

**Status:** Accepted  
**Date:** 2026-02-06

## Context

Copilot instructions were loaded into context on every message (~4,000-5,000 tokens combined). Most content was rarely needed for any single task (Fish shell rules, PostgreSQL quoting, detailed type examples).

## Decision

Split monolithic instruction files into:
- **Core file** (~80-114 lines) - Always loaded, contains critical rules only
- **Topic files** - Loaded on-demand via context triggers

### Workspace root:
- `copilot-instructions.md` (80 lines) → always loaded
- `instructions-dev-container.md` → when running commands
- `instructions-type-safety-yaml.md` → when handling YAML types

### Generator:
- `copilot-instructions.md` (114 lines) → always loaded
- `instructions-type-safety.md` → when fixing types
- `instructions-architecture.md` → when designing features
- `instructions-dev-workflow.md` → when testing/deploying

## Consequences

### Positive
- 73% token reduction per message
- Core rules have stronger signal (less dilution)
- Easier to maintain individual topic files
- More context budget for actual code

### Negative
- AI must actively load topic files when needed (mitigated by context triggers)
- Slightly more files to maintain

### Notes
- Context triggers added to both instruction files to guide auto-loading
- All detailed content preserved, just reorganized
