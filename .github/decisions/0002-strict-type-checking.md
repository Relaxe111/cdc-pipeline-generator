# 0002 - Strict Python Type Checking with pyrightconfig.json

**Status:** Accepted  
**Date:** 2026-02-06

## Context

Python type checking was configured only via VS Code settings (`python.analysis.*`), making it non-portable and invisible to CI. MyPy and Ruff had minimal configuration.

## Decision

1. Created `pyrightconfig.json` with strict mode and explicit error rules
2. Enhanced `pyproject.toml` with strict MyPy (`strict = true`) and expanded Ruff rules (17 categories)
3. Removed Pylance settings from `.vscode/settings.json` (now in pyrightconfig.json)

### Tools and strictness:
- **Pylance/Pyright:** `typeCheckingMode: strict`, all `reportUnknown*` as errors
- **MyPy:** `strict = true`, `disallow_any_explicit`, `warn_unreachable`
- **Ruff:** E, W, F, I, N, UP, ANN, ASYNC, B, A, C4, RET, SIM, ARG, PTH, PL, RUF

## Consequences

### Positive
- Type errors caught at development time
- Config portable across editors (pyrightconfig.json)
- Consistent enforcement via CI-ready tools
- Auto-fixed 2,233 formatting issues on first run

### Negative
- 1,167 remaining Ruff errors + 316 MyPy errors to fix gradually
- Stricter rules may slow initial development on new code

### Notes
- Fix errors incrementally, prioritize high-value files first
- Install `types-psycopg2` for missing type stubs
