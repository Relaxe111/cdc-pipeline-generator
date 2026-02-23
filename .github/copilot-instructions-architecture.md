# Architecture & Design Guidelines

## Code Design Principles

### SOLID Principles

**Follow these when writing code:**

- ✅ **SOLID Principles** - Follow Open/Closed, Single Responsibility, etc.
- ✅ **DRY (Don't Repeat Yourself)** - Extract common logic into reusable functions
- ✅ **Composability** - Build small, focused functions that combine well
- ✅ **Modular design** - Separate concerns into clear modules/packages
- ✅ **No dead code** - Remove unused functions, imports, or commented-out code
- ✅ **Refactor for reuse** - If adding similar code, refactor existing code to be reusable

### 1. Open/Closed Principle
Open for extension, closed for modification
- Use optional parameters with defaults instead of changing signatures
- Use strategy pattern for varying behavior (e.g., `server_group_type`)

### 2. Single Responsibility
Each function/module does ONE thing well
- If a function does multiple things, split it
- Extract helpers for reusable operations

### 3. Composition over Duplication
Reuse existing code through composition
- Check existing helpers before writing new code
- Extract common patterns into shared utilities

---

## Before Modifying Code

1. Understand what the code currently does
2. Identify all places that depend on it
3. Check for similar existing code that could be refactored/reused
4. Design changes that extend, not replace
5. Test that existing use cases still work

---

## Example Patterns

### ✅ CORRECT - Composable, extensible design
```python
def generate_pipeline(service, customer=None, include_metadata=False):
    """Extends existing functionality via optional parameters"""
    pipeline = _build_base_pipeline(service, customer)
    if include_metadata:
        pipeline = _add_metadata(pipeline)
    return pipeline

def _build_base_pipeline(service, customer):
    """Reusable helper for both db-per-tenant and db-shared"""
    # Common pipeline building logic
```

### ❌ WRONG - Changes existing signature, breaks callers
```python
def generate_pipeline(service, include_metadata):
    # Breaks existing code
```

### ❌ WRONG - Duplication instead of composition
```python
def generate_pipeline_with_metadata(service, customer):
    # Duplicate code instead of extending existing function
```

---

## Service Architecture

### How server_group_type drives pipeline generation

**db-per-tenant:**
- For each customer in service → generates 1 source + 1 sink pipeline
- Each customer has dedicated source database

**db-shared:**
- For entire service → generates 1 source + 1 sink pipeline
- Single shared source database with customer_id filtering

### Required fields in service YAML

**All patterns:**
- `server_group` - Reference to server group name
- `cdc_tables` - Tables for CDC (always at root level)
- `reference` - Reference customer/database for validation

**db-per-tenant only:**
- `customers` - Array of customer configurations

### Example server group

```yaml
server_groups:
  adopus:
    server_group_type: db-per-tenant
    server_type: mssql
    database_ref: AdOpusTest  # For schema inspection
```

---

## Module Organization

### File Size Limit
**Maximum: 500 lines per file**
- Exceeds 500? Refactor into focused modules
- Create package structure (`folder/__init__.py`)
- Single responsibility per module

### Before creating new functions, check existing modules:

- `core/` - Pipeline generation logic
- `helpers/` - Reusable utilities (batch ops, type mapping, DB)
- `validators/` - Schema/config validation
- `cli/` - Command-line interface

**Pattern:** `helpers_{domain}.py` (e.g., `helpers_mssql.py`, `helpers_batch.py`)

---

## Code Quality Standards

- Type hints for all functions
- Docstrings for public APIs
- Descriptive error handling
- Logging (not print, except CLI output)

---

## Generator Scope vs Implementation Scope

**Generator scope:** ALL scripts, pipeline generation logic, CLI commands  
**Implementation scope:** YAML configuration files, generated artifacts, .env files

**Detailed architecture index:** See `_docs/README.md`

**Bento runtime migration plan:** See `_docs/architecture/BENTO_MIGRATION_DECISION_PLAN.md`

---

## Future Plans

**Planned pipeline types:**
- PostgreSQL CDC source pipeline (using `sql_raw` or `sql_select` inputs)
- HTTP webhook receiver with signature validation (HMAC, JWT)
- API polling pipelines with OAuth 2.0

**Other planned features:**
- Multi-sink support (1 source → N sink databases)
- PyPI publication
- Automated testing in CI/CD
