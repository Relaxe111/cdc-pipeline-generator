# CDC Pipeline Generator - Copilot Instructions

## 📖 Documentation References

**Before writing or modifying code, always read:**
- **[Coding Guidelines](.github/copilot-coding-guidelines.md)** - Code organization, style, naming conventions, function/file size limits, type hints, and patterns optimized for AI navigation

**For Redpanda Connect / Bloblang transformations:**
- **[Redpanda Connect Docs](_docs/redpanda-connect/README.md)** - Complete Bloblang reference and pipeline patterns

| Document | Use Case |
|----------|----------|
| [Bloblang Fundamentals](_docs/redpanda-connect/01-BLOBLANG-FUNDAMENTALS.md) | Core syntax: assignment, variables, conditionals, maps |
| [Bloblang Methods](_docs/redpanda-connect/02-BLOBLANG-METHODS.md) | String, number, timestamp, array, object, JWT methods |
| [Bloblang Functions](_docs/redpanda-connect/03-BLOBLANG-FUNCTIONS.md) | Built-in functions: uuid, now, env, content, metadata |
| [HTTP Inputs](_docs/redpanda-connect/04-HTTP-INPUTS.md) | Webhook receivers, JWT/signature validation, API polling |
| [SQL Patterns](_docs/redpanda-connect/05-SQL-PATTERNS.md) | PostgreSQL integration, UPSERT, batching, connection pools |
| [Error Handling](_docs/redpanda-connect/06-ERROR-HANDLING.md) | try/catch, DLQ, fallback outputs, error routing |
| [Pipeline Patterns](_docs/redpanda-connect/07-PIPELINE-PATTERNS.md) | Complete CDC pipeline examples, multi-input/output |
| [**Pipeline Templating**](_docs/redpanda-connect/08-PIPELINE-TEMPLATING.md) | **Template structure, .blobl files, generation flow** |

---

## 🎯 Project Purpose

**Abstract, reusable library** for generating Redpanda Connect CDC pipelines.

**CRITICAL:** All scripts and logic live here. Implementations (adopus/asma) contain ONLY YAML files and generated artifacts.

**Generator provides:**
- All `cdc` CLI commands (`manage-service`, `manage-server-group`, etc.)
- Pipeline generation and validation
- Database inspection and schema management
- Configuration helpers and utilities

**Must remain pattern-agnostic** to support both implementations:

| server_group_type | Architecture | Example |
|-------------------|--------------|---------|
| `db-per-tenant` | One server → N pipelines (1 per customer) | adopus-cdc-pipeline |
| `db-shared` | One server → 1 pipeline (all customers) | asma-cdc-pipeline |

**Generator scope:** ALL scripts, pipeline generation logic, CLI commands  
**Implementation scope:** YAML configuration files, generated artifacts, .env files  
**Detailed architecture:** See `_docs/ARCHITECTURE.md`

---

## 🏛️ Abstraction Requirements (CRITICAL)

**ALWAYS use `server_group_type` to drive behavior:**

```python
# ✅ CORRECT - Pattern-agnostic
if server_group.server_group_type == "db-per-tenant":
    for customer in service.customers:
        generate_pipeline(customer.database_name, ...)
elif server_group.server_group_type == "db-shared":
    generate_pipeline(service.database_name, ...)

# ❌ WRONG - Hardcoded assumptions
if service.name == "adopus":  # Never check service names!
```

**Design checklist:**
- [ ] Uses `server_group_type` field, not service/implementation names
- [ ] Works with both db-per-tenant and db-shared examples
- [ ] No hardcoded connections, credentials, or environment details
- [ ] Test against both pattern examples before committing

---

## ⚠️ Modifying Existing Code (CRITICAL)

**When updating, modifying, or extending existing generator code:**

**NEVER break or remove existing functionality unless explicitly requested**

- ✅ **Preserve all existing behavior** - Existing features must continue to work
- ✅ **Add new functionality alongside existing** - Extend, don't replace
- ✅ **Maintain backward compatibility** - Old configurations must still work
- ✅ **Test both patterns** - Verify db-per-tenant AND db-shared still work
- ✅ **Add, don't subtract** - New parameters should be optional with defaults

**Write clean, maintainable code from the start:**

- ✅ **SOLID Principles** - Follow Open/Closed, Single Responsibility, etc.
- ✅ **DRY (Don't Repeat Yourself)** - Extract common logic into reusable functions
- ✅ **Composability** - Build small, focused functions that combine well
- ✅ **Modular design** - Separate concerns into clear modules/packages
- ✅ **No dead code** - Remove unused functions, imports, or commented-out code
- ✅ **Refactor for reuse** - If adding similar code, refactor existing code to be reusable

**Design principles:**

1. **Open/Closed Principle** - Open for extension, closed for modification
   - Use optional parameters with defaults instead of changing signatures
   - Use strategy pattern for varying behavior (e.g., `server_group_type`)
   
2. **Single Responsibility** - Each function/module does ONE thing well
   - If a function does multiple things, split it
   - Extract helpers for reusable operations
   
3. **Composition over Duplication** - Reuse existing code through composition
   - Check existing helpers before writing new code
   - Extract common patterns into shared utilities

**Before modifying:**
1. Understand what the code currently does
2. Identify all places that depend on it
3. Check for similar existing code that could be refactored/reused
4. Design changes that extend, not replace
5. Test that existing use cases still work

**Example:**
```python
# ✅ CORRECT - Composable, extensible design
def generate_pipeline(service, customer=None, include_metadata=False):
    """Extends existing functionality via optional parameters"""
    pipeline = _build_base_pipeline(service, customer)
    if include_metadata:
        pipeline = _add_metadata(pipeline)
    return pipeline

# ✅ CORRECT - Extract common logic to avoid duplication
def _build_base_pipeline(service, customer):
    """Reusable helper for both db-per-tenant and db-shared"""
    # Common pipeline building logic

# ❌ WRONG - Changes existing signature, breaks callers
def generate_pipeline(service, include_metadata):
    # Breaks existing code

# ❌ WRONG - Duplication instead of composition
def generate_pipeline_with_metadata(service, customer):
    # Duplicate code instead of extending existing function
```

---

## 📐 Coding Standards

### File Size Limit
**Maximum: 500 lines per file**
- Exceeds 500? Refactor into focused modules
- Create package structure (`folder/__init__.py`)
- Single responsibility per module

### Module Organization
**Before creating new functions, check existing modules:**
- `core/` - Pipeline generation logic
- `helpers/` - Reusable utilities (batch ops, type mapping, DB)
- `validators/` - Schema/config validation
- `cli/` - Command-line interface

**Pattern:** `helpers_{domain}.py` (e.g., `helpers_mssql.py`, `helpers_batch.py`)

### Code Quality
- Type hints for all functions
- Docstrings for public APIs
- Descriptive error handling
- Logging (not print, except CLI output)

### Python Type Safety (CRITICAL)
**⚠️ NEVER use `# type: ignore` or suppression patterns**

When Pylance reports type errors:
1. ✅ **Fix source functions first (our code):** If the error comes from importing our own functions, fix the type hints in the source file
2. ✅ **Add proper type hints:** Use `list[str]`, `Dict[str, Any]`, `Optional[List[Dict[str, Any]]]`, etc.
3. ✅ **Use `cast()` for external libraries only:** Only use `cast()` when dealing with third-party code you can't modify
4. ✅ **Install type stubs:** `types-PyYAML`, `types-pymssql`, etc. for third-party libraries
5. ✅ **Fix import paths:** Use proper package imports (e.g., `from cdc_generator.helpers.helpers_logging import`)
6. ❌ **NEVER suppress errors:** Don't use `type: ignore`, `# noqa`, or try/except ImportError just to silence warnings

**Resolution Priority (most preferred to least):**
1. **Fix our source code** - Update function signatures with proper types (`Dict[str, Any]` instead of `Dict`)
2. **Install type stubs** - For third-party packages (`types-PyYAML`, `types-pymssql`)
3. **Use `cast()` sparingly** - Only for external library return values you can't control
4. **NEVER suppress** - Never use `type: ignore` or similar suppressions

**Benefits of proper typing:**
- Catches bugs at development time
- Better IDE support and autocomplete
- Safer refactoring
- Self-documenting code

**Example:**
```python
# ❌ WRONG - Casting our own function's return type
from cdc_generator.helpers.service_config import load_service_config
config = cast(Dict[str, Any], load_service_config(service))  # Bad - we own this code!

# ✅ CORRECT - Fix the source function instead
# In service_config.py:
def load_service_config(service_name: str = "adopus") -> Dict[str, Any]:  # Fixed at source

# ❌ WRONG - Suppressing type errors
from helpers_logging import print_error  # type: ignore

# ✅ CORRECT - Proper import path
from cdc_generator.helpers.helpers_logging import print_error

# ✅ ACCEPTABLE - Using cast for third-party library (when type stub unavailable)
from typing import cast, Any
import some_external_lib
result = cast(dict[str, Any], some_external_lib.get_data())
```

**Type stubs in Dockerfile:**
All type stubs must be in `Dockerfile.dev`:
```dockerfile
RUN pip install --no-cache-dir \
    types-PyYAML \
    types-pymssql \
    ...
```

---

## 🏗️ Service Architecture

**How server_group_type drives pipeline generation:**

**db-per-tenant:**
- For each customer in service → generates 1 source + 1 sink pipeline
- Each customer has dedicated source database

**db-shared:**
- For entire service → generates 1 source + 1 sink pipeline
- Single shared source database with customer_id filtering

**Required fields in service YAML:**

All patterns:
- `server_group` - Reference to server group name
- `cdc_tables` - Tables for CDC (always at root level)
- `reference` - Reference customer/database for validation

db-per-tenant only:
- `customers` - Array of customer configurations

**Example server group:**
```yaml
server_groups:
  adopus:
    server_group_type: db-per-tenant
    server_type: mssql
    database_ref: AdOpusTest  # For schema inspection
```

---

## ⚙️ Development Environment

**This generator is the main dev environment:**

**Dev container location:** This project (`cdc-pipeline-generator/`)  
**Mounted implementations:** `/implementations/adopus/`, `/implementations/asma/`  
**Network access:** Host mode - access to implementation infrastructure

**To enter dev container:**
```bash
cd ~/carasent/cdc-pipeline-generator
docker compose exec dev fish
```

**Inside container:**
- `/workspace/` - This generator (editable)
- `/implementations/adopus/` - Adopus implementation (mounted rw)
- `/implementations/asma/` - Asma implementation (mounted rw)

**Edit and test workflow:**
1. Edit generator code: `/workspace/cdc_generator/...`
2. Test against adopus: `cd /implementations/adopus && cdc generate`
3. Verify output in `generated/pipelines/`

---

## 🗂️ Implementation File Structure

**What generator creates/expects:**

| Path | Purpose | Edit? |
|------|---------|-------|
| `server-groups.yaml` | Server group definitions | ⚠️ USE CLI |
| `services/{service}.yaml` | Service config | ⚠️ USE CLI |
| `pipeline-templates/*.yaml` | Templates with `{{VARS}}` | ✅ EDIT |
| `generated/pipelines/` | Auto-generated | ❌ READ-ONLY |

---

## 🎯 Common Tasks

**Add table:**
```bash
cd /implementations/adopus
cdc manage-service --service adopus --add-table Actor --primary-key actno
cdc generate
```

**Inspect database:**
```bash
cdc manage-service --service adopus --inspect --schema dbo
```

**Test generator changes:**
```bash
# Edit code in /workspace/
cd /implementations/adopus
cdc generate  # Uses your modified generator code
```

---

## 📚 Reference Documentation

- **Architecture details:** `_docs/ARCHITECTURE.md`
- **Pattern examples:** `examples/db-per-tenant/`, `examples/db-shared/`
- **API documentation:** `_docs/`
- **Implementation guides:** Implementation repos' copilot-instructions

---

## 🚧 Future Plans

**Planned pipeline types (see [Redpanda Connect Docs](_docs/redpanda-connect/README.md)):****
- PostgreSQL CDC source pipeline (using `sql_raw` or `sql_select` inputs)
- HTTP webhook receiver with signature validation (HMAC, JWT)
- API polling pipelines with OAuth 2.0

**Other planned features:**
- Multi-sink support (1 source → N sink databases)
- PyPI publication
- Automated testing in CI/CD
