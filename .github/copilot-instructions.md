# CDC Pipeline Generator - Copilot Instructions

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
**Detailed architecture:** See `docs/ARCHITECTURE.md`

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

- **Architecture details:** `docs/ARCHITECTURE.md`
- **Pattern examples:** `examples/db-per-tenant/`, `examples/db-shared/`
- **API documentation:** `docs/`
- **Implementation guides:** Implementation repos' copilot-instructions

---

## 🚧 Future Plans

Not yet implemented:
- Field mappings + transformations
- Fan-out pattern (1 record → N records)
- Multi-sink support (1 source → N sink databases)
- PyPI publication
- Automated testing in CI/CD
