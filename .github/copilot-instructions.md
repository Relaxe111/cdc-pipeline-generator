# CDC Pipeline Generator - Copilot Instructions

## 🎯 Project Purpose

**Abstract, reusable library** for generating Redpanda Connect CDC pipelines.

**CRITICAL:** Must remain **pattern-agnostic** to support both implementations:

| server_group_type | Architecture | Example |
|-------------------|--------------|---------|
| `db-per-tenant` | One server → N pipelines (1 per customer) | adopus-cdc-pipeline |
| `db-shared` | One server → 1 pipeline (all customers) | asma-cdc-pipeline |

**Generator scope:** Pipeline generation logic  
**Implementation scope:** Connections, credentials, infrastructure  
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
| `2-services/{service}.yaml` | Service config | ⚠️ USE CLI |
| `3-pipeline-templates/*.yaml` | Templates with `{{VARS}}` | ✅ EDIT |
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
