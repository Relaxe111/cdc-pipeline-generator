# CDC Pipeline Generator - Copilot Instructions

> **📚 Full Documentation:** For detailed instructions, reference the topic-specific guides below when needed.

---

## 🎯 Project Purpose

**Abstract, reusable library** for generating Redpanda Connect CDC pipelines.

**CRITICAL:** All scripts and logic live here. Implementations (adopus/asma) contain ONLY YAML files and generated artifacts.

| server_group_type | Architecture | Example |
|-------------------|--------------|---------|
| `db-per-tenant` | One server → N pipelines (1 per customer) | adopus-cdc-pipeline |
| `db-shared` | One server → 1 pipeline (all customers) | asma-cdc-pipeline |

---

## 🏛️ CRITICAL Rules

### 1. Pattern-Agnostic Code
**ALWAYS use `server_group_type` to drive behavior:**

```python
# ✅ CORRECT
if server_group.server_group_type == "db-per-tenant":
    for customer in service.customers:
        generate_pipeline(customer.database_name, ...)

# ❌ WRONG
if service.name == "adopus":  # Never check service names!
```

### 2. Never Break Existing Functionality
- ✅ Add new functionality alongside existing (extend, don't replace)
- ✅ New parameters must be optional with defaults
- ✅ Test both db-per-tenant AND db-shared patterns
- ❌ Don't change existing function signatures
- ❌ Don't remove features without explicit request

### 3. Shared Data Structures
**Create TypedDict/dataclass for configs:**
```python
# ✅ CORRECT - Single source of truth
class ServiceConfig(TypedDict):
    service: str
    server_group: str
    shared: dict[str, Any]

def load_service_config(service: str) -> ServiceConfig:
    raw = yaml.safe_load(...)
    validate_service_structure(raw)  # Runtime validation
    return cast(ServiceConfig, raw)

# ❌ WRONG - Multiple raw dict accesses
config = yaml.safe_load(...)  # No validation, no types
```

### 4. Type Safety
- ✅ Fix source code type hints (our functions)
- ✅ Install type stubs for external libraries
- ✅ Use `cast()` only for external libraries
- ❌ **NEVER use `# type: ignore`**

---

## 📖 Detailed Instructions

**Load these when working on specific tasks:**

| Guide | When to Use |
|-------|-------------|
| [Coding Guidelines](.github/copilot-coding-guidelines.md) | Code style, naming, organization, file size limits |
| [Type Safety Rules](.github/instructions-type-safety.md) | Fixing type errors, adding type hints |
| [Architecture](.github/instructions-architecture.md) | Understanding patterns, service structure |
| [Development Workflow](.github/instructions-dev-workflow.md) | Dev container, testing, common tasks |
| [Redpanda Connect](_docs/redpanda-connect/README.md) | Pipeline templates, Bloblang syntax |

---

## 🚀 Quick Reference

**Module Structure:**
- `core/` - Pipeline generation
- `helpers/` - Reusable utilities
- `validators/` - Config validation
- `cli/` - Command-line interface

**Design Checklist:**
- [ ] Uses `server_group_type`, not service names
- [ ] Backward compatible (optional params)
- [ ] TypedDict for config structures
- [ ] Runtime validation at load time
- [ ] Type hints (no `# type: ignore`)
- [ ] Tested with both patterns

**File Limit:** 500 lines max - refactor into modules if exceeded

---

## 💡 Common Pitfalls

❌ Hardcoding service/implementation names  
❌ Changing existing function signatures  
❌ Accessing raw YAML dicts without validation  
❌ Using `# type: ignore` instead of fixing types  
❌ Duplicating logic instead of extracting helpers  

✅ Use `server_group_type` field  
✅ Add optional parameters with defaults  
✅ Create shared TypedDict structures  
✅ Fix type hints at source  
✅ Extract common patterns into helpers  
