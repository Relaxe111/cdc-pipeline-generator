# Type Safety Guidelines

## Python Type Safety (CRITICAL)

**⚠️ NEVER use `# type: ignore` or suppression patterns**

When Pylance reports type errors:

1. ✅ **Fix source functions first (our code):** If the error comes from importing our own functions, fix the type hints in the source file
2. ✅ **Add proper type hints:** Use `list[str]`, `Dict[str, Any]`, `Optional[List[Dict[str, Any]]]`, etc.
3. ✅ **Use `cast()` for external libraries only:** Only use `cast()` when dealing with third-party code you can't modify
4. ✅ **Install type stubs:** `types-PyYAML`, `types-pymssql`, etc. for third-party libraries
5. ✅ **Fix import paths:** Use proper package imports (e.g., `from cdc_generator.helpers.helpers_logging import`)
6. ❌ **NEVER suppress errors:** Don't use `type: ignore`, `# noqa`, or try/except ImportError just to silence warnings

---

## Resolution Priority

**Most preferred to least:**

1. **Fix our source code** - Update function signatures with proper types (`Dict[str, Any]` instead of `Dict`)
2. **Install type stubs** - For third-party packages (`types-PyYAML`, `types-pymssql`)
3. **Use `cast()` sparingly** - Only for external library return values you can't control
4. **NEVER suppress** - Never use `type: ignore` or similar suppressions

---

## Benefits

- Catches bugs at development time
- Better IDE support and autocomplete
- Safer refactoring
- Self-documenting code

---

## Examples

### ❌ WRONG - Casting our own function's return type
```python
from cdc_generator.helpers.service_config import load_service_config
config = cast(Dict[str, Any], load_service_config(service))  # Bad - we own this code!
```

### ✅ CORRECT - Fix the source function instead
```python
# In service_config.py:
def load_service_config(service_name: str = "adopus") -> Dict[str, Any]:  # Fixed at source
    ...
```

### ❌ WRONG - Suppressing type errors
```python
from helpers_logging import print_error  # type: ignore
```

### ✅ CORRECT - Proper import path
```python
from cdc_generator.helpers.helpers_logging import print_error
```

### ✅ ACCEPTABLE - Using cast for third-party library
```python
from typing import cast, Any
import some_external_lib
result = cast(dict[str, Any], some_external_lib.get_data())
```

---

## Shared Data Structures (CRITICAL)

**⚠️ ALWAYS create shared, validated structures for configuration objects**

### For service and server-group configurations:

1. **Single Source of Truth** - Create TypedDict or dataclass definitions
2. **Compile-time Validation** - Use type hints for static checking
3. **Runtime Validation** - Validate object keys and types at load time
4. **Centralized Usage** - Pass validated objects, not raw dicts
5. **Catch Changes Early** - Schema changes break at validation, not usage

### Example

```python
# ✅ CORRECT - Shared, validated structure
from typing import TypedDict, Literal, Optional

class ServiceConfig(TypedDict):
    service: str
    server_group: str
    shared: dict[str, Any]
    customers: Optional[list[dict[str, Any]]]
    
class ServerGroupConfig(TypedDict):
    server_group_type: Literal['db-per-tenant', 'db-shared']
    server_type: Literal['mssql', 'postgres']
    database_ref: str
    sources: dict[str, Any]

def load_service_config(service: str) -> ServiceConfig:
    """Load and validate service config"""
    raw = yaml.safe_load(...)
    # Runtime validation here
    validate_service_structure(raw)
    return cast(ServiceConfig, raw)

# ❌ WRONG - Multiple implementations accessing raw dicts
def some_function():
    config = yaml.safe_load(...)  # No validation
    service_name = config['service']  # Could fail
    
def another_function():
    data = yaml.safe_load(...)  # Duplicate loading logic
    server_type = data.get('server_type', 'mssql')  # Different access pattern
```

### Benefits

- ✅ Type errors caught at development time
- ✅ Schema changes propagate automatically
- ✅ Consistent access patterns everywhere
- ✅ No duplicate validation/parsing logic
- ✅ Self-documenting structure
- ✅ Easier refactoring and maintenance

---

## Type Stubs in Dockerfile

All type stubs must be in `Dockerfile.dev`:

```dockerfile
RUN pip install --no-cache-dir \
    types-PyYAML \
    types-pymssql \
    ...
```
