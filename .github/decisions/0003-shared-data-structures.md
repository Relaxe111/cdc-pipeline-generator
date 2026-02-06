# 0003 - Shared Data Structures for Configuration Objects

**Status:** Proposed  
**Date:** 2026-02-06

## Context

Service and server-group configurations are loaded from YAML and accessed as raw `dict[str, Any]` throughout the codebase. This leads to:
- Inconsistent access patterns (`.get()` vs `[]`)
- No compile-time validation of keys
- No runtime validation of structure
- Duplicate parsing logic in multiple files
- Bugs only discovered at runtime deep in the call stack

## Decision

Create shared TypedDict (or dataclass) definitions for all configuration objects:

```python
class ServiceConfig(TypedDict):
    service: str
    server_group: str
    shared: SharedConfig
    customers: NotRequired[list[CustomerConfig]]

class ServerGroupConfig(TypedDict):
    server_group_type: Literal['db-per-tenant', 'db-shared']
    server_type: Literal['mssql', 'postgres']
    database_ref: str
    sources: dict[str, SourceConfig]
```

### Implementation plan:
1. Define types in `cdc_generator/types/` module
2. Add runtime validation in load functions
3. Refactor existing code to use typed objects
4. Catch schema changes at validation, not usage

## Consequences

### Positive
- Type errors caught at development time
- Schema changes propagate automatically
- Consistent access patterns everywhere
- No duplicate validation/parsing logic
- Self-documenting configuration structure
- Easier refactoring and maintenance

### Negative
- Initial refactoring effort across codebase
- Need to keep TypedDicts in sync with YAML schema
- `cast()` needed at YAML load boundary

### Notes
- Start with ServiceConfig and ServerGroupConfig
- Expand to CustomerConfig, EnvironmentConfig, etc. incrementally
- Runtime validation should produce clear error messages
