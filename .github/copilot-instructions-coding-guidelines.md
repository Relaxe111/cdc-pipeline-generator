# Coding Guidelines (AI Quick Reference)

> **⚡ Quick lookup for AI agents - Optimized for fast parsing and decision-making**

## 🚨 Type Safety Policy (STRICT)

**NEVER use `# type: ignore` (except `import-untyped` for external packages)**

Instead:

1. **Cast** based on YAML inspection: `cast(Optional[List[str]], data.get('key'))`
2. **Check** before use: `if x is not None and isinstance(x, list):`
3. **Document** expected structure in docstring
4. **Validate** with clear error messages

> **For detailed documentation see [_docs/development/CODING_STANDARDS.md](../_docs/development/CODING_STANDARDS.md)**

## 🎯 Critical Rules

| Rule | Limit | Why |
| ------ | ------- | ----- |
| **File Size** | Max 600 lines (ideal 200-400) | AI reads entire file in one operation |
| **Function Size** | Max 100 lines (ideal 10-50) | Single responsibility, easy reasoning |
| **Type Hints** | Required all parameters/returns | AI understands data flow instantly |
| **NO `type: ignore`** | Write explicit types instead | Forces runtime checks, prevents silent fails |
| **Runtime Validation** | Check YAML/JSON before use | Structure may be invalid/missing |
| **PostgreSQL Quotes** | Always `"schema"."table"` | Preserves MSSQL PascalCase |
| **Pattern-Agnostic** | Never hardcode asma/adopus | Use `server_group_type` field |
| **YAML Preservation** | Use `ruamel.yaml` | Preserve comments/structure |
| **YAML Loader** | Use `helpers/yaml_loader.py` | Use stubbed loader, no direct `yaml` import |
| **No Implicit Concat** | Use `+` or single f-string | ISC001: implicit string concat breaks linting |

## 📁 Structure

```text
cdc_generator/
├── cli/              # Entry points (routing)
├── core/             # Pipeline logic
├── validators/       # Business logic by command
├── helpers/          # Pure utilities
└── templates/        # Static files
```

**Naming:** Files `db_inspector.py` | Functions `create_server_group()` | Classes `DatabaseInspector` | Constants `MAX_RETRIES`

## 🔧 Style

```python
# ✅ Functions for transforms
def filter_excluded(dbs: List[str], patterns: List[str]) -> List[str]:
    return [db for db in dbs if not any(re.search(p, db) for p in patterns)]

# ✅ Classes for state
class DatabaseInspector:
    def __init__(self, config: ServerConfig):
        self._connection = None

# ✅ Docstrings with examples (REQUIRED)
def extract_service(db_name: str, pattern: str) -> Optional[str]:
    """
    Extract service from database name.
    
    Example:
        >>> extract_service('calendar_dev', r'^(?P<service>\w+)_')
        'calendar'
    """
```

## 🔍 Project Patterns

### 0. No Implicit String Concatenation (ALWAYS!)

```python
# ❌ WRONG - implicit string concatenation (ISC001)
print_error(
    "Server not found in"
    " sink group"
)
msg = ("hello " "world")

# ✅ CORRECT - explicit + operator
print_error(
    "Server not found in"
    + " sink group"
)

# ✅ CORRECT - single f-string (preferred when possible)
print_error(f"Server not found in sink group '{name}'")

# ✅ CORRECT - join for lists
msg = " ".join(["Server not found in", "sink group"])
```

### 1. Pattern-Agnostic

```python
# ✅ Use server_group_type
if server_group_type == 'db-shared':
    return config.get(env, {}).get('database')
elif server_group_type == 'db-per-tenant':
    return get_tenant_database(config, env)
```

### 2. PostgreSQL Quoting (ALWAYS!)

```python
# ✅ Quoted
query = f'SELECT "col" FROM "{schema}"."{table}"'
# ❌ Unquoted fails on PascalCase
query = f'SELECT col FROM {schema}.{table}'
```

### 3. YAML Preservation

```python
from ruamel.yaml import YAML
yaml = YAML()
yaml.preserve_quotes = True
yaml.default_flow_style = False
```

### 3.1 YAML Deduplication (Abstract Rule)

When YAML contains repeated mapping blocks, prefer YAML anchors + merge keys to keep files concise and maintainable.

```yaml
# ✅ Prefer: shared defaults + per-entry override
defaults: &shared_defaults
    enabled: true
    retries: 3

item_a:
    <<: *shared_defaults
    source: a

item_b:
    <<: *shared_defaults
    source: b
```

Rules:

- Apply to any YAML file where duplicated blocks repeat 2+ times.
- Keep anchor names descriptive and domain-agnostic unless a domain requires a fixed convention.
- Put only stable/shared keys in the anchor; keep per-item fields explicit next to each entry.
- If one item needs custom behavior, override only that item; do not fork the shared anchor for everyone.
- Preserve deterministic ordering/style so future tooling updates can re-apply the same compact pattern.

### 3.2 YAML Loader (REQUIRED)

Use the project's stubbed loader instead of importing `yaml` directly.

```python
from cdc_generator.helpers.yaml_loader import load_yaml_file

data = load_yaml_file(path)
```

### 4. Fish Shell (No Bash!)

```fish
# ❌ Heredoc doesn't work
cat << EOF > file.txt
# ✅ Use printf
printf '%s\n' 'line1' > file.txt
```

### 5. Environment Variables

```python
# YAML: '${POSTGRES_HOST}'
# Runtime: os.getenv('POSTGRES_HOST')
```

### 6. YAML Type Safety (STRICT - NO `type: ignore`)

```python
from typing import cast, Optional, List, Dict, Any

# ✅ MANDATORY: Inspect YAML + cast + runtime check
def get_patterns(group_data: Dict[str, Any]) -> List[str]:
    """Get exclude patterns from server group.
    
    Expected YAML structure:
        database_exclude_patterns:
          - pattern1
          - pattern2
    """
    # 1. Cast based on YAML inspection
    patterns = cast(Optional[List[str]], group_data.get('database_exclude_patterns'))
    
    # 2. Runtime validation before use
    if patterns is None:
        return []
    
    if not isinstance(patterns, list):
        raise ValueError(f"Invalid patterns: expected list, got {type(patterns)}")
    
    # 3. Validate items
    return [p for p in patterns if isinstance(p, str)]

# ❌❌❌ ABSOLUTELY FORBIDDEN: NEVER EVER USE type: ignore ❌❌❌
# (ONLY exception: import-untyped for external packages without stubs)
def bad_example(data: Dict[str, Any]) -> List[str]:
    return data.get('patterns')  # type: ignore  # ❌❌❌ ABSOLUTELY NO!

# ✅ ONLY exception: External packages without stubs
try:
    import pymssql  # type: ignore[import-untyped]
except ImportError:
    pass
```

**CRITICAL RULE: DO NOT USE `# type: ignore` - FIX THE ROOT CAUSE INSTEAD**

**Checklist for YAML/JSON data:**

- [ ] Inspected source YAML to know structure
- [ ] Documented expected structure in docstring
- [ ] Used `cast()` with explicit type
- [ ] Added `isinstance()` or `is not None` check
- [ ] Handle missing/invalid with clear error
- [ ] NO `type: ignore` (except import-untyped)

### 7. Avoid `Any` - Use Explicit Types

```python
# ❌ Avoid Any - hides data structure
def process(config: Dict[str, Any]) -> Any:
    return config.get('server')

# ✅ Use explicit TypedDict for known structures
class ServerConfig(TypedDict, total=False):
    type: Literal['postgres', 'mssql']
    host: str
    port: Union[str, int]

def process(config: ServerConfig) -> str:
    return config.get('host', '')

# ✅ Runtime validation for external/questionable data sources
def load_config(path: Path) -> ServerConfig:
    """Load and validate config from YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    
    # Validate structure before using
    if not isinstance(raw, dict):
        raise ValueError(f"Expected dict, got {type(raw).__name__}")
    if 'host' not in raw:
        raise ValueError("Missing required field: 'host'")
    if raw.get('type') not in ('postgres', 'mssql', None):
        raise ValueError(f"Invalid server type: {raw.get('type')}")
    
    return cast(ServerConfig, raw)
```

**When to validate at runtime:**

- Loading from YAML/JSON files
- Reading from environment variables
- Receiving API responses
- Processing user input
- Any data crossing trust boundaries

## ✅ Pre-Commit

- [ ] File <600, function <100 lines
- [ ] Type hints on ALL params/returns
- [ ] NO `type: ignore` (except import-untyped)
- [ ] YAML data: cast() + isinstance() checks
- [ ] Docstrings with expected YAML structure
- [ ] Descriptive names (verb+noun)
- [ ] Single responsibility
- [ ] Pattern-agnostic
- [ ] PostgreSQL identifiers quoted
- [ ] No credentials in code

## 📖 Full Docs

See **[_docs/development/CODING_STANDARDS.md](../_docs/development/CODING_STANDARDS.md)** for:

- Detailed examples
- Error handling patterns  
- Security best practices
- Performance optimization
- Complete code review checklist
