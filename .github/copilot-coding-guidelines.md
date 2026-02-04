# Coding Guidelines (AI Quick Reference)

> **For detailed documentation see [_docs/development/CODING_STANDARDS.md](../_docs/development/CODING_STANDARDS.md)**

## 🎯 Critical Rules

| Rule | Limit | Why |
|------|-------|-----|
| **File Size** | Max 600 lines (ideal 200-400) | AI reads entire file in one operation |
| **Function Size** | Max 100 lines (ideal 10-50) | Single responsibility, easy reasoning |
| **Type Hints** | Required all new code | AI understands data flow instantly |
| **Avoid `Any`** | Use TypedDict/explicit types | Self-documenting, catches bugs early |
| **Runtime Validation** | Validate external data | YAML/JSON/API data needs structure checks |
| **PostgreSQL Quotes** | Always `"schema"."table"` | Preserves MSSQL PascalCase |
| **Pattern-Agnostic** | Never hardcode asma/adopus | Use `server_group_type` field |
| **YAML Preservation** | Use `ruamel.yaml` | Preserve comments/structure |

## 📁 Structure

```
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

### 6. Type Safety - Avoid `Any`
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
- [ ] Type hints + docstrings with examples
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
