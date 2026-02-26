# Coding Guidelines - CDC Pipeline Generator

> **Optimized for AI navigation and human collaboration**

## 🎯 Quick Reference (TL;DR)

| Aspect | Guideline | Why |
|--------|-----------|-----|
| **File Size** | 200-400 lines ideal, **max 600** | AI reads entire file in one operation; easier navigation |
| **Function Size** | 10-50 lines ideal, **max 100** | Single responsibility, easy to reason about |
| **Style** | Functions for transforms, Classes for state | Clear data flow, easier to test |
| **Type Hints** | Required for all new code | AI understands data flow instantly |
| **Docstrings** | Required with examples | Learn intent without reading implementation |
| **Naming** | Descriptive verbs+nouns | Self-documenting, no context needed |

---

## 📁 File Organization

### File Size Limits ⚠️ CRITICAL

**Target: 200-400 lines | Maximum: 600 lines**

**Why this matters:**
- Files <400 lines: AI can read in single operation
- Files >600 lines: Multiple reads required, slows comprehension
- Harder to locate functionality and understand relationships

**When file exceeds 600 lines, refactor immediately:**

```python
# ❌ AVOID - Single 800-line file
# validators/manage_service/service_creator.py (800 lines)
#   - Database inspection (200 lines)
#   - Schema validation (250 lines)
#   - YAML generation (200 lines)
#   - Interactive prompts (150 lines)

# ✅ BETTER - Split into focused modules
# validators/manage_service/
#   ├── __init__.py          - Public API exports
#   ├── db_inspector.py      - Database inspection (200 lines)
#   ├── schema_validator.py  - Schema validation (250 lines)
#   ├── yaml_generator.py    - YAML generation (200 lines)
#   └── interactive.py       - Interactive prompts (150 lines)
```

### Directory Structure

**Current structure (keep this pattern):**

```
cdc_generator/
├── cli/                    # Entry points only (routing to validators)
├── core/                   # Core pipeline generation logic
├── validators/             # Business logic grouped by command
│   ├── manage_service/     # Service management
│   │   └── schema_generator/  # Sub-module for complex logic
│   └── manage_server_group/   # Server group management
├── helpers/                # Pure utility functions
└── templates/              # Static files and templates
```

**Principles:**
- Folders: lowercase with underscores (`manage_server_group`)
- Files: descriptive nouns (`db_inspector.py`, `yaml_writer.py`)
- Organize by **feature**, not by **type** (avoid `utils/`, `models/`, `services/`)
- Each module has **single, clear responsibility**

---

## 🔧 Function and Method Guidelines

### Function Size: 10-50 lines ideal, max 100

**Why:**
- Entire function visible at once
- Clear single responsibility
- Easier to modify without side effects
- Simpler to test

### Good vs Bad Examples

```python
# ✅ GOOD - Focused, single purpose (15 lines)
def extract_service_from_database(db_name: str, pattern: str) -> Optional[str]:
    """
    Extract service name from database using regex pattern.
    
    Args:
        db_name: Database name (e.g., 'calendar_dev')
        pattern: Regex with (?P<service>...) named group
        
    Returns:
        Service name or None if no match
        
    Example:
        >>> extract_service_from_database('calendar_dev', r'^(?P<service>\w+)_')
        'calendar'
    """
    match = re.match(pattern, db_name)
    if not match:
        return None
    
    try:
        return match.group('service')
    except IndexError:
        logger.warning(f"Pattern missing 'service' group: {pattern}")
        return None
```

```python
# ❌ AVOID - Does too much (150+ lines)
def process_database(db_name, config, options, output_dir, ...):
    """Process database and generate output."""
    # Validate input (30 lines)
    # Extract service info (40 lines)
    # Query database schema (50 lines)
    # Format output YAML (40 lines)
    # Write files (20 lines)
    # Log results (20 lines)
```

### Function Composition Pattern

**Prefer composing small functions:**

```python
# ✅ GOOD - Pipeline of focused functions
def create_service_schema(db_name: str, config: Config) -> Path:
    """Create service schema by composing smaller operations."""
    metadata = extract_database_metadata(db_name, config.extraction_pattern)
    schema = query_database_schema(config.connection, db_name)
    yaml_content = format_schema_yaml(schema, metadata)
    output_path = write_schema_file(yaml_content, config.output_dir)
    return output_path
```

---

## 🎨 Code Style: Functional vs Object-Oriented

### Use Functions for Data Transformations

**Pure functions for:**
- Data transformations
- Validation logic
- Filtering/mapping
- Format conversions

```python
# ✅ Pure functions - easy to test, understand, compose
def filter_excluded_databases(
    databases: List[str],
    exclude_patterns: List[str]
) -> List[str]:
    """Filter out databases matching exclusion patterns."""
    return [
        db for db in databases
        if not any(re.search(pattern, db) for pattern in exclude_patterns)
    ]
```

### Use Classes for State and Orchestration

**Classes for:**
- Managing connections
- Coordinating multi-step operations
- Caching/memoization
- Stateful operations

```python
# ✅ Class for managing database connection state
class DatabaseInspector:
    """Inspect database schema and metadata."""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self._connection: Optional[Connection] = None
    
    def __enter__(self) -> 'DatabaseInspector':
        self._connection = self._create_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            self._connection.close()
    
    def list_databases(self) -> List[str]:
        """List all databases on server."""
        # 10-15 lines of query logic
```

**Anti-pattern - Avoid:**

```python
# ❌ AVOID - Class with only static methods (use functions instead)
class StringUtils:
    @staticmethod
    def normalize_name(name: str) -> str:
        return name.lower().replace('-', '_')

# ✅ BETTER
def normalize_name(name: str) -> str:
    """Normalize name to lowercase with underscores."""
    return name.lower().replace('-', '_')
```

---

## 🏷️ Naming Conventions

### Functions: Verb + Noun (Action-oriented)

```python
# ✅ GOOD - Immediately clear
def create_server_group(name: str, config: dict) -> ServerGroup:
def validate_extraction_pattern(pattern: str) -> bool:
def filter_excluded_databases(databases: List[str], patterns: List[str]) -> List[str]:
def extract_service_from_database(db_name: str, pattern: str) -> Optional[str]:
def parse_yaml_config(file_path: Path) -> dict:
def write_pipeline_yaml(pipeline: Pipeline, output_path: Path) -> None:

# ❌ AVOID - Vague
def process(data):
def handle(input):
def do_stuff(config):
```

### Classes: Noun (Thing/Concept)

```python
# ✅ GOOD
class DatabaseInspector:
class PipelineGenerator:
class ServerGroupValidator:
class YamlWriter:

# ❌ AVOID - Manager/Handler anti-pattern
class DatabaseManager:    # Too vague
class ConfigHandler:      # What does it handle?
```

### Variables: Descriptive Nouns

```python
# ✅ GOOD
databases: List[str] = []
excluded_databases: List[str] = []
extraction_pattern: str = r'^(?P<service>\w+)_(?P<env>\w+)$'
service_name: str = 'calendar'

# ❌ AVOID
dbs = []
exc = []
pat = ''
```

### Constants: UPPERCASE_WITH_UNDERSCORES

```python
DEFAULT_EXTRACTION_PATTERN = r'^(?P<service>[a-z_]+?)_(?P<env>\w+)$'
MAX_RETRY_ATTEMPTS = 3
SUPPORTED_DATABASE_TYPES = ['postgres', 'mssql']
SERVER_GROUP_YAML_FILENAME = 'source-groups.yaml'
```

### Files: Descriptive, Matches Content

```python
# ✅ GOOD
db_inspector.py
yaml_writer.py
filters.py
schema_generator.py

# ❌ AVOID
utils.py
helpers.py
common.py
misc.py
```

---

## 📝 Type Hints and Documentation

### Type Hints: Required for All New Code

```python
from typing import List, Dict, Optional, Tuple
from pathlib import Path

# ✅ EXCELLENT - Complete type information
def extract_database_metadata(
    db_name: str,
    extraction_pattern: str,
    environment_aware: bool = True
) -> Optional[Dict[str, str]]:
    """Extract service, environment, and suffix from database name."""

# ❌ AVOID - No type hints
def extract_database_metadata(db_name, extraction_pattern, environment_aware=True):
    """Extract service, environment, and suffix from database name."""
```

### Custom Types for Domain Concepts

```python
from typing import TypedDict, Literal

class DatabaseInfo(TypedDict):
    """Database metadata extracted from server."""
    name: str
    service: str
    environment: str
    table_count: int
    schemas: List[str]

DatabaseType = Literal['postgres', 'mssql']
Environment = Literal['dev', 'stage', 'test', 'prod']
ServerGroupPattern = Literal['db-shared', 'db-per-tenant']
```

### Docstring Format: Google Style with Examples

**Required sections:**
1. One-line summary
2. Args (with types and examples)
3. Returns (with structure)
4. Example (critical for understanding)

```python
def filter_databases_by_pattern(
    databases: List[str],
    pattern: str,
    exclude: bool = False
) -> List[str]:
    """
    Filter database list using regex pattern.
    
    Args:
        databases: List of database names to filter
        pattern: Regular expression pattern to match against
        exclude: If True, return non-matching databases (default: False)
        
    Returns:
        List of database names that match (or don't match) the pattern
        
    Example:
        >>> databases = ['calendar_dev', 'calendar_stage', 'auth_dev']
        >>> filter_databases_by_pattern(databases, r'^calendar_')
        ['calendar_dev', 'calendar_stage']
    """
```

---

## 🔍 Project-Specific Patterns

### 1. Pattern-Agnostic Code

**Always use `pattern` field, never hardcode:**

```python
# ✅ GOOD - Pattern-agnostic
def get_database_for_environment(
    service_config: dict,
    environment: str,
    pattern: str
) -> Optional[str]:
    """Get database name for service in specific environment."""
    if pattern == 'db-shared':
        return service_config.get(environment, {}).get('database')
    elif pattern == 'db-per-tenant':
        return get_tenant_database(service_config, environment)
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

# ❌ AVOID - Hardcoded assumptions
def get_database_for_asma(service_config: dict, environment: str) -> str:
    return service_config[environment]['database']  # Assumes db-shared
```

### 2. YAML Handling - Preserve Comments

```python
from ruamel.yaml import YAML

# ✅ GOOD - Preserves comments and formatting
yaml = YAML()
yaml.preserve_quotes = True
yaml.default_flow_style = False

def update_server_group_yaml(file_path: Path, updates: dict) -> None:
    """Update source-groups.yaml preserving comments."""
    with open(file_path) as f:
        data = yaml.load(f)
    
    data['server_group']['asma'].update(updates)
    
    with open(file_path, 'w') as f:
        yaml.dump(data, f)
```

### 3. PostgreSQL Identifier Quoting ⚠️ ALWAYS

```python
# ✅ GOOD - Quoted identifiers preserve case
def query_table_count(connection: Connection, schema: str, table: str) -> int:
    """Count rows in table with case-sensitive names."""
    query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
    return connection.execute(query).fetchone()[0]

# ❌ AVOID - Unquoted = lowercase = fails on PascalCase tables
def query_table_count(connection: Connection, schema: str, table: str) -> int:
    query = f'SELECT COUNT(*) FROM {schema}.{table}'  # Will fail
```

**Why:** MSSQL uses PascalCase, PostgreSQL is case-sensitive and requires quotes.

### 4. Environment Variables

```python
# ✅ GOOD - YAML stores placeholders
server_config = {
    'host': '${POSTGRES_SOURCE_HOST}',
    'port': '${POSTGRES_SOURCE_PORT}',
    'user': '${POSTGRES_SOURCE_USER}',
    'password': '${POSTGRES_SOURCE_PASSWORD}'
}

# Runtime resolution
def resolve_env_vars(config: dict) -> dict:
    """Replace ${VAR} placeholders with environment values."""
    import os
    import re
    
    resolved = {}
    for key, value in config.items():
        if isinstance(value, str):
            resolved[key] = re.sub(
                r'\$\{(\w+)\}',
                lambda m: os.getenv(m.group(1), m.group(0)),
                value
            )
        else:
            resolved[key] = value
    return resolved
```

### 5. Fish Shell Compatibility ⚠️ CRITICAL

**Never use bash syntax:**

```fish
# ❌ WRONG - Heredocs don't work in Fish
cat << EOF > file.txt
content
EOF

# ✅ CORRECT - Use printf or echo
printf '%s\n' 'line1' 'line2' > file.txt
echo "line1\nline2" > file.txt
```

---

## 🛡️ Error Handling and Logging

### Use helpers_logging for User Messages

```python
from helpers_logging import print_error, print_success, print_info

try:
    result = operation()
    print_success("Operation completed")
except ValueError as e:
    print_error(f"Invalid input: {e}")
    return False
except Exception as e:
    print_error(f"Unexpected error: {e}")
    logger.exception("Full traceback")
    return False
```

### Validate Inputs Early

```python
def process_database(db_name: str, pattern: str) -> Optional[DatabaseInfo]:
    """Process database with validation."""
    # Validate early
    if not db_name:
        raise ValueError("Database name cannot be empty")
    
    if not is_valid_regex(pattern):
        raise ValueError(f"Invalid regex pattern: {pattern}")
    
    # Proceed with logic
    ...
```

---

## 🔐 Security

### Never Hardcode Credentials

```python
# ❌ WRONG
connection = psycopg2.connect(
    host="localhost",
    user="admin",
    password="secret123"
)

# ✅ CORRECT
connection = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
```

### SQL Injection Prevention

```python
# ❌ WRONG - String interpolation
query = f"SELECT * FROM {table} WHERE id = {user_input}"

# ✅ CORRECT - Validate or use parameterized queries
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
    raise ValueError("Invalid table name")
query = f"SELECT * FROM {table} WHERE id = %s"
cursor.execute(query, (user_input,))
```

---

## ⚡ Performance

### Avoid Redundant Operations

```python
# ❌ WRONG - Multiple connections
for table in tables:
    conn = connect_db()
    process(table)
    conn.close()

# ✅ CORRECT - Single connection
conn = connect_db()
for table in tables:
    process(table)
conn.close()
```

### Use Context Managers

```python
# ✅ GOOD
with DatabaseInspector(config) as inspector:
    databases = inspector.list_databases()
    for db in databases:
        tables = inspector.get_tables(db)
```

---

## 📋 Import Organization

```python
# Standard library
import os
import sys
from pathlib import Path

# Third-party
import yaml
import pymssql
from ruamel.yaml import YAML

# Local modules
from helpers_logging import print_info
from service_config import load_service_config

# Package imports
from .config import get_available_services
```

---

## ✅ Code Review Checklist

Before committing:

- [ ] File size <600 lines (ideally <400)
- [ ] Function size <100 lines (ideally <50)
- [ ] Type hints on all function signatures
- [ ] Docstrings with examples on public functions
- [ ] Descriptive names (verb+noun for functions)
- [ ] Single responsibility per function/class/file
- [ ] Pattern-agnostic (no hardcoded asma/adopus logic)
- [ ] PostgreSQL identifiers quoted
- [ ] YAML comments preserved
- [ ] No hardcoded credentials
- [ ] Fish shell compatible (no bash syntax)

---

## 🎯 AI Navigation Optimization

**What helps AI work most effectively:**

1. **Files <400 lines** - Read in one operation
2. **Type hints** - Understand data flow instantly
3. **Docstring examples** - Learn usage patterns
4. **Descriptive names** - Self-documenting code
5. **Small functions** - Single responsibility, easy reasoning
6. **Pure functions** - Easier to compose and test
7. **Clear structure** - Locate functionality quickly

**When in doubt:**
- Readability over cleverness
- Explicitness over brevity
- Simplicity over performance (unless profiling shows bottleneck)
- Small composable pieces over large monolithic blocks
