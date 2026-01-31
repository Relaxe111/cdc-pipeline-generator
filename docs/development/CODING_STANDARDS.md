# CDC Pipeline - Coding Standards

## File Size Limits ⚠️ CRITICAL

**Maximum file size: 500 lines of code**

When a file exceeds 500 lines:
1. **Immediately refactor** into smaller, focused modules
2. Create a package structure if needed (use `__init__.py`)
3. Each module should have a **single, clear responsibility**
4. Document the module structure at the top of the main file

### Example Structure

```
script.py (main CLI - <200 lines)
└── script_package/
    ├── __init__.py           - Public API exports
    ├── module1.py            - Feature A (<500 lines)
    ├── module2.py            - Feature B (<500 lines)
    └── module3.py            - Feature C (<500 lines)
```

## Module Organization

### Package Structure
- **Main file**: Argument parsing + routing only (keep minimal)
- **Modules**: Group related functionality by domain
- **`__init__.py`**: Export only public API functions
- **Naming**: Use descriptive names (`config.py`, `validation.py`, not `utils.py`)

### Single Responsibility Principle
Each module should have ONE clear purpose:
- ✅ `validation.py` - Config validation logic
- ✅ `mssql_inspector.py` - Database inspection
- ❌ `helpers.py` - Too generic, split by domain

### Import Organization
```python
# Standard library
import os
import sys

# Third-party
import yaml
import pymssql

# Local modules
from helpers_logging import print_info
from service_config import load_service_config

# Package imports
from .config import get_available_services
```

## Python Code Style

### Fish Shell Compatibility
**⚠️ CRITICAL: Never use bash syntax in terminal commands**

```bash
# ❌ WRONG - Heredocs don't work in Fish
cat << EOF > file.txt
content
EOF

# ✅ CORRECT - Use printf or echo
printf '%s\n' 'line1' 'line2' > file.txt
```

### Function Documentation
```python
def function_name(arg1: str, arg2: int) -> bool:
    """Short description.
    
    Longer explanation if needed.
    
    Args:
        arg1: Description of arg1
        arg2: Description of arg2
        
    Returns:
        Description of return value
    """
```

### Error Handling
```python
# Use helpers_logging for user-facing messages
from helpers_logging import print_error, print_success

try:
    result = operation()
    print_success("Operation completed")
except Exception as e:
    print_error(f"Operation failed: {e}")
    return False
```

## Database Interactions

### PostgreSQL Quoting (ALWAYS)
```sql
-- ✅ CORRECT - Quote all identifiers
SELECT "actno", "Navn" FROM avansas."Actor" WHERE "actno" = 123;

-- ❌ WRONG - Relation does not exist
SELECT actno FROM avansas.Actor;
```

**Why:** MSSQL uses PascalCase, PostgreSQL is case-sensitive and needs quotes.

### SQL Query Formatting
```python
query = """
    SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = '{schema}'
        AND c.TABLE_NAME = '{table}'
    ORDER BY c.ORDINAL_POSITION
"""
```

## Configuration Files

### YAML Structure
- Use 2-space indentation
- No tabs
- Hierarchical inheritance: `environments → environments.<env> → customers[].environments.<env>`
- Document complex structures with comments

### Service Configuration
```yaml
service: adopus
mode: multi-tenant
reference: avansas  # Reference customer for schema validation

shared:
  cdc_tables:
    - schema: dbo
      tables:
        # Use string format when no extra properties needed
        - Actor
        - Fraver
        # Or object format when properties are needed
        - name: TableName
          primary_key: id
          ignore_columns: [col1, col2]
```

## Testing Requirements

### Before Committing
1. **Validate all commands work**
   ```bash
   cdc manage-service --service adopus --validate-config
   cdc manage-service --service adopus --inspect-mssql --schema dbo
   ```

2. **Check file sizes**
   ```bash
   find scripts -name "*.py" -exec wc -l {} + | awk '$1 > 500 {print}'
   ```

3. **Run validation**
   ```bash
   cdc validate
   ```

## Git Workflow

### Commit Messages
```
feat: Add schema validation for adopus service
fix: Correct hierarchical inheritance validation
refactor: Split manage-service.py into modular package
docs: Update CLI usage examples
```

### File Organization
- Keep related files together in packages
- Use `scripts/` for executable Python scripts
- Use `docs/` for documentation
- Use `generated/` for auto-generated files (gitignore these)

## Performance Considerations

### Avoid Redundant Operations
```python
# ❌ WRONG - Multiple DB connections
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

### Lazy Loading
```python
# Only load when needed
def get_tables():
    if not hasattr(get_tables, '_cache'):
        get_tables._cache = load_from_db()
    return get_tables._cache
```

## Documentation Standards

### File Headers
```python
#!/usr/bin/env python3
"""
Brief description of the file's purpose.

Usage examples if it's a CLI tool.

Module Structure (for packages):
    package_name/
    ├── module1.py - Description
    ├── module2.py - Description
    └── module3.py - Description
"""
```

### README Files
- Every package should have a README
- Include usage examples
- Document dependencies
- Explain key concepts

## Dependencies

### Adding New Dependencies
1. Add to `requirements-dev.txt`
2. Document why it's needed
3. Check for conflicts with existing packages
4. Prefer standard library when possible

### Optional Dependencies
```python
# Check availability and provide fallback
try:
    import pymssql
    HAS_PYMSSQL = True
except ImportError:
    HAS_PYMSSQL = False

# Use in code
if not HAS_PYMSSQL:
    print_error("pymssql not installed - use: pip install pymssql")
    return False
```

## Security

### Credentials
- **Never hardcode** credentials in files
- Use environment variables: `${VAR}` in YAML, `os.getenv()` in Python
- Use `.env` files for local development (gitignored)

### SQL Injection Prevention
```python
# ❌ WRONG - String interpolation
query = f"SELECT * FROM {table} WHERE id = {user_input}"

# ✅ CORRECT - Use parameterized queries when possible
# Or validate input thoroughly before string interpolation
if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
    raise ValueError("Invalid table name")
```

## Code Review Checklist

- [ ] File size under 500 lines
- [ ] Single responsibility per module
- [ ] Proper error handling with user-friendly messages
- [ ] PostgreSQL identifiers quoted
- [ ] Fish shell compatibility (no bash syntax)
- [ ] Documentation updated
- [ ] All commands tested
- [ ] No hardcoded credentials
- [ ] Type hints on function signatures

## References

- [Project Structure](PROJECT_STRUCTURE.md)
- [CLI Documentation](CDC_CLI.md)
- [Development Container](DEV_CONTAINER.md)
- [Environment Variables Guide](ENV_VARIABLES_GUIDE.md)
