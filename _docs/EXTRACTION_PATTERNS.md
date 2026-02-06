# Extraction Patterns - Multi-Pattern Database Name Decomposition

## Overview

Extraction patterns allow you to decompose database names into **service** and **environment** identifiers using ordered regex patterns with named capture groups.

**Key features:**
- **Ordered matching**: Patterns tried in order, first match wins
- **Named capture groups**: `(?P<service>...)` and `(?P<env>...)`
- **Fixed environments**: Override captured env with pattern-specific value
- **Strip patterns**: Remove naming artifacts like `_db` from service names using regex
- **Per-pattern env_mapping**: Transform environment names (e.g., `prod_adcuris` → `prod-adcuris`)
- **Self-documenting**: Description field explains each pattern
- **Backward compatible**: Old single `extraction_pattern` field still works

---

## How It Works

### Pattern Priority

Patterns are tried **in order**, first match wins:

1. **Most specific patterns first** (e.g., `_db_prod_adcuris`)
2. **General patterns next** (e.g., `{service}_{env}`)
3. **Fallback patterns last** (e.g., single-word databases)

### Named Capture Groups

**Required:** `(?P<service>...)` - Captures service name
**Optional:** `(?P<env>...)` - Captures environment name

### Fixed Environment

If pattern has `env` field, it **overrides** captured `(?P<env>)` group:

```yaml
- pattern: '^(?P<service>\w+)_db_prod_adcuris$'
  env: prod_adcuris  # Fixed: always prod_adcuris
```

### Strip Patterns

Remove naming artifacts from captured service name using regex:

```yaml
- pattern: '^(?P<service>\w+)_db_prod_adcuris$'
  env: prod_adcuris
  strip_patterns: ['_db']  # Removes _db anywhere: auth_db → auth
  env_mapping:
    prod_adcuris: prod-adcuris  # Transform environment name
```

**CRITICAL: Capture the part you want to strip!**

```yaml
# ✅ CORRECT - Includes adopus_db_ in capture group
- pattern: '^(?P<service>adopus_db_\w+)_(?P<env>\w+)$'
  strip_patterns: ['_db']
  # adopus_db_directory_dev → captures service=adopus_db_directory → strips to adopus_directory

# ❌ WRONG - adopus_db_ is literal prefix, not captured
- pattern: '^adopus_db_(?P<service>\w+)_(?P<env>\w+)$'
  strip_patterns: ['_db']
  # adopus_db_directory_dev → captures service=directory → strip does nothing → wrong!
```

**Processing order:**
1. Regex matches: `auth_db_prod_adcuris`
2. Capture service: `auth_db`
3. Strip patterns: `auth_db` → `auth` (removes `_db` anywhere in string)
4. Use fixed env: `prod_adcuris`
5. Apply env_mapping: `prod_adcuris` → `prod-adcuris`
6. **Result:** `service=auth, env=prod-adcuris`

---

## Example Patterns

### Pattern 1: Compound Environment with Suffix

**Database:** `auth_db_prod_adcuris`

```yaml
- pattern: '^(?P<service>\w+)_db_prod_adcuris$'
  env: prod_adcuris
  strip_patterns: ['_db']
  env_mapping:
    prod_adcuris: prod-adcuris
  description: 'Service with _db suffix and prod_adcuris environment'
```

**Result:** `service=auth, env=prod-adcuris`

---

### Pattern 2: Standard Service_Env

**Database:** `myservice_dev`

```yaml
- pattern: '^(?P<service>\w+)_(?P<env>\w+)$'
  description: 'Standard service_env pattern'
```

**Result:** `service=myservice, env=dev`

---

### Pattern 3: Single-Word Database (Implicit Env)

**Database:** `auth`

```yaml
- pattern: '^(?P<service>\w+)$'
  env: prod
  description: 'Single word service name (implicit prod environment)'
```

**Result:** `service=auth, env=prod`

---

## CLI Commands

### Add Extraction Pattern

```bash
cdc manage-source-groups --add-extraction-pattern SERVER PATTERN \
  [--env ENV] [--strip-patterns PATTERNS] [--env-mapping from:to] [--description DESC]
```

**Examples:**

```bash
# Pattern for {service}_db_prod_adcuris
cdc manage-source-groups --add-extraction-pattern prod '^(?P<service>\w+)_db_prod_adcuris$' \
  --env prod_adcuris \
  --strip-patterns '_db' \
  --env-mapping 'prod_adcuris:prod-adcuris' \
  --description 'Service with _db suffix and prod_adcuris environment'

# Pattern for {service}_{env}
cdc manage-source-groups --add-extraction-pattern default '^(?P<service>\w+)_(?P<env>\w+)$' \
  --description 'Standard service_env pattern'

# Pattern for single-word databases
cdc manage-source-groups --add-extraction-pattern prod '^(?P<service>\w+)$' \
  --env prod \
  --description 'Single word service name (implicit prod environment)'
```

---

### List Extraction Patterns

```bash
# All servers
cdc manage-source-groups --list-extraction-patterns

# Specific server
cdc manage-source-groups --list-extraction-patterns prod
```

**Output:**

```
Extraction Patterns (ordered by priority)

📍 Server: prod
  [0] Pattern: ^(?P<service>\w+)_db_prod_adcuris$
      Fixed env: prod_adcuris
      Strip patterns: _db
      Env mapping: prod_adcuris → prod-adcuris
      Description: Service with _db suffix and prod_adcuris environment
  [1] Pattern: ^(?P<service>\w+)_(?P<env>\w+)$
      Description: Standard service_env pattern
  [2] Pattern: ^(?P<service>\w+)$
      Fixed env: prod
      Description: Single word service name (implicit prod environment)
```

---

### Remove Extraction Pattern

```bash
cdc manage-source-groups --remove-extraction-pattern SERVER INDEX
```

**Example:**

```bash
# List patterns first to see indices
cdc manage-source-groups --list-extraction-patterns prod

# Remove pattern at index 2
cdc manage-source-groups --remove-extraction-pattern prod 2
```

---

## Real-World Example: Foo Server Group (asma-cdc-pipeline)

### Database Naming Conventions

**Default server databases:**
- `activities_db_dev`, `datalog_db_dev` → `{service}_db_{env}`
- `auth_dev`, `calendar_dev` → `{service}_{env}` (no `_db`)
- `adopus_db_directory_dev` → `adopus_db_{service}_{env}` (special prefix)

**Prod server databases:**
- `activities_db_prod_adcuris` → `{service}_db_prod_adcuris`
- `auth_prod_adcuris` → `{service}_prod_adcuris` (no `_db`)
- `adopus_db_directory_prod_adcuris` → `adopus_db_{service}_prod_adcuris`
- `auth`, `calendar` → Single-word databases

### Default Server Patterns

```yaml
default:
  extraction_patterns:
    # Most specific: adopus databases
    - pattern: '^(?P<service>adopus_db_\w+)_(?P<env>\w+)$'
      strip_patterns: ['_db']
      description: 'Matching pattern: adopus_db_{service}_{env}'
    
    # Standard: databases with _db_
    - pattern: '^(?P<service>\w+)_db_(?P<env>\w+)$'
      strip_patterns: ['_db']
      description: 'Matching pattern: {service}_db_{env}'
    
    # General: simple service_env (single words only)
    - pattern: '^(?P<service>[a-zA-Z0-9]+)_(?P<env>[a-zA-Z0-9]+)$'
      description: 'Matching pattern: {service}_{env} (single words only)'
```

**Examples:**
- `adopus_db_directory_dev` → `service=adopus_directory, env=dev`
- `activities_db_dev` → `service=activities, env=dev`
- `auth_dev` → `service=auth, env=dev`

### Prod Server Patterns

```yaml
prod:
  extraction_patterns:
    # Most specific: adopus + prod_adcuris
    - pattern: '^(?P<service>adopus_db_[a-zA-Z]+)_prod_adcuris$'
      env: prod_adcuris
      strip_patterns: ['_db']
      env_mapping:
        prod_adcuris: prod-adcuris
      description: 'Matching pattern: adopus_db_{service}_prod_adcuris'
    
    # Specific: any service + _db + prod_adcuris
    - pattern: '^(?P<service>\w+)_db_prod_adcuris$'
      env: prod_adcuris
      strip_patterns: ['_db']
      env_mapping:
        prod_adcuris: prod-adcuris
      description: 'Matching pattern: {service}_db_prod_adcuris'
    
    # Specific: adopus + any env
    - pattern: '^(?P<service>adopus_db_\w+)_(?P<env>\w+)$'
      strip_patterns: ['_db']
      description: 'Matching pattern: adopus_db_{service}_{env}'
    
    # Standard: service_db_env
    - pattern: '^(?P<service>\w+)_db_(?P<env>\w+)$'
      strip_patterns: ['_db']
      description: 'Matching pattern: {service}_db_{env}'
    
    # Specific: service_prod_adcuris (no _db)
    - pattern: '^(?P<service>[a-zA-Z0-9]+)_prod_adcuris$'
      env: prod_adcuris
      env_mapping:
        prod_adcuris: prod-adcuris
      description: 'Matching pattern: {service}_prod_adcuris'
    
    # General: simple service_env (single words only)
    - pattern: '^(?P<service>[a-zA-Z0-9]+)_(?P<env>[a-zA-Z0-9]+)$'
      description: 'Matching pattern: {service}_{env} (single words only)'
```

**Examples:**
- `adopus_db_directory_prod_adcuris` → `service=adopus_directory, env=prod-adcuris`
- `activities_db_prod_adcuris` → `service=activities, env=prod-adcuris`
- `auth_prod_adcuris` → `service=auth, env=prod-adcuris`
- `calendar` → `service=calendar, env=prod` (fallback to server name)

### Key Lessons

1. **Pattern order matters**: Most specific patterns first prevents false matches
2. **Capture what you strip**: `(?P<service>adopus_db_\w+)` not `adopus_db_(?P<service>\w+)`
3. **Use character classes for single words**: `[a-zA-Z0-9]+` prevents matching multi-part names
4. **Per-pattern env_mapping**: Transform `prod_adcuris` → `prod-adcuris` at pattern level

---

## Migration from Environments Field

### Old Configuration (Deprecated)

```yaml
servers:
  prod:
    host: ${POSTGRES_SOURCE_HOST_PROD}
    environments:
      - prod_adcuris
      - prod
```

### New Configuration

```yaml
servers:
  prod:
    host: ${POSTGRES_SOURCE_HOST_PROD}
    extraction_patterns:
      - pattern: '^(?P<service>\w+)_prod_adcuris$'
        env: prod_adcuris
        env_mapping:
          prod_adcuris: prod-adcuris
        description: 'Compound environment: prod_adcuris'
      - pattern: '^(?P<service>\w+)_(?P<env>\w+)$'
        description: 'Standard service_env pattern'
      - pattern: '^(?P<service>\w+)$'
        env: prod
        description: 'Single word service name (implicit prod)'
```

### Steps to Migrate

1. **Remove** `environments` field from server config
2. **Add** extraction patterns using CLI commands (order matters!)
3. **Test** with `cdc manage-source-groups --update`
4. **Verify** service/env decomposition is correct

---

## Pattern Design Principles

### 1. Most Specific First

```yaml
# ✅ CORRECT ORDER
extraction_patterns:
  - pattern: '^(?P<service>\w+)_db_prod_adcuris$'  # Most specific
  - pattern: '^(?P<service>\w+)_db_(?P<env>\w+)$'  # Medium specific
  - pattern: '^(?P<service>\w+)_(?P<env>\w+)$'     # General
  - pattern: '^(?P<service>\w+)$'                   # Fallback

# ❌ WRONG ORDER (general pattern matches first, specific never tried)
extraction_patterns:
  - pattern: '^(?P<service>\w+)_(?P<env>\w+)$'     # Too broad!
  - pattern: '^(?P<service>\w+)_db_prod_adcuris$'  # Never reached
```

### 2. Capture What You Want to Strip

```yaml
# ✅ CORRECT - Include prefix in capture group
- pattern: '^(?P<service>adopus_db_\w+)_(?P<env>\w+)$'
  strip_patterns: ['_db']
  # adopus_db_directory_dev → service=adopus_db_directory → strips to adopus_directory ✓

# ❌ WRONG - Prefix outside capture group
- pattern: '^adopus_db_(?P<service>\w+)_(?P<env>\w+)$'
  strip_patterns: ['_db']
  # adopus_db_directory_dev → service=directory → nothing to strip ✗
```

### 3. Use Character Classes for Single-Word Matching

```yaml
# ✅ CORRECT - Matches only single words (no underscores)
- pattern: '^(?P<service>[a-zA-Z0-9]+)_(?P<env>[a-zA-Z0-9]+)$'
  # Matches: auth_dev, calendar_stage
  # Doesn't match: adopus_db_directory_dev (has underscores in service part)

# ❌ WRONG - \w+ includes underscores, matches too broadly
- pattern: '^(?P<service>\w+)_(?P<env>\w+)$'
  # Matches: adopus_db_directory_dev as service=adopus, env=db_directory_dev ✗
```

### 4. Use Per-Pattern env_mapping

```yaml
# ✅ CORRECT - Transform env at pattern level
- pattern: '^(?P<service>\w+)_db_prod_adcuris$'
  env: prod_adcuris
  env_mapping:
    prod_adcuris: prod-adcuris  # Transform to hyphenated form

# ❌ DEPRECATED - Global env_mappings removed
# env_mappings:
#   prod_adcuris: prod-adcuris
```

### 5. Use Fixed Env for Edge Cases

```yaml
# Single-word databases like 'auth' on prod server
- pattern: '^(?P<service>\w+)$'
  env: prod  # Implicit environment
```

### 6. Document Your Patterns

```yaml
- pattern: '^(?P<service>\w+)_prod_adcuris$'
  env: prod_adcuris
  env_mapping:
    prod_adcuris: prod-adcuris
  description: 'Handles compound prod_adcuris environment'  # Explains WHY
```

---

## Troubleshooting

### Pattern Not Matching

1. **List current patterns:**
   ```bash
   cdc manage-source-groups --list-extraction-patterns SERVER
   ```

2. **Check pattern order** - Is a more general pattern matching first?

3. **Test regex independently:**
   ```python
   import re
   pattern = r'^(?P<service>\w+)_prod_adcuris$'
   match = re.match(pattern, 'auth_prod_adcuris')
   print(match.groupdict())  # {'service': 'auth'}
   ```

### Wrong Service/Env Extracted

1. **Check for more specific pattern needed:**
   - Add pattern with fixed `env` field
   - Add pattern with `strip_suffixes`

2. **Reorder patterns:**
   - Remove general pattern
   - Add specific pattern first
   - Re-add general pattern

3. **Re-scan databases:**
   ```bash
   cdc manage-source-groups --update
   ```

---

## API Reference

### ExtractionPattern TypedDict

```python
class ExtractionPattern(TypedDict, total=False):
    pattern: str                    # Required: Regex with named groups
    env: str                        # Optional: Fixed environment (overrides capture)
    strip_patterns: List[str]       # Optional: Regex patterns to strip from service
    env_mapping: Dict[str, str]     # Optional: Environment name transformations
    description: str                # Optional: Human-readable description
```

### Pattern Matcher Function

```python
from cdc_generator.helpers.helpers_pattern_matcher import match_extraction_patterns

def match_extraction_patterns(
    db_name: str,
    patterns: List[ExtractionPattern],
    server_name: str = "default"
) -> Optional[Tuple[str, str]]:
    """Extract service and env from db_name using ordered patterns.
    
    Args:
        db_name: Database name to parse
        patterns: Ordered list of extraction patterns (first match wins)
        server_name: Server name for fallback environment
    
    Returns:
        (service, env) tuple if any pattern matches
        Falls back to (db_name, server_name) if no pattern matches
    
    Pattern validation:
        - If pattern has 'env' field, it must appear in regex pattern string
        - strip_patterns applied after capture using re.sub()
        - env_mapping applied after determining final environment
    """
```

---

## See Also

- [Architecture Documentation](_docs/ARCHITECTURE.md)
- [Server Group Management](../README.md)
- [Pattern Examples](../examples/)
