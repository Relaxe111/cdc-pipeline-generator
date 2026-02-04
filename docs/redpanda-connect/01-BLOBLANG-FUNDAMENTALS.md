# Bloblang Fundamentals

Bloblang is the native mapping language for Redpanda Connect pipelines. This document covers core concepts essential for building CDC transformations.

## Quick Reference

```yaml
# Basic mapping structure
pipeline:
  processors:
    - mapping: |
        root = this                           # Copy entire message
        root.id = this.id                     # Copy specific field
        root.computed = this.a + this.b       # Compute new value
        meta new_key = "value"                # Set metadata
```

---

## Core Concepts

### 1. Assignment Keywords

| Keyword | Description | Example |
|---------|-------------|---------|
| `root` | Target document (output) | `root = this` |
| `this` | Source document (input) | `root.name = this.name` |
| `root = deleted()` | Delete entire message | Filters out message |

```yaml
# Copy entire document
root = this

# Copy with modifications
root = this
root.timestamp = now()
root.processed = true

# Create new structure
root.user.id = this.id
root.user.name = this.first_name + " " + this.last_name
```

### 2. Variables

Use `let` to define variables, reference with `$`:

```yaml
let full_name = this.first_name + " " + this.last_name
let is_valid = this.status == "active"

root.name = $full_name
root.valid = $is_valid
```

**Use cases:**
- Avoid repeating complex expressions
- Store intermediate calculations
- Improve readability

### 3. Metadata

Access and set message metadata using `meta` keyword or `@` operator:

```yaml
# Set metadata
meta kafka_key = this.id
meta source_table = "customers"

# Read metadata
root.topic = meta("kafka_topic")
root.partition = meta("kafka_partition")

# Using @ operator (shorthand)
root.headers = @
root.request_path = @http_server_request_path
```

**Common metadata fields:**
- `kafka_topic`, `kafka_partition`, `kafka_offset` - Kafka message info
- `http_server_request_path`, `http_server_verb` - HTTP request info
- `kafka_key` - Message key for partitioning

### 4. Coalesce Operator (`|`)

Provide fallback values for null/missing fields:

```yaml
# Basic fallback
root.name = this.name | "Unknown"
root.count = this.count | 0

# Chained fallbacks
root.contact = this.email | this.phone | this.address | "No contact"

# With method calls
root.type = this.type.uppercase() | "DEFAULT"
```

### 5. Conditional Logic

#### If/Else Expression

```yaml
root.status = if this.active == true {
    "ACTIVE"
} else if this.suspended == true {
    "SUSPENDED"
} else {
    "INACTIVE"
}

# Ternary-style for simple cases
root.flag = if this.value > 100 { "high" } else { "low" }
```

#### Match Expression (Pattern Matching)

```yaml
root.category = match this.type {
    "A" => "Category Alpha"
    "B" => "Category Beta"
    this.priority > 5 => "High Priority"
    _ => "Other"
}

# Match with guards
root.action = match {
    this.status == "new" && this.priority > 8 => "urgent"
    this.status == "new" => "process"
    this.status == "done" => "archive"
    _ => "skip"
}
```

---

## Error Handling

### The `catch` Method

Handle errors gracefully:

```yaml
# Catch with default value
root.parsed = this.json_string.parse_json().catch({})
root.number = this.value.number().catch(0)

# Catch with custom error handling
root.result = this.field.some_method().catch("error: " + error())
```

### The `or` Method

Alternative to catch for simple defaults:

```yaml
root.name = this.name.or("default")
root.items = this.items.or([])
```

### Error Checking Functions

```yaml
# Check if message has error flag
root = if errored() { deleted() }

# Get error message
root.error_message = error()

# Get error source
root.error_source = error_source_label()
```

---

## Named Maps (Reusable Transformations)

Define reusable mapping logic:

```yaml
map extract_user {
    root.id = this.user_id
    root.name = this.user_name
    root.email = this.user_email.lowercase()
}

map add_metadata {
    root.created_at = now()
    root.version = "1.0"
}

# Apply maps
root = this.apply("extract_user").apply("add_metadata")
```

**Best practices:**
- Use maps for repeated transformations
- Keep maps focused (single responsibility)
- Name maps descriptively

---

## Filtering Messages

### Delete Messages

```yaml
# Filter based on condition
root = if this.type == "internal" { deleted() }

# Multiple conditions
root = match {
    this.status == "deleted" => deleted()
    this.test == true => deleted()
    _ => this
}
```

### Conditional Processing

```yaml
# Only process certain messages
root = if this.process == true {
    this.apply("transform")
} else {
    this
}
```

---

## Working with Null Values

```yaml
# Check for null
root.has_name = this.name != null

# Null coalescing
root.name = this.name | "Unknown"

# Conditional on null
root.status = if this.value == null {
    "missing"
} else {
    "present"
}

# Filter null values from array
root.items = this.items.filter(item -> item != null)
```

---

## Type System

### Type Checking

```yaml
# Get type as string
root.field_type = this.value.type()

# Type-based logic
root.normalized = match this.value.type() {
    "string" => this.value
    "number" => this.value.string()
    "bool" => if this.value { "true" } else { "false" }
    "array" => this.value.join(",")
    _ => "unknown"
}
```

### Type Coercion

```yaml
# String conversions
root.str = this.number.string()
root.str = this.bool.string()

# Number conversions
root.num = this.string.number()
root.int = this.value.int64()
root.float = this.value.float64()

# Boolean conversion
root.bool = this.value.bool()

# Timestamp conversion
root.ts = this.date_string.timestamp("2006-01-02T15:04:05Z")
```

---

## Quick Tips

1. **Always use `|` for optional fields** - Prevents null errors
2. **Use variables for complex expressions** - Improves readability
3. **Prefer `match` over nested `if/else`** - Cleaner pattern matching
4. **Use `.catch()` for method chains** - Graceful error handling
5. **Use maps for repeated logic** - DRY principle
6. **Test with `deleted()` carefully** - Messages are permanently filtered

---

## See Also

- [02-BLOBLANG-METHODS.md](02-BLOBLANG-METHODS.md) - Complete method reference
- [03-BLOBLANG-FUNCTIONS.md](03-BLOBLANG-FUNCTIONS.md) - Built-in functions
- [04-HTTP-INPUTS.md](04-HTTP-INPUTS.md) - HTTP webhook patterns
- [05-SQL-PATTERNS.md](05-SQL-PATTERNS.md) - SQL/PostgreSQL patterns
