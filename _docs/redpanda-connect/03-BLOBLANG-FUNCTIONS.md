# Bloblang Functions Reference

Built-in functions for Redpanda Connect Bloblang mappings.

---

## Message Functions

### Content Access

```yaml
# Get message content as string
root.raw = content()

# Get content as JSON
root.data = json()

# Get specific JSON path
root.name = json("user.name")
root.items = json("data.items")
```

### Metadata Access

```yaml
# Get all metadata
root.meta = metadata()

# Get specific metadata field
root.topic = metadata("kafka_topic")
root.key = metadata("kafka_key")
root.path = metadata("http_server_request_path")
```

### Batch Information

```yaml
# Current message index in batch
root.index = batch_index()

# Total messages in batch
root.total = batch_size()
```

### Error Information

```yaml
# Check if message has error flag
root = if errored() { deleted() }

# Get error message
root.error_msg = error()

# Get error source details
root.error_label = error_source_label()
root.error_name = error_source_name()
root.error_path = error_source_path()
```

---

## Generation Functions

### Unique Identifiers

```yaml
# UUID v4 (random)
root.id = uuid_v4()
# Example: "f47ac10b-58cc-4372-a567-0e02b2c3d479"

# UUID v7 (time-ordered)
root.id = uuid_v7()
# Example: "018f23a9-1234-7def-8abc-0123456789ab"

# NanoID (URL-safe)
root.id = nanoid()
root.id = nanoid(10)  # Custom length

# KSUID (K-Sortable)
root.id = ksuid()

# Snowflake ID
root.id = snowflake_id()
```

### Timestamps

```yaml
# Current time as timestamp
root.now = now()

# Format current time
root.timestamp = now().ts_format("2006-01-02T15:04:05Z07:00")

# Unix timestamp
root.unix = timestamp_unix()
root.unix_ms = timestamp_unix_milli()
root.unix_nano = timestamp_unix_nano()
```

### Counters

```yaml
# Auto-incrementing counter (per stream)
root.seq = counter()

# Counter with custom name (persists across mappings)
root.seq = counter("my_counter")
```

### Random Values

```yaml
# Random integer
root.rand = random_int()
root.rand = random_int(100)      # 0 to 99
root.rand = random_int(10, 20)   # 10 to 19
```

### Ranges

```yaml
# Generate array of numbers
root.nums = range(5)              # [0, 1, 2, 3, 4]
root.nums = range(1, 5)           # [1, 2, 3, 4]
root.nums = range(0, 10, 2)       # [0, 2, 4, 6, 8]
```

---

## Environment Functions

### Environment Variables

```yaml
# Get environment variable
root.host = env("DATABASE_HOST")
root.port = env("DATABASE_PORT").number().catch(5432)

# With default value
root.env = env("MY_VAR").or("default_value")
```

### File Content

```yaml
# Read file content
root.config = file("/path/to/config.json").parse_json()
root.template = file("/templates/email.html")

# Relative to working directory
root.data = file_rel("./data/config.yaml").parse_yaml()
```

### System Information

```yaml
# Current hostname
root.host = hostname()
```

---

## Control Flow Functions

### Delete Message

```yaml
# Remove message from pipeline
root = deleted()

# Conditional deletion
root = if this.type == "internal" { deleted() } else { this }

# Delete in match
root = match this.status {
    "deleted" => deleted()
    "archived" => deleted()
    _ => this
}
```

### Throw Error

```yaml
# Throw custom error
root = if this.required_field == null {
    throw("required_field is missing")
} else {
    this
}

# Throw with dynamic message
root = throw("Invalid value: " + this.value.string())
```

---

## Fake Data Functions

Useful for testing and development:

```yaml
# Person data
root.name = fake("name")
root.first = fake("first_name")
root.last = fake("last_name")
root.email = fake("email")
root.phone = fake("phone_number")

# Address data
root.street = fake("street_address")
root.city = fake("city")
root.country = fake("country")
root.zip = fake("zip_code")

# Business data
root.company = fake("company")
root.job = fake("job_title")

# Internet data
root.domain = fake("domain_name")
root.ipv4 = fake("ipv4_address")
root.url = fake("url")
root.username = fake("username")
root.password = fake("password")

# Payment data
root.cc = fake("credit_card_number")

# Text data
root.sentence = fake("sentence")
root.paragraph = fake("paragraph")
root.word = fake("word")
```

---

## Utility Functions

### Type Checking

```yaml
# Get type as string
root.type = this.value.type()
# Returns: "string", "number", "bool", "array", "object", "null"
```

### Null Handling

```yaml
# Create explicit null
root.field = null
root.empty = null

# Check for null
root = if this.value == null { deleted() }
```

---

## Function Usage Patterns

### Environment Configuration

```yaml
# Database connection from environment
let db_host = env("POSTGRES_HOST").or("localhost")
let db_port = env("POSTGRES_PORT").number().catch(5432)
let db_name = env("POSTGRES_DB").or("mydb")

root.dsn = "postgres://" + $db_host + ":" + $db_port.string() + "/" + $db_name
```

### Message Enrichment

```yaml
# Add metadata to every message
root = this
root._meta.processed_at = now().ts_format("2006-01-02T15:04:05Z")
root._meta.pipeline_id = env("PIPELINE_ID")
root._meta.host = hostname()
root._meta.batch_position = batch_index()
```

### Idempotency Keys

```yaml
# Generate deterministic ID from content
root.idempotency_key = (this.user_id.string() + this.action + this.timestamp)
    .hash("sha256")
    .encode("hex")
    .slice(0, 32)

# Or use UUID for non-deterministic
root.request_id = uuid_v4()
```

### Conditional Processing

```yaml
# Development vs Production
let is_dev = env("ENVIRONMENT") == "development"

root = if $is_dev {
    this
    root.debug = true
    root.fake_email = fake("email")
} else {
    this
}
```

### Error Handling Pattern

```yaml
# Validate and throw
let required_fields = ["id", "name", "email"]

root = if $required_fields.any(f -> this.get(f) == null) {
    throw("Missing required field")
} else {
    this
}
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [02-BLOBLANG-METHODS.md](02-BLOBLANG-METHODS.md) - Method reference
- [04-HTTP-INPUTS.md](04-HTTP-INPUTS.md) - HTTP webhook patterns
