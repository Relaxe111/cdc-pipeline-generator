# Error Handling Patterns

Comprehensive error handling strategies for Redpanda Connect pipelines.

---

## Error Flag System

When a processor fails, Redpanda Connect:
1. Sets an error flag on the message
2. Stores the error message (accessible via `error()`)
3. Continues processing (message is not dropped)

### Checking for Errors

```yaml
# Check if message has error flag
root = if errored() { deleted() }

# Get error message
root.error_message = error()

# Get error source information
root.error_label = error_source_label()
root.error_name = error_source_name()
root.error_path = error_source_path()
```

---

## Try Processor - Abandon on Failure

Execute processors in sequence, skip remaining on failure:

```yaml
pipeline:
  processors:
    - try:
        - mapping: |
            # Step 1: Parse JSON
            root = content().parse_json()
        - mapping: |
            # Step 2: Validate (skipped if Step 1 fails)
            root = if this.id == null { throw("Missing id") } else { this }
        - mapping: |
            # Step 3: Transform (skipped if any prior fails)
            root.processed = true
```

**Behavior:**
- If Step 1 fails → Steps 2 and 3 are skipped
- If Step 2 fails → Step 3 is skipped
- Error flag is set on the message

---

## Catch Processor - Recover from Failures

Handle errors and optionally recover:

```yaml
pipeline:
  processors:
    - mapping: |
        root = content().parse_json()  # Might fail
    
    - catch:
        - mapping: |
            # This runs ONLY if prior processor failed
            root = {
                "raw": content(),
                "error": error(),
                "error_source": error_source_label()
            }
```

**Important:** The `catch` block clears the error flag after completion.

### Nested Try/Catch

```yaml
pipeline:
  processors:
    - try:
        - resource: risky_processor_1
        - resource: risky_processor_2
    
    - catch:
        - log:
            message: "Processing failed: ${!error()}"
        - catch: []  # Clear error flag
        - try:
            # Attempt recovery
            - resource: recovery_processor
```

---

## Error Handling in Mappings

### The `catch` Method

```yaml
root.parsed = this.json_string.parse_json().catch({})
root.number = this.value.number().catch(0)
root.date = this.date_str.ts_parse("2006-01-02").catch(now())

# Catch with error access
root.result = this.field.some_method().catch("Error: " + error())
```

### The `throw` Function

```yaml
# Throw custom error
root = if this.required_field == null {
    throw("required_field is missing")
} else {
    this
}

# Conditional throw
root = match {
    this.id == null => throw("Missing id")
    this.type == "" => throw("Empty type")
    _ => this
}
```

### Defensive Patterns

```yaml
# Safe field access with multiple fallbacks
root.name = this.name | this.username | this.email | "unknown"

# Safe method chaining
root.formatted = this.value
    .string()
    .catch("")
    .uppercase()
    .catch("")

# Validate before processing
let is_valid = this.id != null && this.data != null
root = if $is_valid {
    this
} else {
    throw("Validation failed: " + match {
        this.id == null => "missing id"
        this.data == null => "missing data"
        _ => "unknown"
    })
}
```

---

## Retry Processor

Retry a processing chain with backoff:

```yaml
pipeline:
  processors:
    - retry:
        max_retries: 5
        backoff:
          initial_interval: 100ms
          max_interval: 10s
          max_elapsed_time: 60s
        processors:
          - http:
              url: https://api.example.com/process
              verb: POST
```

---

## Output Error Handling

### Fallback Output - Dead Letter Queue

```yaml
output:
  fallback:
    # Primary output
    - kafka:
        addresses: [ "${KAFKA_BROKERS}" ]
        topic: events
    
    # Fallback on failure
    - kafka:
        addresses: [ "${KAFKA_BROKERS}" ]
        topic: events-dlq
      processors:
        - mapping: |
            root.original = this
            root.error = @fallback_error
            root.failed_at = now().ts_format("2006-01-02T15:04:05Z")
```

### Reject Errored Messages

Route errored messages back to input (nack):

```yaml
output:
  reject_errored:
    kafka:
      addresses: [ "${KAFKA_BROKERS}" ]
      topic: events
```

### Combined Pattern

```yaml
output:
  fallback:
    # First: Reject errored, send good to primary
    - reject_errored:
        kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topic: events
    
    # Second: Handle errored messages
    - kafka:
        addresses: [ "${KAFKA_BROKERS}" ]
        topic: events-dlq
```

---

## Switch Output - Error Routing

Route errors based on type:

```yaml
output:
  switch:
    cases:
      # Validation errors -> specific DLQ
      - check: errored() && error().contains("validation")
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: validation-errors
      
      # Connection errors -> retry queue
      - check: errored() && error().contains("connection")
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: retry-queue
      
      # All other errors -> general DLQ
      - check: errored()
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: errors-dlq
      
      # Success -> main topic
      - output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: processed-events
```

---

## Logging Errors

### Log Processor

```yaml
pipeline:
  processors:
    - resource: risky_processor
    
    - catch:
        - log:
            level: ERROR
            message: |
              Processing failed for message ${!json("id")}
              Error: ${!error()}
              Source: ${!error_source_label()}
```

### Add Error to Message

```yaml
pipeline:
  processors:
    - resource: risky_processor
    
    - catch:
        - mapping: |
            root = this
            root._error = {
                "message": error(),
                "source": error_source_label(),
                "timestamp": now().ts_format("2006-01-02T15:04:05Z"),
                "original_content": content()
            }
```

---

## Drop Failed Messages

```yaml
pipeline:
  processors:
    - resource: risky_processor
    
    # Drop any message with error flag
    - mapping: |
        root = if errored() { deleted() }
```

---

## Complete Error Handling Example

Full pipeline with comprehensive error handling:

```yaml
input:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topics: [ "incoming-events" ]
    consumer_group: event-processor

pipeline:
  processors:
    # 1. Parse and validate
    - try:
        - label: parse_json
          mapping: |
            root = content().parse_json()
        
        - label: validate
          mapping: |
            let errors = []
            let errors = if this.id == null { $errors.append("missing id") } else { $errors }
            let errors = if this.type == null { $errors.append("missing type") } else { $errors }
            
            root = if $errors.length() > 0 {
                throw("Validation failed: " + $errors.join(", "))
            } else {
                this
            }
        
        - label: enrich
          http:
            url: https://api.example.com/enrich
            verb: POST
            retries: 2
        
        - label: transform
          mapping: |
            root = this
            root.processed = true
            root.processed_at = now().ts_format("2006-01-02T15:04:05Z")
    
    # 2. Handle any errors from try block
    - catch:
        - log:
            level: ERROR
            message: "Processing failed: ${!error()} at ${!error_source_label()}"
        
        - mapping: |
            root = this
            root._error = {
                "message": error(),
                "source": error_source_label(),
                "timestamp": now().ts_format("2006-01-02T15:04:05Z")
            }

output:
  switch:
    strict_mode: true
    cases:
      # Route parse errors to parse-errors topic
      - check: 'errored() && error_source_label() == "parse_json"'
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: parse-errors
      
      # Route validation errors to validation-errors topic
      - check: 'errored() && error_source_label() == "validate"'
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: validation-errors
      
      # Route enrichment failures to retry queue
      - check: 'errored() && error_source_label() == "enrich"'
        output:
          fallback:
            - kafka:
                addresses: [ "${KAFKA_BROKERS}" ]
                topic: enrichment-retry
            - kafka:
                addresses: [ "${KAFKA_BROKERS}" ]
                topic: enrichment-dlq
      
      # Route other errors to general DLQ
      - check: errored()
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: processing-dlq
      
      # Success path
      - output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: processed-events
```

---

## Best Practices

1. **Label processors** - Makes `error_source_label()` useful
2. **Use try/catch for chains** - Fail fast, then recover
3. **Validate early** - Catch issues before expensive processing
4. **Log before dropping** - Never silently discard messages
5. **Separate DLQs by error type** - Easier debugging and recovery
6. **Include context in errors** - Message ID, original content, timestamp
7. **Consider retry vs DLQ** - Transient vs permanent failures

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [05-SQL-PATTERNS.md](05-SQL-PATTERNS.md) - Database error handling
- [07-PIPELINE-PATTERNS.md](07-PIPELINE-PATTERNS.md) - Complete examples
