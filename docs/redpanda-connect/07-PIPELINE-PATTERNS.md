# Redpanda Connect Pipeline Patterns

Complete pipeline examples and architectural patterns for CDC implementations.

---

## Pipeline Structure Overview

```yaml
# Basic pipeline structure
input:
  # Source of messages
  kafka: { ... }

pipeline:
  processors:
    # Transform/enrich messages
    - mapping: |
        root = this

output:
  # Destination for messages
  kafka: { ... }
```

---

## Multi-Input Broker

Combine multiple inputs into single stream:

```yaml
input:
  broker:
    inputs:
      - kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topics: [ "events-v1" ]
          consumer_group: processor
      
      - kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topics: [ "events-v2" ]
          consumer_group: processor
      
      - http_server:
          path: /webhook
          allowed_verbs: [ "POST" ]

pipeline:
  processors:
    - mapping: |
        # Normalize all inputs
        root = match {
            @kafka_topic.has_prefix("events-v1") => this.apply("transform_v1")
            @kafka_topic.has_prefix("events-v2") => this.apply("transform_v2")
            @http_server_request_path != null => this.apply("transform_webhook")
            _ => this
        }
```

---

## Multi-Output Fan-Out

Send to multiple destinations:

```yaml
output:
  broker:
    outputs:
      # Primary destination
      - kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topic: processed-events
      
      # Analytics copy
      - kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topic: analytics-events
        processors:
          - mapping: |
              # Slim down for analytics
              root = {
                  "id": this.id,
                  "type": this.type,
                  "timestamp": this.timestamp
              }
      
      # Archive to S3
      - aws_s3:
          bucket: events-archive
          path: "${!meta(\"kafka_topic\")}/${!now().format_timestamp(\"2006/01/02\")}/${!uuid_v4()}.json"
```

---

## Conditional Routing (Switch)

Route based on message content:

```yaml
output:
  switch:
    cases:
      - check: this.priority == "urgent"
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: urgent-queue
      
      - check: this.type == "notification"
        output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: notifications
      
      # Default case (no check)
      - output:
          kafka:
            addresses: [ "${KAFKA_BROKERS}" ]
            topic: general-events
```

---

## CDC Pipeline Pattern

Standard CDC pipeline structure:

```yaml
input:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topics: [ "${CDC_TOPIC}" ]
    consumer_group: ${CONSUMER_GROUP}

pipeline:
  processors:
    # 1. Parse CDC envelope
    - mapping: |
        root = this
        
        # Extract operation type
        root._cdc.operation = match this.op {
            "c" => "INSERT"
            "u" => "UPDATE"
            "d" => "DELETE"
            "r" => "READ"  # Snapshot
            _ => "UNKNOWN"
        }
        
        # Get the data (after for insert/update, before for delete)
        root._cdc.data = if this.op == "d" { this.before } else { this.after }
        root._cdc.source_ts = this.ts_ms
    
    # 2. Transform to target format
    - mapping: |
        root = this._cdc.data
        root._operation = this._cdc.operation
        root._source_timestamp = this._cdc.source_ts

output:
  switch:
    cases:
      - check: this._operation == "DELETE"
        output:
          sql_raw:
            driver: postgres
            dsn: ${POSTGRES_DSN}
            query: DELETE FROM ${TABLE} WHERE id = $1
            args_mapping: root = [this.id]
      
      - output:
          sql_insert:
            driver: postgres
            dsn: ${POSTGRES_DSN}
            table: ${TABLE}
            columns: [id, name, data, updated_at]
            args_mapping: |
              root = [
                this.id,
                this.name,
                this.data.format_json(),
                now().ts_format("2006-01-02T15:04:05Z")
              ]
            suffix: |
              ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                data = EXCLUDED.data,
                updated_at = EXCLUDED.updated_at
```

---

## Webhook to Kafka Pipeline

Receive webhooks and publish to Kafka:

```yaml
input:
  http_server:
    path: /webhooks/{provider}
    allowed_verbs: [ "POST" ]
    rate_limit: webhook_limit

rate_limit_resources:
  - label: webhook_limit
    local:
      count: 1000
      interval: 1s

pipeline:
  processors:
    # 1. Validate signature
    - mapping: |
        let provider = @http_server_path_params.provider
        let body = content()
        let signature = @X-Signature | ""
        
        let secrets = {
            "github": env("GITHUB_SECRET"),
            "stripe": env("STRIPE_SECRET")
        }
        
        let secret = $secrets.get($provider) | ""
        root = if $secret != "" {
            let expected = $body.hash("hmac_sha256", $secret).encode("hex")
            if !$signature.contains($expected) {
                throw("Invalid signature")
            } else {
                this
            }
        } else {
            this  # No validation for unknown providers
        }
    
    # 2. Enrich with metadata
    - mapping: |
        root = this
        root._meta.provider = @http_server_path_params.provider
        root._meta.received_at = now().ts_format("2006-01-02T15:04:05Z")
        root._meta.request_path = @http_server_request_path
        
        # Set Kafka key for ordering
        meta kafka_key = this.id | uuid_v4()

output:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topic: "webhooks.${!@http_server_path_params.provider}"
    key: "${!meta(\"kafka_key\")}"
```

---

## API Polling to Kafka

Poll external API and publish changes:

```yaml
input:
  http_client:
    url: https://api.example.com/events?since=${!metadata("last_sync")}
    verb: GET
    rate_limit: api_limit
    oauth2:
      enabled: true
      client_id: ${OAUTH_CLIENT_ID}
      client_secret: ${OAUTH_CLIENT_SECRET}
      token_url: https://auth.example.com/oauth/token

rate_limit_resources:
  - label: api_limit
    local:
      count: 1
      interval: 60s

pipeline:
  processors:
    # Explode array response into individual messages
    - mapping: |
        root = this.events
    - unarchive:
        format: json_array
    
    # Add metadata
    - mapping: |
        root = this
        root._meta.polled_at = now().ts_format("2006-01-02T15:04:05Z")
        meta kafka_key = this.id

output:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topic: external-events
    key: "${!meta(\"kafka_key\")}"
```

---

## Enrichment Pipeline

Enrich messages with external data:

```yaml
pipeline:
  processors:
    # Parallel enrichment using branch
    - branch:
        request_map: |
          root.user_id = this.user_id
        processors:
          - http:
              url: https://api.example.com/users/${!this.user_id}
              verb: GET
        result_map: |
          root.user = this
    
    - branch:
        request_map: |
          root.product_id = this.product_id
        processors:
          - http:
              url: https://api.example.com/products/${!this.product_id}
              verb: GET
        result_map: |
          root.product = this
    
    # Combine results
    - mapping: |
        root = this
        root.enriched_at = now().ts_format("2006-01-02T15:04:05Z")
```

---

## Batch Processing Pipeline

Process messages in batches:

```yaml
input:
  kafka:
    addresses: [ "${KAFKA_BROKERS}" ]
    topics: [ "events" ]
    consumer_group: batch-processor

pipeline:
  processors:
    # Buffer into batches
    - batching:
        count: 100
        period: 10s
    
    # Process batch as array
    - mapping: |
        root = this.map_each(msg -> msg.assign({
            "batch_id": uuid_v4(),
            "processed_at": now().ts_format("2006-01-02T15:04:05Z")
        }))
    
    # Combine into single document
    - archive:
        format: json_array

output:
  # Bulk insert
  sql_raw:
    driver: postgres
    dsn: ${POSTGRES_DSN}
    query: |
      INSERT INTO events (id, data, batch_id, processed_at)
      SELECT 
        item->>'id',
        item->'data',
        item->>'batch_id',
        (item->>'processed_at')::timestamp
      FROM jsonb_array_elements($1::jsonb) AS item
      ON CONFLICT (id) DO UPDATE SET
        data = EXCLUDED.data,
        processed_at = EXCLUDED.processed_at
    args_mapping: root = [content()]
```

---

## Filter and Transform Pipeline

Filter messages and apply transformations:

```yaml
pipeline:
  processors:
    # 1. Filter out unwanted messages
    - mapping: |
        root = match {
            this.type == "internal" => deleted()
            this.status == "test" => deleted()
            this.ignore == true => deleted()
            _ => this
        }
    
    # 2. Apply named transformations
    - mapping: |
        map normalize_user {
            root.user_id = this.user_id | this.userId | this.user.id
            root.user_name = (this.user_name | this.userName | this.user.name | "").trim()
            root.user_email = (this.user_email | this.userEmail | this.user.email | "").lowercase()
        }
        
        map add_metadata {
            root._meta.version = "2.0"
            root._meta.processed_at = now().ts_format("2006-01-02T15:04:05Z")
            root._meta.pipeline = env("PIPELINE_NAME")
        }
        
        root = this.apply("normalize_user").apply("add_metadata")
```

---

## Dead Letter Queue Pattern

Complete DLQ implementation:

```yaml
output:
  fallback:
    # Primary: Reject errors, send good messages
    - reject_errored:
        kafka:
          addresses: [ "${KAFKA_BROKERS}" ]
          topic: processed-events
    
    # Retry queue for transient errors
    - switch:
        cases:
          - check: 'error().contains("timeout") || error().contains("connection")'
            output:
              kafka:
                addresses: [ "${KAFKA_BROKERS}" ]
                topic: retry-queue
              processors:
                - mapping: |
                    root = this
                    root._retry.count = (this._retry.count | 0) + 1
                    root._retry.last_error = error()
                    root._retry.last_attempt = now().ts_format("2006-01-02T15:04:05Z")
    
    # Final DLQ for permanent errors
    - kafka:
        addresses: [ "${KAFKA_BROKERS}" ]
        topic: dead-letter-queue
      processors:
        - mapping: |
            root.original_message = this
            root.error = error()
            root.error_source = error_source_label()
            root.failed_at = now().ts_format("2006-01-02T15:04:05Z")
            root.pipeline = env("PIPELINE_NAME")
```

---

## Resource Definitions

Reusable components:

```yaml
# Define reusable resources
processor_resources:
  - label: validate_message
    mapping: |
      let required = ["id", "type", "data"]
      let missing = $required.filter(f -> this.get(f) == null)
      root = if $missing.length() > 0 {
          throw("Missing fields: " + $missing.join(", "))
      } else {
          this
      }

  - label: add_metadata
    mapping: |
      root = this
      root._meta.processed_at = now().ts_format("2006-01-02T15:04:05Z")
      root._meta.pipeline = env("PIPELINE_NAME")

output_resources:
  - label: kafka_output
    kafka:
      addresses: [ "${KAFKA_BROKERS}" ]
      topic: "${OUTPUT_TOPIC}"

# Use resources in pipeline
pipeline:
  processors:
    - resource: validate_message
    - resource: add_metadata

output:
  resource: kafka_output
```

---

## See Also

- [01-BLOBLANG-FUNDAMENTALS.md](01-BLOBLANG-FUNDAMENTALS.md) - Core concepts
- [04-HTTP-INPUTS.md](04-HTTP-INPUTS.md) - HTTP webhook patterns
- [05-SQL-PATTERNS.md](05-SQL-PATTERNS.md) - Database patterns
- [06-ERROR-HANDLING.md](06-ERROR-HANDLING.md) - Error handling
