"""Pipeline templates for Redpanda Connect source and sink pipelines."""


def get_source_pipeline_template() -> str:
    """Generate source-pipeline.yaml template.

    Returns:
        Complete source pipeline template as string for MSSQL CDC to Redpanda.
        Uses placeholders for customer-specific values:
        - {{CUSTOMER}}: Customer identifier
        - {{ENV}}: Environment (nonprod, prod)
        - {{DATABASE_NAME}}: Source database name
        - {{SOURCE_TABLE_INPUTS}}: Generated broker input configuration
        - {{TOPIC_PREFIX}}: Kafka topic prefix
        - {{TABLE_ROUTING}}: Table-specific routing configuration
    """
    return """# =============================================================================
# Redpanda Connect Source Pipeline - MSSQL CDC to Redpanda
# =============================================================================
# Customer: {{CUSTOMER}}
# Environment: {{ENV}}
# Database: {{DATABASE_NAME}}
# =============================================================================

# File-based cache to persist LSN state across restarts
cache_resources:
  - label: lsn_cache
    file:
      directory: /data/lsn_cache

input:
  broker:
    inputs:
      {{SOURCE_TABLE_INPUTS}}

    # Poll all tables continuously
    batching:
      count: 100
      period: 1s

pipeline:
  processors:
    # Filter out before-update images (operation 3)
    # MSSQL CDC operations: 1=DELETE, 2=INSERT, 3=before-UPDATE, 4=after-UPDATE
    - bloblang: |
        let op_code = this.get("__$operation")
        # Drop before-update operations (3) - we only need after-update (4)
        root = if $op_code == 3 { deleted() } else { this }

    - bloblang: |
        # Map MSSQL CDC operation codes to Debezium style
        let op_code = this.get("__$operation")
        let debezium_op = match $op_code {
          1 => "d",
          2 => "c",
          4 => "u",
          _ => "c"
        }

        # Get table name from metadata
        let table_name = meta("source_table")

        # Remove CDC metadata columns (including __lsn_hex used for LSN tracking)
        let payload_data = this.without("__$start_lsn", "__$end_lsn", "__$seqval", "__$operation", "__$update_mask", "__lsn_hex")

        # Build Debezium-style CDC envelope
        root.payload = {
          "op": $debezium_op,
          "before": if $op_code == 1 { $payload_data } else { null },
          "after": if $op_code != 1 { $payload_data } else { null },
          "source": {
            "version": "1.0.0",
            "connector": "redpanda-connect-mssql",
            "name": "{{TOPIC_PREFIX}}",
            "ts_ms": now().ts_unix_milli(),
            "db": "{{DATABASE_NAME}}",
            "schema": "dbo",
            "table": $table_name
          },
          "ts_ms": now().ts_unix_milli()
        }

        # Route to correct Kafka topic and key based on table
        let data = if $op_code == 1 { root.payload.before } else { root.payload.after }
        let routing = match $table_name {
        {{TABLE_ROUTING}}
          _ => {"topic": "{{TOPIC_PREFIX}}.dbo.unknown", "key": ""}
        }

        meta kafka_topic = $routing.topic
        meta kafka_key = $routing.key

    # Update LSN cache with the max LSN from this message (only if not null)
    - bloblang: |
        # Only process messages that have a valid LSN from CDC
        let lsn = meta("max_lsn")
        root = if $lsn == null || $lsn == "null" {
          deleted()  # Drop messages without valid LSN
        } else {
          this  # Pass through CDC messages
        }
    - cache:
        resource: lsn_cache
        operator: set
        key: '${! meta("source_table").lowercase() + "_last_lsn" }'
        value: '${! meta("max_lsn") }'

output:
  kafka:
    addresses:
      - "${KAFKA_BOOTSTRAP_SERVERS}"
    topic: '${! meta("kafka_topic") }'
    key: '${! meta("kafka_key") }'
    max_in_flight: 64
    compression: snappy
    batching:
      count: 100
      period: 1s

logger:
  level: INFO
  format: json

metrics:
  prometheus: {}

http:
  enabled: true
  address: "0.0.0.0:4195"
  root_path: /benthos
  debug_endpoints: true
"""


def get_sink_pipeline_template() -> str:
    """Generate sink-pipeline.yaml template.

    Returns:
        Complete sink pipeline template as string for Kafka to PostgreSQL.
        Uses placeholders for customer-specific values:
        - {{ENV}}: Environment (nonprod, prod)
        - {{SINK_TOPICS}}: List of Kafka topics to consume
        - {{TABLE_CASES}}: Switch cases for table routing
    """
    return """# =============================================================================
# Redpanda Connect Sink Pipeline - Consolidated Multi-Schema
# =============================================================================
# This pipeline consumes CDC events from Kafka and routes to multiple
# PostgreSQL schemas in a single pipeline instance.
#
# Architecture:
#   - 1 Sink container handles ALL customers for an environment
#   - Routes by schema (customer) extracted from Kafka topic
#   - Single port, single consumer group per environment
#
# Pattern:
#   1. Consume from all customer topics for environment
#   2. Extract schema (customer) from topic name
#   3. Route to appropriate schema's staging table
#   4. Stored procedure merges staging -> final
#
# Environment: {{ENV}}
# =============================================================================

input:
  kafka:
    addresses:
      - "${KAFKA_BOOTSTRAP_SERVERS}"
    topics:
      {{SINK_TOPICS}}
    consumer_group: "{{ENV}}-sink-group"
    start_from_oldest: true

pipeline:
  processors:
    # Parse JSON and extract routing metadata from topic
    - bloblang: |
        # Parse the JSON content
        root = content().parse_json()

        # Extract schema (customer) from topic: env.customer.db.schema.table
        # Example: nonprod.avansas.AdOpusTest.dbo.Actor -> schema=avansas
        let topic = metadata("kafka_topic")
        let parts = $topic.split(".")
        root.__routing_schema = $parts.index(1)  # customer name = PostgreSQL schema
        root.__routing_table = this.payload.source.table

    # Extract CDC operation and enrich with metadata
    - bloblang: |
        # Map Debezium operation codes
        let op = if this.payload.op == "d" { "DELETE" }
                 else if this.payload.op == "c" { "INSERT" }
                 else if this.payload.op == "u" { "UPDATE" }
                 else { "UNKNOWN" }

        # For DELETE use before, for INSERT/UPDATE use after
        let base = if this.payload.op == "d" { this.payload.before } else { this.payload.after }

        # Build the record with all metadata
        root = $base
        root.__routing_schema = this.__routing_schema
        root.__routing_table = this.__routing_table
        root.__cdc_operation = $op
        root.__source = "kafka-cdc"
        root.__source_db = this.payload.source.db
        root.__source_table = this.payload.source.table
        root.__source_ts_ms = this.payload.source.ts_ms
        root.__sync_timestamp = now().ts_format("2006-01-02T15:04:05Z")

        # Capture Kafka metadata for offset tracking
        root.__kafka_offset = metadata("kafka_offset").number()
        root.__kafka_partition = metadata("kafka_partition").number()
        root.__kafka_timestamp = metadata("kafka_timestamp_unix").ts_format("2006-01-02T15:04:05Z")

    # Log processing info
    - log:
        level: INFO
        message: "Processing ${!this.__cdc_operation} for ${!this.__routing_schema}.${!this.__routing_table} offset=${!this.__kafka_offset}"

output:
  switch:
    cases:
      {{TABLE_CASES}}
      # Fallback for unknown schema/table combinations
      - output:
          drop: {}
          processors:
            - log:
                level: WARN
                message: "Unknown route: ${!this.__routing_schema}.${!this.__routing_table}"

logger:
  level: INFO
  format: json
  add_timestamp: true
  static_fields:
    "@service": redpanda-connect-sink
    environment: "{{ENV}}"

http:
  enabled: true
  address: "0.0.0.0:${HTTP_PORT:-4196}"
  root_path: /
  debug_endpoints: false

metrics:
  prometheus:
    push_url: ""
    push_interval: 30s
    push_job_name: redpanda-connect-sink-{{ENV}}
"""
