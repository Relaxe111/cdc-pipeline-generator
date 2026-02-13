# 0004 - Runtime Bloblang Validation with Sample Data

**Status:** Proposed  
**Date:** 2026-02-10

## Context

Static Bloblang linting (`rpk connect lint`) catches syntax errors and unknown functions,
but cannot detect:

- Typos in variable names (`thi.column` instead of `this.column`) — valid syntax, runtime failure
- References to non-existent columns (`this.wrong_column`) — valid syntax, wrong data
- Type mismatches (calling `parse_json()` on an integer column)
- Logic errors in conditions

We need a runtime validation layer that tests Bloblang expressions against realistic
sample data generated from actual database schemas.

## Decision

### 1. Generate Sample Data During `cdc generate`

During pipeline generation, inspect each source table's schema and generate a sample
data file with realistic values based on column types and names.

**Output location:**
```
generated/
  sample-data/
    {service}/
      {schema}.{table}.json
```

**Example** (`generated/sample-data/directory/public.customers.json`):
```json
{
  "id": 12345,
  "name": "sample_name",
  "subdomain": "sample_subdomain",
  "email": "sample@example.com",
  "created_at": "2024-01-15T10:30:00Z",
  "metadata": "{\"key\": \"value\"}",
  "status": 1,
  "is_active": true
}
```

### 2. Type-Based Sample Value Generation

Map database column types to realistic sample values:

| DB Type | Sample Value | Smart Inference |
|---------|-------------|-----------------|
| `INTEGER` / `BIGINT` | `12345` | — |
| `BOOLEAN` | `true` | — |
| `VARCHAR` / `TEXT` | `"sample_text"` | Column name heuristics (see below) |
| `TIMESTAMP` / `DATE` | `"2024-01-15T10:30:00Z"` | — |
| `JSON` / `JSONB` | `{"key": "value"}` | — |
| `NUMERIC` / `DECIMAL` | `99.99` | — |
| `UUID` | `"550e8400-e29b-41d4-a716-446655440000"` | — |
| `BYTEA` / `BINARY` | `"base64encoded=="` | — |

**Column name heuristics** for smarter text values:

| Column name contains | Sample value |
|----------------------|-------------|
| `email` | `"sample@example.com"` |
| `url` / `link` | `"https://example.com"` |
| `phone` | `"+47 12345678"` |
| `name` | `"sample_name"` |
| `ip` | `"192.168.1.1"` |
| `json` / `metadata` / `data` | `"{\"key\": \"value\"}"` (string-encoded JSON) |

### 3. Runtime Validation Command

```bash
# Validate with runtime checks (requires generated sample data)
cdc manage-service --service directory --validate-bloblang --runtime

# Or as part of comprehensive validation
cdc manage-service --service directory --validate-config --runtime
```

### 4. Runtime Validation Flow

```
┌─────────────────────┐
│  Load sample data   │  generated/sample-data/{service}/{table}.json
│  for each table     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  For each template/ │  column-templates.yaml, transform-rules.yaml
│  transform rule     │  Check applies_to to find matching tables
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Build test pipeline│  input: generate (sample data)
│  with mapping       │  pipeline: mapping (expression)
│                     │  output: stdout
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Run with rpk       │  rpk connect run --timeout 5s pipeline.yaml
│  Capture output     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Report results     │  ✓ Produced output: "active"
│                     │  ✗ Runtime error: undefined variable 'thi'
└─────────────────────┘
```

### 5. Test Pipeline Structure

```yaml
input:
  generate:
    mapping: |
      root = {SAMPLE_DATA_JSON}
      # Simulate CDC metadata
      meta source_table = "{table_name}"
      meta schema = "{schema_name}"
      meta kafka_topic = "cdc.{service}.{table}"
      meta kafka_offset = "0"
      meta kafka_partition = "0"
      meta operation = "c"
      meta lsn = "00000000/00000001"
    interval: ""
    count: 1
pipeline:
  processors:
    - mapping: |
        {BLOBLANG_EXPRESSION}
output:
  stdout: {}
```

### 6. Implementation Modules

**New file:** `cdc_generator/validators/runtime_validator.py`

```python
def generate_sample_value(column_name: str, column_type: str) -> Any:
    """Generate realistic sample value based on type and name."""

def generate_sample_data(service: str, table_key: str) -> dict:
    """Generate sample data for a table from database schema."""

def save_sample_data(service: str, table_key: str, data: dict) -> Path:
    """Save sample data to generated/sample-data/."""

def validate_expression_runtime(
    expression: str,
    sample_data: dict,
    metadata: dict | None = None,
) -> tuple[bool, Any, str | None]:
    """Run Bloblang expression against sample data.
    Returns: (success, output_value, error_message)
    """

def validate_templates_runtime(service: str) -> tuple[int, int]:
    """Validate all templates with runtime checks."""

def validate_transforms_runtime(service: str) -> tuple[int, int]:
    """Validate all transforms with runtime checks."""
```

**Modified:** `cdc_generator/core/pipeline_generator.py`
- Add sample data generation during `cdc generate`

**Modified:** `cdc_generator/cli/service.py`
- Add `--runtime` flag to validation commands

### 7. Integration with `cdc generate`

```python
# In pipeline generation flow
def generate_pipelines(service: str):
    # ... existing pipeline generation ...

    # Generate sample data for runtime validation
    for table_key in service_config.tables:
        sample = generate_sample_data(service, table_key)
        save_sample_data(service, table_key, sample)
```

### 8. Expected Output

```
$ cdc manage-service --service directory --validate-bloblang --runtime

Runtime validating column templates against sample data...

  public.customers:
    ✓ source_table → "customers" (meta)
    ✓ sync_timestamp → "2024-01-15T10:30:00Z" (generated)
    ✓ row_hash → "a1b2c3d4..." (computed)
    ✗ extract_email → RuntimeError: variable 'thi' undefined
        services/_schemas/_bloblang/json_extractor.blobl:15
        Expected: this.metadata  Got: thi.metadata

  public.customer_user:
    ✓ source_table → "customer_user" (meta)
    ✓ sync_id → "550e8400-..." (generated)
```

## Consequences

### Positive
- Catches typos and logic errors that static linting misses
- Sample data generated once, used many times (fast validation)
- Sample data is version-controllable (review changes)
- Enables CI/CD pipeline validation before deployment
- Realistic testing without touching production data
- Foundation for future dry-run pipeline testing

### Negative
- Adds time to `cdc generate` (database inspection for sample data)
- Sample data may not cover all edge cases
- Requires `rpk connect run` available in environment

### Notes
- Sample data files go in `generated/` (read-only, gitignored or committed)
- Runtime validation is optional (`--runtime` flag), not default
- Static validation (`--validate-bloblang`) remains the default quick check
- Consider adding `--generate-sample-data` as standalone command
- Future: could extend to test full pipeline flow (source → transform → sink)
