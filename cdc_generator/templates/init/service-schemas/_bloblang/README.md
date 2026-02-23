# Bloblang Files

This directory contains reusable Bloblang expressions stored in `.blobl` files.

## Usage

### In `column-templates.yaml`

```yaml
templates:
  user_status:
    name: user_status_label
    type: text
    value: file://services/_bloblang/examples/user_status_mapper.blobl
    description: Maps status codes to labels
```

### In sink table transforms

```yaml
rule_type: calculated
mapping: file://services/_bloblang/examples/priority_calculator.blobl
description: Dynamic priority calculation
```

## File Paths

Use relative paths from the project root with the `file://` prefix.

- ✅ `file://services/_bloblang/examples/user_status_mapper.blobl`
- ✅ `file://services/_bloblang/my_custom_logic.blobl`
- ❌ `file:///absolute/path/to/file.blobl` (avoid absolute paths)

## Validation

Validate all Bloblang references:

```bash
cdc manage-service --service <name> --validate-bloblang
```

Validate one file:

```bash
rpk connect lint services/_bloblang/examples/user_status_mapper.blobl
```

## Examples

- `user_status_mapper.blobl` - Simple pattern matching
- `priority_calculator.blobl` - Conditional logic
- `json_extractor.blobl` - Safe JSON parsing
