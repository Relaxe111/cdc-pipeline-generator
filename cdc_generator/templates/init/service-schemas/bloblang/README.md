# Bloblang Files

This directory contains reusable Bloblang expressions stored in `.blobl` files.

## Why External Files?

✅ **Syntax highlighting** - Better editor support for `.blobl` files  
✅ **Reusability** - Use same logic across multiple templates/transforms  
✅ **Version control** - Cleaner diffs for complex expressions  
✅ **Validation** - Validate with `rpk connect lint`  
✅ **Testing** - Unit test Bloblang independently  

## Usage

### In column-templates.yaml

```yaml
templates:
  user_status:
    name: user_status_label
    type: text
    value: file://service-schemas/bloblang/examples/user_status_mapper.blobl
    description: Maps status codes to labels
```

### In transform-rules.yaml

```yaml
rules:
  calculate_priority:
    rule_type: calculated
    mapping: file://service-schemas/bloblang/examples/priority_calculator.blobl
    description: Dynamic priority calculation
```

## File Paths

Use **relative paths** from the project root with `file://` prefix:
- ✅ `file://service-schemas/bloblang/examples/user_status_mapper.blobl`
- ✅ `file://service-schemas/bloblang/my_custom_logic.blobl`
- ❌ `file:///absolute/path/to/file.blobl` (avoid absolute paths)

## Validation

Validate all Bloblang (inline + files):
```bash
cdc manage-service --service <name> --validate-bloblang
```

Validate individual file:
```bash
rpk connect lint service-schemas/bloblang/examples/user_status_mapper.blobl
```

## Examples

See `examples/` directory for reference implementations:
- `user_status_mapper.blobl` - Simple pattern matching
- `priority_calculator.blobl` - Conditional logic
- `json_extractor.blobl` - Safe JSON parsing

## Best Practices

1. **Keep it simple** - Use external files for complex logic (>3 lines)
2. **Document** - Add comments explaining the transformation
3. **Test** - Validate with `rpk connect lint` before using
4. **Name descriptively** - `calculate_user_age.blobl` not `transform1.blobl`
5. **One purpose** - Each file should do one thing well
