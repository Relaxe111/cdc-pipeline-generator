# CLI Color Scheme

The CDC CLI tools now use ANSI colors for better readability and visual feedback.

## Color Codes

All scripts use a consistent color scheme:

### Status Icons & Colors

| Status | Color | Icon | Usage |
|--------|-------|------|-------|
| **Info** | Blue | ℹ | General information messages |
| **Success** | Green | ✓ | Successful operations, completions |
| **Warning** | Yellow | ⚠ | Warnings, non-critical issues |
| **Error** | Red | ✗ | Errors, failures |

### Section Headers

| Element | Color | Style |
|---------|-------|-------|
| Main headers | Cyan | Bold with separator lines |
| Subsections | Cyan | Regular with box-drawing characters |
| Lists | Gray | Dimmed text for secondary info |

## Examples

### Info Messages
```
ℹ  Loaded environment variables from /workspace/.env
ℹ  Using 6 generated table definitions
```

### Success Messages
```
✓ Generated table definition: Actor.yaml
✓ Updated customer config: avansas.yaml
✓ DONE
```

### Warnings
```
⚠  No primary key detected, using first column: ActorId
⚠  Could not inspect table: InvalidTable
⚠  DRY RUN MODE
```

### Errors
```
✗ Docker inspection failed: connection refused
✗ Config not found: nonexistent-customer
```

### Headers
```
============================================================
CDC Table Definition Generator
============================================================

──────────────────────────────────────────────────────────
  Customer: avansas
──────────────────────────────────────────────────────────

📋 Next steps:
  1. Review updated customer configs
  2. Run pipeline generation
```

## Implementation

Colors are implemented using ANSI escape codes in a `Colors` class:

```python
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    GRAY = '\033[90m'
```

Helper functions provide consistent formatting:
- `print_info(msg)` - Blue info messages
- `print_success(msg)` - Green success messages  
- `print_warning(msg)` - Yellow warnings
- `print_error(msg)` - Red errors
- `print_header(msg)` - Cyan bold headers
- `cprint(text, color, bold)` - Custom colored output

## Benefits

1. **Quick Scanning** - Color-coded messages help identify important information at a glance
2. **Status Recognition** - Instant visual feedback on operation success/failure
3. **Hierarchical Structure** - Headers and sections clearly delineated
4. **Professional Output** - Modern CLI appearance
5. **Consistency** - Same color scheme across all CDC scripts

## Terminal Compatibility

ANSI colors work in most modern terminals:
- ✅ macOS Terminal
- ✅ iTerm2
- ✅ Linux terminals (bash, zsh, fish)
- ✅ VS Code integrated terminal
- ✅ Windows Terminal
- ✅ WSL terminals

Legacy terminals (cmd.exe without Windows Terminal) may show escape codes as plain text, but functionality is not affected.
