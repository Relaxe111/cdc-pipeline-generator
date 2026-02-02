# Metadata Comments Fix - Testing Guide

## Problem Fixed
Metadata header comments in `server_group.yaml` were disappearing when running `cdc manage-server-group --create` and other operations. This happened repeatedly across different development sessions.

## Solution Implemented
Created a centralized **metadata comment manager** (`metadata_comments.py`) that ensures:

1. **File header comments are ALWAYS added** when creating new files
2. **File header comments are ALWAYS preserved** when updating existing files
3. **Validation before writing** to catch any missing comments before they hit disk
4. **Single source of truth** for all comment-related logic

## Architecture

### New Module: `metadata_comments.py`
Location: `cdc_generator/validators/manage_server_group/metadata_comments.py`

**Key Functions:**
- `get_file_header_comments()` - Returns standard file header block
- `ensure_file_header_exists()` - Adds header if missing
- `validate_output_has_metadata()` - Safety check before writing
- `get_update_timestamp_comment()` - Timestamp for updates
- `add_metadata_stats_comments()` - Statistics comments

### Modified Files
1. **cli_handlers.py** - `handle_add_group()` now uses metadata manager
2. **config.py** - `save_database_exclude_patterns()` and `save_schema_exclude_patterns()` use metadata manager
3. **yaml_writer.py** - `update_server_group_yaml()` uses metadata manager
4. **__init__.py** - Exports metadata comment utilities

## Testing Steps

### Step 1: Rebuild Docker Image
From the cdc-pipeline-generator directory on the HOST:

```fish
cd /Users/igor/carasent/cdc-pipelines-development/cdc-pipeline-generator
docker compose build dev
docker compose down
docker compose up -d
```

### Step 2: Verify Version Inside Container
```fish
# Enter container
docker compose exec dev fish

# Check version
pip show cdc-pipeline-generator | grep "^Version:"
# Should show: Version: 0.1.12

# Check cdc command
cdc --version
```

### Step 3: Test in adopus-cdc-pipeline
```fish
# Inside container, navigate to adopus implementation
cd /implementations/adopus

# Backup existing server_group.yaml
cp server_group.yaml server_group.yaml.backup

# Test 1: --create command (create new file)
rm server_group.yaml  # Remove to test fresh creation
cdc manage-server-group --create test-group \\
    --pattern db-per-tenant \\
    --source-type mssql \\
    --host localhost \\
    --port 1433 \\
    --user sa \\
    --password Test123

# Verify metadata comments exist
head -n 20 server_group.yaml
# Should see:
# - "AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY"
# - "Use 'cdc manage-server-group' commands"
# - Multiple separator lines (=============)
```

### Step 4: Test --add-to-ignore-list
```fish
# Restore backup
rm server_group.yaml
cp server_group.yaml.backup server_group.yaml

# Add a pattern
cdc manage-server-group --add-to-ignore-list "test_pattern"

# Verify metadata comments still present
head -n 20 server_group.yaml
# Should STILL have header comments
```

### Step 5: Test --add-to-schema-excludes
```fish
# Add a schema exclude pattern
cdc manage-server-group --add-to-schema-excludes "test_schema"

# Verify metadata comments still present
head -n 20 server_group.yaml
```

### Step 6: Test --update
```fish
# Run update (connects to database)
cdc manage-server-group --update

# Verify metadata comments still present AND timestamp updated
head -n 30 server_group.yaml
# Should have:
# - File header comments
# - "Updated at:" timestamp
# - Statistics (Total: X databases...)
```

## Expected File Structure

After ANY operation, `server_group.yaml` should have this structure:

```yaml
# ============================================================================
# AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY
# Use 'cdc manage-server-group' commands to modify this file
# ============================================================================
# 
# This file contains the server group configuration for CDC pipelines.
# Changes made directly to this file may be overwritten by CLI commands.
# 
# Common commands:
#   - cdc manage-server-group --update              # Refresh database/schema info
#   - cdc manage-server-group --info                # Show configuration details
#   - cdc manage-server-group --add-to-ignore-list  # Add database exclude patterns
#   - cdc manage-server-group --add-to-schema-excludes  # Add schema exclude patterns
# 
# For detailed documentation, see:
#   - CDC_CLI.md in the implementation repository
#   - cdc-pipeline-generator/docs/ for generator documentation
# ============================================================================

server_group:
  # ============================================================================
  # AdOpus Server Group (db-per-tenant)
  # ============================================================================
  adopus:
    pattern: db-per-tenant
    # ... rest of config
```

## Validation Checks

### Check 1: File Header Present
```fish
grep -c "AUTO-GENERATED FILE" server_group.yaml
# Should output: 1 (or more)
```

### Check 2: Commands Documentation Present
```fish
grep -c "cdc manage-server-group" server_group.yaml
# Should output: at least 4
```

### Check 3: Separators Present
```fish
grep -c "============" server_group.yaml
# Should output: at least 4 (2 for file header, 2 for server group section)
```

## Rollback Plan

If issues occur, restore from backup:
```fish
cd /implementations/adopus
cp server_group.yaml.backup server_group.yaml
```

## Future Protection

The solution includes **three layers of protection**:

1. **Automatic Addition**: `ensure_file_header_exists()` adds header if missing
2. **Validation**: `validate_output_has_metadata()` prevents writing without header
3. **Centralization**: All write operations MUST use the metadata manager

**Any new code that writes to server_group.yaml MUST:**
- Import from `metadata_comments`
- Call `ensure_file_header_exists()` on preserved comments
- Call `validate_output_has_metadata()` before writing

This architectural pattern prevents the issue from recurring even with future development.
