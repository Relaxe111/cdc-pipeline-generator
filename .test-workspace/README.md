# .test-workspace - Manual Testing Directory

This directory is for **manual testing only**. All contents are gitignored.

## Quick Start

```bash
# Create a test project
mkdir -p .test-workspace/my-project
cd .test-workspace/my-project

# Initialize structure (example)
cat > source-groups.yaml << 'EOF'
asma:
  pattern: db-shared
  type: postgres
  sources: {}
EOF

mkdir -p services service-schemas

# Test commands
# (run from generator root)
cd ../..
cdc manage-source-groups --info

# Cleanup
rm -rf .test-workspace/my-project
```

## Why This Directory Exists

- **Automated tests** use pytest's `tmp_path` (auto-cleanup)
- **Manual testing** needs a persistent workspace → `.test-workspace/`
- Everything in this directory is gitignored
- Safe to delete entire directory at any time

## Alternative: Use Pytest

For most testing, prefer using pytest:

```bash
# Run existing tests
pytest tests/ -k test_something

# Or use Python REPL with fixtures
python -c "
from pathlib import Path
tmp = Path('/tmp/my-test')
tmp.mkdir(exist_ok=True)
# ... your test code ...
"
```

## See Also

- `_docs/development/TEST_CLEANUP.md` - Full cleanup strategy
- `tests/conftest.py` - Shared test fixtures
