# Test Cleanup Strategy

## Problem

Test artifacts (`source-groups.yaml`, service files, generated content) were persisting in the generator library root directory, polluting the clean library codebase.

## Solution

### Centralized Test Workspace

All test artifacts are isolated using **pytest's `tmp_path` fixture**, which creates unique temporary directories per test. Additionally, a `.test-workspace/` directory at the project root is gitignored for manual testing.

**Test Isolation Hierarchy:**
1. **Automated Tests** → `pytest tmp_path` (auto-created, auto-deleted)
2. **Manual Testing** → `.test-workspace/` (gitignored, manual cleanup)
3. **E2E Testing** → `implementation/` (gitignored, documented)

This ensures:
- ✅ Zero artifacts in generator root after test runs
- ✅ Each test gets isolated workspace
- ✅ Failed tests leave no residue
- ✅ Manual testing has designated space

### 1. Git Ignore Configuration

Added to `.gitignore`:
```gitignore
# Testing (pytest artifacts and manual test workspace)
.pytest_cache/
.coverage
htmlcov/
.tox/
.test-workspace/*
!.test-workspace/README.md

# Test artifacts (safety net - should be in tmp_path, not root)
source-groups.yaml
service-schemas/

# E2E test workspace (manual testing)
implementation/*
!implementation/.gitkeep
!implementation/README.md
```.test-workspace/         # Manual test workspace (gitignored)
│   └── (create test projects here for manual testing)
├── implementation/          # E2E test workspace (gitignored)
│   ├── .gitkeep            # Tracked (keeps dir in git)
│   └── README.md           # Tracked (documentation)
├── services/               # Empty (implementation-specific)
│   └── .gitkeep           # Tracked (keeps dir in git)
├── examples/              # Tracked examples for documentation
│   ├── db-shared/
│   └── db-per-tenant/
└── tests/                 # All tests use tmp_path fixtures
    ├── conftest.py        # project_dir fixture using tmp_path
    └── test_*.py          # Unit tests (isolated via tmp_path)
```

**Key Directories:**

| Directory | Purpose | Lifecycle | Tracked in Git? |
|-----------|---------|-----------|-----------------|
| `tests/` | Test code | Permanent | ✅ Yes |
| `tmp_path` (pytest) | Auto test workspace | Per-test | ❌ N/A (temp) |
| `.test-workspace/` | Manual testing | Manual cleanup | ❌ Gitignored |
| `implementation/` | E2E testing docs | Permanent (docs only) | ⚠️ Partial (.gitkeep, README.md) |
| `utomated Tests (Recommended):**

All tests use pytest's `tmp_path` fixture for automatic isolation and cleanup:

```python
def test_something(tmp_path: Path) -> None:
    """Tests run in isolated temporary directories (auto-deleted)."""
    # tmp_path is unique per test: /tmp/pytest-xxx/test_something0/
    project = tmp_path / "test-project"
    project.mkdir()
    
    # Create test structure
    (project / "source-groups.yaml").write_text(...)
    (project / "services").mkdir()
    (project / "service-schemas").mkdir()
    
    # Run test
    result = some_function(project)
    assert result == expected
    
    # ✅ Automatic cleanup - tmp_path deleted after test completes
```

**Shared Fixture (Project Directory):**

Use the `project_dir` fixture from `tests/conftest.py`:

```python
def test_with_project(project_dir: Path) -> None:
    """Use pre-configured project structure."""
    # project_dir already has:
    # - services/
    # - source-groups.yaml
    # - sink-groups.yaml
    # - service-schemas/
    
    result = some_function(project_dir)
    assert result == expected
    
    # ✅ Automatic cleanup via tmp_path
```

**Manual Testing (When Needed):**

FoVerify gitignore patterns
cat .gitignore | grep -A5 "Test artifacts"

# Create test artifacts (simulating failed test cleanup)
mkdir -p .test-workspace/manual-test
echo "test" > source-groups.yaml
echo "test" > services/test.yaml
mkdir -p service-schemas/chat/public
echo "columns: []" > service-schemas/chat/public/audit_log.yaml

# Verify they're all gitignored
git status --short | grep -E "(test-workspace|source-groups|services|service-schemas)"
# Should return nothing (all ignored)

# Clean up
rm -rf .test-workspace/ source-groups.yaml services/test.yaml service-schemas/

# Run tests and verify no artifacts remain
pytest tests/ -q
ls -la | grep -E "(source-groups|service-schemas)"
# Should return nothing (clean root)rvice myservice --inspect

# Manual cleanup when done
cd ../..
rm -rf .test-workspace/my-testert result == expected
    
    # No cleanup needed - tmp_path auto-deleted after test
```

**Shared fixture for common patterns:**

```python
@pytest.fixture
def isolated_project(tmp_path: Path) -> Path:
    """Create isolated project with standard structure."""
    # See tests/conftest.py for implementation
    ...
```

### 4. Verification

```bash
# Create test artifacts
echo "test" > source-groups.yaml
echo "test" > services/test.yaml
mkdir -p service-schemas/chat/public
echo "columns: []" > service-schemas/chat/public/audit_log.yaml

# Verify they're gitignored
git status --short
# Should NOT show these files

# Clean up
rm -f source-groups.yaml services/test.yaml
rm -rf service-schemas/
```

## Benefits

✅ **Clean library root** - No test artifacts persist  
✅ **Automatic cleanup** - pytest's tmp_path handles it  
✅ **Test isolation** - Each test gets fresh workspace  
✅ **Git safety** - Impossible to accidentally commit test files  
✅ **Clear separation** - Library code vs test code vs implementation code  

## Migration Status

### Already Using tmp_path ✅

All existing tests already use proper isolation:
- `tests/conftest.py` - `isolated_project` fixture
- `tests/cli/test_scaffold.py` - E2E scaffold testing
- `tests/cli/test_manage_service.py` - Service management
- `tests/test_server_group_*.py` - Server group tests (80 tests)

### Cleanup Completed ✅

- ✅ Removed `source-groups.yaml` from generator root
- ✅ Removed `service-schemas/` directory from generator root
- ✅ Added `.gitkeep` files to preserve empty directories
- ✅ Updated `.gitignore` to prevent test artifacts
- ✅ Created `implementation/README.md` documentation
- ✅ Verified all tests still pass
- ✅ Verified test artifacts are properly gitignored

## Manual E2E Testing (When Needed)

If you need to manually test generator commands:

```bash
# 1. Create temporary workspace
mkdir -p implementation/manual-test
cd implementation/manual-test

# 2. Initialize
cdc scaffold --implementation test --pattern db-shared

# 3. Test commands
cdc manage-source-groups --update
cdc manage-service --service myservice --inspect

# 4. Clean up when done
cd ../..
rm -rf implementation/manual-test
```

All contents in `implementation/` (except `.gitkeep` and `README.md`) are gitignored and safe to delete.

## Reference

- **Generator Library**: `cdc-pipeline-generator/` (this repo)
- **Real Implementations**: 
  - `adopus-cdc-pipeline/` (separate Bitbucket repo)
  - `asma-cdc-pipeline/` (separate Bitbucket repo)
- **Test Strategy**: All tests use `tmp_path` for isolation
- **E2E Workspace**: `implementation/` (gitignored, manual testing only)
