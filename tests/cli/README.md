# CDC Pipeline Generator — Test Plan

> **AI-friendly reference** for understanding testing structure and planned coverage.

---

## 📁 Directory Structure

```
tests/
├── __init__.py
├── conftest.py                      # (future) shared unit test fixtures
├── test_column_templates.py         # Unit: column template loader
├── test_extra_columns.py            # Unit: extra columns manager
├── test_transform_rules.py          # Unit: transform rules loader
├── test_type_mapper.py              # Unit: type mapping engine
├── test_structure_replicator.py     # Unit: DDL structure replication
├── test_sink_from_field.py          # Unit: sink --from field handling
│
└── cli/                             # End-to-end CLI tests (via fish shell)
    ├── __init__.py
    ├── conftest.py                  # Shared fixtures: run_cdc, run_cdc_completion
    ├── README.md                    # ← this file
    └── test_scaffold.py             # E2E: cdc scaffold
```

---

## 🏗️ Test Categories

### Unit Tests (`tests/*.py`)

**Run with:** `cdc test`

Test individual modules and functions in isolation. No subprocess calls,
no file system side effects outside `tmp_path`. Fast — typically < 1s.

| File | Module Under Test | What It Covers |
|------|------------------|----------------|
| `test_column_templates.py` | `core.column_templates` | YAML parsing, caching, validation, frozen dataclasses |
| `test_transform_rules.py` | `core.transform_rules` | Rule types, conditions, output columns, caching |
| `test_extra_columns.py` | `core.extra_columns` | Add/remove/list/resolve for extra columns & transforms |
| `test_type_mapper.py` | `helpers.type_mapper` | MSSQL↔PgSQL type mapping, adapters, case insensitivity |
| `test_structure_replicator.py` | `validators.manage_service.structure_replicator` | DDL replication from source to sink |
| `test_sink_from_field.py` | `validators.manage_service.sink_operations` | `--from` field mapping on sink tables |

### CLI End-to-End Tests (`tests/cli/*.py`)

**Run with:** `cdc test --cli`

Test full CLI command flows through a real **fish** shell subprocess — the
same way a user types commands in the dev container terminal.  Each test
gets an isolated temporary directory (`isolated_project` fixture) — no
pollution between tests or the real workspace.

The `run_cdc` fixture runs `fish -c "cdc <args>"` so every test validates
the full chain: fish shell → `cdc` entry point → `commands.py` dispatch →
subcommand module → handler.

| File | CLI Command | What It Covers |
|------|------------|----------------|
| `test_scaffold.py` | `cdc scaffold` | Full project creation, directory structure, file content, error handling, --update mode |

---

## 🔮 Planned Tests (Not Yet Implemented)

### Unit Tests

| Planned File | Module | Coverage |
|-------------|--------|----------|
| `test_pipeline_generator.py` | `core.pipeline_generator` | Pipeline YAML generation, variable substitution |
| `test_yaml_loader.py` | `helpers.yaml_loader` | YAML loading/saving, comment preservation |
| `test_service_config.py` | `helpers.service_config` | Service config loading, project root detection |
| `test_sink_operations.py` | `validators.manage_service.sink_operations` | Sink add/remove/list, table mapping |
| `test_schema_generator.py` | `validators.manage_service.schema_generator` | JSON Schema generation from service YAML |
| `test_sink_extra_ops.py` | `validators.manage_service.sink_extra_ops` | Extra columns/transforms service-level operations |

### CLI End-to-End Tests

| Planned File | CLI Command | What It Should Cover |
|-------------|------------|---------------------|
| `test_manage_source_groups.py` | `cdc manage-source-groups` | Add/remove/update server groups, multi-server support, validation |
| `test_manage_sink_groups.py` | `cdc manage-sink-groups` | Add/remove sink groups, server registration |
| `test_manage_service.py` | `cdc manage-service` | Full service lifecycle: create → add tables → add sinks → add sink tables → generate |
| `test_manage_service_sinks.py` | `cdc manage-service --add-sink` | Sink operations: add/remove sinks, add/remove sink tables, column mapping |
| `test_manage_service_extras.py` | `cdc manage-service --add-extra-column` | Extra columns and transforms via CLI |
| `test_generate.py` | `cdc generate` | Pipeline generation from configured services |
| `test_scaffold_update.py` | `cdc scaffold --update` | Detailed update mode: merge settings, add new dirs, preserve existing |
| `test_full_flow.py` | Multiple commands | Complete flow: scaffold → manage-source-groups → manage-service → generate |
| `test_completions.py` | Fish autocompletions | Verify all `cdc` subcommands and flags are offered by fish Tab-completion |

---

## 🧪 Running Tests

```bash
# Run unit tests only (default)
cdc test

# Run CLI end-to-end tests only
cdc test --cli

# Run everything (unit + CLI e2e)
cdc test --all

# Verbose output
cdc test -v

# Run specific test by name
cdc test -k scaffold

# Run specific test file
cdc test tests/cli/test_scaffold.py
```

---

## 📋 Writing New Tests

### Adding a Unit Test

1. Create `tests/test_<module>.py`
2. Import the module under test directly
3. Use `pytest.fixture()` for setup (e.g., `tmp_path` for file operations)
4. Keep tests fast — no subprocess, no network

```python
"""Tests for helpers.my_module."""

import pytest
from cdc_generator.helpers.my_module import my_function


class TestMyFunction:
    def test_basic_case(self) -> None:
        result = my_function("input")
        assert result == "expected"
```

### Adding a CLI E2E Test

1. Create `tests/cli/test_<command>.py`
2. Add `pytestmark = pytest.mark.cli` at module level
3. Use `run_cdc` fixture to invoke commands through a real fish shell
4. Use `isolated_project` fixture when you need to inspect generated files
5. Assert on exit code, stdout, and generated files

```python
"""End-to-end tests for cdc <command>."""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc

pytestmark = pytest.mark.cli


class TestCommand:
    def test_happy_path(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc("mycommand", "arg1", "--flag")
        assert result.returncode == 0
        assert (isolated_project / "expected_file.yaml").is_file()
```

### Adding an Autocompletion Test

1. Use `run_cdc_completion` fixture to query fish completions
2. Assert that expected subcommands / flags appear in stdout

```python
"""Tests for fish autocompletions."""

import pytest

from tests.cli.conftest import RunCdcCompletion

pytestmark = pytest.mark.cli


class TestCompletions:
    def test_scaffold_flags(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        result = run_cdc_completion("cdc scaffold --")
        assert "--pattern" in result.stdout
        assert "--source-type" in result.stdout
```

### Test Naming Convention

- **Unit:** `test_<module_name>.py` → classes `TestFeatureName`
- **CLI E2E:** `test_<cli_command>.py` → classes `TestCommandScenario`
- **Methods:** `test_<what_it_verifies>` (descriptive, no abbreviations)

---

## ⚙️ Pytest Configuration

Tests use the `cli` marker to separate unit tests from CLI e2e tests:

```ini
# pyproject.toml
[tool.pytest.ini_options]
markers = ["cli: End-to-end CLI tests (run with cdc test --cli)"]
```

---

## 🎯 Coverage Goals

| Category | Target | Current |
|----------|--------|---------|
| Unit tests | All core modules | `column_templates`, `transform_rules`, `extra_columns`, `type_mapper`, `structure_replicator`, `sink_from_field` |
| CLI e2e | All `cdc` subcommands | `scaffold` only |
| Integration | Full workflow flows | Not started |

**Priority for next tests:**
1. `test_manage_service.py` — most complex command, most user-facing
2. `test_manage_source_groups.py` — foundational for project setup
3. `test_generate.py` — validates pipeline output
4. `test_full_flow.py` — end-to-end confidence
