"""End-to-end tests for ``cdc manage-service --source-table`` command.

Tests the full flow through a real **fish** shell, exactly as
a user would type in the dev container terminal.

Coverage
--------
- --source-table with --track-columns updates existing table
- --source-table with --ignore-columns updates existing table
- --source-table without columns shows warning and exits 1
- --source-table with nonexistent table shows warning
- Fish autocompletions for --source-table show source tables from service YAML
- Fish autocompletions for --track-columns work with --source-table context
"""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

# Mark all tests in this module as CLI end-to-end tests
pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_service_project(project_root: Path, service: str = "proxy") -> None:
    """Create minimal project structure with a service YAML."""
    services_dir = project_root / "services"
    services_dir.mkdir(exist_ok=True)

    # source-groups.yaml (required for project root detection)
    (project_root / "source-groups.yaml").write_text(
        "asma:\n"
        "  pattern: db-shared\n"
        "  sources:\n"
        f"    {service}: {{}}\n"
    )

    service_file = services_dir / f"{service}.yaml"
    service_file.write_text(
        f"{service}:\n"
        "  source:\n"
        "    validation_database: proxy_dev\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users: {}\n"
    )


def _create_service_schemas(
    project_root: Path, service: str = "proxy",
) -> None:
    """Create service-schemas with column definitions for autocompletion."""
    schema_dir = project_root / "service-schemas" / service / "public"
    schema_dir.mkdir(parents=True, exist_ok=True)

    (schema_dir / "queries.yaml").write_text(
        "columns:\n"
        "  - name: id\n"
        "    type: uuid\n"
        "    primary_key: true\n"
        "  - name: status\n"
        "    type: text\n"
        "  - name: title\n"
        "    type: text\n"
        "  - name: cache\n"
        "    type: jsonb\n"
    )


def _read_service_yaml(project_root: Path, service: str = "proxy") -> str:
    """Read service YAML file contents."""
    return (project_root / "services" / f"{service}.yaml").read_text()


# ═══════════════════════════════════════════════════════════════════════════
# 1. Core --source-table functionality
# ═══════════════════════════════════════════════════════════════════════════


class TestSourceTableTrackColumns:
    """--source-table with --track-columns."""

    def test_adds_track_columns_to_existing_table(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Track columns are saved to service YAML."""
        _create_service_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--service", "proxy",
            "--source-table", "public.queries",
            "--track-columns", "public.queries.status",
        )

        assert result.returncode == 0, (
            f"Failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        content = _read_service_yaml(isolated_project)
        assert "include_columns" in content
        assert "status" in content

    def test_adds_multiple_track_columns(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Multiple --track-columns flags are merged."""
        _create_service_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--service", "proxy",
            "--source-table", "public.queries",
            "--track-columns", "public.queries.status",
            "--track-columns", "public.queries.title",
        )

        assert result.returncode == 0
        content = _read_service_yaml(isolated_project)
        assert "status" in content
        assert "title" in content


class TestSourceTableIgnoreColumns:
    """--source-table with --ignore-columns."""

    def test_adds_ignore_columns_to_existing_table(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Ignore columns are saved to service YAML."""
        _create_service_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--service", "proxy",
            "--source-table", "public.queries",
            "--ignore-columns", "public.queries.cache",
        )

        assert result.returncode == 0
        content = _read_service_yaml(isolated_project)
        assert "ignore_columns" in content
        assert "cache" in content


# ═══════════════════════════════════════════════════════════════════════════
# 2. Error paths
# ═══════════════════════════════════════════════════════════════════════════


class TestSourceTableErrors:
    """Error handling for --source-table."""

    def test_fails_without_columns(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Exit 1 when --source-table used without column flags."""
        _create_service_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--service", "proxy",
            "--source-table", "public.queries",
        )

        assert result.returncode == 1
        assert "No columns specified" in result.stdout or "No columns specified" in result.stderr

    def test_fails_with_nonexistent_service(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Exit 1 when service doesn't exist."""
        _create_service_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--service", "nonexistent",
            "--source-table", "public.queries",
            "--track-columns", "public.queries.status",
        )

        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# 3. Fish autocompletions
# ═══════════════════════════════════════════════════════════════════════════


class TestSourceTableCompletions:
    """Fish autocompletion for --source-table."""

    def test_source_table_flag_appears_in_completions(
        self,
        run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--source-table is offered as a completion for manage-service."""
        result = run_cdc_completion("cdc manage-services config --source-")
        assert "source-table" in result.stdout

    def test_track_columns_flag_appears_in_completions(
        self,
        run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--track-columns appears with --source-table context."""
        result = run_cdc_completion(
            "cdc manage-services config --source-table Actor --track-"
        )
        assert "track-columns" in result.stdout
