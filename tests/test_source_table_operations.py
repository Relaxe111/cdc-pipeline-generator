"""Unit tests for --source-table (update existing source table) operations.

Tests the handler and underlying table_operations logic for updating
track_columns and ignore_columns on existing source tables.
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_source import (
    handle_update_source_table,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_service.table_operations import (
    add_table_to_service,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Create an isolated project dir with services/ and chdir into it."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    # source-groups.yaml marker so get_project_root() finds it
    (tmp_path / "source-groups.yaml").write_text("asma:\n  pattern: db-shared\n")
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    with patch(
        "cdc_generator.validators.manage_service.config.SERVICES_DIR",
        services_dir,
    ):
        try:
            yield tmp_path
        finally:
            os.chdir(original_cwd)


@pytest.fixture()
def service_with_tables(project_dir: Path) -> Path:
    """Create a service YAML with two existing source tables."""
    service_file = project_dir / "services" / "proxy.yaml"
    service_file.write_text(
        "proxy:\n"
        + "  source:\n"
        + "    validation_database: proxy_dev\n"
        + "    tables:\n"
        + "      public.queries: {}\n"
        + "      public.users: {}\n"
    )
    return service_file


def _make_args(
    service: str = "proxy",
    source_table: str = "public.queries",
    track_columns: list[str] | None = None,
    ignore_columns: list[str] | None = None,
) -> argparse.Namespace:
    """Build a mock argparse.Namespace for handle_update_source_table."""
    return argparse.Namespace(
        service=service,
        source_table=source_table,
        schema=None,
        track_columns=track_columns,
        ignore_columns=ignore_columns,
    )


# ---------------------------------------------------------------------------
# Unit tests: add_table_to_service (update path)
# ---------------------------------------------------------------------------


class TestAddTableToServiceUpdatePath:
    """Tests for add_table_to_service when table already exists."""

    def test_update_adds_track_columns_to_existing_table(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Track columns are added to an existing table."""
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, None, ["status", "title"],
        )

        assert result is True
        saved = load_yaml_file(service_with_tables)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert sorted(table_def["include_columns"]) == ["status", "title"]

    def test_handler_accepts_multiple_track_columns_in_single_flag_group(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Parser-style nested list for --track-columns is flattened correctly."""
        args = argparse.Namespace(
            service="proxy",
            source_table="public.queries",
            schema=None,
            track_columns=[["public.queries.status", "public.queries.title"]],
            ignore_columns=None,
        )

        result = handle_update_source_table(args)
        assert result == 0

        saved = load_yaml_file(service_with_tables)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert sorted(table_def["include_columns"]) == ["status", "title"]

    def test_update_adds_ignore_columns_to_existing_table(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Ignore columns are added to an existing table."""
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, ["cache", "token"], None,
        )

        assert result is True
        saved = load_yaml_file(service_with_tables)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert sorted(table_def["ignore_columns"]) == ["cache", "token"]

    def test_update_merges_with_existing_track_columns(
        self, project_dir: Path,
    ) -> None:
        """New track columns are merged with pre-existing ones."""
        service_file = project_dir / "services" / "proxy.yaml"
        service_file.write_text(
            "proxy:\n"
            + "  source:\n"
            + "    tables:\n"
            + "      public.queries:\n"
            + "        include_columns:\n"
            + "          - status\n"
        )
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, None, ["title"],
        )

        assert result is True
        saved = load_yaml_file(service_file)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert sorted(table_def["include_columns"]) == ["status", "title"]

    def test_update_merges_with_existing_ignore_columns(
        self, project_dir: Path,
    ) -> None:
        """New ignore columns are merged with pre-existing ones."""
        service_file = project_dir / "services" / "proxy.yaml"
        service_file.write_text(
            "proxy:\n"
            + "  source:\n"
            + "    tables:\n"
            + "      public.queries:\n"
            + "        ignore_columns:\n"
            + "          - cache\n"
        )
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, ["token"], None,
        )

        assert result is True
        saved = load_yaml_file(service_file)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert sorted(table_def["ignore_columns"]) == ["cache", "token"]

    def test_update_both_track_and_ignore_columns(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Both track and ignore columns can be set at once."""
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, ["cache"], ["status"],
        )

        assert result is True
        saved = load_yaml_file(service_with_tables)
        table_def = saved["proxy"]["source"]["tables"]["public.queries"]
        assert table_def["ignore_columns"] == ["cache"]
        assert table_def["include_columns"] == ["status"]

    def test_update_no_columns_warns_and_returns_false(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Warning when table exists but no columns specified."""
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, None, None,
        )

        assert result is False

    def test_update_does_not_affect_other_tables(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Updating one table doesn't modify other tables."""
        add_table_to_service(
            "proxy", "public", "queries",
            None, None, ["status"],
        )

        saved = load_yaml_file(service_with_tables)
        # Other table should be unchanged (empty dict)
        users_def = saved["proxy"]["source"]["tables"]["public.users"]
        assert users_def is None or users_def == {}

    def test_deduplicates_columns(
        self, project_dir: Path,
    ) -> None:
        """Duplicate columns are deduplicated on merge."""
        service_file = project_dir / "services" / "proxy.yaml"
        service_file.write_text(
            "proxy:\n"
            + "  source:\n"
            + "    tables:\n"
            + "      public.queries:\n"
            + "        include_columns:\n"
            + "          - status\n"
        )
        result = add_table_to_service(
            "proxy", "public", "queries",
            None, None, ["status", "title"],
        )

        assert result is True
        saved = load_yaml_file(service_file)
        cols = saved["proxy"]["source"]["tables"]["public.queries"]["include_columns"]
        assert sorted(cols) == ["status", "title"]


# ---------------------------------------------------------------------------
# Handler tests: handle_update_source_table
# ---------------------------------------------------------------------------


class TestHandleUpdateSourceTable:
    """Tests for the CLI handler handle_update_source_table."""

    def test_handler_returns_0_on_success(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Handler returns 0 on successful update."""
        args = _make_args(
            track_columns=["public.queries.status"],
        )
        result = handle_update_source_table(args)

        assert result == 0

    def test_handler_returns_1_when_no_columns(self) -> None:
        """Handler returns 1 when neither track nor ignore columns given."""
        args = _make_args()
        result = handle_update_source_table(args)
        assert result == 1

    def test_handler_strips_table_prefix_from_columns(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Handler strips schema.table prefix from column specs."""
        args = _make_args(
            track_columns=[
                "public.queries.status",
                "public.queries.title",
            ],
        )
        handle_update_source_table(args)

        saved = load_yaml_file(service_with_tables)
        cols = saved["proxy"]["source"]["tables"]["public.queries"]["include_columns"]
        # Should be bare column names (prefix stripped)
        assert sorted(cols) == ["status", "title"]

    def test_handler_parses_schema_table_from_spec(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Handler correctly splits schema.table from --source-table value."""
        args = _make_args(
            source_table="public.users",
            track_columns=["public.users.email"],
        )
        result = handle_update_source_table(args)

        assert result == 0
        saved = load_yaml_file(service_with_tables)
        users_def = saved["proxy"]["source"]["tables"]["public.users"]
        assert users_def["include_columns"] == ["email"]

    def test_handler_ignores_columns_from_wrong_table(
        self, project_dir: Path, service_with_tables: Path,
    ) -> None:
        """Columns prefixed with a different table are ignored."""
        args = _make_args(
            source_table="public.queries",
            track_columns=["public.users.email"],
        )
        # Column prefix doesn't match source_table → no columns extracted → returns 1
        result = handle_update_source_table(args)
        assert result == 1
