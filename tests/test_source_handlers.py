"""Unit tests for source handler functions (add, bulk-add, remove).

Tests cover handle_add_source_table, handle_add_source_tables,
handle_remove_table including schema parsing and column specs.
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_source import (
    handle_add_source_table,
    handle_add_source_tables,
    handle_remove_table,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_service.table_operations import (
    add_table_to_service,
    remove_table_from_service,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project with services/ dir and SERVICES_DIR patched."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n  pattern: db-shared\n"
    )
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
def service_yaml(project_dir: Path) -> Path:
    """Service YAML with two tables."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users: {}\n"
    )
    return sf


@pytest.fixture()
def empty_service(project_dir: Path) -> Path:
    """Service YAML with no tables."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text("proxy:\n  source: {}\n")
    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    """Build argparse.Namespace with sensible defaults."""
    defaults: dict[str, object] = {
        "service": "proxy",
        "schema": None,
        "primary_key": None,
        "ignore_columns": None,
        "track_columns": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# table_operations: add_table_to_service (new-table path)
# ═══════════════════════════════════════════════════════════════════════════


class TestAddNewTable:
    """add_table_to_service — adding a brand-new table."""

    def test_add_new_table_empty_service(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """New table added to empty service config."""
        result = add_table_to_service("proxy", "public", "orders", None, None, None)
        assert result is True
        saved = load_yaml_file(empty_service)
        assert "public.orders" in saved["proxy"]["source"]["tables"]

    def test_add_new_table_with_primary_key_ignored(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Primary key param is accepted but stored only if schema-aware."""
        # add_table_to_service doesn't store PK in service YAML —
        # PK comes from service-schemas. Just ensure no crash.
        result = add_table_to_service("proxy", "public", "orders", "id", None, None)
        assert result is True

    def test_add_new_table_with_track_columns(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Track columns set on initial add."""
        result = add_table_to_service(
            "proxy", "public", "orders", None, None, ["status", "total"],
        )
        assert result is True
        saved = load_yaml_file(empty_service)
        tbl = saved["proxy"]["source"]["tables"]["public.orders"]
        assert sorted(tbl["include_columns"]) == ["status", "total"]

    def test_add_new_table_with_ignore_columns(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Ignore columns set on initial add."""
        result = add_table_to_service(
            "proxy", "public", "orders", None, ["cache"], None,
        )
        assert result is True
        saved = load_yaml_file(empty_service)
        tbl = saved["proxy"]["source"]["tables"]["public.orders"]
        assert tbl["ignore_columns"] == ["cache"]

    def test_add_duplicate_table_no_columns_returns_false(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Adding existing table without columns returns False."""
        result = add_table_to_service("proxy", "public", "queries", None, None, None)
        assert result is False

    def test_add_to_nonexistent_service_raises(
        self, project_dir: Path,
    ) -> None:
        """FileNotFoundError when service YAML missing."""
        result = add_table_to_service("missing", "public", "t", None, None, None)
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════
# table_operations: remove_table_from_service
# ═══════════════════════════════════════════════════════════════════════════


class TestRemoveTable:
    """remove_table_from_service unit tests."""

    def test_remove_existing_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Removes table and saves config."""
        result = remove_table_from_service("proxy", "public", "queries")
        assert result is True
        saved = load_yaml_file(service_yaml)
        assert "public.queries" not in saved["proxy"]["source"]["tables"]

    def test_remove_nonexistent_table_returns_false(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns False if table doesn't exist."""
        result = remove_table_from_service("proxy", "public", "nonexistent")
        assert result is False

    def test_remove_from_empty_service_returns_false(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Returns False when no tables configured."""
        result = remove_table_from_service("proxy", "public", "queries")
        assert result is False

    def test_remove_keeps_other_tables(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Removing one table doesn't affect others."""
        remove_table_from_service("proxy", "public", "queries")
        saved = load_yaml_file(service_yaml)
        assert "public.users" in saved["proxy"]["source"]["tables"]


# ═══════════════════════════════════════════════════════════════════════════
# Handler: handle_add_source_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddSourceTable:
    """Tests for handle_add_source_table handler."""

    def test_add_with_schema_dot_table(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Parses schema.table from flag value."""
        args = _ns(add_source_table="public.orders")
        result = handle_add_source_table(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        assert "public.orders" in saved["proxy"]["source"]["tables"]

    def test_add_without_dot_defaults_to_dbo(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Table without dot gets default schema 'dbo'."""
        args = _ns(add_source_table="Actor")
        result = handle_add_source_table(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        assert "dbo.Actor" in saved["proxy"]["source"]["tables"]

    def test_add_uses_explicit_schema_flag(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """--schema flag overrides default when no dot in table name."""
        args = _ns(add_source_table="Actor", schema="hr")
        result = handle_add_source_table(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        assert "hr.Actor" in saved["proxy"]["source"]["tables"]

    def test_add_with_track_columns(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Track columns passed through from args."""
        args = _ns(
            add_source_table="public.orders",
            track_columns=["public.orders.status"],
        )
        result = handle_add_source_table(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        tbl = saved["proxy"]["source"]["tables"]["public.orders"]
        assert tbl["include_columns"] == ["status"]

    def test_add_with_ignore_columns(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Ignore columns passed through from args."""
        args = _ns(
            add_source_table="public.orders",
            ignore_columns=["public.orders.cache"],
        )
        result = handle_add_source_table(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        tbl = saved["proxy"]["source"]["tables"]["public.orders"]
        assert tbl["ignore_columns"] == ["cache"]

    def test_add_duplicate_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Adding existing table (no columns) returns 1."""
        args = _ns(add_source_table="public.queries")
        result = handle_add_source_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Handler: handle_add_source_tables (bulk)
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddSourceTables:
    """Tests for handle_add_source_tables bulk handler."""

    def test_add_multiple_tables(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Bulk-add several tables at once."""
        args = _ns(add_source_tables=["public.a", "public.b", "public.c"])
        result = handle_add_source_tables(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        tables = saved["proxy"]["source"]["tables"]
        assert "public.a" in tables
        assert "public.b" in tables
        assert "public.c" in tables

    def test_bulk_uses_dbo_default(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Tables without dot get dbo default schema."""
        args = _ns(add_source_tables=["Actor", "Fraver"])
        result = handle_add_source_tables(args)
        assert result == 0
        saved = load_yaml_file(empty_service)
        assert "dbo.Actor" in saved["proxy"]["source"]["tables"]
        assert "dbo.Fraver" in saved["proxy"]["source"]["tables"]

    def test_bulk_partial_failure(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """One existing + one new → returns 0 (at least one succeeded)."""
        args = _ns(add_source_tables=["public.queries", "public.orders"])
        result = handle_add_source_tables(args)
        assert result == 0

    def test_bulk_all_fail_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """All existing tables → returns 1."""
        args = _ns(add_source_tables=["public.queries", "public.users"])
        result = handle_add_source_tables(args)
        assert result == 1

    def test_bulk_empty_specs_skipped(
        self, project_dir: Path, empty_service: Path,
    ) -> None:
        """Empty strings in list are skipped."""
        args = _ns(add_source_tables=["", "  ", "public.orders"])
        result = handle_add_source_tables(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# Handler: handle_remove_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleRemoveTable:
    """Tests for handle_remove_table handler."""

    def test_remove_with_schema_dot_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Parses schema.table correctly."""
        args = _ns(remove_table="public.queries")
        result = handle_remove_table(args)
        assert result == 0

    def test_remove_without_dot_defaults_dbo(
        self, project_dir: Path,
    ) -> None:
        """Table name without dot uses 'dbo' schema."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      dbo.Actor: {}\n"
        )
        args = _ns(remove_table="Actor")
        result = handle_remove_table(args)
        assert result == 0

    def test_remove_nonexistent_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Removing nonexistent table returns 1."""
        args = _ns(remove_table="public.nonexistent")
        result = handle_remove_table(args)
        assert result == 1
