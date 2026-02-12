"""Tests for mapping columns on existing sink tables.

Covers:
- map_sink_columns() in sink_operations.py (validation logic)
- handle_sink_map_column_on_table() in service_handlers_sink.py (CLI handler)
- Dispatch routing in service.py (--map-column with --sink-table)
- Autocompletion: list_source_columns_for_sink_table() (1st arg)
- Autocompletion: list_target_columns_for_sink_table() (2nd arg)
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_sink import (
    handle_sink_map_column_on_table,
)
from cdc_generator.helpers.autocompletions.sinks import (
    list_source_columns_for_sink_table,
    list_target_columns_for_sink_table,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_service.sink_operations import (
    map_sink_columns,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


SOURCE_TABLE_SCHEMA = (
    "columns:\n"
    "  - name: customer_id\n"
    "    type: uuid\n"
    "    nullable: false\n"
    "    primary_key: true\n"
    "  - name: user_id\n"
    "    type: uuid\n"
    "    nullable: false\n"
    "    primary_key: true\n"
    "  - name: full_name\n"
    "    type: text\n"
    "    nullable: true\n"
    "  - name: email\n"
    "    type: text\n"
    "    nullable: true\n"
    "  - name: created_at\n"
    "    type: timestamptz\n"
    "    nullable: false\n"
    "  - name: status\n"
    "    type: integer\n"
    "    nullable: true\n"
)

SINK_TABLE_SCHEMA = (
    "columns:\n"
    "  - name: user_id\n"
    "    type: uuid\n"
    "    nullable: false\n"
    "    primary_key: true\n"
    "  - name: first_name\n"
    "    type: text\n"
    "    nullable: false\n"
    "  - name: last_name\n"
    "    type: text\n"
    "    nullable: true\n"
    "  - name: middle_name\n"
    "    type: text\n"
    "    nullable: true\n"
)


SERVICE_YAML = (
    "myservice:\n"
    "  source:\n"
    "    tables:\n"
    "      public.customer_user: {}\n"
    "  sinks:\n"
    "    sink_asma.proxy:\n"
    "      tables:\n"
    "        public.directory_user_name:\n"
    "          target_exists: true\n"
    "          from: public.customer_user\n"
)


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project with services/, service-schemas/, and configs."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n  pattern: db-shared\n"
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_asma:\n"
        "  type: postgres\n"
        "  server: sink-pg\n"
    )
    service_schemas_dir = tmp_path / "service-schemas"
    service_schemas_dir.mkdir()
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    with patch(
        "cdc_generator.validators.manage_service.config.SERVICES_DIR",
        services_dir,
    ), patch(
        "cdc_generator.validators.manage_service.sink_operations.SERVICE_SCHEMAS_DIR",
        service_schemas_dir,
    ):
        try:
            yield tmp_path
        finally:
            os.chdir(original_cwd)


@pytest.fixture()
def service_with_sink(project_dir: Path) -> Path:
    """Service YAML with one sink and a table with 'from' field."""
    sf = project_dir / "services" / "myservice.yaml"
    sf.write_text(SERVICE_YAML)

    # Create source table schema (myservice/public/customer_user.yaml)
    source_dir = project_dir / "service-schemas" / "myservice" / "public"
    source_dir.mkdir(parents=True)
    (source_dir / "customer_user.yaml").write_text(SOURCE_TABLE_SCHEMA)

    # Create sink table schema (proxy/public/directory_user_name.yaml)
    sink_dir = project_dir / "service-schemas" / "proxy" / "public"
    sink_dir.mkdir(parents=True)
    (sink_dir / "directory_user_name.yaml").write_text(SINK_TABLE_SCHEMA)

    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "myservice",
        "sink": None,
        "add_sink": None,
        "remove_sink": None,
        "add_sink_table": None,
        "remove_sink_table": None,
        "update_schema": None,
        "sink_table": None,
        "from_table": None,
        "replicate_structure": False,
        "sink_schema": None,
        "target_exists": None,
        "target": None,
        "target_schema": None,
        "map_column": None,
        "include_sink_columns": None,
        "add_custom_sink_table": None,
        "column": None,
        "modify_custom_table": None,
        "add_column": None,
        "remove_column": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# map_sink_columns() — operations layer
# ═══════════════════════════════════════════════════════════════════════════


class TestMapSinkColumns:
    """Tests for map_sink_columns() in sink_operations.py."""

    def test_maps_valid_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Maps valid source→sink column pairs."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name"), ("email", "last_name")],
        )
        assert result is True

        # Verify YAML was updated
        data = load_yaml_file(service_with_sink)
        sinks = cast(dict[str, Any], data["myservice"])["sinks"]
        tbl = sinks["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        assert tbl["columns"]["full_name"] == "first_name"
        assert tbl["columns"]["email"] == "last_name"

    def test_maps_single_column(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Maps a single column pair."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("user_id", "user_id")],
        )
        assert result is True

        data = load_yaml_file(service_with_sink)
        sinks = cast(dict[str, Any], data["myservice"])["sinks"]
        tbl = sinks["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        assert tbl["columns"]["user_id"] == "user_id"

    def test_fails_missing_source_column(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when source column doesn't exist."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("nonexistent_col", "first_name")],
        )
        assert result is False

    def test_fails_missing_sink_column(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when sink column doesn't exist."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "nonexistent_col")],
        )
        assert result is False

    def test_fails_type_mismatch(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when column types are incompatible (integer → text)."""
        # status is integer, first_name is text
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("status", "first_name")],
        )
        assert result is False

    def test_fails_missing_service(
        self, project_dir: Path,
    ) -> None:
        """Fails when service doesn't exist."""
        result = map_sink_columns(
            "nonexistent",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("a", "b")],
        )
        assert result is False

    def test_fails_missing_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when sink doesn't exist."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.nonexistent",
            "public.directory_user_name",
            [("a", "b")],
        )
        assert result is False

    def test_fails_missing_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when table doesn't exist in sink."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.nonexistent_table",
            [("a", "b")],
        )
        assert result is False

    def test_fails_no_source_schema(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when source table schema file is missing."""
        # Remove source schema
        source_schema = (
            project_dir
            / "service-schemas"
            / "myservice"
            / "public"
            / "customer_user.yaml"
        )
        source_schema.unlink()

        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )
        assert result is False

    def test_fails_no_sink_schema(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails when sink table schema file is missing."""
        # Remove sink schema
        sink_schema = (
            project_dir
            / "service-schemas"
            / "proxy"
            / "public"
            / "directory_user_name.yaml"
        )
        sink_schema.unlink()

        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )
        assert result is False

    def test_allows_compatible_types(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """text → text is compatible and succeeds."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "last_name")],
        )
        assert result is True

    def test_sets_target_exists(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Sets target_exists=True after mapping."""
        map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )

        data = load_yaml_file(service_with_sink)
        sinks = cast(dict[str, Any], data["myservice"])["sinks"]
        tbl = sinks["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        assert tbl["target_exists"] is True

    def test_preserves_existing_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Preserves existing column mappings when adding new ones."""
        # First mapping
        map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )
        # Second mapping
        map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("email", "last_name")],
        )

        data = load_yaml_file(service_with_sink)
        sinks = cast(dict[str, Any], data["myservice"])["sinks"]
        tbl = sinks["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        assert tbl["columns"]["full_name"] == "first_name"
        assert tbl["columns"]["email"] == "last_name"

    def test_warns_unmapped_required_columns(
        self, project_dir: Path, service_with_sink: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Warns about unmapped non-nullable sink columns."""
        # Map only one column — first_name is non-nullable and unmapped
        map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("email", "last_name")],
        )

        captured = capsys.readouterr()
        # first_name is not_null and not mapped → should warn
        assert "first_name" in captured.out

    def test_no_warning_when_all_required_mapped(
        self, project_dir: Path, service_with_sink: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """No warning when all required columns are mapped or identity-covered."""
        # user_id exists in both source and sink (identity mapped)
        # first_name is required — map it explicitly
        map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )

        captured = capsys.readouterr()
        assert "Unmapped required" not in captured.out

    def test_multiple_errors_reported(
        self, project_dir: Path, service_with_sink: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Multiple validation errors are all reported."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.directory_user_name",
            [("bad_src", "first_name"), ("full_name", "bad_tgt")],
        )
        assert result is False

        captured = capsys.readouterr()
        assert "bad_src" in captured.out
        assert "bad_tgt" in captured.out

    def test_fails_invalid_sink_key_format(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Fails for malformed sink key (no dot separator)."""
        result = map_sink_columns(
            "myservice",
            "invalid_no_dot",
            "public.directory_user_name",
            [("full_name", "first_name")],
        )
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════
# Sink table without 'from' field — fallback to table_key as source
# ═══════════════════════════════════════════════════════════════════════════


class TestMapSinkColumnsNoFromField:
    """Tests for map_sink_columns() when sink table has no 'from' field."""

    @pytest.fixture()
    def service_no_from(self, project_dir: Path) -> Path:
        """Service where sink table has no 'from' field but matches a source."""
        sf = project_dir / "services" / "myservice.yaml"
        sf.write_text(
            "myservice:\n"
            "  source:\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.proxy:\n"
            "      tables:\n"
            "        public.users:\n"
            "          target_exists: true\n"
        )

        # Source schema (myservice/public/users.yaml)
        src_dir = project_dir / "service-schemas" / "myservice" / "public"
        src_dir.mkdir(parents=True, exist_ok=True)
        (src_dir / "users.yaml").write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: uuid\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: name\n"
            "    type: text\n"
            "    nullable: true\n"
        )

        # Sink schema (proxy/public/users.yaml)
        sink_dir = project_dir / "service-schemas" / "proxy" / "public"
        sink_dir.mkdir(parents=True, exist_ok=True)
        (sink_dir / "users.yaml").write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: uuid\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: display_name\n"
            "    type: text\n"
            "    nullable: true\n"
        )
        return sf

    def test_resolves_source_without_from(
        self, project_dir: Path, service_no_from: Path,
    ) -> None:
        """Falls back to table_key when no 'from' field."""
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.users",
            [("name", "display_name")],
        )
        assert result is True

    def test_fails_unresolvable_source(
        self, project_dir: Path,
    ) -> None:
        """Fails when no 'from' and table_key doesn't match a source."""
        sf = project_dir / "services" / "myservice.yaml"
        sf.write_text(
            "myservice:\n"
            "  source:\n"
            "    tables:\n"
            "      public.other: {}\n"
            "  sinks:\n"
            "    sink_asma.proxy:\n"
            "      tables:\n"
            "        public.mystery_table:\n"
            "          target_exists: true\n"
        )
        result = map_sink_columns(
            "myservice",
            "sink_asma.proxy",
            "public.mystery_table",
            [("a", "b")],
        )
        assert result is False


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_map_column_on_table() — CLI handler
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkMapColumnOnTable:
    """Tests for handle_sink_map_column_on_table() CLI handler."""

    def test_maps_valid_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 on successful column mapping."""
        args = _ns(
            sink="sink_asma.proxy",
            sink_table="public.directory_user_name",
            map_column=[["full_name", "first_name"]],
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 0

    def test_fails_missing_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(
            sink="sink_asma.proxy",
            sink_table=None,
            map_column=[["full_name", "first_name"]],
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 1

    def test_fails_invalid_source_column(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when source column doesn't exist."""
        args = _ns(
            sink="sink_asma.proxy",
            sink_table="public.directory_user_name",
            map_column=[["nonexistent", "first_name"]],
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 1

    def test_auto_resolves_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Auto-defaults sink when only one is configured."""
        args = _ns(
            sink=None,
            sink_table="public.directory_user_name",
            map_column=[["full_name", "first_name"]],
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 0

    def test_multiple_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Maps multiple column pairs at once."""
        args = _ns(
            sink="sink_asma.proxy",
            sink_table="public.directory_user_name",
            map_column=[
                ["full_name", "first_name"],
                ["email", "last_name"],
            ],
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 0

        data = load_yaml_file(service_with_sink)
        sinks = cast(dict[str, Any], data["myservice"])["sinks"]
        tbl = sinks["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        assert tbl["columns"]["full_name"] == "first_name"
        assert tbl["columns"]["email"] == "last_name"


# ═══════════════════════════════════════════════════════════════════════════
# Autocompletion — list_source_columns_for_sink_table
# ═══════════════════════════════════════════════════════════════════════════


class TestAutocompletionSourceColumns:
    """Tests for list_source_columns_for_sink_table() autocompletion."""

    def test_lists_source_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns source columns based on 'from' field."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            # Also need to mock services dir for loading service YAML
            with patch(
                "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
            ) as mock_find_all:
                def side_effect(name: str) -> Path | None:
                    if name == "services":
                        return project_dir / "services"
                    if name == "service-schemas":
                        return project_dir / "service-schemas"
                    return None

                mock_find_all.side_effect = side_effect

                result = list_source_columns_for_sink_table(
                    "myservice",
                    "sink_asma.proxy",
                    "public.directory_user_name",
                )

        assert "full_name" in result
        assert "email" in result
        assert "customer_id" in result
        assert "user_id" in result

    def test_returns_empty_for_missing_service(
        self, project_dir: Path,
    ) -> None:
        """Returns empty list when service file doesn't exist."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"
            result = list_source_columns_for_sink_table(
                "nonexistent", "sink_asma.proxy", "public.t",
            )
        assert result == []

    def test_returns_empty_for_missing_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns empty list when table not in sink config."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            def side_effect(name: str) -> Path | None:
                if name == "services":
                    return project_dir / "services"
                if name == "service-schemas":
                    return project_dir / "service-schemas"
                return None

            mock_find.side_effect = side_effect
            result = list_source_columns_for_sink_table(
                "myservice", "sink_asma.proxy", "public.nonexistent",
            )
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════
# Autocompletion — list_target_columns_for_sink_table (2nd arg completion)
# ═══════════════════════════════════════════════════════════════════════════


class TestAutocompletionTargetColumns:
    """Tests for list_target_columns_for_sink_table() autocompletion.

    This function powers the 2nd-position --map-column completion:
    after the user types a source column, Tab should suggest sink columns.
    """

    def test_lists_sink_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns target columns from service-schemas/{target}/{schema}/{table}.yaml."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "public.directory_user_name",
            )

        assert "user_id" in result
        assert "first_name" in result
        assert "last_name" in result
        assert "middle_name" in result

    def test_columns_are_sorted(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returned columns are sorted alphabetically."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "public.directory_user_name",
            )

        assert result == sorted(result)

    def test_returns_empty_for_invalid_sink_key(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns empty list when sink_key has no dot separator."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "invalid_no_dot",
                "public.directory_user_name",
            )

        assert result == []

    def test_returns_empty_for_invalid_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns empty list when target_table has no dot separator."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "no_dot_table",
            )

        assert result == []

    def test_returns_empty_for_missing_schema_file(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns empty list when the schema YAML file doesn't exist."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "public.nonexistent_table",
            )

        assert result == []

    def test_returns_empty_when_no_service_schemas_dir(
        self, project_dir: Path,
    ) -> None:
        """Returns empty list when service-schemas directory not found."""
        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = None

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "public.directory_user_name",
            )

        assert result == []

    def test_excludes_columns_without_name(
        self, project_dir: Path,
    ) -> None:
        """Columns missing 'name' field are excluded from results."""
        # Create a schema with one valid and one invalid column entry
        schemas_dir = project_dir / "service-schemas" / "proxy" / "public"
        schemas_dir.mkdir(parents=True, exist_ok=True)
        (schemas_dir / "odd_table.yaml").write_text(
            "columns:\n"
            "  - name: valid_col\n"
            "    type: text\n"
            "  - type: integer\n"  # Missing 'name' key
            "  - name: another_valid\n"
            "    type: uuid\n"
        )

        with patch(
            "cdc_generator.helpers.autocompletions.sinks.find_directory_upward",
        ) as mock_find:
            mock_find.return_value = project_dir / "service-schemas"

            result = list_target_columns_for_sink_table(
                "sink_asma.proxy",
                "public.odd_table",
            )

        assert result == ["another_valid", "valid_col"]
