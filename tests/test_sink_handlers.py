"""Unit tests for sink handler functions.

Covers handle_sink_add, handle_sink_remove, handle_sink_list,
handle_sink_validate, handle_sink_add_table, handle_sink_remove_table,
handle_sink_update_schema, handle_sink_map_column_error,
handle_sink_add_custom_table, handle_modify_custom_table,
and _resolve_sink_key.
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_sink import (
    _resolve_sink_key,
    handle_modify_custom_table,
    handle_sink_add,
    handle_sink_add_custom_table,
    handle_sink_add_table,
    handle_sink_list,
    handle_sink_map_column_error,
    handle_sink_remove,
    handle_sink_remove_table,
    handle_sink_update_schema,
    handle_sink_validate,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project with services/ and sink-groups.yaml."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n  pattern: db-shared\n"
    )
    # sink-groups.yaml needed for add_sink_to_service validation
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
    """Service YAML with one sink and one table."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users: {}\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables:\n"
        "        public.users:\n"
        "          target_exists: true\n"
        "          target: public.users\n"
    )
    # Create service-schemas for chat so _validate_table_in_schemas passes
    schemas_dir = project_dir / "service-schemas" / "chat" / "public"
    schemas_dir.mkdir(parents=True)
    (schemas_dir / "users.yaml").write_text("columns: []\n")
    (schemas_dir / "orders.yaml").write_text("columns: []\n")
    (schemas_dir / "attachments.yaml").write_text("columns: []\n")
    return sf


@pytest.fixture()
def service_multi_sink(project_dir: Path) -> Path:
    """Service YAML with multiple sinks."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables: {}\n"
        "    sink_asma.calendar:\n"
        "      tables: {}\n"
    )
    return sf


@pytest.fixture()
def service_no_sinks(project_dir: Path) -> Path:
    """Service YAML with no sinks at all."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text("proxy:\n  source:\n    tables: {}\n")
    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "proxy",
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
# _resolve_sink_key
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveSinkKey:
    """Tests for _resolve_sink_key auto-detection."""

    def test_returns_explicit_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns --sink value when explicitly provided."""
        args = _ns(sink="sink_asma.chat")
        result = _resolve_sink_key(args)
        assert result == "sink_asma.chat"

    def test_auto_defaults_single_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Auto-selects when service has exactly one sink."""
        args = _ns(sink=None)
        result = _resolve_sink_key(args)
        assert result == "sink_asma.chat"

    def test_returns_none_multiple_sinks(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """Returns None when multiple sinks and no --sink flag."""
        args = _ns(sink=None)
        result = _resolve_sink_key(args)
        assert result is None

    def test_returns_none_no_sinks(
        self, project_dir: Path, service_no_sinks: Path,
    ) -> None:
        """Returns None when no sinks configured."""
        args = _ns(sink=None)
        result = _resolve_sink_key(args)
        assert result is None

    def test_returns_none_missing_service(
        self, project_dir: Path,
    ) -> None:
        """Returns None when service file doesn't exist."""
        args = _ns(service="nonexistent", sink=None)
        result = _resolve_sink_key(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_add / handle_sink_remove
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkAdd:
    """Tests for handle_sink_add."""

    def test_add_single_sink(
        self, project_dir: Path, service_no_sinks: Path,
    ) -> None:
        """Adds a sink destination to service."""
        args = _ns(add_sink=["sink_asma.chat"])
        result = handle_sink_add(args)
        assert result == 0

    def test_add_duplicate_returns_1(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Adding existing sink returns 1."""
        args = _ns(add_sink=["sink_asma.chat"])
        result = handle_sink_add(args)
        assert result == 1

    def test_add_multiple_sinks(
        self, project_dir: Path, service_no_sinks: Path,
    ) -> None:
        """Adds multiple sinks via append list."""
        args = _ns(add_sink=["sink_asma.chat", "sink_asma.calendar"])
        result = handle_sink_add(args)
        assert result == 0


class TestHandleSinkRemove:
    """Tests for handle_sink_remove."""

    def test_remove_existing_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Removes an existing sink."""
        args = _ns(remove_sink=["sink_asma.chat"])
        result = handle_sink_remove(args)
        assert result == 0

    def test_remove_nonexistent_sink_returns_1(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when sink doesn't exist."""
        args = _ns(remove_sink=["sink_asma.nonexistent"])
        result = handle_sink_remove(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_list / handle_sink_validate
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkList:
    """Tests for handle_sink_list."""

    def test_list_sinks_returns_0(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when sinks exist."""
        args = _ns()
        result = handle_sink_list(args)
        assert result == 0

    def test_list_no_sinks_returns_1(
        self, project_dir: Path, service_no_sinks: Path,
    ) -> None:
        """Returns 1 when no sinks configured."""
        args = _ns()
        result = handle_sink_list(args)
        assert result == 1


class TestHandleSinkValidate:
    """Tests for handle_sink_validate."""

    def test_validate_returns_0_on_valid(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 for valid sink config."""
        args = _ns()
        result = handle_sink_validate(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_add_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkAddTable:
    """Tests for handle_sink_add_table."""

    def test_requires_sink(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """Returns 1 when --sink missing and multiple sinks exist."""
        args = _ns(add_sink_table="public.orders")
        result = handle_sink_add_table(args)
        assert result == 1

    def test_requires_target_exists(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --target-exists not provided."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_add_table_with_target_exists_true(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Adds sink table with target_exists=true."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="true",
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_add_table_with_target_exists_false(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Adds sink table with target_exists=false."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="false",
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_replicate_requires_sink_schema(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --replicate-structure without --sink-schema."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            replicate_structure=True,
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_from_table_used_as_name(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--from value used as table name when --add-sink-table omitted."""
        # Add logs.audit to source tables so the from-table reference is valid
        import yaml as _yaml

        sf = service_with_sink
        config = _yaml.safe_load(sf.read_text())
        config["proxy"]["source"]["tables"]["logs.audit"] = {}
        sf.write_text(_yaml.dump(config, default_flow_style=False))
        # Also add logs schema to service-schemas
        logs_dir = project_dir / "service-schemas" / "chat" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        (logs_dir / "audit.yaml").write_text("columns: []\n")
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table=None,
            from_table="logs.audit",
            target_exists="false",
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_with_target_mapping(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Adds table with --target override."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.attachments",
            target_exists="true",
            target="public.chat_attachments",
        )
        result = handle_sink_add_table(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_remove_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkRemoveTable:
    """Tests for handle_sink_remove_table."""

    def test_remove_existing_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Removes table from sink."""
        args = _ns(
            sink="sink_asma.chat",
            remove_sink_table="public.users",
        )
        result = handle_sink_remove_table(args)
        assert result == 0

    def test_remove_nonexistent_returns_1(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 for nonexistent table."""
        args = _ns(
            sink="sink_asma.chat",
            remove_sink_table="public.nonexistent",
        )
        result = handle_sink_remove_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_update_schema
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkUpdateSchema:
    """Tests for handle_sink_update_schema."""

    def test_requires_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(
            sink="sink_asma.chat",
            update_schema="new_schema",
        )
        result = handle_sink_update_schema(args)
        assert result == 1

    def test_update_schema_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Updates table schema when all args provided."""
        args = _ns(
            sink="sink_asma.chat",
            sink_table="public.users",
            update_schema="new_schema",
        )
        result = handle_sink_update_schema(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_map_column_error
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkMapColumnError:
    """Tests for handle_sink_map_column_error."""

    def test_always_returns_1(self) -> None:
        """Static error handler always returns 1."""
        result = handle_sink_map_column_error()
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_add_custom_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkAddCustomTable:
    """Tests for handle_sink_add_custom_table."""

    def test_requires_columns_or_from(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when neither --column nor --from provided."""
        args = _ns(
            sink="sink_asma.chat",
            add_custom_sink_table="public.audit",
            column=None,
        )
        result = handle_sink_add_custom_table(args)
        assert result == 1

    def test_requires_sink(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """Returns 1 when multiple sinks and no --sink."""
        args = _ns(
            add_custom_sink_table="public.audit",
            column=["id:uuid:pk"],
        )
        result = handle_sink_add_custom_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_modify_custom_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleModifyCustomTable:
    """Tests for handle_modify_custom_table."""

    def test_requires_add_or_remove_column(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when neither --add-column nor --remove-column."""
        args = _ns(
            sink="sink_asma.chat",
            modify_custom_table="public.audit",
        )
        result = handle_modify_custom_table(args)
        assert result == 1

    def test_requires_sink(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """Returns 1 when multiple sinks and no --sink."""
        args = _ns(
            modify_custom_table="public.audit",
            add_column="name:text",
        )
        result = handle_modify_custom_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_remove_table — custom-table file cleanup
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkRemoveTableCleansUpFiles:
    """Ensure remove-sink-table deletes the custom-table YAML file."""

    def test_removes_custom_table_file(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Custom-table YAML is deleted when the sink table is removed."""
        # Create custom-table file for chat target
        ct_dir = project_dir / "service-schemas" / "chat" / "custom-tables"
        ct_dir.mkdir(parents=True)
        ct_file = ct_dir / "public.users.yaml"
        ct_file.write_text("columns:\n  - name: id\n    type: integer\n")

        args = _ns(sink="sink_asma.chat", remove_sink_table="public.users")
        result = handle_sink_remove_table(args)

        assert result == 0
        assert not ct_file.exists(), "Custom-table YAML should be deleted"

    def test_succeeds_without_custom_table_file(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Removal succeeds even when no custom-table file exists."""
        args = _ns(sink="sink_asma.chat", remove_sink_table="public.users")
        result = handle_sink_remove_table(args)

        assert result == 0

    def test_custom_table_file_with_slash_in_key(
        self, project_dir: Path,
    ) -> None:
        """Table keys with '/' are converted to '_' in filename."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.queries: {}\n"
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables:\n"
            "        myschema/mytable:\n"
            "          target_exists: false\n"
        )
        ct_dir = project_dir / "service-schemas" / "chat" / "custom-tables"
        ct_dir.mkdir(parents=True)
        ct_file = ct_dir / "myschema_mytable.yaml"
        ct_file.write_text("columns: []\n")

        args = _ns(sink="sink_asma.chat", remove_sink_table="myschema/mytable")
        result = handle_sink_remove_table(args)

        assert result == 0
        assert not ct_file.exists(), "File with slash→underscore should be deleted"
