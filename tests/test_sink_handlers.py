"""Unit tests for sink handler functions.

Covers handle_sink_add, handle_sink_remove, handle_sink_list,
handle_sink_validate, handle_sink_add_table, handle_sink_remove_table,
handle_sink_update_schema, handle_sink_map_column_error,
handle_sink_map_column_on_table, handle_sink_add_custom_table,
handle_modify_custom_table, and _resolve_sink_key.
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
    handle_sink_map_column_on_table,
    handle_sink_remove,
    handle_sink_remove_table,
    handle_sink_update_schema,
    handle_sink_validate,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file

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
        "all": False,
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
        "add_column_template": None,
        "column_name": None,
        "value": None,
        "add_transform": None,
        "skip_validation": False,
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

    def test_validate_returns_1_on_invalid(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when sink validation fails."""
        args = _ns()
        with patch(
            "cdc_generator.cli.service_handlers_sink.validate_sinks",
            return_value=False,
        ):
            result = handle_sink_validate(args)
        assert result == 1


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
            from_table="public.users",
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
            from_table="public.users",
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_auto_sets_target_exists_true_when_map_column_present(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Infers target_exists=true when mapping to an existing target table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            from_table="public.users",
            target="public.orders",
            target_exists=None,
            map_column=[["user_id", "user_id"]],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_sink_table_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        add_sink_table_mock.assert_called_once()
        call_kwargs = add_sink_table_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["target_exists"] is True

    def test_accepts_target_source_map_column_format(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Parses --map-column TARGET:SOURCE into source->target mapping."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            from_table="public.users",
            target_exists="true",
            map_column=["user_id:id"],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_sink_table_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        add_sink_table_mock.assert_called_once()
        call_kwargs = add_sink_table_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["columns"] == {"id": "user_id"}

    def test_requires_from_for_add_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --from is missing for --add-sink-table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="false",
            from_table=None,
        )
        result = handle_sink_add_table(args)
        assert result == 1

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

    def test_replicate_auto_sets_target_exists_false(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Replicate mode auto-sets target_exists=false when omitted."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            replicate_structure=True,
            sink_schema="chat",
            from_table="public.users",
            target_exists=None,
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_sink_table_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        add_sink_table_mock.assert_called_once()
        call_kwargs = add_sink_table_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["target_exists"] is False
        assert table_opts["replicate_structure"] is True
        assert table_opts["sink_schema"] == "chat"

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

    def test_requires_table_name_or_from(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when neither --add-sink-table nor --from provided."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table=None,
            from_table=None,
            target_exists="false",
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_with_target_mapping(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Adds table with --target override."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.attachments",
            target_exists="true",
            target="public.chat_attachments",
            from_table="public.users",
        )
        result = handle_sink_add_table(args)
        assert result == 0

    def test_sink_schema_override_non_replicate(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--sink-schema is passed to table_opts without replicate mode."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="true",
            sink_schema="custom_schema",
            from_table="public.users",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        call_kwargs = add_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["sink_schema"] == "custom_schema"

    def test_add_column_template_is_forwarded(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--add-column-template options are forwarded during add-sink-table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="false",
            from_table="public.users",
            add_column_template="customer_id",
            column_name="customer_id",
            value="{adopus.sources.*.customer_id}",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        call_kwargs = add_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["column_template"] == "customer_id"
        assert table_opts["column_template_name"] == "customer_id"
        assert (
            table_opts["column_template_value"]
            == "{adopus.sources.*.customer_id}"
        )

    def test_add_column_template_target_template_format_is_forwarded(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """`target:template` format maps to template key + column_name override."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="true",
            from_table="public.users",
            add_column_template="customer_id:tenant_id",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        call_kwargs = add_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["column_template"] == "tenant_id"
        assert table_opts["column_template_name"] == "customer_id"

    def test_add_transform_is_applied_after_add_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--add-transform on --add-sink-table applies validated transform."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="false",
            from_table="public.users",
            add_transform="file://services/_bloblang/examples/user_class_splitter.blobl",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock, patch(
            "cdc_generator.cli.service_handlers_sink.add_transform_to_table",
            return_value=True,
        ) as transform_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        assert add_mock.call_count == 1
        transform_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.orders",
            "file://services/_bloblang/examples/user_class_splitter.blobl",
            False,
        )

    def test_add_transform_failure_rolls_back_added_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Transform validation failure rolls back newly added sink table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="false",
            from_table="public.users",
            add_transform="file://services/_bloblang/examples/user_class_splitter.blobl",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ), patch(
            "cdc_generator.cli.service_handlers_sink.add_transform_to_table",
            return_value=False,
        ), patch(
            "cdc_generator.cli.service_handlers_sink.remove_sink_table",
            return_value=True,
        ) as rollback_mock:
            result = handle_sink_add_table(args)

        assert result == 1
        rollback_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.orders",
        )

    def test_accept_column_is_forwarded(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--accept-column values are forwarded during add-sink-table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.orders",
            target_exists="true",
            from_table="public.users",
            accept_column=["user_id", "region"],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        call_kwargs = add_mock.call_args.kwargs
        table_opts = call_kwargs["table_opts"]
        assert isinstance(table_opts, dict)
        assert table_opts["accepted_columns"] == ["user_id", "region"]

    def test_all_mode_requires_replicate_structure(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """--all add-sink-table is allowed only with --replicate-structure."""
        args = _ns(
            sink=None,
            all=True,
            add_sink_table=None,
            from_table="public.users",
            target_exists="false",
            replicate_structure=False,
            sink_schema="shared",
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_all_mode_requires_omitted_table_name(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """--all replicate mode requires bare --add-sink-table (no value)."""
        args = _ns(
            sink=None,
            all=True,
            add_sink_table="public.users",
            from_table="public.users",
            replicate_structure=True,
            sink_schema="shared",
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_all_mode_adds_to_each_sink(
        self, project_dir: Path, service_multi_sink: Path,
    ) -> None:
        """--all replicate mode applies add-sink-table to all configured sinks."""
        args = _ns(
            sink=None,
            all=True,
            add_sink_table=None,
            from_table="public.users",
            replicate_structure=True,
            sink_schema="shared",
            target_exists=None,
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        assert add_mock.call_count == 2
        called_sinks = {call.args[1] for call in add_mock.call_args_list}
        assert called_sinks == {"sink_asma.calendar", "sink_asma.chat"}

    def test_from_all_requires_omitted_table_name(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--from all requires bare --add-sink-table (no TABLE value)."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table="public.users",
            from_table="all",
            replicate_structure=True,
            sink_schema="shared",
            target_exists=None,
        )
        result = handle_sink_add_table(args)
        assert result == 1

    def test_from_all_adds_all_source_tables(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--from all applies add-sink-table to every source table."""
        args = _ns(
            sink="sink_asma.chat",
            add_sink_table=None,
            from_table="all",
            replicate_structure=True,
            sink_schema="chat",
            target_exists=None,
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_sink_table",
            return_value=True,
        ) as add_mock:
            result = handle_sink_add_table(args)

        assert result == 0
        assert add_mock.call_count == 2
        called_tables = {call.args[2] for call in add_mock.call_args_list}
        assert called_tables == {"public.queries", "public.users"}
        for call in add_mock.call_args_list:
            opts = call.kwargs["table_opts"]
            assert isinstance(opts, dict)
            assert opts["replicate_structure"] is True
            assert opts["sink_schema"] == "chat"


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
# handle_sink_map_column_on_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkMapColumnOnTable:
    """Tests for handle_sink_map_column_on_table."""

    def test_requires_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(
            sink="sink_asma.chat",
            map_column=[["id", "user_id"]],
            sink_table=None,
        )
        result = handle_sink_map_column_on_table(args)
        assert result == 1

    def test_returns_0_on_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when column mapping succeeds."""
        args = _ns(
            sink="sink_asma.chat",
            sink_table="public.users",
            map_column=[["id", "user_id"]],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.map_sink_columns",
            return_value=True,
        ) as map_mock:
            result = handle_sink_map_column_on_table(args)
        assert result == 0
        map_mock.assert_called_once()

    def test_accepts_target_source_format_on_existing_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Supports TARGET:SOURCE map-column format on existing sink tables."""
        args = _ns(
            sink="sink_asma.chat",
            sink_table="public.users",
            map_column=["user_id:id"],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.map_sink_columns",
            return_value=True,
        ) as map_mock:
            result = handle_sink_map_column_on_table(args)

        assert result == 0
        map_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.users",
            [("id", "user_id")],
        )

    def test_returns_1_on_mapping_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when column mapping operation fails."""
        args = _ns(
            sink="sink_asma.chat",
            sink_table="public.users",
            map_column=[["nonexistent", "user_id"]],
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.map_sink_columns",
            return_value=False,
        ):
            result = handle_sink_map_column_on_table(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_add_custom_table
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleSinkAddCustomTable:
    """Tests for handle_sink_add_custom_table."""

    def test_requires_from(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --from is missing."""
        args = _ns(
            sink="sink_asma.chat",
            add_custom_sink_table="public.audit",
            column=["id:uuid:pk"],
            from_table=None,
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

    def test_from_custom_table_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Supports --from mode without inline --column definitions."""
        args = _ns(
            sink="sink_asma.chat",
            add_custom_sink_table="public.audit_copy",
            column=None,
            from_table="public.audit_template",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_custom_sink_table",
            return_value=True,
        ) as add_custom_mock:
            result = handle_sink_add_custom_table(args)

        assert result == 0
        add_custom_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.audit_copy",
            [],
            from_custom_table="public.audit_template",
        )

    def test_inline_columns_with_from_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Allows inline custom columns when --from is also provided."""
        args = _ns(
            sink="sink_asma.chat",
            add_custom_sink_table="public.audit_copy",
            column=["id:uuid:pk"],
            from_table="public.audit_template",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink.add_custom_sink_table",
            return_value=True,
        ) as add_custom_mock:
            result = handle_sink_add_custom_table(args)

        assert result == 0
        add_custom_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.audit_copy",
            ["id:uuid:pk"],
            from_custom_table="public.audit_template",
        )


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


# ═══════════════════════════════════════════════════════════════════════════
# handle_sink_add_custom_table — happy path
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddCustomSinkTableHappyPath:
    """Happy-path tests for handle_sink_add_custom_table."""

    def test_returns_0_on_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when custom table is successfully created."""
        args = _ns(
            add_custom_sink_table="public.e2e_custom_test",
            sink="sink_asma.chat",
            column=["id:uuid:pk", "event_type:text:not_null"],
            from_table="public.users",
        )
        with patch(
            "cdc_generator.cli.service_handlers_sink_custom.SERVICE_SCHEMAS_DIR",
            project_dir / "service-schemas",
        ):
            result = handle_sink_add_custom_table(args)
        assert result == 0

        data = load_yaml_file(service_with_sink)
        tables = data["proxy"]["sinks"]["sink_asma.chat"]["tables"]
        table_cfg = tables["public.e2e_custom_test"]
        assert table_cfg["custom"] is True
        assert table_cfg["managed"] is True
        assert "columns" not in table_cfg

        schema_data = load_yaml_file(
            project_dir / "service-schemas" / "chat" / "public" / "e2e_custom_test.yaml"
        )
        cols = schema_data.get("columns", [])
        assert isinstance(cols, list)
        assert any(isinstance(c, dict) and c.get("name") == "id" for c in cols)
        assert any(
            isinstance(c, dict) and c.get("name") == "event_type" for c in cols
        )

    def test_from_uses_source_table_columns(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--from should load columns from source table schemas."""
        source_dir = project_dir / "service-schemas" / "proxy" / "public"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "users.yaml").write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: uuid\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: username\n"
            "    type: text\n"
            "    nullable: false\n"
        )

        args = _ns(
            add_custom_sink_table="public.users_clone",
            sink="sink_asma.chat",
            column=None,
            from_table="public.users",
        )

        with patch(
            "cdc_generator.cli.service_handlers_sink_custom.SERVICE_SCHEMAS_DIR",
            project_dir / "service-schemas",
        ):
            result = handle_sink_add_custom_table(args)

        assert result == 0

        data = load_yaml_file(service_with_sink)
        tables = data["proxy"]["sinks"]["sink_asma.chat"]["tables"]
        table_cfg = tables["public.users_clone"]
        assert table_cfg["custom"] is True
        assert table_cfg["managed"] is True
        assert table_cfg["from"] == "public.users"
        assert "columns" not in table_cfg

        schema_data = load_yaml_file(
            project_dir / "service-schemas" / "chat" / "public" / "users_clone.yaml"
        )
        cols = schema_data.get("columns", [])
        assert isinstance(cols, list)
        assert any(isinstance(c, dict) and c.get("name") == "id" for c in cols)
        assert any(
            isinstance(c, dict) and c.get("name") == "username" for c in cols
        )

    def test_from_fails_when_not_in_source_tables(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """--from must reference a declared source.tables key."""
        args = _ns(
            add_custom_sink_table="public.invalid_clone",
            sink="sink_asma.chat",
            column=None,
            from_table="public.nonexistent",
        )

        result = handle_sink_add_custom_table(args)

        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_modify_custom_table — happy paths
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleModifyCustomTableHappyPath:
    """Happy-path tests for handle_modify_custom_table."""

    @pytest.fixture()
    def service_with_custom_table(
        self, project_dir: Path,
    ) -> Path:
        """Service with a custom managed table in the sink."""
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
            "        public.audit_log:\n"
            "          target_exists: false\n"
            "          custom: true\n"
            "          managed: true\n"
            "          columns:\n"
            "            id:\n"
            "              type: uuid\n"
            "              primary_key: true\n"
            "            event_type:\n"
            "              type: text\n"
        )
        return sf

    def test_add_column_returns_0(
        self, project_dir: Path, service_with_custom_table: Path,
    ) -> None:
        """Returns 0 when column is successfully added."""
        args = _ns(
            modify_custom_table="public.audit_log",
            sink="sink_asma.chat",
            add_column="created_at:timestamptz:default_now",
        )
        result = handle_modify_custom_table(args)
        assert result == 0

    def test_remove_column_returns_0(
        self, project_dir: Path, service_with_custom_table: Path,
    ) -> None:
        """Returns 0 when column is successfully removed."""
        args = _ns(
            modify_custom_table="public.audit_log",
            sink="sink_asma.chat",
            remove_column="event_type",
        )
        result = handle_modify_custom_table(args)
        assert result == 0
