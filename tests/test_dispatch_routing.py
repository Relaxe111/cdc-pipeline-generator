"""Dispatch routing tests for CLI flags not covered by test_dispatch.py.

Covers routing gaps identified in the coverage audit:
- _dispatch_validation: --validate-hierarchy, --validate-bloblang,
  --generate-validation, --inspect, --inspect-sink
- _dispatch_extra_columns: --add-column-template, --remove-column-template,
  --list-column-templates, --add-transform, --remove-transform,
  --list-transforms (all with --service)
- _dispatch_sink: --add-sink-table, --add-custom-sink-table,
  --modify-custom-table
- _dispatch_sink_conditional: --map-column + --sink-table,
  --add-sink-table auto-name from --from
- Full _dispatch: priority ordering across categories
"""

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service import (
    _dispatch,
    _dispatch_extra_columns,
    _dispatch_sink,
    _dispatch_sink_conditional,
    _dispatch_validation,
    main,
)


# project_dir fixture is provided by tests/conftest.py


@pytest.fixture()
def service_yaml(project_dir: Path) -> Path:
    """Service YAML with tables and a sink."""
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
        "          from: public.users\n"
    )
    return sf


@pytest.fixture()
def service_with_schemas(
    project_dir: Path, service_yaml: Path,
) -> Path:
    """Service YAML plus source and sink schema files."""
    schemas_dir = project_dir / "service-schemas"

    # Source schema: proxy/public/users.yaml
    src_dir = schemas_dir / "proxy" / "public"
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
        "  - name: email\n"
        "    type: text\n"
        "    nullable: true\n"
    )

    # Sink schema: chat/public/users.yaml
    sink_dir = schemas_dir / "chat" / "public"
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

    return service_yaml


@pytest.fixture()
def service_with_custom_table(
    project_dir: Path, service_yaml: Path,
) -> Path:
    """Service YAML with a custom managed table in the sink."""
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


def _full_ns(**kwargs: object) -> argparse.Namespace:
    """Build a complete argparse.Namespace with all dispatch-relevant attrs."""
    defaults: dict[str, object] = {
        # Core
        "service": "proxy",
        "create_service": None,
        "server": None,
        # Source
        "add_source_table": None,
        "add_source_tables": None,
        "remove_table": None,
        "source_table": None,
        "list_source_tables": False,
        "primary_key": None,
        "schema": None,
        "ignore_columns": None,
        "track_columns": None,
        # Inspect
        "inspect": False,
        "inspect_sink": None,
        "all": False,
        "env": "nonprod",
        "save": False,
        # Validation
        "validate_config": False,
        "validate_hierarchy": False,
        "validate_bloblang": False,
        "generate_validation": False,
        # Sink
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
        "list_sinks": False,
        "validate_sinks": False,
        "add_custom_sink_table": None,
        "column": None,
        "modify_custom_table": None,
        "add_column": None,
        "remove_column": None,
        # Templates
        "add_column_template": None,
        "remove_column_template": None,
        "list_column_templates": False,
        "column_name": None,
        "value": None,
        "add_transform": None,
        "remove_transform": None,
        "list_transforms": False,
        "list_template_keys": False,
        "list_transform_rule_keys": False,
        "skip_validation": True,
        # Legacy
        "source": None,
        "source_schema": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_validation — missing routes
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchValidationE2E:
    """E2E dispatch tests for validation flags not in test_dispatch.py."""

    def test_routes_validate_hierarchy(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--validate-hierarchy dispatches and returns int."""
        args = _full_ns(validate_hierarchy=True)
        result = _dispatch_validation(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_validate_bloblang(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--validate-bloblang dispatches and returns int."""
        with patch(
            "cdc_generator.cli.service_handlers_bloblang.validate_service_bloblang",
            return_value=True,
        ):
            args = _full_ns(validate_bloblang=True)
            result = _dispatch_validation(args)
        assert result is not None
        assert result == 0

    def test_routes_generate_validation(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--generate-validation dispatches and returns int."""
        args = _full_ns(generate_validation=True)
        result = _dispatch_validation(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_inspect(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--inspect dispatches to inspect handler."""
        args = _full_ns(inspect=True, all=True)
        # inspect tries to resolve DB type and connect; mock the handler
        with patch(
            "cdc_generator.cli.service.handle_inspect",
            return_value=0,
        ) as mock_handler:
            result = _dispatch_validation(args)
        mock_handler.assert_called_once_with(args)
        assert result == 0

    def test_routes_inspect_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--inspect-sink dispatches to inspect_sink handler."""
        args = _full_ns(inspect_sink="sink_asma.chat", all=True)
        # inspect_sink tries to connect to DB; mock the handler
        with patch(
            "cdc_generator.cli.service.handle_inspect_sink",
            return_value=0,
        ) as mock_handler:
            result = _dispatch_validation(args)
        mock_handler.assert_called_once_with(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_extra_columns — per-service template/transform routes
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchExtraColumnsE2E:
    """E2E dispatch tests for column template and transform flags with --service."""

    def test_routes_add_column_template(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-column-template dispatches to handler."""
        args = _full_ns(
            add_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_remove_column_template(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--remove-column-template dispatches to handler."""
        args = _full_ns(
            remove_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_list_column_templates(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--list-column-templates dispatches to handler."""
        args = _full_ns(
            list_column_templates=True,
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_add_transform(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-transform dispatches to handler."""
        args = _full_ns(
            add_transform="map_boolean",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_remove_transform(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--remove-transform dispatches to handler."""
        args = _full_ns(
            remove_transform="map_boolean",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_list_transforms(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--list-transforms dispatches to handler."""
        args = _full_ns(
            list_transforms=True,
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_sink — missing routes
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSinkE2E:
    """E2E dispatch tests for sink flags not in test_dispatch.py."""

    def test_routes_add_sink_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-sink-table + --sink dispatches to handler."""
        args = _full_ns(
            add_sink_table="public.queries",
            sink="sink_asma.chat",
            target_exists="false",
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_add_sink_table_with_map_column(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-sink-table + --map-column dispatches correctly."""
        args = _full_ns(
            add_sink_table="public.users",
            sink="sink_asma.chat",
            target_exists="true",
            map_column=[["id", "user_id"]],
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_add_custom_sink_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--add-custom-sink-table dispatches to handler."""
        args = _full_ns(
            add_custom_sink_table="public.audit_log",
            sink="sink_asma.chat",
            column=["id:uuid:pk", "name:text:not_null"],
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_modify_custom_table_add_column(
        self, project_dir: Path, service_with_custom_table: Path,
    ) -> None:
        """--modify-custom-table + --add-column dispatches to handler."""
        args = _full_ns(
            modify_custom_table="public.audit_log",
            sink="sink_asma.chat",
            add_column="created_at:timestamptz:default_now",
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_routes_modify_custom_table_remove_column(
        self, project_dir: Path, service_with_custom_table: Path,
    ) -> None:
        """--modify-custom-table + --remove-column dispatches to handler."""
        args = _full_ns(
            modify_custom_table="public.audit_log",
            sink="sink_asma.chat",
            remove_column="event_type",
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_modify_custom_table_no_action_returns_1(
        self, project_dir: Path, service_with_custom_table: Path,
    ) -> None:
        """--modify-custom-table without --add-column/--remove-column → error."""
        args = _full_ns(
            modify_custom_table="public.audit_log",
            sink="sink_asma.chat",
        )
        result = _dispatch_sink(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# _dispatch_sink_conditional — map-column on existing table, from auto-name
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchSinkConditionalE2E:
    """E2E tests for conditional sink dispatch routes."""

    def test_routes_map_column_on_existing_table(
        self, project_dir: Path, service_with_schemas: Path,
    ) -> None:
        """--map-column + --sink-table (no --add-sink-table) dispatches."""
        with patch(
            "cdc_generator.validators.manage_service.sink_operations.SERVICE_SCHEMAS_DIR",
            project_dir / "service-schemas",
        ):
            args = _full_ns(
                map_column=[["name", "display_name"]],
                sink="sink_asma.chat",
                sink_table="public.users",
            )
            result = _dispatch_sink_conditional(args)
        assert result is not None
        assert isinstance(result, int)

    def test_add_sink_table_auto_name_from_from(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--from without --add-sink-table auto-names from source table."""
        args = _full_ns(
            add_sink_table=None,
            from_table="public.queries",
            sink="sink_asma.chat",
            target_exists="false",
        )
        result = _dispatch_sink_conditional(args)
        assert result is not None
        assert isinstance(result, int)


# ═══════════════════════════════════════════════════════════════════════════
# Full _dispatch — cross-category priority
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchFullE2E:
    """E2E tests for top-level _dispatch priority ordering."""

    def test_extra_columns_before_sink(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Template flag dispatched before sink flag when both present."""
        args = _full_ns(
            add_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
            list_sinks=True,
        )
        # add_column_template should win (extra_columns checked before sink)
        with patch(
            "cdc_generator.cli.service.handle_add_column_template",
            return_value=0,
        ) as mock_handler:
            result = _dispatch(args)
        mock_handler.assert_called_once()
        assert result == 0

    def test_validation_before_extra_columns(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Validation flag dispatched before template flag when both present."""
        args = _full_ns(
            validate_hierarchy=True,
            add_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        with patch(
            "cdc_generator.cli.service.handle_validate_hierarchy",
            return_value=0,
        ) as mock_val:
            result = _dispatch(args)
        mock_val.assert_called_once()
        assert result == 0

    def test_sink_before_source(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Sink flag dispatched before source flag when both present."""
        args = _full_ns(
            list_sinks=True,
            add_source_table="public.orders",
        )
        with patch(
            "cdc_generator.cli.service.handle_sink_list",
            return_value=0,
        ) as mock_sink:
            result = _dispatch(args)
        mock_sink.assert_called_once()
        assert result == 0

    def test_validate_bloblang_through_dispatch(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--validate-bloblang routes through full dispatch chain."""
        with patch(
            "cdc_generator.cli.service_handlers_bloblang.validate_service_bloblang",
            return_value=True,
        ):
            args = _full_ns(validate_bloblang=True)
            result = _dispatch(args)
        assert result == 0

    def test_inspect_through_dispatch(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--inspect routes through full dispatch chain."""
        args = _full_ns(inspect=True, all=True)
        with patch(
            "cdc_generator.cli.service.handle_inspect",
            return_value=0,
        ):
            result = _dispatch(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# Dispatch gaps: context flags consumed by handlers
# ═══════════════════════════════════════════════════════════════════════════


class TestDispatchContextFlags:
    """Tests for context flags that are consumed inside handlers."""

    def test_target_passed_to_add_sink_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--target is consumed by add_sink_table handler."""
        args = _full_ns(
            add_sink_table="public.queries",
            sink="sink_asma.chat",
            target_exists="true",
            target="public.query_results",
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_include_sink_columns_passed_to_add_sink_table(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--include-sink-columns is consumed by add_sink_table handler."""
        args = _full_ns(
            add_sink_table="public.queries",
            sink="sink_asma.chat",
            target_exists="false",
            include_sink_columns=["id", "name"],
        )
        result = _dispatch_sink(args)
        assert result is not None
        assert isinstance(result, int)

    def test_value_passed_to_add_column_template(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--value is consumed by add_column_template handler."""
        args = _full_ns(
            add_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
            value="{asma.sources.*.customer_id}",
        )
        result = _dispatch_extra_columns(args)
        assert result is not None
        assert isinstance(result, int)

    def test_save_passed_to_inspect(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--save is consumed by inspect handler."""
        args = _full_ns(inspect=True, all=True, save=True)
        with patch(
            "cdc_generator.cli.service.handle_inspect",
            return_value=0,
        ) as mock_handler:
            result = _dispatch_validation(args)
        mock_handler.assert_called_once_with(args)
        assert result == 0
        # Verify the args object carries save=True
        assert args.save is True


# ═══════════════════════════════════════════════════════════════════════════
# main() — positional service_name merge
# ═══════════════════════════════════════════════════════════════════════════


class TestMainPositionalService:
    """Tests for main() parsing positional service_name."""

    def test_positional_service_name_merged(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Positional arg 'proxy' is merged into --service."""
        with patch(
            "sys.argv", ["manage-service", "proxy", "--list-source-tables"],
        ):
            result = main()
        assert result == 0

    def test_flag_service_takes_precedence(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--service flag wins over positional service_name."""
        with patch(
            "sys.argv",
            ["manage-service", "ignored", "--service", "proxy",
             "--list-source-tables"],
        ):
            result = main()
        assert result == 0
