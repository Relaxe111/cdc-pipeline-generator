"""Unit tests for CLI completion callbacks."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch

import click
from click.shell_completion import CompletionItem

from cdc_generator.cli.completions import (
    complete_available_sink_keys,
    complete_accept_column,
    complete_column_templates,
    complete_migration_envs,
    complete_map_column,
    complete_target_schema,
)


def _values(items: list[CompletionItem]) -> list[str]:
    """Extract completion values."""
    return [item.value for item in items]


@patch("cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service")
@patch("cdc_generator.helpers.autocompletions.sinks.list_available_sink_keys")
def test_complete_available_sink_keys_filters_existing_for_flag_service(
    mock_available: Mock,
    mock_existing: Mock,
) -> None:
    """When --service is present, already-added sinks are hidden."""
    mock_available.return_value = [
        "sink_asma.chat",
        "sink_asma.directory",
        "sink_asma.proxy",
    ]
    mock_existing.return_value = ["sink_asma.directory"]

    ctx = cast(click.Context, SimpleNamespace(params={"service": "directory"}, args=[]))
    items = complete_available_sink_keys(
        ctx,
        cast(click.Parameter, None),
        "sink_asma.",
    )

    assert _values(items) == ["sink_asma.chat", "sink_asma.proxy"]


@patch("cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service")
@patch("cdc_generator.helpers.autocompletions.sinks.list_available_sink_keys")
def test_complete_available_sink_keys_filters_existing_for_positional_service(
    mock_available: Mock,
    mock_existing: Mock,
) -> None:
    """Positional service shorthand also enables sink filtering."""
    mock_available.return_value = [
        "sink_asma.activities",
        "sink_asma.chat",
    ]
    mock_existing.return_value = ["sink_asma.activities"]

    ctx = cast(click.Context, SimpleNamespace(params={}, args=["directory"]))
    items = complete_available_sink_keys(
        ctx,
        cast(click.Parameter, None),
        "sink_asma.",
    )

    assert _values(items) == ["sink_asma.chat"]


@patch("cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service")
@patch("cdc_generator.helpers.autocompletions.sinks.list_available_sink_keys")
def test_complete_available_sink_keys_without_service_returns_all(
    mock_available: Mock,
    mock_existing: Mock,
) -> None:
    """Without service context, return all sink keys (current behavior)."""
    mock_available.return_value = [
        "sink_asma.activities",
        "sink_asma.chat",
    ]
    mock_existing.return_value = ["sink_asma.activities"]

    ctx = cast(click.Context, SimpleNamespace(params={}, args=[]))
    items = complete_available_sink_keys(
        ctx,
        cast(click.Parameter, None),
        "sink_asma.",
    )

    assert _values(items) == ["sink_asma.activities", "sink_asma.chat"]
    mock_existing.assert_not_called()


@patch("cdc_generator.helpers.autocompletions.sinks.list_sink_keys_for_service")
@patch("cdc_generator.helpers.autocompletions.sinks.list_available_sink_keys")
def test_complete_available_sink_keys_filters_same_source_service_target(
    mock_available: Mock,
    mock_existing: Mock,
) -> None:
    """Exclude sink keys targeting the same service as source service."""
    mock_available.return_value = [
        "sink_asma.activities",
        "sink_asma.directory",
        "sink_asma.tracing",
    ]
    mock_existing.return_value = []

    ctx = cast(click.Context, SimpleNamespace(params={"service": "directory"}, args=[]))
    items = complete_available_sink_keys(
        ctx,
        cast(click.Parameter, None),
        "sink_asma.",
    )

    assert _values(items) == ["sink_asma.activities", "sink_asma.tracing"]


@patch("cdc_generator.helpers.service_config.load_service_config")
@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_custom_table_definitions_for_sink_target"
)
@patch("cdc_generator.helpers.autocompletions.sinks.get_default_sink_for_service")
@patch("cdc_generator.helpers.autocompletions.schemas.list_schemas_for_service")
def test_complete_target_schema_includes_custom_and_configured_schemas(
    mock_schemas: Mock,
    mock_default_sink: Mock,
    mock_custom_tables: Mock,
    mock_load_config: Mock,
) -> None:
    """--sink-schema includes target schemas + custom-table + configured sink schemas."""
    mock_schemas.return_value = ["public"]
    mock_default_sink.return_value = "sink_asma.directory"
    mock_custom_tables.return_value = ["audit.login_events"]
    mock_load_config.return_value = {
        "sinks": {
            "sink_asma.directory": {
                "tables": {
                    "analytics.user_rollups": {},
                },
            },
        }
    }

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={"service": "adopus"},
            args=[],
        ),
    )

    items = complete_target_schema(ctx, cast(click.Parameter, None), "")

    assert _values(items) == ["analytics", "custom:audit", "public"]
    audit_items = [item for item in items if item.value == "custom:audit"]
    assert len(audit_items) == 1
    assert audit_items[0].help == "custom-table schema"


@patch("cdc_generator.helpers.service_config.load_service_config")
@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_custom_table_definitions_for_sink_target"
)
@patch("cdc_generator.helpers.autocompletions.schemas.list_schemas_for_service")
def test_complete_target_schema_all_mode_uses_common_including_custom_tables(
    mock_schemas: Mock,
    mock_custom_tables: Mock,
    mock_load_config: Mock,
) -> None:
    """--all mode returns common schemas across sinks, including custom-table schemas."""
    mock_schemas.return_value = ["public", "core"]

    def _custom_for_sink(sink_key: str) -> list[str]:
        if sink_key == "sink_asma.directory":
            return ["audit.login_events", "public.audit_log"]
        if sink_key == "sink_asma.chat":
            return ["audit.message_events", "public.chat_audit"]
        return []

    mock_custom_tables.side_effect = _custom_for_sink
    mock_load_config.return_value = {
        "sinks": {
            "sink_asma.directory": {"tables": {}},
            "sink_asma.chat": {"tables": {}},
        }
    }

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={"service": "adopus", "all_flag": True},
            args=[],
        ),
    )

    items = complete_target_schema(ctx, cast(click.Parameter, None), "")

    assert _values(items) == [
        "custom:audit",
        "core",
        "custom:public",
    ]


def test_click_cli_registers_manage_services_command() -> None:
    """Canonical top-level manage-services command is registered."""
    from cdc_generator.cli.commands import _click_cli

    assert "manage-services" in _click_cli.commands
    assert "manage-service" not in _click_cli.commands


@patch("cdc_generator.helpers.service_config.get_project_root")
def test_complete_migration_envs_reads_manifest_databases(
    mock_project_root: Mock,
    tmp_path: Path,
) -> None:
    """Migration env completion should come from manifest sink_target.databases."""
    sink_dir = tmp_path / "migrations" / "sink_asma.directory"
    sink_dir.mkdir(parents=True)
    (sink_dir / "manifest.yaml").write_text(
        "sink_target:\n"
        "  databases:\n"
        "    dev: directory_dev\n"
        "    prod: directory\n"
        "    stage: directory_stage\n",
        encoding="utf-8",
    )
    mock_project_root.return_value = tmp_path

    ctx = cast(click.Context, SimpleNamespace(params={"sink": "sink_asma.directory"}, args=[]))
    items = complete_migration_envs(
        ctx,
        cast(click.Parameter, None),
        "",
    )

    assert _values(items) == ["dev", "prod", "stage"]


@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_compatible_target_prefixes_for_map_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_compatible_target_columns_for_source_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.load_sink_tables_for_autocomplete"
)
def test_complete_map_column_first_position_uses_compatible_sources(
    mock_load_tables: Mock,
    mock_legacy_targets: Mock,
    mock_target_prefixes: Mock,
) -> None:
    """First --map-column token suggests unique sink target prefixes."""
    mock_load_tables.return_value = {
        "public.directory_user_name": {
            "from": "public.customer_user",
            "target": "public.directory_user_name",
        },
    }
    mock_legacy_targets.return_value = []
    mock_target_prefixes.return_value = ["user_id:"]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "sink_table": "public.directory_user_name",
                "map_column": None,
            },
            args=[],
        ),
    )

    items = complete_map_column(
        ctx,
        cast(click.Parameter, None),
        "u",
    )

    assert _values(items) == ["user_id:"]
    mock_target_prefixes.assert_called_once_with(
        "myservice",
        "sink_asma.proxy",
        "public.customer_user",
        "public.directory_user_name",
        40,
    )


@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_compatible_target_columns_for_source_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.load_sink_tables_for_autocomplete"
)
def test_complete_map_column_second_position_uses_selected_source(
    mock_load_tables: Mock,
    mock_compatible_targets: Mock,
) -> None:
    """Legacy second token still suggests compatible target columns."""
    mock_load_tables.return_value = {
        "public.directory_user_name": {
            "from": "public.customer_user",
            "target": "public.directory_user_name",
        },
    }
    mock_compatible_targets.return_value = ["first_name", "last_name"]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "sink_table": "public.directory_user_name",
                "map_column": ["full_name"],
            },
            args=[],
        ),
    )

    items = complete_map_column(
        ctx,
        cast(click.Parameter, None),
        "f",
    )

    assert _values(items) == ["first_name"]
    mock_compatible_targets.assert_called_once_with(
        "myservice",
        "sink_asma.proxy",
        "public.customer_user",
        "public.directory_user_name",
        "full_name",
    )


@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_compatible_map_column_pairs_for_target_prefix"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_compatible_target_columns_for_source_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.load_sink_tables_for_autocomplete"
)
def test_complete_map_column_target_prefix_then_source_pairs(
    mock_load_tables: Mock,
    mock_legacy_targets: Mock,
    mock_pair_helper: Mock,
) -> None:
    """After typing target:, completion suggests target:source pairs."""
    mock_load_tables.return_value = {
        "public.directory_user_name": {
            "from": "public.customer_user",
            "target": "public.directory_user_name",
        },
    }
    mock_legacy_targets.return_value = []
    mock_pair_helper.return_value = ["user_id:user_id"]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "sink_table": "public.directory_user_name",
                "map_column": None,
            },
            args=[],
        ),
    )

    items = complete_map_column(
        ctx,
        cast(click.Parameter, None),
        "user_id:",
    )

    assert _values(items) == ["user_id:user_id"]
    mock_pair_helper.assert_called_once_with(
        "myservice",
        "sink_asma.proxy",
        "public.customer_user",
        "public.directory_user_name",
        "user_id",
        "",
        40,
    )


@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_compatible_target_prefixes_for_map_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_compatible_target_columns_for_source_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.load_sink_tables_for_autocomplete"
)
def test_complete_map_column_next_flag_excludes_mapped_target_prefixes(
    mock_load_tables: Mock,
    mock_legacy_targets: Mock,
    mock_target_prefixes: Mock,
) -> None:
    """When one mapping exists, next --map-column still completes and skips mapped targets."""
    mock_load_tables.return_value = {
        "public.directory_user_name": {
            "from": "public.customer_user",
            "target": "public.directory_user_name",
        },
    }
    mock_legacy_targets.return_value = []
    mock_target_prefixes.return_value = ["email:", "pnr:", "status:"]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "sink_table": "public.directory_user_name",
                "map_column": ["email:epost"],
            },
            args=[],
        ),
    )

    items = complete_map_column(
        ctx,
        cast(click.Parameter, None),
        "",
    )

    assert _values(items) == ["pnr:", "status:"]


@patch(
    "cdc_generator.helpers.autocompletions.sinks."
    "list_compatible_map_column_pairs_for_target_prefix"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_compatible_target_columns_for_source_column"
)
@patch(
    "cdc_generator.helpers.autocompletions.sinks.load_sink_tables_for_autocomplete"
)
def test_complete_map_column_pair_step_excludes_mapped_sources(
    mock_load_tables: Mock,
    mock_legacy_targets: Mock,
    mock_pair_helper: Mock,
) -> None:
    """Pair suggestions exclude already mapped source columns."""
    mock_load_tables.return_value = {
        "public.directory_user_name": {
            "from": "public.customer_user",
            "target": "public.directory_user_name",
        },
    }
    mock_legacy_targets.return_value = []
    mock_pair_helper.return_value = [
        "pnr:epost",
        "pnr:empno",
        "pnr:extraNotat",
    ]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "sink_table": "public.directory_user_name",
                "map_column": ["email:epost"],
            },
            args=[],
        ),
    )

    items = complete_map_column(
        ctx,
        cast(click.Parameter, None),
        "pnr:e",
    )

    assert _values(items) == ["pnr:empno", "pnr:extraNotat"]


@patch(
    "cdc_generator.helpers.autocompletions.column_template_completions."
    "list_compatible_target_prefixes_for_column_template"
)
@patch(
    "cdc_generator.helpers.autocompletions.column_template_completions."
    "list_compatible_column_template_pairs_for_target_prefix"
)
def test_complete_column_templates_excludes_mapped_and_selected_targets(
    mock_pair_helper: Mock,
    mock_prefix_helper: Mock,
) -> None:
    """Prefix suggestions hide targets already used by map or template pair."""
    mock_prefix_helper.return_value = ["customer_id:", "region_id:", "user_id:"]
    mock_pair_helper.return_value = []

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "add_sink_table": "public.customer_user",
                "from_table": "dbo.Actor",
                "map_column": ["customer_id:KundeId"],
                "add_column_template": ["region_id:tenant_region"],
            },
            args=[],
        ),
    )

    items = complete_column_templates(
        ctx,
        cast(click.Parameter, None),
        "",
    )

    assert _values(items) == ["user_id:"]


@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_target_columns_for_sink_table"
)
def test_complete_accept_column_excludes_covered_targets(
    mock_target_columns: Mock,
) -> None:
    """--accept-column shows sink target columns left after map/template/accept."""
    mock_target_columns.return_value = [
        "customer_id",
        "email",
        "journal_role",
        "user_id",
    ]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "add_sink_table": "public.customer_user",
                "from_table": "dbo.Actor",
                "map_column": ["email:epost"],
                "add_column_template": ["customer_id:customer_id"],
                "accept_column": ["journal_role"],
            },
            args=[],
        ),
    )

    items = complete_accept_column(
        ctx,
        cast(click.Parameter, None),
        "",
    )

    assert _values(items) == ["user_id"]


@patch(
    "cdc_generator.helpers.autocompletions.sinks.list_target_columns_for_sink_table"
)
def test_complete_accept_column_considers_left_side_raw_args(
    mock_target_columns: Mock,
) -> None:
    """Raw left-side args are considered when filtering accept-column suggestions."""
    mock_target_columns.return_value = ["customer_id", "email", "journal_role"]

    ctx = cast(
        click.Context,
        SimpleNamespace(
            params={
                "service": "myservice",
                "sink": "sink_asma.proxy",
                "add_sink_table": "public.customer_user",
                "from_table": "dbo.Actor",
                "map_column": None,
                "add_column_template": None,
                "accept_column": None,
            },
            args=[
                "--map-column", "email:epost",
                "--add-column-template", "customer_id:customer_id",
            ],
        ),
    )

    items = complete_accept_column(
        ctx,
        cast(click.Parameter, None),
        "",
    )

    assert _values(items) == ["journal_role"]


