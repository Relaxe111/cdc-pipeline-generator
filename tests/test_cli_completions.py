"""Unit tests for CLI completion callbacks."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch

import click
from click.shell_completion import CompletionItem

from cdc_generator.cli.completions import (
    complete_available_sink_keys,
    complete_map_column,
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


def test_click_cli_registers_manage_services_command() -> None:
    """Canonical top-level manage-services command is registered."""
    from cdc_generator.cli.commands import _click_cli

    assert "manage-services" in _click_cli.commands
    assert "manage-service" not in _click_cli.commands


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
