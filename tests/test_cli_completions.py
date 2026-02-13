"""Unit tests for CLI completion callbacks."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch

import click
from click.shell_completion import CompletionItem

from cdc_generator.cli.completions import complete_available_sink_keys


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
