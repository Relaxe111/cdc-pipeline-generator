"""Regression tests for manage-services resources custom-tables passthrough."""

import sys
from unittest.mock import patch

from cdc_generator.cli.click_commands import (
    manage_column_templates_cmd,
    manage_services_schema_custom_tables_cmd,
)


def _invoke_custom_tables_callback() -> int:
    """Invoke underlying callback without requiring active click context."""
    callback = manage_services_schema_custom_tables_cmd.callback
    wrapped = getattr(callback, "__wrapped__", None)
    if wrapped is None:
        msg = "Expected wrapped callback for custom-tables command"
        raise AssertionError(msg)
    return wrapped(None)


def _invoke_column_templates_callback() -> int:
    """Invoke underlying callback without requiring active click context."""
    callback = manage_column_templates_cmd.callback
    wrapped = getattr(callback, "__wrapped__", None)
    if wrapped is None:
        msg = "Expected wrapped callback for column-templates command"
        raise AssertionError(msg)
    return wrapped(None)


def test_custom_tables_passthrough_full_command() -> None:
    """Full command forwards all args after custom-tables token."""
    forwarded_args: list[str] = []

    def _capture(_group: str, _subcommand: str, args: list[str]) -> int:
        nonlocal forwarded_args
        forwarded_args = args
        return 0

    with patch(
        "cdc_generator.cli.commands.execute_grouped_command",
        side_effect=_capture,
    ):
        original_argv = sys.argv.copy()
        try:
            sys.argv = [
                "cdc",
                "manage-services",
                "resources",
                "custom-tables",
                "--service",
                "directory",
                "--list-custom-tables",
            ]
            result = _invoke_custom_tables_callback()
        finally:
            sys.argv = original_argv

    assert result == 0
    assert forwarded_args == [
        "--service",
        "directory",
        "--list-custom-tables",
    ]


def test_custom_tables_passthrough_msr_alias() -> None:
    """msr alias forwards all args after custom-tables token."""
    forwarded_args: list[str] = []

    def _capture(_group: str, _subcommand: str, args: list[str]) -> int:
        nonlocal forwarded_args
        forwarded_args = args
        return 0

    with patch(
        "cdc_generator.cli.commands.execute_grouped_command",
        side_effect=_capture,
    ):
        original_argv = sys.argv.copy()
        try:
            sys.argv = [
                "cdc",
                "msr",
                "custom-tables",
                "--service",
                "directory",
                "--add-custom-table",
                "public.audit_log",
                "--column",
                "id:uuid:pk",
            ]
            result = _invoke_custom_tables_callback()
        finally:
            sys.argv = original_argv

    assert result == 0
    assert forwarded_args == [
        "--service",
        "directory",
        "--add-custom-table",
        "public.audit_log",
        "--column",
        "id:uuid:pk",
    ]


def test_column_templates_passthrough_msr_alias() -> None:
    """msr alias forwards args after column-templates token."""
    forwarded_args: list[str] = []

    def _capture(
        _label: str,
        _info: dict[str, str],
        _paths: dict[str, object],
        args: list[str],
        _workspace_root: object,
    ) -> int:
        nonlocal forwarded_args
        forwarded_args = args
        return 0

    with (
        patch(
            "cdc_generator.cli.commands.detect_environment",
            return_value=(".", None, False),
        ),
        patch(
            "cdc_generator.cli.commands.get_script_paths",
            return_value={"runner": "generator"},
        ),
        patch(
            "cdc_generator.cli.commands.run_generator_spec",
            side_effect=_capture,
        ),
    ):
        original_argv = sys.argv.copy()
        try:
            sys.argv = [
                "cdc",
                "msr",
                "column-templates",
                "--add",
                "customer_id",
                "--type",
                "text",
                "--value",
                "{asma.sources.*.customer_id}",
            ]
            result = _invoke_column_templates_callback()
        finally:
            sys.argv = original_argv

    assert result == 0
    assert forwarded_args == [
        "--add",
        "customer_id",
        "--type",
        "text",
        "--value",
        "{asma.sources.*.customer_id}",
    ]
