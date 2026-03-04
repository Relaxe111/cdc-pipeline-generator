"""Runtime helpers for schema-related `manage-services resources` Click commands."""

from __future__ import annotations

import argparse
import sys

import click

_NESTED_SCHEMA_SUBCOMMAND_INDEX = 3


def execute_manage_services_schema_custom_tables() -> int:
    """Run custom-tables grouped passthrough dispatch."""
    from cdc_generator.cli.commands import execute_grouped_command

    try:
        subcommand_index = sys.argv.index("custom-tables")
    except ValueError:
        subcommand_index = _NESTED_SCHEMA_SUBCOMMAND_INDEX

    return execute_grouped_command(
        "manage-services",
        "resources",
        sys.argv[subcommand_index + 1:],
    )


def execute_manage_services_schema_transforms(  # noqa: PLR0913
    service: str | None,
    sink: str | None,
    sink_table: str | None,
    add_transform: str | None,
    remove_transform: str | None,
    list_transforms: bool,
    skip_validation: bool,
    list_rules: bool,
    list_transform_rules: bool,
) -> int:
    """Handle transform actions for manage-services resources transforms."""
    from cdc_generator.cli.service_handlers_templates import (
        handle_add_transform,
        handle_list_transform_rules,
        handle_list_transforms,
        handle_remove_transform,
    )

    args = argparse.Namespace(
        service=service,
        sink=sink,
        sink_table=sink_table,
        add_transform=add_transform,
        remove_transform=remove_transform,
        list_transforms=list_transforms,
        skip_validation=skip_validation,
    )

    if add_transform:
        return handle_add_transform(args)
    if remove_transform:
        return handle_remove_transform(args)
    if list_transforms:
        return handle_list_transforms(args)

    if not list_rules and not list_transform_rules:
        click.echo("❌ Missing action for manage-services resources transforms")
        click.echo(
            "   Try: cdc manage-services resources transforms --service <svc> "
            + "--sink <key> --sink-table <schema.table> --list-transforms"
        )
        click.echo("        cdc manage-services resources transforms --list-rules")
        return 1

    return handle_list_transform_rules(argparse.Namespace())
