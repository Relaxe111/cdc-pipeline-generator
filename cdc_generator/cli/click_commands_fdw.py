"""Click command definitions for the canonical fdw command."""

from __future__ import annotations

import sys

import click

from cdc_generator.cli.completions import (
    complete_available_envs,
    complete_existing_services,
)

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}


def _dispatch_command_passthrough(command: str) -> int:
    """Dispatch to top-level argparse handlers using current CLI argv tail."""
    from cdc_generator.cli.commands import execute_command

    return execute_command(command, sys.argv[2:])


@click.group(
    name="fdw",
    help="Plan and generate metadata-driven MSSQL FDW bootstrap SQL",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def fdw_cmd(ctx: click.Context) -> int:
    """Top-level fdw command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for fdw")
        click.echo("   Try: cdc fdw plan --service adopus")
        return 1
    return 0


def _add_common_fdw_options(func: click.Command) -> click.Command:
    """Apply shared fdw options to subcommands."""
    options = [
        click.option(
            "--service",
            required=True,
            shell_complete=complete_existing_services,
            help="Service name",
        ),
        click.option(
            "--source-env",
            default="default",
            shell_complete=complete_available_envs,
            help="Source environment key from source-groups.yaml",
        ),
        click.option(
            "--customer",
            "customers",
            multiple=True,
            help="Limit to one source/customer name; repeat to include multiple",
        ),
        click.option(
            "--table",
            "tables",
            multiple=True,
            help="Limit to one tracked source table; repeat to include multiple",
        ),
        click.option(
            "--target-schema",
            default=None,
            help="Override target schema name stored in source_table_registration",
        ),
        click.option(
            "--runner-role",
            default="cdc_runner",
            help="PostgreSQL role name for CREATE USER MAPPING",
        ),
        click.option(
            "--fdw-server-prefix",
            default="mssql",
            help="Prefix for generated FDW server names",
        ),
        click.option(
            "--fdw-schema-prefix",
            default="fdw",
            help="Prefix for generated FDW schema names",
        ),
        click.option(
            "--keep-placeholders",
            is_flag=True,
            help="Keep ${VAR} placeholders instead of resolving values from the environment",
        ),
    ]

    decorated = func
    for option in reversed(options):
        decorated = option(decorated)
    return decorated


@fdw_cmd.command(
    name="plan",
    help="Preview derived FDW source and table registrations",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@_add_common_fdw_options
@click.pass_context
def fdw_plan_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """fdw plan passthrough."""
    return _dispatch_command_passthrough("fdw")


@fdw_cmd.command(
    name="sql",
    help="Render idempotent SQL for metadata and FDW objects",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--metadata-only",
    is_flag=True,
    help="Render only cdc_management metadata registration SQL",
)
@click.option(
    "--output",
    default=None,
    help="Write SQL to this file instead of stdout",
)
@_add_common_fdw_options
@click.pass_context
def fdw_sql_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """fdw sql passthrough."""
    return _dispatch_command_passthrough("fdw")