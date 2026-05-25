"""Click command definitions for the canonical fdw command."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

import click

from cdc_generator.cli import completions as completion_callbacks

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}

CommandCallback = Callable[..., object]
completion_callbacks_any: Any = completion_callbacks


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


def _add_common_fdw_options(func: CommandCallback) -> CommandCallback:
    """Apply shared fdw options to subcommands."""
    options = [
        click.option(
            "--service",
            required=False,
            shell_complete=completion_callbacks_any.complete_existing_services,
            help="Service name; inferred when exactly one service exists",
        ),
        click.option(
            "--source-env",
            default=None,
            shell_complete=completion_callbacks_any.complete_available_envs,
            help="Optional source environment key from source-groups.yaml",
        ),
        click.option(
            "--target-sink-env",
            default=None,
            shell_complete=completion_callbacks_any.complete_fdw_target_sink_envs,
            help="Only include source routes whose target_sink_env matches this sink env",
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


def _add_apply_fdw_options(func: CommandCallback) -> CommandCallback:
    """Apply fdw apply-specific options to the subcommand."""
    options = [
        click.option(
            "--sink",
            default=None,
            help="Sink target key in the form <sink-group>.<sink-service>",
        ),
        click.option(
            "--sql-path",
            default=None,
            type=click.Path(dir_okay=False, path_type=str),
            help="Override the SQL file to apply",
        ),
        click.option(
            "--psql-bin",
            default=None,
            type=click.Path(dir_okay=False, path_type=str),
            help="Override the psql executable path",
        ),
        click.option(
            "--dry-run",
            is_flag=True,
            help="Print the resolved target and psql command without applying the SQL",
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
@_add_common_fdw_options
@click.pass_context
def fdw_sql_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """fdw sql passthrough."""
    return _dispatch_command_passthrough("fdw")


@fdw_cmd.command(
    name="apply",
    help="Apply a generated FDW SQL file to the target PostgreSQL sink environment",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@_add_apply_fdw_options
@_add_common_fdw_options
@click.pass_context
def fdw_apply_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """fdw apply passthrough."""
    return _dispatch_command_passthrough("fdw")
