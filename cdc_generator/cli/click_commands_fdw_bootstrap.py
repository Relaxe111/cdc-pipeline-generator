"""Click command definitions for ``cdc fdw bootstrap``."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

import click

from cdc_generator.cli import completions as completion_callbacks
from cdc_generator.cli import completions_bootstrap as bootstrap_completion_callbacks

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}

CommandCallback = Callable[..., object]
bootstrap_completion_callbacks_any: Any = bootstrap_completion_callbacks
completion_callbacks_any: Any = completion_callbacks


def _dispatch_bootstrap_passthrough() -> int:
    """Dispatch to the argparse bootstrap handler using the raw argv tail."""
    from cdc_generator.cli.fdw_bootstrap import main as bootstrap_main

    return bootstrap_main(sys.argv[3:])


def _apply_options(
    func: CommandCallback,
    options: list[Callable[[CommandCallback], CommandCallback]],
) -> CommandCallback:
    """Apply a list of Click option decorators in declaration order."""
    decorated = func
    for option in reversed(options):
        decorated = option(decorated)
    return decorated


def _add_common_bootstrap_options(func: CommandCallback) -> CommandCallback:
    """Apply options shared by bootstrap subcommands."""
    return _apply_options(
        func,
        [
            click.option(
                "--service",
                required=False,
                shell_complete=completion_callbacks_any.complete_existing_services,
                help="Service name; inferred when exactly one service exists",
            ),
            click.option(
                "--target-sink-env",
                required=True,
                shell_complete=completion_callbacks_any.complete_fdw_target_sink_envs,
                help="Target sink environment (dev, stage, prod)",
            ),
            click.option(
                "--table",
                "tables",
                multiple=True,
                shell_complete=bootstrap_completion_callbacks_any.complete_bootstrap_tables,
                help="Limit to one tracked source table; repeat to include multiple",
            ),
            click.option(
                "--psql-bin",
                default=None,
                type=click.Path(dir_okay=False, path_type=str),
                help="Override the psql executable path",
            ),
        ],
    )


def _add_source_filter_option(func: CommandCallback) -> CommandCallback:
    """Apply the repeatable source filter option."""
    return _apply_options(
        func,
        [
            click.option(
                "--source",
                "sources",
                multiple=True,
                shell_complete=bootstrap_completion_callbacks_any.complete_bootstrap_sources,
                help="Source database name from source-groups.yaml; repeat to include multiple",
            ),
        ],
    )


def _add_run_retry_options(func: CommandCallback) -> CommandCallback:
    """Apply source-selection options shared by run and retry."""
    return _apply_options(
        func,
        [
            click.option(
                "--source",
                "sources",
                multiple=True,
                shell_complete=bootstrap_completion_callbacks_any.complete_bootstrap_sources,
                help="Source database name from source-groups.yaml; repeat to include multiple",
            ),
            click.option(
                "--all-sources",
                is_flag=True,
                help="Bootstrap or retry all registered source databases",
            ),
            click.option(
                "--dry-run",
                is_flag=True,
                help="Print the SQL that would run without executing",
            ),
            click.option(
                "--json",
                "output_json",
                is_flag=True,
                help="Print machine-readable JSON output",
            ),
        ],
    )


@click.group(
    name="bootstrap",
    help="Execute native CDC table bootstrapping against a PostgreSQL sink",
    context_settings=_PASSTHROUGH_CTX,
    invoke_without_command=True,
)
@click.pass_context
def fdw_bootstrap_cmd(ctx: click.Context) -> int:
    """Top-level ``cdc fdw bootstrap`` command group."""
    if ctx.invoked_subcommand is None:
        return _dispatch_bootstrap_passthrough()
    return 0


@fdw_bootstrap_cmd.command(
    name="status",
    help="Show bootstrap state for tracked tables",
)
@click.option(
    "--pending",
    is_flag=True,
    help="Show only pending tables",
)
@click.option(
    "--failed",
    is_flag=True,
    help="Show only failed tables",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Print machine-readable JSON output",
)
@_add_source_filter_option
@_add_common_bootstrap_options
@click.pass_context
def fdw_bootstrap_status_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Typed Click wrapper for ``cdc fdw bootstrap status``."""
    return _dispatch_bootstrap_passthrough()


@fdw_bootstrap_cmd.command(
    name="run",
    help="Execute bootstrap for pending tables",
)
@click.option(
    "--failed",
    is_flag=True,
    help="Retry only failed tables for the selected sources",
)
@click.option(
    "--no-enable-after",
    is_flag=True,
    help="Leave tables disabled after bootstrap (default: enable after)",
)
@_add_run_retry_options
@_add_common_bootstrap_options
@click.pass_context
def fdw_bootstrap_run_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Typed Click wrapper for ``cdc fdw bootstrap run``."""
    return _dispatch_bootstrap_passthrough()


@fdw_bootstrap_cmd.command(
    name="retry",
    help="Retry bootstrap for previously failed tables",
)
@_add_run_retry_options
@_add_common_bootstrap_options
@click.pass_context
def fdw_bootstrap_retry_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Typed Click wrapper for ``cdc fdw bootstrap retry``."""
    return _dispatch_bootstrap_passthrough()
