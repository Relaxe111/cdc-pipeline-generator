"""Click command definitions for pipelines, migrations, and test passthrough."""

from __future__ import annotations

import sys

import click

from cdc_generator.cli.completions import (
    complete_available_envs,
    complete_migration_envs,
)

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}


def _dispatch_grouped_passthrough(group: str, subcommand: str) -> int:
    """Dispatch to grouped argparse handlers using current CLI argv tail."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command(group, subcommand, sys.argv[3:])


def _dispatch_command_passthrough(command: str) -> int:
    """Dispatch to top-level argparse handlers using current CLI argv tail."""
    from cdc_generator.cli.commands import execute_command

    return execute_command(command, sys.argv[2:])


@click.group(
    name="manage-pipelines",
    help="Manage pipeline lifecycle commands",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def manage_pipelines_cmd(ctx: click.Context) -> int:
    """Manage-pipelines command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for manage-pipelines")
        click.echo("   Try: cdc manage-pipelines generate --all")
        return 1
    return 0


@manage_pipelines_cmd.command(
    name="generate",
    help="Generate Bento pipelines",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--all", "all_flag", is_flag=True,
              help="Generate for all customers")
@click.option("--force", is_flag=True, help="Force regeneration")
@click.pass_context
def manage_pipelines_generate_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines generate passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "generate")


@manage_pipelines_cmd.command(
    name="list",
    help="List available customers and generated pipeline summary",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--status", "status_flag", is_flag=True,
              help="Show readiness/freshness status summary")
@click.pass_context
def manage_pipelines_list_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines list passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "list")


@manage_pipelines_cmd.command(
    name="verify",
    help="Verify pipeline templates/configuration",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--full", "full_mode", is_flag=True,
              help="Run full verification (generate + validate outputs)")
@click.option("--sink", "sink_mode", is_flag=True,
              help="Verify sink PostgreSQL connectivity")
@click.option("--service", help="Optional service name filter for --sink")
@click.pass_context
def manage_pipelines_verify_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines verify passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "verify")


@manage_pipelines_cmd.command(
    name="diff",
    help="Detect drift in generated pipeline files",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.pass_context
def manage_pipelines_diff_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines diff passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "diff")


@manage_pipelines_cmd.command(
    name="health",
    help="Check Bento/runtime readiness",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--url", "health_urls", multiple=True,
              help="Optional endpoint URL to probe")
@click.pass_context
def manage_pipelines_health_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines health passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "health")


@manage_pipelines_cmd.command(
    name="prune",
    help="Detect/remove orphaned generated pipeline files",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--confirm", is_flag=True,
              help="Delete orphaned files (default is dry-run)")
@click.pass_context
def manage_pipelines_prune_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines prune passthrough."""
    return _dispatch_grouped_passthrough("manage-pipelines", "prune")


@click.command(
    name="test",
    help="Run project tests (unit and CLI e2e)",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--cli", is_flag=True, help="Run CLI end-to-end tests only")
@click.option("--all", "all_flag", is_flag=True,
              help="Run all tests (unit + CLI e2e)")
@click.option("-v", "verbose", is_flag=True, help="Verbose pytest output")
@click.option("-k", "filter_expr", help="Filter tests by expression")
@click.pass_context
def test_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Test passthrough."""
    return _dispatch_command_passthrough("test")


@click.command(
    name="test-coverage",
    help="Show test coverage report by cdc command",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("-v", "verbose", is_flag=True,
              help="Verbose: show individual test names")
@click.option("--json", "json_output", is_flag=True,
              help="Output as JSON")
@click.pass_context
def test_coverage_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Test-coverage passthrough."""
    return _dispatch_command_passthrough("test-coverage")


@click.group(
    name="manage-migrations",
    help="Manage migration and DB lifecycle commands",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def manage_migrations_cmd(ctx: click.Context) -> int:
    """Manage-migrations command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for manage-migrations")
        click.echo("   Try: cdc manage-migrations generate [--service adopus] [--table Actor]")
        return 1
    return 0


@manage_migrations_cmd.command(
    name="generate",
    help="Generate PostgreSQL migration SQL files",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", default="adopus", help="Service name")
@click.option("--table", default=None, help="Filter by table name")
@click.option("--dry-run", is_flag=True, help="Preview without writing")
@click.option(
    "--topology",
    type=click.Choice(["redpanda", "fdw", "pg_native"]),
    default=None,
    help="User-facing topology selection",
)
@click.pass_context
def manage_migrations_generate_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations generate passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "generate")


@manage_migrations_cmd.command(
    name="schema-docs",
    help="Generate database schema documentation YAML files",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--env", shell_complete=complete_available_envs, help="Environment")
@click.pass_context
def manage_migrations_schema_docs_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations schema-docs passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "schema-docs")


@manage_migrations_cmd.command(
    name="diff",
    help="Compare schema definitions against generated migrations",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", default="adopus", help="Service name")
@click.option("--table", default=None, help="Filter by table name")
@click.pass_context
def manage_migrations_diff_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations diff passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "diff")


@manage_migrations_cmd.command(
    name="apply",
    help="Apply pending migrations to target PostgreSQL database",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--env",
    required=True,
    shell_complete=complete_migration_envs,
    help="Target environment",
)
@click.option("--dry-run", is_flag=True, help="Preview without applying")
@click.option("--sink", default=None, help="Filter by sink name")
@click.pass_context
def manage_migrations_apply_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations apply passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "apply")


@manage_migrations_cmd.command(
    name="status",
    help="Show applied vs pending migration status",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--env",
    required=False,
    shell_complete=complete_migration_envs,
    help="Target environment (required unless --offline)",
)
@click.option("--offline", is_flag=True, help="Offline mode (no DB)")
@click.option("--sink", default=None, help="Filter by sink name")
@click.pass_context
def manage_migrations_status_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations status passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "status")


@manage_migrations_cmd.command(
    name="enable-cdc",
    help="Enable CDC tracking on MSSQL source tables",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--env",
    required=True,
    shell_complete=complete_available_envs,
    help="MSSQL environment",
)
@click.option("--table", default=None, help="Filter by table name")
@click.option("--dry-run", is_flag=True, help="Preview without enabling")
@click.pass_context
def manage_migrations_enable_cdc_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations enable-cdc passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "enable-cdc")


@manage_migrations_cmd.command(
    name="clean-cdc",
    help="Clean old CDC change tracking data from MSSQL",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--env",
    required=True,
    shell_complete=complete_available_envs,
    help="MSSQL environment",
)
@click.option("--days", type=int, default=30, help="Clean entries older than N days")
@click.option("--table", default=None, help="Filter by table name")
@click.option("--dry-run", is_flag=True, help="Preview without cleaning")
@click.pass_context
def manage_migrations_clean_cdc_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations clean-cdc passthrough."""
    return _dispatch_grouped_passthrough("manage-migrations", "clean-cdc")
