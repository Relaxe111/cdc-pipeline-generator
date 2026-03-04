"""Click command definitions with option declarations for shell completion.

Each command is declared with its full set of Click options/arguments.
Options are declared for Click's shell completion system — the actual
argument parsing is still done by the existing argparse handlers.

The commands use ``allow_extra_args=True`` and ``ignore_unknown_options=True``
so Click doesn't reject args it doesn't know about (argparse handles that).

Smart completion branching
--------------------------
Commands that use ``SmartCommand`` (via ``cls=SmartCommand``) only show
options relevant to what the user has already typed.  For example, once
``--inspect`` is on the command line, only ``--schema``, ``--all``,
``--save``, and ``--env`` are offered — not the full 40+ option list.

The branching rules live in ``smart_command.py`` and are passed as
``smart_groups`` to each command.
"""

from __future__ import annotations

import sys

import click

from cdc_generator.cli.click_commands_column_templates import (
    execute_manage_column_templates,
)
from cdc_generator.cli.click_commands_pipeline_migrations import (
    manage_migrations_cmd,
    manage_pipelines_cmd,
    test_cmd,
    test_coverage_cmd,
)
from cdc_generator.cli.click_commands_resources import (
    execute_manage_services_resources,
    options_from_kwargs,
)
from cdc_generator.cli.click_commands_resources_inspect import (
    execute_manage_services_resources_inspect,
)
from cdc_generator.cli.click_commands_schema_resources import (
    execute_manage_services_schema_custom_tables,
    execute_manage_services_schema_transforms,
)
from cdc_generator.cli.click_commands_server_groups import (
    manage_sink_groups_cmd,
    manage_source_groups_cmd,
)
from cdc_generator.cli.click_commands_service_config import (
    manage_services_config_cmd,
)
from cdc_generator.cli.click_commands_source_overrides import (
    execute_source_overrides_group,
    execute_source_overrides_list,
    execute_source_overrides_remove,
    execute_source_overrides_set,
)
from cdc_generator.cli.completions import (
    complete_column_templates,
    complete_custom_table_column_spec,
    complete_existing_services,
    complete_pg_types,
    complete_remove_source_override,
    complete_schema_services,
    complete_schemas,
    complete_set_source_override,
    complete_sink_keys,
    complete_sink_tables,
    complete_source_override_type_for_ref,
    complete_track_tables,
    complete_transform_rules,
    complete_transforms_on_table,
)
from cdc_generator.cli.smart_command import (
    MANAGE_SCHEMA_CUSTOM_TABLES_ALWAYS,
    MANAGE_SCHEMA_CUSTOM_TABLES_GROUPS,
    MANAGE_SCHEMA_CUSTOM_TABLES_REQUIRES,
    SmartCommand,
)

# Shared context settings for all passthrough commands
_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}

_MIN_GROUPED_ARGS = 3
_GROUPED_COMMAND_INDEX = 1
_GROUPED_SUBCOMMAND_INDEX = 2
_GROUPED_EXTRA_ARGS_START = 3
_MIN_NESTED_SCHEMA_ARGS = 4
_NESTED_SCHEMA_SUBCOMMAND_INDEX = 3
_NESTED_SCHEMA_EXTRA_ARGS_START = 4
_INSPECT_ALL_SINKS = "__all_sinks__"


def _dispatch_grouped_passthrough(group: str, subcommand: str) -> int:
    """Dispatch to grouped argparse handlers using current CLI argv tail."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command(group, subcommand, sys.argv[3:])


def _dispatch_command_passthrough(command: str) -> int:
    """Dispatch to top-level argparse handlers using current CLI argv tail."""
    from cdc_generator.cli.commands import execute_command

    return execute_command(command, sys.argv[2:])


# ============================================================================

# ============================================================================
# manage-services resources custom-tables
# ============================================================================

@click.command(
    name="custom-tables",
    cls=SmartCommand,
    smart_groups=MANAGE_SCHEMA_CUSTOM_TABLES_GROUPS,
    smart_always=MANAGE_SCHEMA_CUSTOM_TABLES_ALWAYS,
    smart_requires=MANAGE_SCHEMA_CUSTOM_TABLES_REQUIRES,
    help="Manage custom table schema definitions (services/_schemas/)",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", shell_complete=complete_schema_services,
              help="Service name")
@click.option("--list-services", is_flag=True,
              help="List all services with schema directories")
@click.option("--list-custom-tables", is_flag=True,
              help="List custom tables for a service")
@click.option("--add-custom-table",
              help="Create custom table schema (schema.table)")
@click.option("--show-custom-table",
              help="Show custom table details")
@click.option("--remove-custom-table",
              help="Remove custom table schema")
@click.option("--column", multiple=True,
              shell_complete=complete_custom_table_column_spec,
              help="Column definition: name:type[:pk][:not_null]")
@click.pass_context
def manage_services_schema_custom_tables_cmd(
    _ctx: click.Context,
    **_kwargs: object,
) -> int:
    """manage-services resources custom-tables passthrough."""
    return execute_manage_services_schema_custom_tables()


@click.command(
    name="transforms",
    help="Manage sink table transforms",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name")
@click.option("--sink", shell_complete=complete_sink_keys,
              help="Sink key")
@click.option("--sink-table", shell_complete=complete_sink_tables,
              help="Sink table")
@click.option("--add-transform", shell_complete=complete_transform_rules,
              help="Add transform Bloblang ref")
@click.option("--remove-transform", shell_complete=complete_transforms_on_table,
              help="Remove transform Bloblang ref")
@click.option("--list-transforms", is_flag=True,
              help="List transforms on sink table")
@click.option("--skip-validation", is_flag=True,
              help="Skip database schema validation")
@click.option("--list-rules", is_flag=True,
              help="List available Bloblang transform refs")
@click.option("--list-transform-rules", is_flag=True,
              help="Alias for --list-rules")
def manage_services_schema_transforms_cmd(  # noqa: PLR0913
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
    """manage-services resources transforms passthrough."""
    return execute_manage_services_schema_transforms(
        service,
        sink,
        sink_table,
        add_transform,
        remove_transform,
        list_transforms,
        skip_validation,
        list_rules,
        list_transform_rules,
    )


@click.group(
    name="resources",
    help="Manage service resources",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name (alias for --source)")
@click.option("--source", shell_complete=complete_existing_services,
              help="Source service name (optional if only one exists)")
@click.option("--sink", shell_complete=complete_sink_keys,
              help="Sink key (optional if source has exactly one sink)")
@click.option("--track-table", multiple=True,
              shell_complete=complete_track_tables,
              help="Track whitelist table(s) as schema.table")
@click.option("--custom-tables", is_flag=True,
              help="Alias hint for resources custom-tables subcommand")
@click.option("--column-templates", is_flag=True,
              help="Alias hint for resources column-templates subcommand")
@click.option("--transforms", is_flag=True,
              help="Alias hint for resources transforms subcommand")
@click.option("--list-source-overrides", is_flag=True,
              help="List source type overrides")
@click.option("--set-source-override", "--set-source-ovveride",
              shell_complete=complete_set_source_override,
              help="Set source override as schema.table.column:type")
@click.option("--remove-source-override",
              shell_complete=complete_remove_source_override,
              help="Remove source override for schema.table.column")
@click.pass_context
def manage_services_resources_cmd(
    ctx: click.Context,
    **kwargs: object,
) -> int:
    """manage-services resources command group."""
    return execute_manage_services_resources(ctx, options_from_kwargs(kwargs))


@click.group(
    name="source-overrides",
    help="Manage source type overrides",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name")
@click.option("--source", shell_complete=complete_existing_services,
              help="Alias for --service")
@click.pass_context
def manage_services_source_overrides_cmd(
    ctx: click.Context,
    service: str | None,
    source: str | None,
) -> int:
    """manage-services resources source-overrides command group."""
    return execute_source_overrides_group(ctx, service, source)


@manage_services_source_overrides_cmd.command(
    name="set",
    help="Set source override for one column",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.argument("source_spec", shell_complete=complete_set_source_override)
@click.argument("override_type", required=False, shell_complete=complete_source_override_type_for_ref)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name")
@click.option("--source", shell_complete=complete_existing_services,
              help="Alias for --service")
@click.option("--reason", required=True,
              help="Required reason for applying this source type override")
@click.pass_context
def manage_services_source_overrides_set_cmd(
    ctx: click.Context,
    source_spec: str,
    override_type: str | None,
    service: str | None,
    source: str | None,
    reason: str,
) -> int:
    """Set a source type override via canonical subcommand."""
    return execute_source_overrides_set(
        ctx,
        source_spec,
        override_type,
        service,
        source,
        reason,
    )


@manage_services_source_overrides_cmd.command(
    name="remove",
    help="Remove source override for one column",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.argument("source_ref", shell_complete=complete_remove_source_override)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name")
@click.option("--source", shell_complete=complete_existing_services,
              help="Alias for --service")
@click.pass_context
def manage_services_source_overrides_remove_cmd(
    ctx: click.Context,
    source_ref: str,
    service: str | None,
    source: str | None,
) -> int:
    """Remove a source type override via canonical subcommand."""
    return execute_source_overrides_remove(
        ctx,
        source_ref,
        service,
        source,
    )


@manage_services_source_overrides_cmd.command(
    name="list",
    help="List configured source overrides",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", shell_complete=complete_existing_services,
              help="Service name")
@click.option("--source", shell_complete=complete_existing_services,
              help="Alias for --service")
@click.pass_context
def manage_services_source_overrides_list_cmd(
    ctx: click.Context,
    service: str | None,
    source: str | None,
) -> int:
    """List source type overrides via canonical subcommand."""
    return execute_source_overrides_list(ctx, service, source)


# ============================================================================
# manage-services
# ============================================================================

@click.group(
    name="manage-services",
    help="Manage service config and resources commands",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def manage_services_cmd(ctx: click.Context) -> int:
    """Manage-services command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for manage-services")
        click.echo("   Try: cdc manage-services config --service directory --list-source-tables")
        return 1
    return 0


manage_services_cmd.add_command(manage_services_config_cmd, name="config")


# ============================================================================
# manage-services resources column-templates
# ============================================================================

@click.command(
    name="column-templates",
    help="Manage column template definitions (column-templates.yaml)",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--list", "list_flag", is_flag=True,
              help="List all column template definitions")
@click.option("--show", shell_complete=complete_column_templates,
              help="Show template details")
@click.option("--add", help="Add new template definition")
@click.option("--edit", shell_complete=complete_column_templates,
              help="Edit existing template")
@click.option("--remove", shell_complete=complete_column_templates,
              help="Remove template definition")
@click.option("--name", help="Column name")
@click.option("--type", "col_type", shell_complete=complete_pg_types,
              help="PostgreSQL column type")
@click.option("--value", help="Bloblang expression or env var")
@click.option("--value-source", type=click.Choice(["bloblang", "source_ref", "sql", "env"]),
              help="Value generation mode")
@click.option("--description", help="Human-readable description")
@click.option("--not-null", is_flag=True, help="Mark column as NOT NULL")
@click.option("--nullable", is_flag=True, help="Mark column as nullable")
@click.option("--default", "sql_default", help="SQL default expression for DDL")
@click.option("--applies-to", multiple=True,
              help="Table glob pattern restriction")
@click.pass_context
def manage_column_templates_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-services resources column-templates passthrough."""
    return execute_manage_column_templates()


manage_services_resources_cmd.add_command(
    manage_services_schema_custom_tables_cmd,
    name="custom-tables",
)
manage_services_resources_cmd.add_command(
    manage_column_templates_cmd,
    name="column-templates",
)
manage_services_resources_cmd.add_command(
    manage_services_schema_transforms_cmd,
    name="transforms",
)
manage_services_resources_cmd.add_command(
    manage_services_source_overrides_cmd,
    name="source-overrides",
)


@click.command(
    name="inspect",
    help=(
        "Inspect source/sink database schemas and optionally "
        + "save resources under services/_schemas"
    ),
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--service", shell_complete=complete_existing_services,
              help="Existing service name")
@click.option("--inspect", is_flag=True,
              help="Inspect source database schema")
@click.option("--inspect-sink", "--sink-inspect",
              shell_complete=complete_sink_keys,
              is_flag=False,
              flag_value=_INSPECT_ALL_SINKS,
              default=None,
              metavar="SINK_KEY",
              help=(
                  "Inspect sink database schema for the selected service. "
                  + "Provide SINK_KEY for one sink, or use --inspect-sink --all "
                  + "to inspect all configured sinks."
              ))
@click.option("--schema", shell_complete=complete_schemas,
              help="Database schema to inspect or filter")
@click.option("--all", "--sink-all", "all_flag", is_flag=True,
              help="Process all schemas")
@click.option("--save", "--sink-save", is_flag=True,
              help="Save detailed table schemas to YAML")
@click.option("--track-table", multiple=True,
              shell_complete=complete_track_tables,
              help=(
                  "Add tracked whitelist table(s) as schema.table under "
                  + "services/_schemas/<service>/tracked-tables.yaml"
              ))
@click.option("--env", default="nonprod",
              help="Environment for inspection (nonprod/prod)")
def manage_services_resources_inspect_cmd(  # noqa: PLR0913
    service: str | None,
    inspect: bool,
    inspect_sink: str | None,
    schema: str | None,
    all_flag: bool,
    save: bool,
    track_table: tuple[str, ...],
    env: str,
) -> int:
    """manage-services resources inspect handler."""
    return execute_manage_services_resources_inspect(
        service,
        inspect,
        inspect_sink,
        schema,
        all_flag,
        save,
        track_table,
        env,
    )


manage_services_resources_cmd.add_command(
    manage_services_resources_inspect_cmd,
    name="inspect",
)
manage_services_cmd.add_command(manage_services_resources_cmd, name="resources")


# ============================================================================
# scaffold
# ============================================================================

@click.command(
    name="scaffold",
    help="Scaffold a new CDC pipeline project with server group configuration",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.argument("name", required=False, default=None)
@click.option("--update", is_flag=True,
              help="Update existing project scaffold")
@click.option("--pattern",
              type=click.Choice(["db-per-tenant", "db-shared"]),
              help="Server group pattern")
@click.option("--source-type",
              type=click.Choice(["postgres", "mssql"]),
              help="Source database type")
@click.option("--extraction-pattern",
              help="Regex pattern with named groups")
@click.option("--environment-aware", is_flag=True,
              help="Enable environment-aware grouping")
@click.option("--kafka-topology",
              type=click.Choice(["shared", "per-server"]),
              help="Kafka topology")
@click.option("--host", help="Database host")
@click.option("--port", help="Database port")
@click.option("--user", help="Database user")
@click.option("--password", help="Database password")
@click.pass_context
def scaffold_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Scaffold passthrough."""
    from cdc_generator.cli.commands import execute_command

    return execute_command("scaffold", sys.argv[2:])


# ============================================================================
# setup-local
# ============================================================================

@click.command(
    name="setup-local",
    help="Set up local development environment with on-demand services",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--postgres", is_flag=True,
              help="Start PostgreSQL and Adminer")
@click.option("--mssql", is_flag=True,
              help="Start MSSQL for local testing")
@click.option("--enable-streaming", is_flag=True,
              help="Start Redpanda and Console")
@click.option("--all", "all_flag", is_flag=True,
              help="Start all infrastructure services")
@click.option("--stop", is_flag=True,
              help="Stop all services")
@click.pass_context
def setup_local_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Setup-local passthrough."""
    from cdc_generator.cli.commands import execute_command

    return execute_command("setup-local", sys.argv[2:])


# ============================================================================
# Registry of all typed commands
# ============================================================================

CLICK_COMMANDS: dict[str, click.Command] = {
    "manage-services": manage_services_cmd,
    "manage-source-groups": manage_source_groups_cmd,
    "manage-sink-groups": manage_sink_groups_cmd,
    "manage-pipelines": manage_pipelines_cmd,
    "manage-migrations": manage_migrations_cmd,
    "scaffold": scaffold_cmd,
    "setup-local": setup_local_cmd,
    "test": test_cmd,
    "test-coverage": test_coverage_cmd,
}
