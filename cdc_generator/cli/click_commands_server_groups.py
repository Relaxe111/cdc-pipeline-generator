"""Click command definitions for source/sink group management passthrough."""

from __future__ import annotations

import sys

import click

from cdc_generator.cli.completions import (
    complete_available_envs,
    complete_available_validation_databases,
    complete_non_inherited_sink_group_names,
    complete_server_group_names,
    complete_server_names,
    complete_set_source_name_map,
    complete_set_target_sink_env,
    complete_sink_group_context_aware,
    complete_sink_group_names,
    complete_sink_group_servers,
)
from cdc_generator.cli.smart_command import (
    MANAGE_SINK_GROUPS_ALWAYS,
    MANAGE_SINK_GROUPS_GROUPS,
    MANAGE_SINK_GROUPS_REQUIRES,
    MANAGE_SOURCE_GROUPS_ALWAYS,
    MANAGE_SOURCE_GROUPS_GROUPS,
    MANAGE_SOURCE_GROUPS_REQUIRES,
    SmartCommand,
)

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}


@click.command(
    name="manage-source-groups",
    cls=SmartCommand,
    smart_groups=MANAGE_SOURCE_GROUPS_GROUPS,
    smart_always=MANAGE_SOURCE_GROUPS_ALWAYS,
    smart_requires=MANAGE_SOURCE_GROUPS_REQUIRES,
    help="Manage source groups configuration (source-groups.yaml)",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option(
    "--update", nargs=1, required=False, default=None, shell_complete=complete_server_names, help="Update server group from database inspection"
)
@click.option("--all", "all_flag", is_flag=True, help="Update all servers")
@click.option("--info", is_flag=True, help="Show detailed server group information")
@click.option("--list-env-services", is_flag=True, help="View environment-grouped services")
@click.option("--add-to-ignore-list", help="Add pattern to database exclude list")
@click.option("--list-ignore-patterns", is_flag=True, help="List current database exclude patterns")
@click.option("--add-to-schema-excludes", help="Add pattern to schema exclude list")
@click.option("--add-source-custom-key", help="Add/update source custom key name")
@click.option("--custom-key-value", help="SQL value/query for source custom key")
@click.option("--custom-key-exec-type", type=click.Choice(["sql"]), help="Execution type for source custom key")
@click.option("--list-schema-excludes", is_flag=True, help="List current schema exclude patterns")
@click.option("--add-to-table-includes", help="Add pattern to table include list")
@click.option("--list-table-includes", is_flag=True, help="List current table include patterns")
@click.option("--add-to-table-excludes", help="Add pattern to table exclude list")
@click.option("--list-table-excludes", is_flag=True, help="List current table exclude patterns")
@click.option("--add-env-mapping", help="Add env mapping 'from:to'")
@click.option("--list-env-mappings", is_flag=True, help="List current environment mappings")
@click.option(
    "--set-target-sink-env", nargs=3, shell_complete=complete_set_target_sink_env, help="Set target sink env: SOURCE SOURCE_ENV TARGET_SINK_ENV"
)
@click.option(
    "--set-source-name-map",
    nargs=2,
    shell_complete=complete_set_source_name_map,
    help="Set source_name_map override: DATABASE SOURCE_NAME",
)
@click.option(
    "--remove-source-name-map",
    shell_complete=complete_available_validation_databases,
    help="Remove source_name_map override by database name",
)
@click.option("--list-source-name-map", is_flag=True, help="List configured source_name_map overrides")
@click.option("--add-server", help="Add new server configuration")
@click.option("--list-servers", is_flag=True, help="List all configured servers")
@click.option("--remove-server", help="Remove a server configuration")
@click.option("--set-broker-topology", type=click.Choice(["shared", "per-server"]), help="Change broker topology")
@click.option(
    "--set-topology",
    type=click.Choice(["redpanda", "fdw", "pg_native"]),
    help=("Set the user-facing topology. MSSQL supports redpanda|fdw and PostgreSQL supports redpanda|pg_native."),
)
@click.option("--add-extraction-pattern", shell_complete=complete_server_names, help="Add extraction pattern: SERVER PATTERN")
@click.option("--set-extraction-pattern", shell_complete=complete_server_names, help="Set single extraction pattern: SERVER PATTERN")
@click.option("--list-extraction-patterns", is_flag=True, shell_complete=complete_server_names, help="List extraction patterns")
@click.option("--remove-extraction-pattern", help="Remove extraction pattern: SERVER INDEX")
@click.option("--env", help="Fixed environment for extraction pattern")
@click.option("--strip-suffixes", help="Comma-separated suffixes to strip")
@click.option("--description", help="Human-readable description for extraction pattern")
@click.option("--set-validation-env", shell_complete=complete_available_envs, help="Set validation environment")
@click.option("--list-envs", is_flag=True, help="List available environments")
@click.option("--introspect-types", is_flag=True, help="Introspect column types from source DB")
@click.option("--db-definitions", is_flag=True, help="Generate services/_schemas/_definitions type file once")
@click.option("--server", shell_complete=complete_server_names, help="Server for --introspect-types/--db-definitions")
@click.option("--pattern", type=click.Choice(["db-per-tenant", "db-shared"]), help="Server group pattern")
@click.option("--source-type", type=click.Choice(["postgres", "mssql"]), help="Source database type")
@click.option("--host", help="Database host")
@click.option("--port", help="Database port")
@click.option("--user", help="Database user")
@click.option("--password", help="Database password")
@click.option("--extraction-pattern", help="Regex pattern with named groups")
@click.option("--environment-aware", is_flag=True, help="Enable environment-aware grouping")
@click.option("--help", "help", is_flag=True, help="Show this help message and exit")
@click.pass_context
def manage_source_groups_cmd(_ctx: click.Context, help: bool = False, **_kwargs: object) -> int:
    """Manage-source-groups passthrough."""
    if help:
        click.echo(_ctx.get_help())
        return 0
    from cdc_generator.cli.commands import execute_command

    return execute_command("manage-source-groups", sys.argv[2:])


@click.command(
    name="manage-sink-groups",
    cls=SmartCommand,
    smart_groups=MANAGE_SINK_GROUPS_GROUPS,
    smart_always=MANAGE_SINK_GROUPS_ALWAYS,
    smart_requires=MANAGE_SINK_GROUPS_REQUIRES,
    help="Manage sink groups configuration (sink-groups.yaml)",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.argument(
    "sink_group_positional",
    required=False,
    shell_complete=complete_sink_group_names,
)
@click.option("--create", is_flag=True, help="Create sink groups")
@click.option("--source-group", shell_complete=complete_server_group_names, help="Source group to inherit from")
@click.option("--add-new-sink-group", help=("Add new standalone sink group (auto-prefix sink_; defaults: type=postgres, pattern=db-shared)"))
@click.option("--type", type=click.Choice(["postgres", "mssql", "http_client", "http_server"]), help="Type of sink")
@click.option("--pattern", type=click.Choice(["db-shared", "db-per-tenant"]), help="Pattern for sink group")
@click.option("--environment-aware", is_flag=True, help="Enable environment-aware grouping")
@click.option("--no-environment-aware", is_flag=True, help="Disable environment-aware grouping")
@click.option(
    "--for-source-group",
    shell_complete=complete_server_group_names,
    help=("Source group this standalone sink consumes from (if omitted, first source group from source-groups.yaml is used)"),
)
@click.option("--list", "list_flag", is_flag=True, help="List all sink groups")
@click.option("--info", shell_complete=complete_sink_group_names, help="Show detailed information about a sink group")
@click.option("--inspect", is_flag=True, help="Inspect databases on sink server")
@click.option("--update", is_flag=True, help="Inspect and update sink group sources")
@click.option("--server", shell_complete=complete_sink_group_servers, help="Server name to inspect or update with --extraction-patterns")
@click.option("--include-pattern", help="Only include databases matching regex")
@click.option("--introspect-types", is_flag=True, help="Introspect column types from database server")
@click.option("--db-definitions", is_flag=True, help="Generate services/_schemas/_definitions type file once")
@click.option("--validate", is_flag=True, help="Validate sink group configuration")
@click.option("--add-to-include-list", help="Add pattern to database include list")
@click.option("--set-include-list", help="Replace database include list")
@click.option("--add-to-ignore-list", help="Add pattern to database exclude list")
@click.option("--add-to-schema-excludes", help="Add pattern to schema exclude list")
@click.option("--add-to-table-excludes", help="Add pattern to table exclude list")
@click.option("--list-table-excludes", is_flag=True, help="List current table exclude patterns")
@click.option("--add-source-custom-key", help="Add/update source custom key name")
@click.option("--custom-key-value", help="SQL value/query for source custom key")
@click.option("--custom-key-exec-type", type=click.Choice(["sql"]), help="Execution type for source custom key")
@click.option("--sink-group", shell_complete=complete_sink_group_context_aware, help="Sink group to operate on")
@click.option("--add-server", help="Add a server to a sink group")
@click.option("--remove-server", shell_complete=complete_sink_group_servers, help="Remove a server from a sink group")
@click.option("--host", help="Server host")
@click.option("--port", help="Server port")
@click.option("--user", help="Server user")
@click.option("--password", help="Server password")
@click.option("--extraction-patterns", help="Regex extraction patterns for server (with --add-server or --server update)")
@click.option("--env", help="Fixed environment for extraction patterns")
@click.option("--strip-patterns", help="Comma-separated regex patterns to strip from service name")
@click.option("--env-mapping", multiple=True, help="Environment mapping from:to (repeatable)")
@click.option("--description", help="Description for extraction pattern entries")
@click.option(
    "--list-server-extraction-patterns",
    shell_complete=complete_sink_group_servers,
    flag_value="",
    default=None,
    help="List sink server extraction patterns (optional server filter)",
)
@click.option("--remove", shell_complete=complete_non_inherited_sink_group_names, help="Remove a sink group")
@click.option("--help", "help", is_flag=True, help="Show this help message and exit")
@click.pass_context
def manage_sink_groups_cmd(_ctx: click.Context, help: bool = False, **_kwargs: object) -> int:
    """Manage-sink-groups passthrough."""
    if help:
        click.echo(_ctx.get_help())
        return 0
    from cdc_generator.cli.commands import execute_command

    return execute_command("manage-sink-groups", sys.argv[2:])
