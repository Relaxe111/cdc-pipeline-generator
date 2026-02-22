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

import argparse
import sys

import click

from cdc_generator.cli.completions import (
    complete_add_custom_sink_table,
    complete_add_sink_table,
    complete_available_envs,
    complete_available_services,
    complete_available_sink_keys,
    complete_available_tables,
    complete_available_validation_databases,
    complete_column_templates,
    complete_columns,
    complete_custom_table_column_spec,
    complete_custom_table_columns,
    complete_custom_tables,
    complete_existing_services,
    complete_from_table,
    complete_include_sink_columns,
    complete_map_column,
    complete_non_inherited_sink_group_names,
    complete_pg_types,
    complete_remove_sink_table,
    complete_schema_services,
    complete_schemas,
    complete_server_group_names,
    complete_server_names,
    complete_service_positional,
    complete_sink_group_context_aware,
    complete_sink_group_names,
    complete_sink_group_servers,
    complete_sink_keys,
    complete_sink_tables,
    complete_source_tables,
    complete_target_schema,
    complete_target_tables,
    complete_templates_on_table,
    complete_transform_rules,
    complete_transforms_on_table,
)
from cdc_generator.cli.smart_command import (
    MANAGE_SCHEMA_CUSTOM_TABLES_ALWAYS,
    MANAGE_SCHEMA_CUSTOM_TABLES_GROUPS,
    MANAGE_SCHEMA_CUSTOM_TABLES_REQUIRES,
    MANAGE_SERVICE_ALWAYS,
    MANAGE_SERVICE_GROUPS,
    MANAGE_SERVICE_REQUIRES,
    MANAGE_SINK_GROUPS_ALWAYS,
    MANAGE_SINK_GROUPS_GROUPS,
    MANAGE_SINK_GROUPS_REQUIRES,
    MANAGE_SOURCE_GROUPS_ALWAYS,
    MANAGE_SOURCE_GROUPS_GROUPS,
    MANAGE_SOURCE_GROUPS_REQUIRES,
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


# ============================================================================

@click.command(
    name="manage-service",
    cls=SmartCommand,
    smart_groups=MANAGE_SERVICE_GROUPS,
    smart_always=MANAGE_SERVICE_ALWAYS,
    smart_requires=MANAGE_SERVICE_REQUIRES,
    help="Manage CDC service definitions",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
# -- Core --
@click.argument("service_positional", required=False, default=None,
                shell_complete=complete_service_positional)
@click.option("--service", shell_complete=complete_existing_services,
              help="Existing service name")
@click.option("--create-service", shell_complete=complete_available_services,
              help="Create service from source-groups.yaml")
@click.option("--add-validation-database",
              shell_complete=complete_available_validation_databases,
              help="Override validation database for create-service")
@click.option("--remove-service", shell_complete=complete_existing_services,
              help="Remove service and related local configuration")
@click.option("--server", help="Server name for multi-server setups")
@click.option("--list-services", is_flag=True,
              help="List all services from services/*.yaml")
# -- Source table management --
@click.option("--list-source-tables", is_flag=True,
              help="List all source tables in service")
@click.option("--add-source-table", shell_complete=complete_available_tables,
              multiple=True,
              help="Add single table (schema.table)")
@click.option("--remove-table", shell_complete=complete_source_tables,
              help="Remove table from service")
@click.option("--source-table", shell_complete=complete_source_tables,
              help="Manage existing source table")
# -- Inspect / validation --
@click.option("--inspect", is_flag=True, help="Inspect database schema")
@click.option("--inspect-sink", "--sink-inspect", is_flag=True,
              help="Inspect sink database schema (use with --all for all sinks)")
@click.option("--schema", shell_complete=complete_schemas,
              help="Database schema to inspect or filter")
@click.option("--save", "--sink-save", is_flag=True,
              help="Save detailed table schemas to YAML")
@click.option("--generate-validation", is_flag=True,
              help="Generate JSON Schema for validation")
@click.option("--validate-hierarchy", is_flag=True,
              help="Validate hierarchical inheritance")
@click.option("--validate-config", is_flag=True,
              help="Comprehensive configuration validation")
@click.option("--validate-bloblang", is_flag=True,
              help="Validate Bloblang syntax using rpk")
@click.option("--all", "--sink-all", "all_flag", is_flag=True,
              help="Process all schemas")
@click.option("--env", help="Environment (nonprod/prod)")
@click.option("--primary-key", help="Primary key column name")
@click.option("--ignore-columns", shell_complete=complete_columns,
              multiple=True,
              help="Column to ignore (schema.table.column)")
@click.option("--track-columns", shell_complete=complete_columns,
              multiple=True,
              help="Column to track (schema.table.column)")
# -- Sink management --
@click.option("--add-sink", shell_complete=complete_available_sink_keys,
              help="Add sink destination (sink_group.target_service)")
@click.option("--remove-sink", shell_complete=complete_sink_keys,
              help="Remove sink destination")
@click.option("--sink", shell_complete=complete_sink_keys,
              help="Target sink for table operations")
@click.option("--add-sink-table", shell_complete=complete_add_sink_table,
              is_flag=False,
              flag_value="",
              help="Add table to sink (schema.table)")
@click.option("--remove-sink-table", shell_complete=complete_remove_sink_table,
              help="Remove table from sink")
@click.option("--target", shell_complete=complete_target_tables,
              help="Target table for mapping (schema.table)")
@click.option("--target-exists",
              type=click.Choice(["true", "false"]),
              help="Table exists in target?")
@click.option("--from", "from_table", shell_complete=complete_from_table,
              help="Source table reference")
@click.option("--replicate-structure", is_flag=True,
              help="Auto-generate sink table DDL from source schema")
@click.option("--sink-schema", shell_complete=complete_target_schema,
              help="Override sink table schema")
@click.option("--sink-table", shell_complete=complete_sink_tables,
              help="Target sink table for update operations")
@click.option("--update-schema", help="Update schema of sink table")
@click.option("--target-schema", shell_complete=complete_target_schema,
              help="Override target schema for cloned sink table")
@click.option("--map-column", nargs=2, shell_complete=complete_map_column,
              help="Map source column to target column")
@click.option("--include-sink-columns",
              shell_complete=complete_include_sink_columns,
              help="Only sync these columns to sink")
@click.option("--list-sinks", is_flag=True,
              help="List all sink configurations")
@click.option("--validate-sinks", is_flag=True,
              help="Validate sink configuration")
# -- Column templates & transforms --
@click.option("--add-column-template", shell_complete=complete_column_templates,
              help="Add column template to sink table")
@click.option("--remove-column-template",
              shell_complete=complete_templates_on_table,
              help="Remove column template from sink table")
@click.option("--list-column-templates", is_flag=True,
              help="List column templates on sink table")
@click.option("--column-name", help="Override column name for template")
@click.option("--value", help="Override column value for template")
@click.option("--add-transform", shell_complete=complete_transform_rules,
              help="Add transform rule to sink table")
@click.option("--remove-transform",
              shell_complete=complete_transforms_on_table,
              help="Remove transform rule from sink table")
@click.option("--list-transforms", is_flag=True,
              help="List transforms on sink table")
@click.option("--skip-validation", is_flag=True,
              help="Skip database schema validation")
# -- Custom sink table --
@click.option("--add-custom-sink-table",
              shell_complete=complete_add_custom_sink_table,
              help="Create custom table in sink (schema.table)")
@click.option("--column", multiple=True,
              help="Column def: name:type[:pk][:not_null][:default_X]")
@click.option("--modify-custom-table", shell_complete=complete_custom_tables,
              help="Modify custom table columns")
@click.option("--add-column", help="Add column to custom table")
@click.option("--remove-column", shell_complete=complete_custom_table_columns,
              help="Remove column from custom table")
@click.pass_context
def manage_service_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Manage-service passthrough (legacy alias for manage-services config)."""
    from cdc_generator.cli.commands import execute_grouped_command

    if (
        len(sys.argv) >= _MIN_GROUPED_ARGS
        and sys.argv[_GROUPED_COMMAND_INDEX] == "manage-services"
        and sys.argv[_GROUPED_SUBCOMMAND_INDEX] == "config"
    ):
        return execute_grouped_command(
            "manage-services", "config", sys.argv[_GROUPED_EXTRA_ARGS_START:]
        )

    return execute_grouped_command("manage-services", "config", sys.argv[2:])


# ============================================================================
# manage-source-groups
# ============================================================================

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
# -- General --
@click.option("--update", nargs=1, required=False, default=None,
              shell_complete=complete_server_names,
              help="Update server group from database inspection")
@click.option("--all", "all_flag", is_flag=True,
              help="Update all servers")
@click.option("--info", is_flag=True,
              help="Show detailed server group information")
@click.option("--list-env-services", is_flag=True,
              help="View environment-grouped services")
# -- Exclude patterns --
@click.option("--add-to-ignore-list",
              help="Add pattern to database exclude list")
@click.option("--list-ignore-patterns", is_flag=True,
              help="List current database exclude patterns")
@click.option("--add-to-schema-excludes",
              help="Add pattern to schema exclude list")
@click.option("--add-source-custom-key",
              help="Add/update source custom key name")
@click.option("--custom-key-value",
              help="SQL value/query for source custom key")
@click.option("--custom-key-exec-type",
              type=click.Choice(["sql"]),
              help="Execution type for source custom key")
@click.option("--list-schema-excludes", is_flag=True,
              help="List current schema exclude patterns")
# -- Environment mappings --
@click.option("--add-env-mapping",
              help="Add env mapping 'from:to'")
@click.option("--list-env-mappings", is_flag=True,
              help="List current environment mappings")
# -- Multi-server management --
@click.option("--add-server",
              help="Add new server configuration")
@click.option("--list-servers", is_flag=True,
              help="List all configured servers")
@click.option("--remove-server",
              help="Remove a server configuration")
@click.option("--set-kafka-topology",
              type=click.Choice(["shared", "per-server"]),
              help="Change Kafka topology")
# -- Extraction patterns --
@click.option("--add-extraction-pattern",
              shell_complete=complete_server_names,
              help="Add extraction pattern: SERVER PATTERN")
@click.option("--set-extraction-pattern",
              shell_complete=complete_server_names,
              help="Set single extraction pattern: SERVER PATTERN")
@click.option("--list-extraction-patterns",
              is_flag=True,
              shell_complete=complete_server_names,
              help="List extraction patterns")
@click.option("--remove-extraction-pattern",
              help="Remove extraction pattern: SERVER INDEX")
@click.option("--env", help="Fixed environment for extraction pattern")
@click.option("--strip-suffixes",
              help="Comma-separated suffixes to strip")
@click.option("--description",
              help="Human-readable description for extraction pattern")
# -- Validation environment --
@click.option("--set-validation-env",
              shell_complete=complete_available_envs,
              help="Set validation environment")
@click.option("--list-envs", is_flag=True,
              help="List available environments")
# -- Type introspection --
@click.option("--introspect-types", is_flag=True,
              help="Introspect column types from source DB")
@click.option("--db-definitions", is_flag=True,
              help="Generate services/_schemas/_definitions type file once")
@click.option("--server", shell_complete=complete_server_names,
              help="Server for --introspect-types/--db-definitions")
# -- Creation flags --
@click.option("--pattern",
              type=click.Choice(["db-per-tenant", "db-shared"]),
              help="Server group pattern")
@click.option("--source-type",
              type=click.Choice(["postgres", "mssql"]),
              help="Source database type")
@click.option("--host", help="Database host")
@click.option("--port", help="Database port")
@click.option("--user", help="Database user")
@click.option("--password", help="Database password")
@click.option("--extraction-pattern",
              help="Regex pattern with named groups")
@click.option("--environment-aware", is_flag=True,
              help="Enable environment-aware grouping")
@click.pass_context
def manage_source_groups_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Manage-source-groups passthrough."""
    from cdc_generator.cli.commands import execute_command

    return execute_command("manage-source-groups", sys.argv[2:])


# ============================================================================
# manage-sink-groups
# ============================================================================

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
# -- Create --
@click.option("--create", is_flag=True,
              help="Create sink groups")
@click.option("--source-group", shell_complete=complete_server_group_names,
              help="Source group to inherit from")
@click.option("--add-new-sink-group",
              help=(
                  "Add new standalone sink group "
                  "(auto-prefix sink_; defaults: type=postgres, pattern=db-shared)"
              ))
@click.option("--type",
              type=click.Choice(["postgres", "mssql", "http_client", "http_server"]),
              help="Type of sink")
@click.option("--pattern",
              type=click.Choice(["db-shared", "db-per-tenant"]),
              help="Pattern for sink group")
@click.option("--environment-aware", is_flag=True,
              help="Enable environment-aware grouping")
@click.option("--no-environment-aware", is_flag=True,
              help="Disable environment-aware grouping")
@click.option("--for-source-group", shell_complete=complete_server_group_names,
              help=(
                  "Source group this standalone sink consumes from "
                  "(if omitted, first source group from source-groups.yaml is used)"
              ))
# -- List / Info --
@click.option("--list", "list_flag", is_flag=True,
              help="List all sink groups")
@click.option("--info", shell_complete=complete_sink_group_names,
              help="Show detailed information about a sink group")
# -- Inspect / Validate --
@click.option("--inspect", is_flag=True,
              help="Inspect databases on sink server")
@click.option("--update", is_flag=True,
              help="Inspect and update sink group sources")
@click.option("--server",
              shell_complete=complete_sink_group_servers,
              help="Server name to inspect or update with --extraction-patterns")
@click.option("--include-pattern",
              help="Only include databases matching regex")
@click.option("--introspect-types", is_flag=True,
              help="Introspect column types from database server")
@click.option("--db-definitions", is_flag=True,
              help="Generate services/_schemas/_definitions type file once")
@click.option("--validate", is_flag=True,
              help="Validate sink group configuration")
@click.option("--add-to-ignore-list",
              help="Add pattern to database exclude list")
@click.option("--add-to-schema-excludes",
              help="Add pattern to schema exclude list")
@click.option("--add-source-custom-key",
              help="Add/update source custom key name")
@click.option("--custom-key-value",
              help="SQL value/query for source custom key")
@click.option("--custom-key-exec-type",
              type=click.Choice(["sql"]),
              help="Execution type for source custom key")
# -- Server management --
@click.option("--sink-group",
              shell_complete=complete_sink_group_context_aware,
              help="Sink group to operate on")
@click.option("--add-server",
              help="Add a server to a sink group")
@click.option("--remove-server",
              shell_complete=complete_sink_group_servers,
              help="Remove a server from a sink group")
@click.option("--host", help="Server host")
@click.option("--port", help="Server port")
@click.option("--user", help="Server user")
@click.option("--password", help="Server password")
@click.option("--extraction-patterns",
              help="Regex extraction patterns for server (with --add-server or --server update)")
@click.option("--env",
              help="Fixed environment for extraction patterns")
@click.option("--strip-patterns",
              help="Comma-separated regex patterns to strip from service name")
@click.option("--env-mapping",
              multiple=True,
              help="Environment mapping from:to (repeatable)")
@click.option("--description",
              help="Description for extraction pattern entries")
@click.option("--list-server-extraction-patterns",
              shell_complete=complete_sink_group_servers,
              flag_value="",
              default=None,
              help="List sink server extraction patterns (optional server filter)")
# -- Removal --
@click.option("--remove",
              shell_complete=complete_non_inherited_sink_group_names,
              help="Remove a sink group")
@click.pass_context
def manage_sink_groups_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """Manage-sink-groups passthrough."""
    from cdc_generator.cli.commands import execute_command

    return execute_command("manage-sink-groups", sys.argv[2:])


# ============================================================================
# manage-services schema custom-tables
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
    """manage-services schema custom-tables passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    try:
        subcommand_index = sys.argv.index("custom-tables")
    except ValueError:
        subcommand_index = _NESTED_SCHEMA_SUBCOMMAND_INDEX

    return execute_grouped_command(
        "manage-services",
        "schema",
        sys.argv[subcommand_index + 1:],
    )


@click.command(
    name="transforms",
    help="Manage global transform rule catalog",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--list-rules", is_flag=True,
              help="List all available transform rules")
@click.option("--list-transform-rules", is_flag=True,
              help="Alias for --list-rules")
def manage_services_schema_transforms_cmd(
    list_rules: bool,
    list_transform_rules: bool,
) -> int:
    """manage-services schema transforms passthrough."""
    from cdc_generator.cli.service_handlers_templates import (
        handle_list_transform_rules,
    )

    if not list_rules and not list_transform_rules:
        click.echo("❌ Missing action for manage-services schema transforms")
        click.echo("   Try: cdc manage-services schema transforms --list-rules")
        return 1

    return handle_list_transform_rules(argparse.Namespace())


@click.group(
    name="schema",
    help="Manage service schema resources",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def manage_services_schema_cmd(ctx: click.Context) -> int:
    """manage-services schema command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for manage-services schema")
        click.echo("   Try: cdc manage-services schema custom-tables --list-services")
        click.echo("        cdc manage-services schema column-templates --list")
        click.echo("        cdc manage-services schema transforms --list-rules")
        return 1

    return 0


# ============================================================================
# manage-services
# ============================================================================

@click.group(
    name="manage-services",
    help="Manage service config and schema commands",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
    invoke_without_command=True,
)
@click.pass_context
def manage_services_cmd(ctx: click.Context) -> int:
    """Manage-services command group."""
    if ctx.invoked_subcommand is None:
        click.echo("❌ Missing subcommand for manage-services")
        click.echo("   Try: cdc manage-services config --service directory --inspect --all")
        return 1
    return 0


manage_services_cmd.add_command(manage_service_cmd, name="config")


# ============================================================================
# manage-services schema column-templates
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
    """manage-services schema column-templates passthrough."""
    from cdc_generator.cli.commands import (
        detect_environment,
        get_script_paths,
        run_generator_spec,
    )

    try:
        start_index = sys.argv.index("column-templates") + 1
    except ValueError:
        start_index = 2

    workspace_root, _implementation_name, is_dev_container = detect_environment()
    paths = get_script_paths(workspace_root, is_dev_container)
    cmd_info = {
        "runner": "generator",
        "module": "cdc_generator.cli.column_templates",
        "script": "cli/column_templates.py",
    }
    return run_generator_spec(
        "manage-services schema column-templates",
        cmd_info,
        paths,
        sys.argv[start_index:],
        workspace_root,
    )


manage_services_schema_cmd.add_command(
    manage_services_schema_custom_tables_cmd,
    name="custom-tables",
)
manage_services_schema_cmd.add_command(
    manage_column_templates_cmd,
    name="column-templates",
)
manage_services_schema_cmd.add_command(
    manage_services_schema_transforms_cmd,
    name="transforms",
)
manage_services_cmd.add_command(manage_services_schema_cmd, name="schema")


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
# manage-pipelines
# ============================================================================

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
    help="Generate Redpanda Connect pipelines",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--all", "all_flag", is_flag=True,
              help="Generate for all customers")
@click.option("--force", is_flag=True, help="Force regeneration")
@click.pass_context
def manage_pipelines_generate_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines generate passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-pipelines", "generate", sys.argv[3:])


@manage_pipelines_cmd.command(
    name="reload",
    help="Regenerate and reload Redpanda Connect pipelines",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.pass_context
def manage_pipelines_reload_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines reload passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-pipelines", "reload", sys.argv[3:])


@manage_pipelines_cmd.command(
    name="verify",
    help="Verify pipeline connections",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.pass_context
def manage_pipelines_verify_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines verify passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-pipelines", "verify", sys.argv[3:])


# ============================================================================
# test
# ============================================================================

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
    from cdc_generator.cli.commands import execute_command

    return execute_command("test", sys.argv[2:])


# ============================================================================
# test-coverage
# ============================================================================

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
    from cdc_generator.cli.commands import execute_command

    return execute_command("test-coverage", sys.argv[2:])


# ============================================================================
# manage-pipelines verify-sync
# ============================================================================

@manage_pipelines_cmd.command(
    name="verify-sync",
    help="Verify CDC synchronization and detect gaps",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--customer", help="Specify customer")
@click.option("--service", help="Specify service")
@click.option("--table", help="Specify table")
@click.option("--all", "all_flag", is_flag=True, help="Check all tables")
@click.pass_context
def manage_pipelines_verify_sync_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-pipelines verify-sync passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-pipelines", "verify-sync", sys.argv[3:])


# ============================================================================
# manage-pipelines stress-test
# ============================================================================

@manage_pipelines_cmd.command(
    name="stress-test",
    help="CDC stress test with real-time monitoring",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--customer", help="Specify customer")
@click.option("--service", help="Specify service")
@click.option("--duration", help="Test duration in seconds")
@click.option("--interval", help="Monitoring interval in seconds")
@click.pass_context
def manage_pipelines_stress_test_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-pipelines stress-test passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-pipelines", "stress-test", sys.argv[3:])


# ============================================================================
# manage-migrations
# ============================================================================

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
        click.echo("   Try: cdc manage-migrations apply-replica CUSTOMER --env nonprod")
        return 1
    return 0


@manage_migrations_cmd.command(
    name="enable-cdc",
    help="Enable CDC on MSSQL tables",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.pass_context
def manage_migrations_enable_cdc_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations enable-cdc passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-migrations", "enable-cdc", sys.argv[3:])


@manage_migrations_cmd.command(
    name="apply-replica",
    help="Apply PostgreSQL migrations to replica databases",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--env", help="Environment")
@click.pass_context
def manage_migrations_apply_replica_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations apply-replica passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-migrations", "apply-replica", sys.argv[3:])


@manage_migrations_cmd.command(
    name="clean-cdc",
    help="Clean CDC change tracking tables",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--env", help="Environment")
@click.option("--table", help="Table name")
@click.option("--all", "all_flag", is_flag=True, help="Process all tables")
@click.pass_context
def manage_migrations_clean_cdc_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations clean-cdc passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-migrations", "clean-cdc", sys.argv[3:])


@manage_migrations_cmd.command(
    name="schema-docs",
    help="Generate database schema documentation YAML files",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
@click.option("--env", help="Environment")
@click.pass_context
def manage_migrations_schema_docs_cmd(
    _ctx: click.Context, **_kwargs: object,
) -> int:
    """manage-migrations schema-docs passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    return execute_grouped_command("manage-migrations", "schema-docs", sys.argv[3:])


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
