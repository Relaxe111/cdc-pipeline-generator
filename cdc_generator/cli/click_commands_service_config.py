"""Click command definition for `manage-services config` passthrough."""

from __future__ import annotations

import sys

import click

from cdc_generator.cli.completions import (
    complete_accept_column,
    complete_add_custom_sink_table,
    complete_add_sink_table,
    complete_available_services,
    complete_available_sink_keys,
    complete_available_tables,
    complete_available_validation_databases,
    complete_column_templates,
    complete_columns,
    complete_custom_table_columns,
    complete_custom_tables,
    complete_existing_services,
    complete_from_table,
    complete_include_sink_columns,
    complete_map_column,
    complete_remove_sink_table,
    complete_schemas,
    complete_service_positional,
    complete_target_sink_env,
    complete_sink_keys,
    complete_sink_tables,
    complete_source_tables,
    complete_track_tables,
    complete_target_schema,
    complete_target_tables,
    complete_templates_on_table,
    complete_transform_rules,
)
from cdc_generator.cli.smart_command import (
    MANAGE_SERVICE_ALWAYS,
    MANAGE_SERVICE_GROUPS,
    MANAGE_SERVICE_REQUIRES,
    SmartCommand,
)

_PASSTHROUGH_CTX: dict[str, object] = {
    "allow_extra_args": True,
    "ignore_unknown_options": True,
}

_MIN_GROUPED_ARGS = 3
_GROUPED_COMMAND_INDEX = 1
_GROUPED_EXTRA_ARGS_START = 3
_INSPECT_ALL_SINKS = "__all_sinks__"


@click.command(
    name="config",
    cls=SmartCommand,
    smart_groups=MANAGE_SERVICE_GROUPS,
    smart_always=MANAGE_SERVICE_ALWAYS,
    smart_requires=MANAGE_SERVICE_REQUIRES,
    help="Manage CDC service definitions",
    context_settings=_PASSTHROUGH_CTX,
    add_help_option=False,
)
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
@click.option("--list-source-tables", is_flag=True,
              help="List all source tables in service")
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
@click.option("--add-source-table", shell_complete=complete_available_tables,
              multiple=True,
              help="Add single table (schema.table)")
@click.option("--remove-table", shell_complete=complete_source_tables,
              help="Remove table from service")
@click.option("--source-table", shell_complete=complete_source_tables,
              help="Manage existing source table")
@click.option("--schema", shell_complete=complete_schemas,
              help="Database schema hint for source-table operations")
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
@click.option("--generate-validation", is_flag=True,
              help="Generate JSON Schema for validation")
@click.option("--validate-hierarchy", is_flag=True,
              help="Validate hierarchical inheritance")
@click.option("--validate-config", is_flag=True,
              help="Comprehensive configuration validation")
@click.option("--validate-bloblang", is_flag=True,
              help="Validate Bloblang syntax using rpk")
@click.option("--all", "--sink-all", "all_flag", is_flag=True,
              help="Process all sinks/tables where supported")
@click.option("--primary-key", help="Primary key column name")
@click.option("--ignore-columns", shell_complete=complete_columns,
              multiple=True,
              help="Column to ignore (schema.table.column)")
@click.option("--track-columns", shell_complete=complete_columns,
              multiple=True,
              help="Column to track (schema.table.column)")
@click.option("--add-sink", shell_complete=complete_available_sink_keys,
              help="Add sink destination (sink_group.target_service)")
@click.option("--target-sink-env", shell_complete=complete_target_sink_env,
              help=(
                  "Target sink environment key for env-aware sinks. "
                  + "Required when adding an env-aware sink to a non-env-aware source group."
              ))
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
              help="Source table reference (schema.table or all)")
@click.option("--replicate-structure", is_flag=True,
              help="Auto-generate sink table DDL from source schema")
@click.option("--sink-schema", shell_complete=complete_target_schema,
              help="Override sink table schema")
@click.option("--sink-table", shell_complete=complete_sink_tables,
              help="Target sink table for update operations")
@click.option("--update-schema", help="Update schema of sink table")
@click.option("--target-schema", shell_complete=complete_target_schema,
              help="Override target schema for cloned sink table")
@click.option("--map-column", multiple=True, shell_complete=complete_map_column,
              help="Map columns as target:source")
@click.option("--include-sink-columns",
              shell_complete=complete_include_sink_columns,
              help="Only sync these columns to sink")
@click.option("--accept-column", multiple=True,
              shell_complete=complete_accept_column,
              help="Allow required sink columns to remain unmapped")
@click.option("--list-sinks", is_flag=True,
              help="List all sink configurations")
@click.option("--validate-sinks", is_flag=True,
              help="Validate sink configuration")
@click.option("--add-column-template", shell_complete=complete_column_templates,
              help="Add column template to sink table")
@click.option("--remove-column-template",
              shell_complete=complete_templates_on_table,
              help="Remove column template from sink table")
@click.option("--list-column-templates", is_flag=True,
              help="List column templates on sink table")
@click.option("--column-name", help="Override column name for template")
@click.option("--value", help="Override column value for template")
@click.option("--add-transform", "--add-transfrom",
              shell_complete=complete_transform_rules,
              help="Add transform Bloblang ref to sink table")
@click.option("--skip-validation", is_flag=True,
              help="Skip database schema validation")
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
def manage_services_config_cmd(_ctx: click.Context, **_kwargs: object) -> int:
    """manage-services config passthrough."""
    from cdc_generator.cli.commands import execute_grouped_command

    if len(sys.argv) >= _MIN_GROUPED_ARGS and sys.argv[_GROUPED_COMMAND_INDEX] in {
        "manage-services",
        "ms",
    }:
        return execute_grouped_command(
            "manage-services",
            "config",
            sys.argv[_GROUPED_EXTRA_ARGS_START:],
        )

    return execute_grouped_command("manage-services", "config", sys.argv[2:])
