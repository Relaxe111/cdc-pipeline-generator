#!/usr/bin/env python3
"""Shell autocompletion helpers for CDC CLI.

Provides dynamic completion data for Fish shell and other shells.
All extraction logic is centralized here for easy maintenance.

This module serves as the main CLI dispatcher for autocompletion queries.
"""

import sys
from collections.abc import Callable
from dataclasses import dataclass

from cdc_generator.helpers.autocompletions.column_template_completions import (
    list_column_template_keys,
    list_column_templates_for_table,
    list_transform_rule_keys,
    list_transforms_for_table,
)
from cdc_generator.helpers.autocompletions.scaffold import (
    scaffold_flag_completions,
)
from cdc_generator.helpers.autocompletions.schemas import (
    list_schemas_for_service,
)
from cdc_generator.helpers.autocompletions.server_groups import (
    list_databases_from_server_group,
    list_non_inherited_sink_group_names,
    list_server_group_names,
    list_servers_for_sink_group,
    list_servers_from_server_group,
    list_sink_group_names,
)
from cdc_generator.helpers.autocompletions.service_schemas import (
    list_custom_table_columns_for_mapping,
    list_custom_tables_for_schema_service,
    list_schema_services,
)
from cdc_generator.helpers.autocompletions.services import (
    list_available_services_from_server_group,
    list_existing_services,
)
from cdc_generator.helpers.autocompletions.sinks import (
    get_default_sink_for_service,
    list_available_sink_keys,
    list_custom_table_columns_for_autocomplete,
    list_custom_tables_for_service_sink,
    list_sink_keys_for_service,
    list_sink_tables_for_service,
    list_source_columns_for_sink_table,
    list_tables_for_sink_target,
    list_target_columns_for_sink_table,
    list_target_tables_for_sink,
)
from cdc_generator.helpers.autocompletions.tables import (
    list_columns_for_table,
    list_source_tables_for_service,
    list_tables_for_service,
)
from cdc_generator.helpers.autocompletions.types import list_pg_column_types

_MIN_ARGS_WITH_COMMAND = 2


# ---------------------------------------------------------------------------
# Command registration types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompletionCommand:
    """A single autocompletion command definition.

    Attributes:
        arg_count: Number of positional args required (after command).
        arg_desc: Description of required args (for error messages).
        handler: Function to call with positional args.
    """

    arg_count: int
    arg_desc: str
    handler: Callable[..., list[str] | str | None]


# ---------------------------------------------------------------------------
# No-arg commands (return list[str])
# ---------------------------------------------------------------------------

_NO_ARG_COMMANDS: dict[str, Callable[[], list[str]]] = {
    "--list-existing-services": list_existing_services,
    "--list-available-services": list_available_services_from_server_group,
    "--list-databases": list_databases_from_server_group,
    "--list-server-names": list_servers_from_server_group,
    "--list-server-group-names": list_server_group_names,
    "--list-sink-group-names": list_sink_group_names,
    "--list-non-inherited-sink-group-names": list_non_inherited_sink_group_names,
    "--list-available-sink-keys": list_available_sink_keys,
    "--list-pg-types": list_pg_column_types,
    "--list-column-templates": list_column_template_keys,
    "--list-transform-rules": list_transform_rule_keys,
    "--list-schema-services": list_schema_services,
}

# ---------------------------------------------------------------------------
# Commands with positional args
# ---------------------------------------------------------------------------

_ARG_COMMANDS: dict[str, CompletionCommand] = {
    "--list-sink-group-servers": CompletionCommand(
        arg_count=1,
        arg_desc="sink_group_name",
        handler=list_servers_for_sink_group,
    ),
    "--list-schemas": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=list_schemas_for_service,
    ),
    "--list-tables": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=list_tables_for_service,
    ),
    "--scaffold-flag-values": CompletionCommand(
        arg_count=1,
        arg_desc="flag_name",
        handler=scaffold_flag_completions,
    ),
    "--list-sink-keys": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=list_sink_keys_for_service,
    ),
    "--list-source-tables": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=list_source_tables_for_service,
    ),
    "--list-sink-target-tables": CompletionCommand(
        arg_count=1,
        arg_desc="sink_key",
        handler=list_tables_for_sink_target,
    ),
    "--get-default-sink": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=get_default_sink_for_service,
    ),
    "--list-target-tables": CompletionCommand(
        arg_count=2,
        arg_desc="service_name sink_key",
        handler=list_target_tables_for_sink,
    ),
    "--list-target-columns": CompletionCommand(
        arg_count=2,
        arg_desc="sink_key target_table",
        handler=list_target_columns_for_sink_table,
    ),
    "--list-custom-tables": CompletionCommand(
        arg_count=2,
        arg_desc="service_name sink_key",
        handler=list_custom_tables_for_service_sink,
    ),
    "--list-sink-tables": CompletionCommand(
        arg_count=2,
        arg_desc="service_name sink_key",
        handler=list_sink_tables_for_service,
    ),
    "--list-columns": CompletionCommand(
        arg_count=3,
        arg_desc="service_name schema table",
        handler=list_columns_for_table,
    ),
    "--list-custom-table-columns": CompletionCommand(
        arg_count=3,
        arg_desc="service_name sink_key table_key",
        handler=list_custom_table_columns_for_autocomplete,
    ),
    "--list-column-templates-on-table": CompletionCommand(
        arg_count=3,
        arg_desc="service_name sink_key table_key",
        handler=list_column_templates_for_table,
    ),
    "--list-table-transforms": CompletionCommand(
        arg_count=3,
        arg_desc="service_name sink_key table_key",
        handler=list_transforms_for_table,
    ),
    "--list-schema-custom-tables": CompletionCommand(
        arg_count=1,
        arg_desc="service_name",
        handler=list_custom_tables_for_schema_service,
    ),
    "--list-custom-table-columns-for-mapping": CompletionCommand(
        arg_count=2,
        arg_desc="service_name table_ref",
        handler=list_custom_table_columns_for_mapping,
    ),
    "--list-source-columns-for-sink-table": CompletionCommand(
        arg_count=3,
        arg_desc="service_name sink_key table_key",
        handler=list_source_columns_for_sink_table,
    ),
}


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------


def _dispatch_no_arg(command: str) -> int | None:
    """Handle commands that take no positional args.

    Returns:
        0 on success, None if command not found.
    """
    handler = _NO_ARG_COMMANDS.get(command)
    if handler is None:
        return None

    for item in handler():
        print(item)
    return 0


def _dispatch_arg_command(command: str) -> int | None:
    """Handle commands that require positional args.

    Returns:
        0 on success, 1 on missing args, None if command not found.
    """
    cmd = _ARG_COMMANDS.get(command)
    if cmd is None:
        return None

    required_argc = _MIN_ARGS_WITH_COMMAND + cmd.arg_count
    if len(sys.argv) < required_argc:
        print(
            f"Error: {command} requires {cmd.arg_desc}",
            file=sys.stderr,
        )
        return 1

    positional = sys.argv[2 : 2 + cmd.arg_count]
    result = cmd.handler(*positional)
    _print_result(result)
    return 0


def _print_result(result: list[str] | str | None) -> None:
    """Print handler result (list, single string, or None)."""
    if result is None:
        return
    if isinstance(result, str):
        print(result)
        return
    for item in result:
        print(item)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """CLI entry point for autocompletion queries.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    if len(sys.argv) < _MIN_ARGS_WITH_COMMAND:
        print(
            "Usage: python -m cdc_generator.helpers.autocompletions <command>",
            file=sys.stderr,
        )
        return 1

    command = sys.argv[1]

    result = _dispatch_no_arg(command)
    if result is not None:
        return result

    result = _dispatch_arg_command(command)
    if result is not None:
        return result

    print(f"Unknown command: {command}", file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
