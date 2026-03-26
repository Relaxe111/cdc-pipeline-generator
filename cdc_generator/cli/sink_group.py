"""Manage sink server group configuration file (sink-groups.yaml).

Facade module — delegates to extracted sink_group_* modules.
"""

from __future__ import annotations

import sys

from cdc_generator.cli.sink_group_common import (
    get_sink_file_path,
    get_source_group_file_path,
)
from cdc_generator.cli.sink_group_create import (
    handle_add_new_sink_group,
    handle_create,
)
from cdc_generator.cli.sink_group_info import (
    handle_info_command,
    handle_list,
)
from cdc_generator.cli.sink_group_inspect import (
    handle_inspect_command,
)
from cdc_generator.cli.sink_group_parser import (
    SinkGroupArgumentParser,
    build_parser,
)
from cdc_generator.cli.sink_group_patterns import (
    handle_add_source_custom_key_command,
    handle_add_to_ignore_list_command,
    handle_add_to_schema_excludes_command,
    handle_add_to_table_excludes_command,
    handle_list_table_excludes_command,
    handle_remove_sink_group_command,
)
from cdc_generator.cli.sink_group_server_ops import (
    handle_add_server_command,
    handle_list_server_extraction_patterns_command,
    handle_remove_server_command,
    handle_update_server_extraction_patterns_command,
)
from cdc_generator.cli.sink_group_update import (
    handle_db_definitions_command,
    handle_introspect_types_command,
    handle_update_command,
)
from cdc_generator.cli.sink_group_validate import (
    handle_validate_command,
)

__all__ = [
    "SinkGroupArgumentParser",
    "build_parser",
    "get_sink_file_path",
    "get_source_group_file_path",
    "handle_add_new_sink_group",
    "handle_add_server_command",
    "handle_add_source_custom_key_command",
    "handle_add_to_ignore_list_command",
    "handle_add_to_schema_excludes_command",
    "handle_add_to_table_excludes_command",
    "handle_create",
    "handle_db_definitions_command",
    "handle_info_command",
    "handle_inspect_command",
    "handle_introspect_types_command",
    "handle_list",
    "handle_list_server_extraction_patterns_command",
    "handle_list_table_excludes_command",
    "handle_remove_server_command",
    "handle_remove_sink_group_command",
    "handle_update_command",
    "handle_update_server_extraction_patterns_command",
    "handle_validate_command",
    "main",
]


def main() -> int:
    """CLI entry point for manage-sink-groups command."""
    parser = build_parser()
    args = parser.parse_args()

    if (
        isinstance(args.update, str)
        and args.update not in {"", "__AUTO__"}
        and not args.sink_group
    ):
        args.sink_group = args.update

    # Route to appropriate handler
    handlers = {
        "create": (args.create, lambda: handle_create(args)),
        "add_new_sink_group": (args.add_new_sink_group, lambda: handle_add_new_sink_group(args)),
        "list": (args.list, lambda: handle_list(args)),
        "info": (args.info, lambda: handle_info_command(args)),
        "inspect": (args.inspect, lambda: handle_inspect_command(args)),
        "update": (args.update is not None, lambda: handle_update_command(args)),
        "introspect_types": (args.introspect_types, lambda: handle_introspect_types_command(args)),
        "db_definitions": (args.db_definitions, lambda: handle_db_definitions_command(args)),
        "validate": (args.validate, lambda: handle_validate_command(args)),
        "add_to_ignore_list": (
            args.add_to_ignore_list,
            lambda: handle_add_to_ignore_list_command(args),
        ),
        "add_to_schema_excludes": (
            args.add_to_schema_excludes,
            lambda: handle_add_to_schema_excludes_command(args),
        ),
        "add_to_table_excludes": (
            args.add_to_table_excludes,
            lambda: handle_add_to_table_excludes_command(args),
        ),
        "list_table_excludes": (
            args.list_table_excludes,
            lambda: handle_list_table_excludes_command(args),
        ),
        "add_source_custom_key": (
            args.add_source_custom_key,
            lambda: handle_add_source_custom_key_command(args),
        ),
        "add_server": (args.add_server, lambda: handle_add_server_command(args)),
        "update_server_extraction_patterns": (
            bool(
                args.server
                and args.sink_group
                and args.extraction_patterns
                and not args.inspect
                and not args.add_server
                and not args.remove_server
            ),
            lambda: handle_update_server_extraction_patterns_command(args),
        ),
        "list_server_extraction_patterns": (
            args.list_server_extraction_patterns is not None,
            lambda: handle_list_server_extraction_patterns_command(args),
        ),
        "remove_server": (args.remove_server, lambda: handle_remove_server_command(args)),
        "remove": (args.remove, lambda: handle_remove_sink_group_command(args)),
    }

    for condition, handler in handlers.values():
        if condition:
            return handler()

    # No action specified
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
