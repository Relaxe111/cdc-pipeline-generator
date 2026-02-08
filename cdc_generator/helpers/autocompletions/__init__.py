#!/usr/bin/env python3
"""Shell autocompletion helpers for CDC CLI.

Provides dynamic completion data for Fish shell and other shells.
All extraction logic is centralized here for easy maintenance.

This module serves as the main CLI dispatcher for autocompletion queries.
"""

import sys

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


def main() -> int:
    """CLI entry point for autocompletion queries.

    Returns:
        Exit code (0 for success, 1 for error).
    """
    if len(sys.argv) < 2:
        print(
            "Usage: python -m cdc_generator.helpers.autocompletions <command>",
            file=sys.stderr,
        )
        return 1

    command = sys.argv[1]

    if command == '--list-existing-services':
        services = list_existing_services()
        for service in services:
            print(service)

    elif command == '--list-available-services':
        services = list_available_services_from_server_group()
        for service in services:
            print(service)

    elif command == '--list-databases':
        databases = list_databases_from_server_group()
        for db in databases:
            print(db)

    elif command == '--list-server-names':
        servers = list_servers_from_server_group()
        for server in servers:
            print(server)

    elif command == '--list-server-group-names':
        groups = list_server_group_names()
        for group in groups:
            print(group)

    elif command == '--list-sink-group-names':
        sink_groups = list_sink_group_names()
        for group in sink_groups:
            print(group)

    elif command == '--list-non-inherited-sink-group-names':
        sink_groups = list_non_inherited_sink_group_names()
        for group in sink_groups:
            print(group)

    elif command == '--list-sink-group-servers':
        if len(sys.argv) < 3:
            print(
                "Error: --list-sink-group-servers requires sink group name",
                file=sys.stderr,
            )
            return 1
        sink_group_name = sys.argv[2]
        servers = list_servers_for_sink_group(sink_group_name)
        for server in servers:
            print(server)

    elif command == '--list-schemas':
        if len(sys.argv) < 3:
            print(
                "Error: --list-schemas requires service name",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        schemas = list_schemas_for_service(service_name)
        for schema in schemas:
            print(schema)

    elif command == '--list-tables':
        if len(sys.argv) < 3:
            print(
                "Error: --list-tables requires service name",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        tables = list_tables_for_service(service_name)
        for table in tables:
            print(table)

    elif command == '--list-columns':
        if len(sys.argv) < 5:
            print(
                "Error: --list-columns requires service_name schema table",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        schema = sys.argv[3]
        table = sys.argv[4]
        columns = list_columns_for_table(service_name, schema, table)
        for column in columns:
            print(column)

    elif command == '--scaffold-flag-values':
        if len(sys.argv) < 3:
            print(
                "Error: --scaffold-flag-values requires flag name",
                file=sys.stderr,
            )
            return 1
        flag = sys.argv[2]
        completions = scaffold_flag_completions(flag)
        for completion in completions:
            print(completion)

    elif command == '--list-sink-keys':
        if len(sys.argv) < 3:
            print(
                "Error: --list-sink-keys requires service name",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        keys = list_sink_keys_for_service(service_name)
        for key in keys:
            print(key)

    elif command == '--list-available-sink-keys':
        keys = list_available_sink_keys()
        for key in keys:
            print(key)

    elif command == '--list-source-tables':
        if len(sys.argv) < 3:
            print(
                "Error: --list-source-tables requires service name",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        tables = list_source_tables_for_service(service_name)
        for table in tables:
            print(table)

    elif command == '--list-target-tables':
        if len(sys.argv) < 4:
            print(
                "Error: --list-target-tables requires service_name sink_key",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        sink_key = sys.argv[3]
        tables = list_target_tables_for_sink(service_name, sink_key)
        for table in tables:
            print(table)

    elif command == '--list-sink-target-tables':
        if len(sys.argv) < 3:
            print(
                "Error: --list-sink-target-tables requires sink_key",
                file=sys.stderr,
            )
            return 1
        sink_key = sys.argv[2]
        tables = list_tables_for_sink_target(sink_key)
        for table in tables:
            print(table)

    elif command == '--get-default-sink':
        if len(sys.argv) < 3:
            print(
                "Error: --get-default-sink requires service_name",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        default_sink = get_default_sink_for_service(service_name)
        if default_sink:
            print(default_sink)

    elif command == '--list-target-columns':
        if len(sys.argv) < 4:
            print(
                "Error: --list-target-columns requires sink_key target_table",
                file=sys.stderr,
            )
            return 1
        sink_key = sys.argv[2]
        target_table = sys.argv[3]
        columns = list_target_columns_for_sink_table(sink_key, target_table)
        for col in columns:
            print(col)

    elif command == '--list-custom-tables':
        if len(sys.argv) < 4:
            print(
                "Error: --list-custom-tables requires service_name sink_key",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        sink_key = sys.argv[3]
        tables = list_custom_tables_for_service_sink(service_name, sink_key)
        for table in tables:
            print(table)

    elif command == '--list-custom-table-columns':
        if len(sys.argv) < 5:
            print(
                "Error: --list-custom-table-columns requires "
                + "service_name sink_key table_key",
                file=sys.stderr,
            )
            return 1
        service_name = sys.argv[2]
        sink_key = sys.argv[3]
        table_key = sys.argv[4]
        columns = list_custom_table_columns_for_autocomplete(
            service_name, sink_key, table_key,
        )
        for col in columns:
            print(col)

    elif command == '--list-pg-types':
        types = list_pg_column_types()
        for t in types:
            print(t)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
