#!/usr/bin/env python3
"""
Shell autocompletion helpers for CDC CLI.

Provides dynamic completion data for Fish shell and other shells.
All extraction logic is centralized here for easy maintenance.
"""

import sys
from pathlib import Path
from typing import Any, cast

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]


def find_file_upward(filename: str, max_depth: int = 3) -> Path | None:
    """Search for a file by walking up the directory tree."""
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / filename
        if candidate.exists():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def find_directory_upward(dirname: str, max_depth: int = 3) -> Path | None:
    """Search for a directory by walking up the directory tree."""
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / dirname
        if candidate.is_dir():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def list_existing_services() -> list[str]:
    """
    List existing service files from services/*.yaml.
    
    Used for --service flag autocompletion (shows created services).
    
    Returns:
        List of service names (without .yaml extension)
    """
    services_dir = find_directory_upward('services')
    if not services_dir:
        return []

    services: list[str] = []
    for yaml_file in services_dir.glob('*.yaml'):
        if yaml_file.is_file():
            services.append(yaml_file.stem)

    return sorted(services)


def list_available_services_from_server_group() -> list[str]:
    """
    List sources defined in source-groups.yaml (sources: section).
    
    Used for --create-service flag autocompletion (shows sources that can be created).
    
    Returns:
        List of source names from source-groups.yaml
    """
    if yaml is None:
        return []

    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        # Extract sources from server_group structure (flat format)
        # Look for root key with 'pattern' field (server group marker)
        sources_obj: object = {}

        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(dict[str, Any], group_data)
                # Found server group - check for 'sources' key
                sources_obj = group_dict.get('sources', {})
                if not sources_obj:
                    # Fallback to legacy 'services' key
                    sources_obj = group_dict.get('services', {})
                break

        if isinstance(sources_obj, dict):
            sources_dict = cast(dict[str, Any], sources_obj)
            return sorted(sources_dict.keys())

        return []

    except Exception:
        return []


def list_servers_from_server_group() -> list[str]:
    """
    List server names defined in source-groups.yaml (servers: section).
    
    Used for --update server selection autocompletion.
    
    Returns:
        List of server names from source-groups.yaml
    """
    if yaml is None:
        return []

    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        servers_obj: object = {}

        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(dict[str, Any], group_data)
                servers_obj = group_dict.get('servers', {})
                break

        if isinstance(servers_obj, dict):
            servers_dict = cast(dict[str, Any], servers_obj)
            return sorted(servers_dict.keys())

        return []

    except Exception:
        return []


def list_server_group_names() -> list[str]:
    """
    List all server group names from source-groups.yaml.
    
    Used for --source-group autocompletion.
    
    Returns:
        List of server group names (top-level keys with 'pattern' field)
    """
    if yaml is None:
        return []

    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        # Find all top-level keys that have 'pattern' field (server groups)
        groups: list[str] = []
        for key, value in config.items():
            if isinstance(value, dict) and 'pattern' in value:
                groups.append(key)

        return sorted(groups)

    except Exception:
        return []


def list_sink_group_names() -> list[str]:
    """
    List all sink group names from sink-groups.yaml.
    
    Used for --info and --sink-group autocompletion.
    
    Returns:
        List of sink group names (top-level keys)
    """
    if yaml is None:
        return []

    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        with open(sink_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        return sorted(config.keys())

    except Exception:
        return []


def list_non_inherited_sink_group_names() -> list[str]:
    """
    List sink group names that are NOT inherited (inherits: true).

    Inherited sink groups cannot have servers added/removed manually
    because their servers come from the source group via source_ref.

    Used for --sink-group autocompletion with --add-server/--remove-server.

    Returns:
        List of non-inherited sink group names
    """
    if yaml is None:
        return []

    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        with open(sink_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        return sorted(
            name
            for name, group in config.items()
            if isinstance(group, dict) and not group.get("inherits", False)
        )

    except Exception:
        return []


def list_servers_for_sink_group(sink_group_name: str) -> list[str]:
    """
    List server names for a specific sink group.

    Reads sink-groups.yaml and returns server names from the
    specified sink group's 'servers' section.

    Args:
        sink_group_name: Name of the sink group (e.g., 'sink_test')

    Returns:
        List of server names (e.g., ['default', 'prod'])
    """
    if yaml is None:
        return []

    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        with open(sink_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        group = config.get(sink_group_name)
        if not isinstance(group, dict):
            return []

        servers = group.get("servers", {})
        if not isinstance(servers, dict):
            return []

        return sorted(servers.keys())

    except Exception:
        return []


def list_databases_from_server_group() -> list[str]:
    """
    List all databases from source-groups.yaml.
    
    Used for database-related autocompletions.
    
    Returns:
        List of database names
    """
    if yaml is None:
        return []

    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)

        if not config:
            return []

        databases: set[str] = set()

        # Extract from server_group structure
        server_group = config.get('server_group', {})
        if isinstance(server_group, dict):
            server_group_dict = cast(dict[str, Any], server_group)
            for group_data in server_group_dict.values():
                if isinstance(group_data, dict):
                    group_dict = cast(dict[str, Any], group_data)
                    # Get databases list
                    dbs = group_dict.get('databases', [])
                    if isinstance(dbs, list):
                        dbs_list = cast(list[Any], dbs)
                        for db in dbs_list:
                            if isinstance(db, str):
                                databases.add(db)
                            elif isinstance(db, dict):
                                db_dict = cast(dict[str, Any], db)
                                db_name = db_dict.get('name')
                                if db_name:
                                    databases.add(str(db_name))

        return sorted(databases)

    except Exception:
        return []


def list_schemas_for_service(service_name: str) -> list[str]:
    """
    List available schemas for a specific service from source-groups.yaml sources.
    
    Args:
        service_name: Name of the service
    
    Returns:
        List of schema names
    """
    if not yaml:
        return []

    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        with open(server_group_file, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not data:
            return []

        # Find first server group (should only be one)
        for server_group_name, server_group_data in data.items():
            if isinstance(server_group_data, dict) and 'sources' in server_group_data:
                sources = server_group_data.get('sources', {})
                if service_name in sources:
                    schemas = sources[service_name].get('schemas', [])
                    return sorted(schemas) if schemas else []

        return []
    except Exception:
        return []


def list_tables_for_service(service_name: str) -> list[str]:
    """
    List available tables for a specific service from service-schemas directory.
    
    Format: schema.TableName
    
    Args:
        service_name: Name of the service
    
    Returns:
        List of tables in format "schema.TableName"
    """
    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []

    service_dir = schemas_dir / service_name
    if not service_dir.is_dir():
        return []

    tables: list[str] = []
    for schema_dir in service_dir.iterdir():
        if schema_dir.is_dir():
            schema_name = schema_dir.name
            for table_file in schema_dir.glob('*.yaml'):
                if table_file.is_file():
                    table_name = table_file.stem
                    tables.append(f"{schema_name}.{table_name}")

    return sorted(tables)


def list_columns_for_table(service_name: str, schema: str, table: str) -> list[str]:
    """
    List columns for a specific table from service-schemas directory.
    
    Format: schema.table.column
    
    Args:
        service_name: Name of the service
        schema: Schema name
        table: Table name
    
    Returns:
        List of columns in format "schema.table.column"
    """
    if not yaml:
        return []

    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []

    table_file = schemas_dir / service_name / schema / f'{table}.yaml'
    if not table_file.is_file():
        return []

    try:
        with open(table_file, encoding='utf-8') as f:
            table_schema = yaml.safe_load(f)

        if not table_schema:
            return []

        columns = table_schema.get('columns', [])
        if not columns:
            return []

        # Return in format schema.table.column
        result: list[str] = []
        for col in columns:
            if isinstance(col, dict):
                col_name = col.get('name')
                if col_name:
                    result.append(f"{schema}.{table}.{col_name}")

        return sorted(result)

    except Exception:
        return []


def list_sink_keys_for_service(service_name: str) -> list[str]:
    """List configured sink keys for a service from services/{service}.yaml.

    Format: sink_group.target_service

    Args:
        service_name: Name of the service.

    Returns:
        List of sink keys (e.g., ['sink_asma.chat', 'sink_asma.directory']).
    """
    if not yaml:
        return []

    services_dir = find_directory_upward('services')
    if not services_dir:
        return []

    service_file = services_dir / f'{service_name}.yaml'
    if not service_file.is_file():
        return []

    try:
        with open(service_file, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not data:
            return []

        # Support new format (service name as root key)
        if service_name in data and isinstance(data[service_name], dict):
            config = data[service_name]
        else:
            config = data

        sinks = config.get('sinks', {})
        if isinstance(sinks, dict):
            return sorted(sinks.keys())
        return []

    except Exception:
        return []


def list_available_sink_keys() -> list[str]:
    """Generate possible sink keys from sink-groups.yaml.

    For inherited sinks: uses inherited_sources list.
    For standalone sinks: uses sources dict keys.

    Returns:
        List of possible sink key suggestions (sink_group.source_name).
    """
    if not yaml:
        return []

    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        with open(sink_file, encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if not config or not isinstance(config, dict):
            return []

        suggestions: list[str] = []
        sink_config = cast(dict[str, Any], config)

        for group_name, group_data in sink_config.items():
            if not isinstance(group_data, dict):
                continue
            group_dict = cast(dict[str, Any], group_data)

            if group_dict.get("inherits"):
                # Inherited sink: suggest from inherited_sources
                sources = group_dict.get("inherited_sources", [])
                if isinstance(sources, list):
                    for src in cast(list[str], sources):
                        suggestions.append(f"{group_name}.{src}")
            else:
                # Standalone sink: suggest from sources keys
                sources = group_dict.get("sources", {})
                if isinstance(sources, dict):
                    sources_dict = cast(dict[str, Any], sources)
                    for src in sources_dict:
                        suggestions.append(f"{group_name}.{src}")

        return sorted(suggestions)

    except Exception:
        return []


def list_source_tables_for_service(service_name: str) -> list[str]:
    """List source tables from a service's source.tables configuration.

    Args:
        service_name: Name of the service.

    Returns:
        List of table keys in format 'schema.table'.
    """
    if not yaml:
        return []

    services_dir = find_directory_upward('services')
    if not services_dir:
        return []

    service_file = services_dir / f'{service_name}.yaml'
    if not service_file.is_file():
        return []

    try:
        with open(service_file, encoding='utf-8') as f:
            data = yaml.safe_load(f)

        if not data:
            return []

        # Support new format (service name as root key)
        if service_name in data and isinstance(data[service_name], dict):
            config = data[service_name]
        else:
            config = data

        source = config.get('source', {})
        if not isinstance(source, dict):
            return []

        tables = source.get('tables', {})
        if not isinstance(tables, dict):
            return []

        return sorted(str(k) for k in tables)

    except Exception:
        return []


def list_target_tables_for_sink(
    service_name: str,
    sink_key: str,
) -> list[str]:
    """List available target tables from service-schemas for sink's target service.

    Parses sink_key to extract target_service, then lists tables
    from service-schemas/{target_service}/.

    Args:
        service_name: Source service name (unused, kept for API consistency).
        sink_key: Sink key in format 'sink_group.target_service'.

    Returns:
        List of target table options in format 'schema.table'.
    """
    parts = sink_key.split('.', 1)
    if len(parts) != 2:
        return []

    target_service = parts[1]
    return list_tables_for_service(target_service)


def list_tables_for_sink_target(sink_key: str) -> list[str]:
    """List available tables from service-schemas for a sink's target service.

    Parses sink_key to extract target_service, then lists tables
    from service-schemas/{target_service}/.

    This is used for --add-sink-table autocomplete: given --sink sink_asma.chat,
    it returns tables from service-schemas/chat/ (e.g., public.users, public.rooms).

    Args:
        sink_key: Sink key in format 'sink_group.target_service'.

    Returns:
        List of tables in format 'schema.table'.
    """
    parts = sink_key.split('.', 1)
    if len(parts) != 2:
        return []

    target_service = parts[1]
    return list_tables_for_service(target_service)


def get_default_sink_for_service(service_name: str) -> str:
    """Return the only sink key if a service has exactly one sink.

    Used to auto-default --sink when there's only one option.

    Args:
        service_name: Name of the service.

    Returns:
        The single sink key, or empty string if zero or multiple sinks.
    """
    sinks = list_sink_keys_for_service(service_name)
    if len(sinks) == 1:
        return sinks[0]
    return ""


def list_target_columns_for_sink_table(
    sink_key: str,
    target_table: str,
) -> list[str]:
    """List columns for a target table from service-schemas.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.chat').
        target_table: Target table in format 'schema.table'.

    Returns:
        List of column names.
    """
    if not yaml:
        return []

    parts = sink_key.split('.', 1)
    if len(parts) != 2:
        return []

    target_service = parts[1]

    table_parts = target_table.split('.', 1)
    if len(table_parts) != 2:
        return []

    schema, table = table_parts

    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []

    table_file = schemas_dir / target_service / schema / f'{table}.yaml'
    if not table_file.is_file():
        return []

    try:
        with open(table_file, encoding='utf-8') as f:
            table_schema = yaml.safe_load(f)

        if not table_schema:
            return []

        columns = table_schema.get('columns', [])
        return sorted(
            str(col.get('name', ''))
            for col in columns
            if isinstance(col, dict) and col.get('name')
        )

    except Exception:
        return []


def scaffold_flag_completions(flag: str) -> list[str]:
    """
    Return appropriate completions for scaffold subcommand flags.
    
    Args:
        flag: The flag name (--pattern, --source-type, etc.)
    
    Returns:
        List of "value\tDescription" formatted completions
    """
    completions = {
        '--pattern': [
            'db-per-tenant\tOne database per tenant',
            'db-shared\tShared database for all tenants',
        ],
        '--source-type': [
            'postgres\tPostgreSQL database',
            'mssql\tMicrosoft SQL Server',
        ],
    }

    return completions.get(flag, [])


def main() -> int:
    """CLI entry point for autocompletion queries."""
    if len(sys.argv) < 2:
        print("Usage: python -m cdc_generator.helpers.autocompletions <command>", file=sys.stderr)
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
            print("Error: --list-sink-group-servers requires sink group name", file=sys.stderr)
            return 1
        sink_group_name = sys.argv[2]
        servers = list_servers_for_sink_group(sink_group_name)
        for server in servers:
            print(server)

    elif command == '--list-schemas':
        if len(sys.argv) < 3:
            print("Error: --list-schemas requires service name", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        schemas = list_schemas_for_service(service_name)
        for schema in schemas:
            print(schema)

    elif command == '--list-tables':
        if len(sys.argv) < 3:
            print("Error: --list-tables requires service name", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        tables = list_tables_for_service(service_name)
        for table in tables:
            print(table)

    elif command == '--list-columns':
        if len(sys.argv) < 5:
            print("Error: --list-columns requires service_name schema table", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        schema = sys.argv[3]
        table = sys.argv[4]
        columns = list_columns_for_table(service_name, schema, table)
        for column in columns:
            print(column)

    elif command == '--scaffold-flag-values':
        if len(sys.argv) < 3:
            print("Error: --scaffold-flag-values requires flag name", file=sys.stderr)
            return 1
        flag = sys.argv[2]
        completions = scaffold_flag_completions(flag)
        for completion in completions:
            print(completion)

    elif command == '--list-sink-keys':
        if len(sys.argv) < 3:
            print("Error: --list-sink-keys requires service name", file=sys.stderr)
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
            print("Error: --list-source-tables requires service name", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        tables = list_source_tables_for_service(service_name)
        for table in tables:
            print(table)

    elif command == '--list-target-tables':
        if len(sys.argv) < 4:
            print("Error: --list-target-tables requires service_name sink_key", file=sys.stderr)
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
            print("Error: --list-target-columns requires sink_key target_table", file=sys.stderr)
            return 1
        sink_key = sys.argv[2]
        target_table = sys.argv[3]
        columns = list_target_columns_for_sink_table(sink_key, target_table)
        for col in columns:
            print(col)

    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
