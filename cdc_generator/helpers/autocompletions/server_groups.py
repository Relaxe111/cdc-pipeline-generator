"""Server and sink group autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.utils import find_file_upward
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_servers_from_server_group() -> list[str]:
    """List server names defined in source-groups.yaml (servers: section).

    Used for --update server selection autocompletion.

    Returns:
        List of server names from source-groups.yaml.

    Expected YAML structure:
        server_group_name:
          pattern: "..."
          servers:
            server1: {...}
            server2: {...}

    Example:
        >>> list_servers_from_server_group()
        ['default', 'prod']
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not config or not isinstance(config, dict):
            return []

        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(dict[str, Any], group_data)
                servers_obj = group_dict.get('servers', {})

                if isinstance(servers_obj, dict):
                    servers_dict = cast(dict[str, Any], servers_obj)
                    return sorted(servers_dict.keys())
                break

        return []

    except Exception:
        return []


def list_server_group_names() -> list[str]:
    """List all server group names from source-groups.yaml.

    Used for --source-group autocompletion.

    Returns:
        List of server group names (top-level keys with 'pattern' field).

    Example:
        >>> list_server_group_names()
        ['asma', 'adopus']
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not config or not isinstance(config, dict):
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
    """List all sink group names from sink-groups.yaml.

    Used for --info and --sink-group autocompletion.

    Returns:
        List of sink group names (top-level keys).

    Example:
        >>> list_sink_group_names()
        ['sink_asma', 'sink_test']
    """
    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        config = load_yaml_file(sink_file)
        if not config or not isinstance(config, dict):
            return []

        return sorted(config.keys())

    except Exception:
        return []


def list_non_inherited_sink_group_names() -> list[str]:
    """List sink group names that are NOT inherited (inherits: true).

    Inherited sink groups cannot have servers added/removed manually
    because their servers come from the source group via source_ref.

    Used for --sink-group autocompletion with --add-server/--remove-server.

    Returns:
        List of non-inherited sink group names.

    Expected YAML structure:
        sink_group_name:
          inherits: false  # or missing (defaults to false)
          servers: {...}

    Example:
        >>> list_non_inherited_sink_group_names()
        ['sink_standalone']
    """
    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        config = load_yaml_file(sink_file)
        if not config or not isinstance(config, dict):
            return []

        return sorted(
            name
            for name, group in config.items()
            if isinstance(group, dict) and not group.get("inherits", False)
        )

    except Exception:
        return []


def list_servers_for_sink_group(sink_group_name: str) -> list[str]:
    """List server names for a specific sink group.

    Reads sink-groups.yaml and returns server names from the
    specified sink group's 'servers' section.

    Args:
        sink_group_name: Name of the sink group (e.g., 'sink_test').

    Returns:
        List of server names (e.g., ['default', 'prod']).

    Expected YAML structure:
        sink_group_name:
          servers:
            server1: {...}
            server2: {...}

    Example:
        >>> list_servers_for_sink_group('sink_asma')
        ['default', 'prod']
    """
    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        config = load_yaml_file(sink_file)
        if not config or not isinstance(config, dict):
            return []

        group = config.get(sink_group_name)
        if not isinstance(group, dict):
            return []

        servers = group.get("servers", {})
        return sorted(servers.keys()) if isinstance(servers, dict) else []

    except Exception:
        return []


def list_databases_from_server_group() -> list[str]:
    """List all databases from source-groups.yaml.

    Used for database-related autocompletions.

    Returns:
        List of database names.

    Expected YAML structure:
        server_group_name:
          databases:
            - name: db1
            - name: db2
          # OR legacy format:
            - db1
            - db2

    Example:
        >>> list_databases_from_server_group()
        ['adopus_calendar_dev', 'adopus_chat_prod']
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not config or not isinstance(config, dict):
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
                        for db in dbs:
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
