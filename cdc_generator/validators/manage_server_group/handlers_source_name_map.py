"""CLI handlers for source_name_map management."""

from argparse import Namespace
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)

from .config import get_single_server_group, load_server_groups
from .types import ServerGroupConfig
from .yaml_io import write_server_group_yaml


def _load_server_group_for_source_name_map() -> tuple[ServerGroupConfig | None, str | None, int]:
    """Load the current server group and validate source_name_map eligibility."""
    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as exc:
        print_error(f"Failed to load server groups: {exc}")
        return (None, None, 1)

    if not server_group:
        print_error("No server group found in configuration")
        return (None, None, 1)

    server_group_name = str(server_group.get("name", "unknown"))
    pattern = str(server_group.get("pattern", ""))
    if pattern != "db-per-tenant":
        print_error("source_name_map is only supported for db-per-tenant source groups")
        print_info(f"Current pattern for server group '{server_group_name}': {pattern or 'unknown'}")
        return (None, None, 1)

    return (server_group, server_group_name, 0)


def _get_known_database_names(server_group: ServerGroupConfig) -> list[str]:
    """Return discovered database names from sources.*.<env>.database entries."""
    names: set[str] = set()
    sources = server_group.get("sources", {})
    if not isinstance(sources, dict):
        return []

    for source_data in sources.values():
        if not isinstance(source_data, dict):
            continue
        source_dict = cast(dict[str, object], source_data)
        for env_name, env_data in source_dict.items():
            if env_name == "schemas" or not isinstance(env_data, dict):
                continue
            database_name = env_data.get("database")
            if isinstance(database_name, str) and database_name.strip():
                names.add(database_name.strip())

    return sorted(names)


def _get_source_name_map(server_group: ServerGroupConfig) -> dict[str, str]:
    """Normalize source_name_map to a clean string-to-string mapping."""
    raw_map = server_group.get("source_name_map", {})
    if not isinstance(raw_map, dict):
        return {}

    result: dict[str, str] = {}
    for database_name_raw, source_name_raw in raw_map.items():
        if not isinstance(database_name_raw, str) or not isinstance(source_name_raw, str):
            continue
        database_name = database_name_raw.strip()
        source_name = source_name_raw.strip()
        if database_name and source_name:
            result[database_name] = source_name
    return result


def handle_set_source_name_map(args: Namespace) -> int:
    """Set or update a database-to-source override in source_name_map."""
    server_group, server_group_name, exit_code = _load_server_group_for_source_name_map()
    if not server_group or not server_group_name:
        return exit_code

    database_name_raw, source_name_raw = args.set_source_name_map
    database_name = str(database_name_raw).strip()
    source_name = str(source_name_raw).strip()

    if not database_name or not source_name:
        print_error("Both database name and source name are required")
        return 1

    source_name_map = _get_source_name_map(server_group)
    old_source_name = source_name_map.get(database_name)
    if old_source_name == source_name:
        print_warning(f"source_name_map already maps '{database_name}' to '{source_name}'")
        return 0

    known_database_names = _get_known_database_names(server_group)
    if known_database_names and database_name not in known_database_names:
        print_warning(f"Database '{database_name}' is not currently present in discovered sources")
        print_info("Saving override anyway. Run --update after the database exists.")

    source_name_map[database_name] = source_name
    server_group["source_name_map"] = source_name_map

    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"✓ Set source_name_map for database '{database_name}'")
        if old_source_name:
            print_info(f"  Old source name: {old_source_name}")
        print_info(f"  New source name: {source_name}")
        return 0
    except Exception as exc:
        print_error(f"Failed to save configuration: {exc}")
        return 1


def handle_remove_source_name_map(args: Namespace) -> int:
    """Remove a database-to-source override from source_name_map."""
    server_group, server_group_name, exit_code = _load_server_group_for_source_name_map()
    if not server_group or not server_group_name:
        return exit_code

    database_name = str(args.remove_source_name_map).strip()
    if not database_name:
        print_error("Database name is required")
        return 1

    source_name_map = _get_source_name_map(server_group)
    if database_name not in source_name_map:
        print_error(f"No source_name_map entry found for database '{database_name}'")
        return 1

    removed_source_name = source_name_map.pop(database_name)
    if source_name_map:
        server_group["source_name_map"] = source_name_map
    else:
        server_group.pop("source_name_map", None)

    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"✓ Removed source_name_map entry for database '{database_name}'")
        print_info(f"  Removed source name: {removed_source_name}")
        return 0
    except Exception as exc:
        print_error(f"Failed to save configuration: {exc}")
        return 1


def handle_list_source_name_map(_args: Namespace) -> int:
    """List configured source_name_map entries."""
    server_group, server_group_name, exit_code = _load_server_group_for_source_name_map()
    if not server_group or not server_group_name:
        return exit_code

    source_name_map = _get_source_name_map(server_group)
    print_header(f"Source Name Map for Server Group '{server_group_name}'")

    if not source_name_map:
        print_warning("No source_name_map entries configured.")
        print_info("Use: cdc manage-source-groups --set-source-name-map <database> <source-name>")
        return 0

    known_database_names = set(_get_known_database_names(server_group))
    for database_name, source_name in sorted(source_name_map.items()):
        marker = "" if not known_database_names or database_name in known_database_names else " (database not currently discovered)"
        print_info(f"  • {database_name} -> {source_name}{marker}")

    return 0
