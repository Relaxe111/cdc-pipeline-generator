"""CLI handlers for validation_env management."""

from argparse import Namespace
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)

from .config import (
    get_single_server_group,
    load_server_groups,
)
from .types import ServerGroupConfig
from .yaml_io import write_server_group_yaml


def _collect_envs_from_sources(server_group: ServerGroupConfig) -> list[str]:
    """Collect unique environment names from sources section.

    For db-shared pattern.
    """
    envs: set[str] = set()
    sources = server_group.get("sources", {})

    for _service_name, service_data in sources.items():
        source_data = cast(dict[str, object], service_data)
        for key in source_data:
            # Skip non-environment keys
            if key in ("schemas", "table_count"):
                continue
            # Environment keys are those that have 'database' field
            env_data = source_data.get(key)
            if isinstance(env_data, dict) and "database" in env_data:
                envs.add(key)

    return sorted(envs)


def _collect_envs_from_servers(server_group: ServerGroupConfig) -> list[str]:
    """Collect server names as environments.

    For db-per-tenant pattern when environment_aware is false.
    """
    servers = server_group.get("servers", {})
    return sorted(servers.keys())


def get_available_envs(server_group: ServerGroupConfig) -> list[str]:
    """Get available environments for a server group.

    Logic:
    - db-shared: Use environment names from sources (dev, prod, stage, etc.)
    - db-per-tenant with environment_aware=false: Use server names as envs
    - db-per-tenant with environment_aware=true: Use environment names from sources
    """
    pattern = server_group.get("pattern", "db-shared")
    environment_aware = server_group.get("environment_aware", True)

    if pattern == "db-shared":
        return _collect_envs_from_sources(server_group)
    if pattern == "db-per-tenant":
        if environment_aware:
            return _collect_envs_from_sources(server_group)
        # Server names are the environments
        return _collect_envs_from_servers(server_group)
    # Fallback to sources
    return _collect_envs_from_sources(server_group)


def handle_set_validation_env(args: Namespace) -> int:
    """Set the validation_env for the server group.

    Args:
        args: Parsed CLI arguments with set_validation_env

    Returns:
        0 on success, 1 on error
    """
    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as e:
        print_error(f"Failed to load server groups: {e}")
        return 1

    if not server_group:
        print_error("No server group found in configuration")
        return 1

    server_group_name = server_group.get("name", "unknown")

    validation_env = args.set_validation_env

    # Get available environments
    available_envs = get_available_envs(server_group)

    if not available_envs:
        print_warning("No environments discovered yet.")
        print_info("Run 'cdc manage-source-groups --update' to discover environments.")
        print_info(f"Setting validation_env to '{validation_env}' anyway...")
    elif validation_env not in available_envs:
        print_warning(f"Environment '{validation_env}' not found in available environments.")
        print_info("Available environments:")
        for env in available_envs:
            print_info(f"  • {env}")
        print()
        print_info(f"Setting validation_env to '{validation_env}' anyway...")
        print_info("You can update it later after running --update.")

    # Set the validation_env
    server_group["validation_env"] = validation_env

    # Update envs list
    if available_envs:
        server_group["envs"] = available_envs

    # Save
    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(
            "✓ Set validation_env to "
            + f"'{validation_env}' for server group '{server_group_name}'"
        )

        if available_envs:
            print_info(
                "  Available environments "
                + f"({len(available_envs)}): {', '.join(available_envs)}"
            )

        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_list_envs(_args: Namespace) -> int:
    """List available environments for the server group.

    Args:
        args: Parsed CLI arguments

    Returns:
        0 on success, 1 on error
    """
    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as e:
        print_error(f"Failed to load server groups: {e}")
        return 1

    if not server_group:
        print_error("No server group found in configuration")
        return 1

    server_group_name = server_group.get("name", "unknown")

    # Get current validation_env
    current_validation_env = server_group.get("validation_env")

    # Get available environments
    available_envs = get_available_envs(server_group)

    # Also check stored envs list
    stored_envs = server_group.get("envs", [])

    print_header(f"Environments for Server Group '{server_group_name}'")

    if current_validation_env:
        print_info(f"Current validation_env: {current_validation_env}")
    else:
        print_warning("validation_env not set")
        print_info("Use: cdc manage-source-groups --set-validation-env <env>")

    print()

    if available_envs:
        print_info(f"Available environments ({len(available_envs)}):")
        for env in available_envs:
            marker = " ✓ (current)" if env == current_validation_env else ""
            print_info(f"  • {env}{marker}")
    else:
        print_warning("No environments discovered yet.")
        print_info("Run 'cdc manage-source-groups --update' to discover environments.")

    if stored_envs and stored_envs != available_envs:
        print()
        print_info(f"Stored envs list ({len(stored_envs)}): {', '.join(stored_envs)}")
        print_warning("Note: Stored list differs from current discovery. Run --update to refresh.")

    return 0


def update_envs_list(server_group: ServerGroupConfig) -> None:
    """Update the envs list in server group based on discovered environments.

    Should be called after --update to keep envs list fresh.

    Args:
        server_group: Server group configuration dict (modified in place)
    """
    available_envs = get_available_envs(server_group)
    if available_envs:
        server_group["envs"] = available_envs
