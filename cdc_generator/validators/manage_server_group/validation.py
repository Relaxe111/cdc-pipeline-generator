"""Validation helpers for server group operations."""

from typing import Any

from cdc_generator.helpers.helpers_logging import print_error


def validate_server_name(server_name: str, allow_default: bool = False) -> bool:
    """Validate server name meets requirements.

    Args:
        server_name: Server name to validate
        allow_default: Whether to allow 'default' as a name

    Returns:
        True if valid, False otherwise (prints error)
    """
    if not allow_default and server_name == 'default':
        print_error(
            "Cannot add a server named 'default' - it already exists by convention"
        )
        return False

    if not server_name.isidentifier():
        print_error(
            f"Invalid server name '{server_name}'. "
            + "Must be a valid identifier (alphanumeric and underscores)."
        )
        return False

    return True


def validate_server_exists(
    server_name: str,
    servers: dict[str, Any],
    should_exist: bool = True,
) -> bool:
    """Validate server existence matches expectation.

    Args:
        server_name: Server name to check
        servers: Dictionary of servers
        should_exist: True if server should exist, False if it shouldn't

    Returns:
        True if validation passes, False otherwise (prints error)
    """
    exists = server_name in servers

    if should_exist and not exists:
        print_error(f"Server '{server_name}' not found in server group.")
        available = ', '.join(servers.keys()) if servers else '(none)'
        print_error(f"Available servers: {available}")
        return False

    if not should_exist and exists:
        print_error(f"Server '{server_name}' already exists")
        return False

    return True


def validate_source_type_match(
    group_type: str | None,
    source_type: str | None,
) -> tuple[bool, str]:
    """Validate source type matches group type.

    Args:
        group_type: Type defined at group level
        source_type: Type provided by user

    Returns:
        Tuple of (is_valid, final_type)
    """
    if not group_type and not source_type:
        print_error("Could not determine source type. Use --source-type to specify.")
        return (False, "")

    # If both provided, they must match
    if group_type and source_type and group_type != source_type:
        print_error(
            f"Server type '{source_type}' does not match group type '{group_type}'"
        )
        from cdc_generator.helpers.helpers_logging import print_info
        print_info(
            "All servers in a server group must have the same database type "
            + "(defined at group level)."
        )
        return (False, "")

    # Use group type as the source of truth
    return (True, str(group_type or source_type))
