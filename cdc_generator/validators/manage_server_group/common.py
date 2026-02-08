"""Common utilities for server group handlers."""

from typing import cast

from cdc_generator.helpers.helpers_logging import print_error, print_info

from .types import ServerConfig, ServerGroupConfig, ServerGroupFile, SourceConfig


def get_server_group_name_from_config(config: ServerGroupFile) -> str | None:
    """Extract server group name from configuration.

    Args:
        config: Server groups configuration dict

    Returns:
        Server group name or None if not found
    """
    for name, data in config.items():
        if 'pattern' in data:
            return name
    return None


def check_sources_using_server(
    services_dir: str,
    server_name: str,
) -> list[str]:
    """Find all services using a specific server.

    Args:
        services_dir: Path to services directory
        server_name: Name of server to check

    Returns:
        List of service names using this server
    """
    from pathlib import Path

    import yaml

    sources_using_server: list[str] = []
    services_path = Path(services_dir)

    if services_path.exists():
        for service_file in services_path.glob('*.yaml'):
            try:
                with service_file.open() as f:
                    service_data = yaml.safe_load(f)
                    if not service_data:
                        continue

                    # Check root-level server_group
                    if service_data.get('server_group') == server_name:
                        sources_using_server.append(service_file.stem)
                        continue

                    # Check customers array
                    customers = service_data.get('customers', [])
                    for customer in customers:
                        if not isinstance(customer, dict):
                            continue
                        customer_dict: dict[str, str] = customer  # type: ignore[assignment]
                        if customer_dict.get('server_group') == server_name:
                            sources_using_server.append(
                                f"{service_file.stem} (customer: {customer_dict.get('name', '?')})"
                            )
                            break
            except Exception:
                continue

    return sources_using_server


def display_sources_using_server(
    sources: list[str],
    server_name: str,
    max_display: int = 5,
) -> bool:
    """Display sources using a server with error message.

    Args:
        sources: List of source names
        server_name: Server name
        max_display: Maximum sources to display

    Returns:
        True if sources were found (blocking operation), False otherwise
    """
    if not sources:
        return False

    print_error(
        f"Cannot remove server '{server_name}' - "
        + f"it has {len(sources)} source(s):"
    )
    for src in sources[:max_display]:
        print_info(f"    • {src}")
    if len(sources) > max_display:
        remaining = len(sources) - max_display
        print_info(f"    ... and {remaining} more")
    print_info("\nRemove or reassign these sources first.")
    return True


def load_config_and_get_server_group(
    file_name: str = "source-groups.yaml",
) -> tuple[ServerGroupFile | None, ServerGroupConfig | None, str | None]:
    """Load configuration and extract server group.

    Args:
        file_name: Name of the configuration file

    Returns:
        Tuple of (config, server_group, server_group_name) or (None, None, None) if error
    """
    from .config import get_single_server_group, load_server_groups

    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error(f"{file_name} not found. Run 'cdc scaffold' first.")
        return (None, None, None)

    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return (None, None, None)

    server_group_name = get_server_group_name_from_config(config)
    if not server_group_name:
        print_error("Could not determine server group name")
        return (None, None, None)

    return (config, server_group, server_group_name)


def validate_server_in_group(
    server_name: str,
    servers: dict[str, ServerConfig],
    should_exist: bool = True,
) -> bool:
    """Validate server exists/doesn't exist in server group.

    Args:
        server_name: Server name to validate
        servers: Dictionary of servers
        should_exist: True if server should exist, False if it shouldn't

    Returns:
        True if validation passes, False otherwise
    """
    if not servers:
        print_error("Server group uses legacy single-server format. Please migrate first.")
        return False

    exists = server_name in servers

    if should_exist and not exists:
        print_error(f"Server '{server_name}' not found in server group.")
        if servers:
            print_info(f"Available servers: {', '.join(servers.keys())}")
        return False

    if not should_exist and exists:
        print_error(f"Server '{server_name}' already exists")
        return False

    return True


def display_server_info(
    name: str,
    server_config: ServerConfig,
    group_type: str,
) -> None:
    """Display information for a single server.

    Args:
        name: Server name
        server_config: Server configuration dict
        group_type: Database type from group level
    """
    host = str(server_config.get('host', 'N/A'))
    port = str(server_config.get('port', 'N/A'))
    kafka_bs = str(server_config.get('kafka_bootstrap_servers', 'N/A'))

    marker = "⭐" if name == 'default' else "📡"
    print_info(f"\n{marker} {name}")
    print_info(f"    Type: {group_type}")
    print_info(f"    Host: {host}")
    print_info(f"    Port: {port}")
    print_info(f"    Kafka: {kafka_bs}")


def count_sources_per_server(
    sources: dict[str, SourceConfig],
    servers: dict[str, ServerConfig],
) -> dict[str, int]:
    """Count how many sources use each server.

    Args:
        sources: Dictionary of source configurations
        servers: Dictionary of server configurations

    Returns:
        Dictionary mapping server name to source count
    """
    server_source_count: dict[str, int] = dict.fromkeys(servers, 0)

    for _src_name, src_config in sources.items():
        # Check each environment entry for server reference
        # SourceConfig has dynamic keys, so we need to iterate
        src_dict = cast(dict[str, object], src_config)
        for key, value in src_dict.items():
            if key == 'schemas':
                continue
            if isinstance(value, dict):
                env_entry = cast(dict[str, str], value)
                if 'server' in env_entry:
                    src_server = env_entry.get('server', 'default')
                    if src_server in server_source_count:
                        server_source_count[src_server] += 1

    return server_source_count


def update_kafka_bootstrap_servers(
    servers: dict[str, ServerConfig],
    topology: str,
) -> None:
    """Update kafka_bootstrap_servers for all servers based on topology.

    Args:
        servers: Dictionary of server configurations
        topology: 'shared' or 'per-server'
    """
    for srv_name, srv_config in servers.items():
        if topology == 'per-server':
            # For per-server topology:
            # - 'default' server uses ${KAFKA_BOOTSTRAP_SERVERS} (no postfix)
            # - Other servers use ${KAFKA_BOOTSTRAP_SERVERS_<NAME>}
            if srv_name == 'default':
                srv_config['kafka_bootstrap_servers'] = '${KAFKA_BOOTSTRAP_SERVERS}'
            else:
                postfix = f"_{srv_name.upper()}"
                srv_config['kafka_bootstrap_servers'] = f"${{KAFKA_BOOTSTRAP_SERVERS{postfix}}}"
        else:
            # For shared topology: all servers use the same Kafka cluster
            srv_config['kafka_bootstrap_servers'] = '${KAFKA_BOOTSTRAP_SERVERS}'

