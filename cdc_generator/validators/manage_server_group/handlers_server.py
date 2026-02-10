"""CLI handlers for multi-server management."""

from argparse import Namespace
from typing import Any, cast

from cdc_generator.helpers.helpers_env import (
    append_env_vars_to_dotenv,
    print_env_removal_summary,
    print_env_update_summary,
    remove_env_vars_from_dotenv,
    source_server_env_vars,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)

from .common import (
    check_sources_using_server,
    count_sources_per_server,
    display_server_info,
    display_sources_using_server,
    load_config_and_get_server_group,
    update_kafka_bootstrap_servers,
    validate_server_in_group,
)
from .config import (
    get_single_server_group,
    load_server_groups,
)
from .types import ServerConfig
from .validation import (
    validate_server_name,
    validate_source_type_match,
)
from .yaml_io import write_server_group_yaml


def default_connection_placeholders(
    source_type: str, server_name: str, kafka_topology: str
) -> dict[str, str]:
    """Return default environment variable placeholders for a server.

    Delegates to shared source_server_env_vars helper.

    Args:
        source_type: 'postgres' or 'mssql'
        server_name: Server name (e.g., 'default', 'analytics')
        kafka_topology: 'shared' or 'per-server'

    Returns:
        Dict with host, port, user, password, kafka_bootstrap_servers placeholders

    Example:
        >>> default_connection_placeholders('postgres', 'analytics', 'per-server')
        {'host': '${POSTGRES_SOURCE_HOST_ANALYTICS}', ...}
    """
    return source_server_env_vars(source_type, server_name, kafka_topology)


def handle_add_server(args: Namespace) -> int:
    """Handle adding a new server to an existing server group.

    Args:
        args: Parsed arguments with:
            - add_server: Server name to add
            - source_type: Optional, must match group type
            - host, port, user, password: Optional connection overrides

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        cdc manage-source-groups --add-server analytics --source-type postgres \\
            --host '${POSTGRES_SOURCE_HOST_ANALYTICS}'
    """
    server_name = args.add_server.lower()

    # Validate server name
    if not validate_server_name(server_name, allow_default=False):
        return 1

    # Load configuration
    config, server_group, server_group_name = load_config_and_get_server_group()
    if not config or not server_group or not server_group_name:
        return 1

    # Validate server doesn't exist
    servers: dict[str, Any] = server_group.get('servers', {})
    if not validate_server_in_group(server_name, servers, should_exist=False):
        return 1

    # Validate source type
    group_type = server_group.get('type')
    source_type = getattr(args, 'source_type', None)
    is_valid, final_source_type = validate_source_type_match(group_type, source_type)
    if not is_valid:
        return 1

    # Get kafka topology
    kafka_topology = cast(str, server_group.get('kafka_topology', 'shared'))

    # Generate default placeholders
    placeholders = default_connection_placeholders(final_source_type, server_name, kafka_topology)

    # Create new server config (NO 'type' - it's at group level)
    new_server: dict[str, Any] = {
        'host': getattr(args, 'host', None) or placeholders['host'],
        'port': getattr(args, 'port', None) or placeholders['port'],
        'user': getattr(args, 'user', None) or placeholders['user'],
        'password': getattr(args, 'password', None) or placeholders['password'],
        'kafka_bootstrap_servers': placeholders['kafka_bootstrap_servers'],
    }

    # Add server to configuration
    servers[server_name] = new_server

    # Save the updated configuration
    config[server_group_name]['servers'] = servers

    # Write YAML file
    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"✓ Added server '{server_name}' to server group '{server_group_name}'")
        print_info(f"  Type: {source_type}")
        print_info(f"  Host: {new_server['host']}")
        print_info(f"  Kafka: {new_server['kafka_bootstrap_servers']}")
        # Append env variables to .env
        env_count = append_env_vars_to_dotenv(
            placeholders,
            f"Source Server: {server_name} ({final_source_type})",
        )
        print_env_update_summary(env_count, placeholders)
        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_list_servers(_args: Namespace) -> int:
    """Handle listing all servers in the server group.

    Args:
        _args: Parsed command-line arguments (not used, but required for handler signature)

    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("source-groups.yaml not found")
        return 1

    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found")
        return 1

    # Get server group name
    server_group_name = "Unknown"
    for name, data in config.items():
        if 'pattern' in cast(dict[str, Any], data):
            server_group_name = name
            break

    servers: dict[str, Any] = server_group.get('servers', {})
    kafka_topology = server_group.get('kafka_topology', 'shared')

    # Handle legacy single-server format
    if not servers:
        print_warning("Server group uses legacy single-server format")
        legacy_server = server_group.get('server', {})
        servers = {'default': legacy_server} if legacy_server else {}

    print_header(f"Servers in '{server_group_name}' (kafka_topology: {kafka_topology})")

    if not servers:
        print_warning("No servers configured")
        return 0

    # Get type from group level (new structure)
    group_type = str(server_group.get('type', 'unknown'))

    # Display all servers
    for name, server_config in servers.items():
        display_server_info(name, cast(ServerConfig, server_config), group_type)

    # Count and display sources per server
    sources: dict[str, Any] = server_group.get('sources', server_group.get('services', {}))
    if sources:
        print_info("\n📦 Sources by server:")
        server_source_count = count_sources_per_server(sources, servers)
        for srv_name, count in server_source_count.items():
            print_info(f"    {srv_name}: {count} source(s)")

    return 0


def handle_remove_server(args: Namespace) -> int:
    """Handle removing a server from the server group.

    Args:
        args: Parsed arguments with remove_server (server name)

    Returns:
        Exit code (0 for success, 1 for error)
    """
    server_name = args.remove_server.lower()

    # Cannot remove default server
    if server_name == 'default':
        print_error("Cannot remove the 'default' server")
        return 1

    # Load configuration
    config, server_group, server_group_name = load_config_and_get_server_group()
    if not config or not server_group or not server_group_name:
        return 1

    servers: dict[str, Any] = server_group.get('servers', {})
    if not validate_server_in_group(server_name, servers, should_exist=True):
        return 1

    # Check if any sources use this server
    sources_using_server = check_sources_using_server('services', server_name)
    if display_sources_using_server(sources_using_server, server_name):
        return 1

    # Remove the server
    del servers[server_name]

    # Save the updated configuration
    try:
        write_server_group_yaml(server_group_name, server_group)

        print_success(f"✓ Removed server '{server_name}' from server group '{server_group_name}'")
        # Remove env variables from .env
        group_type = str(server_group.get('type', 'postgres'))
        kafka_topology = cast(str, server_group.get('kafka_topology', 'shared'))
        placeholders = source_server_env_vars(
            group_type, server_name, kafka_topology,
        )
        env_count = remove_env_vars_from_dotenv(placeholders)
        print_env_removal_summary(env_count, placeholders)
        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_set_kafka_topology(args: Namespace) -> int:
    """Handle changing the Kafka topology setting.

    When changing topology, all servers' kafka_bootstrap_servers are updated:
    - shared: All servers get '${KAFKA_BOOTSTRAP_SERVERS}'
    - per-server: Each server gets '${KAFKA_BOOTSTRAP_SERVERS_<NAME>}'

    Args:
        args: Parsed arguments with set_kafka_topology ('shared' or 'per-server')

    Returns:
        Exit code (0 for success, 1 for error)
    """
    new_topology = args.set_kafka_topology

    # Load configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("source-groups.yaml not found")
        return 1

    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found")
        return 1

    # Get server group name
    server_group_name: str | None = None
    for name, data in config.items():
        if 'servers' in cast(dict[str, Any], data):
            server_group_name = name
            break

    if not server_group_name:
        print_error("Could not determine server group name")
        return 1

    current_topology = server_group.get('kafka_topology', 'shared')
    if current_topology == new_topology:
        print_info(f"Kafka topology is already '{new_topology}'")
        return 0

    # Update topology
    server_group['kafka_topology'] = new_topology

    # Update all servers' kafka_bootstrap_servers
    servers: dict[str, Any] = server_group.get('servers', {})
    update_kafka_bootstrap_servers(servers, new_topology)

    # Save the updated configuration
    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"✓ Changed Kafka topology from '{current_topology}' to '{new_topology}'")

        # Show updated bootstrap servers
        print_info("\nUpdated kafka_bootstrap_servers:")
        for srv_name, srv_config in servers.items():
            srv = cast(dict[str, Any], srv_config)
            print_info(f"    {srv_name}: {srv.get('kafka_bootstrap_servers')}")

        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_set_extraction_pattern(args: Namespace) -> int:
    """Handle setting extraction pattern for a specific server.

    Args:
        args: Parsed arguments with:
            - set_extraction_pattern: [server_name, pattern]

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        cdc manage-source-groups --set-extraction-pattern default \
            '^(?P<service>\\w+)_(?P<env>\\w+)$'
    """
    server_name, pattern = args.set_extraction_pattern
    server_name = server_name.lower()

    # Load configuration
    config, server_group, server_group_name = load_config_and_get_server_group()
    if not config or not server_group or not server_group_name:
        return 1

    servers = server_group.get('servers', {})
    if not validate_server_in_group(server_name, servers, should_exist=True):
        return 1

    # Validate regex pattern
    import re
    try:
        re.compile(pattern)
    except re.error as e:
        print_error(f"Invalid regex pattern: {e}")
        return 1

    # Update pattern
    print_header(f"Setting Extraction Pattern for Server: {server_name}")

    server_config = cast(dict[str, Any], servers[server_name])
    old_pattern = server_config.get('extraction_pattern', '')

    if old_pattern == pattern:
        print_warning(f"Server '{server_name}' already has this extraction pattern.")
        return 0

    server_config['extraction_pattern'] = pattern

    # Save configuration
    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"✓ Set extraction pattern for server '{server_name}'")
        if old_pattern:
            print_info(f"  Old: {old_pattern}")
        print_info(f"  New: {pattern}")

        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_add_extraction_pattern(args: Namespace) -> int:
    """Add an extraction pattern to a specific server.

    Patterns are tried in order (first match wins). Use --description to document
    what each pattern matches. Most specific patterns should be added first.

    Args:
        args: Parsed arguments with:
            - add_extraction_pattern: [server_name, regex_pattern]
            - env: Optional fixed environment name (overrides captured (?P<env>) group)
            - strip_patterns: Optional comma-separated regex patterns to remove from service name
            - description: Optional human-readable description

    Returns:
        Exit code (0 for success, 1 for error)

    Examples:
        # Pattern for {service}_db_prod_adcuris databases (strip _db suffix)
        cdc manage-source-groups --add-extraction-pattern prod \
            '^(?P<service>\\w+)_db_prod_adcuris$' \
            --env prod_adcuris --strip-patterns '_db$' \
            --description 'Service with _db suffix and prod_adcuris environment'

        # Pattern for adopus_db_{service}_prod_adcuris databases (strip _db anywhere)
        cdc manage-source-groups --add-extraction-pattern prod \
            '^(?P<service>adopus_db_\\w+)_prod_adcuris$' \
            --env prod_adcuris --strip-patterns '_db' \
            --description 'AdOpus service with _db infix and prod_adcuris environment'

        # Pattern for {service}_{env} databases
        cdc manage-source-groups --add-extraction-pattern default \
            '^(?P<service>\\w+)_(?P<env>\\w+)$' \
            --description 'Standard service_env pattern'

        # Pattern for single-word databases (implicit prod env)
        cdc manage-source-groups --add-extraction-pattern prod '^(?P<service>\\w+)$' \\
            --env prod --description 'Single word service name (implicit prod)'
    """
    server_name, pattern = args.add_extraction_pattern
    server_name = server_name.lower()

    # Load configuration
    config, server_group, server_group_name = load_config_and_get_server_group()
    if not config or not server_group or not server_group_name:
        return 1

    servers = server_group.get('servers', {})
    if not validate_server_in_group(server_name, servers, should_exist=True):
        return 1

    # Build pattern config
    from .patterns import build_extraction_pattern_config, display_pattern_info

    pattern_config = build_extraction_pattern_config(args, pattern)

    # Get server config and current patterns
    server_config = cast(dict[str, Any], servers[server_name])
    current_patterns = server_config.get('extraction_patterns', [])

    # Add new pattern to the list
    current_patterns.append(pattern_config)
    server_config['extraction_patterns'] = current_patterns

    # Save configuration
    try:
        write_server_group_yaml(server_group_name, server_group)
        print_success(f"\u2713 Added extraction pattern to server '{server_name}'")
        display_pattern_info(pattern_config, pattern)

        print_info(f"\n  Total patterns for '{server_name}': {len(current_patterns)}")
        print_info(
            "\n💡 Tip: Use 'cdc manage-source-groups --list-extraction-patterns' "
            + "to view all patterns"
        )
        print_info(
            "💡 Tip: Use 'cdc manage-source-groups --update' to re-scan databases "
            + "with new patterns"
        )

        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_list_extraction_patterns(args: Namespace) -> int:
    """List extraction patterns for all servers or a specific server.

    Args:
        args: Parsed arguments with:
            - list_extraction_patterns: Optional server name

    Returns:
        Exit code (0 for success, 1 for error)
    """
    from .patterns import display_pattern_help, display_server_patterns

    server_filter = args.list_extraction_patterns if args.list_extraction_patterns else None

    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("source-groups.yaml not found. Run 'cdc scaffold' first.")
        return 1

    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return 1

    servers = server_group.get('servers', {})
    if not servers:
        print_warning("No servers configured")
        return 1

    # Filter to specific server if requested
    if server_filter:
        if server_filter not in servers:
            print_error(f"Server '{server_filter}' not found in configuration")
            return 1
        servers = {server_filter: servers[server_filter]}

    print_header("Extraction Patterns (ordered by priority)")

    # Display patterns for each server
    has_any = False
    for server_name in sorted(servers.keys()):
        server_config = cast(dict[str, Any], servers[server_name])
        if display_server_patterns(server_name, server_config):
            has_any = True

    # Show help if no patterns configured
    if not has_any:
        display_pattern_help()

    return 0


def handle_remove_extraction_pattern(args: Namespace) -> int:
    """Remove an extraction pattern from a specific server by index.

    Args:
        args: Parsed arguments with:
            - remove_extraction_pattern: [server_name, pattern_index]

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        cdc manage-source-groups --list-extraction-patterns prod
        cdc manage-source-groups --remove-extraction-pattern prod 2
    """
    from .patterns import validate_pattern_index

    server_name, index_str = args.remove_extraction_pattern
    server_name = server_name.lower()

    # Load configuration
    config, server_group, server_group_name = load_config_and_get_server_group()
    if not config or not server_group or not server_group_name:
        return 1

    servers = server_group.get('servers', {})
    if not validate_server_in_group(server_name, servers, should_exist=True):
        return 1

    # Get server config and current patterns
    server_config = cast(dict[str, Any], servers[server_name])
    patterns = server_config.get('extraction_patterns', [])

    # Validate index
    is_valid, index = validate_pattern_index(index_str, patterns)
    if not is_valid:
        return 1

    # Remove pattern
    removed_pattern = patterns.pop(index)
    server_config['extraction_patterns'] = patterns

    # Save configuration
    try:
        write_server_group_yaml(server_group_name, server_group)

        print_success(f"✓ Removed extraction pattern [{index}] from server '{server_name}'")
        print_info(f"  Removed: {removed_pattern.get('pattern', '(missing)')}")
        print_info(f"  Remaining patterns: {len(patterns)}")

        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1
