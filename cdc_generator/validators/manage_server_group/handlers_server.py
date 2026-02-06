"""CLI handlers for multi-server management."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import Dict, Any, List, cast, Optional
from argparse import Namespace

from .config import (
    SERVER_GROUPS_FILE,
    load_server_groups,
    get_single_server_group,
)
from .metadata_comments import get_file_header_comments
from cdc_generator.helpers.helpers_logging import (
    print_header, 
    print_info, 
    print_success, 
    print_warning, 
    print_error
)


def default_connection_placeholders(source_type: str, server_name: str, kafka_topology: str) -> Dict[str, str]:
    """Return default environment variable placeholders for a server.
    
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
    prefix = 'POSTGRES_SOURCE' if source_type == 'postgres' else 'MSSQL_SOURCE'
    
    # Non-default servers get postfix
    if server_name != 'default':
        postfix = f"_{server_name.upper()}"
        placeholders = {
            'host': f"${{{prefix}_HOST{postfix}}}",
            'port': f"${{{prefix}_PORT{postfix}}}",
            'user': f"${{{prefix}_USER{postfix}}}",
            'password': f"${{{prefix}_PASSWORD{postfix}}}",
        }
    else:
        placeholders = {
            'host': f"${{{prefix}_HOST}}",
            'port': f"${{{prefix}_PORT}}",
            'user': f"${{{prefix}_USER}}",
            'password': f"${{{prefix}_PASSWORD}}",
        }
    
    # Kafka bootstrap servers: depends on topology
    if kafka_topology == 'per-server':
        postfix = f"_{server_name.upper()}"
        placeholders['kafka_bootstrap_servers'] = f"${{KAFKA_BOOTSTRAP_SERVERS{postfix}}}"
    else:
        # shared topology: same for all servers
        placeholders['kafka_bootstrap_servers'] = '${KAFKA_BOOTSTRAP_SERVERS}'
    
    return placeholders


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
        cdc manage-server-group --add-server analytics --source-type postgres \\
            --host '${POSTGRES_SOURCE_HOST_ANALYTICS}'
    """
    server_name = args.add_server.lower()
    
    # Validate server name
    if server_name == 'default':
        print_error("Cannot add a server named 'default' - it already exists by convention")
        return 1
    
    if not server_name.isidentifier():
        print_error(f"Invalid server name '{server_name}'. Must be a valid identifier (alphanumeric and underscores).")
        return 1
    
    # Load existing configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found. Run 'cdc scaffold' first to create a server group.")
        return 1
    
    # Get the server group
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return 1
    
    # Get server group name (root key)
    server_group_name: Optional[str] = None
    for name, data in config.items():
        if 'pattern' in cast(Dict[str, Any], data):
            server_group_name = name
            break
    
    if not server_group_name:
        print_error("Could not determine server group name")
        return 1
    
    # Check if server already exists
    servers: Dict[str, Any] = server_group.get('servers', {})
    if not servers:
        # Migrate from old single-server format
        print_error("Server group uses legacy single-server format. Please migrate first.")
        return 1
    
    if server_name in servers:
        print_error(f"Server '{server_name}' already exists")
        return 1
    
    # Get source type from GROUP level (not server level)
    # This is the new structure where 'type' is at group level
    group_type = server_group.get('type')
    
    # Allow override via --source-type, but validate it matches
    source_type = getattr(args, 'source_type', None)
    
    if not group_type and not source_type:
        print_error("Could not determine source type. Use --source-type to specify.")
        return 1
    
    # If both provided, they must match
    if group_type and source_type and group_type != source_type:
        print_error(f"Server type '{source_type}' does not match group type '{group_type}'")
        print_info("All servers in a server group must have the same database type (defined at group level).")
        return 1
    
    # Use group type as the source of truth
    final_source_type = str(group_type or source_type)
    
    # Get kafka topology
    kafka_topology = cast(str, server_group.get('kafka_topology', 'shared'))
    
    # Generate default placeholders
    placeholders = default_connection_placeholders(final_source_type, server_name, kafka_topology)
    
    # Create new server config (NO 'type' - it's at group level)
    new_server: Dict[str, Any] = {
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
    
    # Use yaml_writer to save with proper formatting
    try:
        # Build and write the YAML with metadata comments preserved
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        # Add server group section
        pattern = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {server_group_name.title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        # Don't include 'name' in YAML output - root key is the name
        sg_to_save = {k: v for k, v in server_group.items() if k != 'name'}
        sg_yaml = yaml.dump(sg_to_save, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
        print_success(f"✓ Added server '{server_name}' to server group '{server_group_name}'")
        print_info(f"  Type: {source_type}")
        print_info(f"  Host: {new_server['host']}")
        print_info(f"  Kafka: {new_server['kafka_bootstrap_servers']}")
        
        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1


def handle_list_servers(args: Namespace) -> int:
    """Handle listing all servers in the server group.
    
    Args:
        args: Parsed command-line arguments (not used, but required for handler signature)
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found")
        return 1
    
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found")
        return 1
    
    # Get server group name
    server_group_name = "Unknown"
    for name, data in config.items():
        if 'pattern' in cast(Dict[str, Any], data):
            server_group_name = name
            break
    
    servers: Dict[str, Any] = server_group.get('servers', {})
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
    
    for name, server_config in servers.items():
        srv = cast(Dict[str, Any], server_config)
        host = str(srv.get('host', 'N/A'))
        port = str(srv.get('port', 'N/A'))
        kafka_bs = str(srv.get('kafka_bootstrap_servers', 'N/A'))
        
        marker = "⭐" if name == 'default' else "📡"
        print_info(f"\n{marker} {name}")
        print_info(f"    Type: {group_type}")  # Type from group level
        print_info(f"    Host: {host}")
        print_info(f"    Port: {port}")
        print_info(f"    Kafka: {kafka_bs}")
    
    # Count sources per server (use 'sources' with fallback to 'services')
    sources: Dict[str, Any] = server_group.get('sources', server_group.get('services', {}))
    if sources:
        print_info("\n📦 Sources by server:")
        server_source_count: Dict[str, int] = {s: 0 for s in servers}
        for _src_name, src_config in sources.items():
            src = cast(Dict[str, Any], src_config)
            # Check each environment entry for server reference
            for key, value in src.items():
                if key == 'schemas':
                    continue
                if isinstance(value, dict) and 'server' in value:
                    val = cast(Dict[str, Any], value)
                    src_server = str(val.get('server', 'default'))
                    if src_server in server_source_count:
                        server_source_count[src_server] += 1
        
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
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found")
        return 1
    
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found")
        return 1
    
    # Get server group name
    server_group_name: Optional[str] = None
    for name, data in config.items():
        if 'pattern' in cast(Dict[str, Any], data):
            server_group_name = name
            break
    
    if not server_group_name:
        print_error("Could not determine server group name")
        return 1
    
    servers: Dict[str, Any] = server_group.get('servers', {})
    if not servers:
        print_error("Server group uses legacy format")
        return 1
    
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found")
        return 1
    
    # Check if any sources use this server (use 'sources' with fallback to 'services')
    sources: Dict[str, Any] = server_group.get('sources', server_group.get('services', {}))
    sources_using_server: List[str] = []
    for src_name, src_config in sources.items():
        src = cast(Dict[str, Any], src_config)
        # Check each environment entry for server reference
        for key, value in src.items():
            if key == 'schemas':
                continue
            if isinstance(value, dict) and 'server' in value:
                val = cast(Dict[str, Any], value)
                if val.get('server') == server_name:
                    sources_using_server.append(str(src_name))
                    break
    
    if sources_using_server:
        print_error(f"Cannot remove server '{server_name}' - it has {len(sources_using_server)} source(s):")
        for src in sources_using_server[:5]:
            print_info(f"    • {src}")
        if len(sources_using_server) > 5:
            print_info(f"    ... and {len(sources_using_server) - 5} more")
        print_info("\nRemove or reassign these sources first.")
        return 1
    
    # Remove the server
    del servers[server_name]
    
    # Save the updated configuration
    try:
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        pattern = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {str(server_group_name).title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        sg_to_save = {k: v for k, v in server_group.items() if k != 'name'}
        sg_yaml = yaml.dump(sg_to_save, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
        print_success(f"✓ Removed server '{server_name}' from server group '{server_group_name}'")
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
        print_error("server_group.yaml not found")
        return 1
    
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found")
        return 1
    
    # Get server group name
    server_group_name: Optional[str] = None
    for name, data in config.items():
        if 'servers' in cast(Dict[str, Any], data):
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
    servers: Dict[str, Any] = server_group.get('servers', {})
    for srv_name, srv_config in servers.items():
        srv = cast(Dict[str, Any], srv_config)
        if new_topology == 'per-server':
            # For per-server topology:
            # - 'default' server uses ${KAFKA_BOOTSTRAP_SERVERS} (no postfix)
            # - Other servers use ${KAFKA_BOOTSTRAP_SERVERS_<NAME>}
            if srv_name == 'default':
                srv['kafka_bootstrap_servers'] = '${KAFKA_BOOTSTRAP_SERVERS}'
            else:
                postfix = f"_{srv_name.upper()}"
                srv['kafka_bootstrap_servers'] = f"${{KAFKA_BOOTSTRAP_SERVERS{postfix}}}"
        else:
            # For shared topology: all servers use the same Kafka cluster
            srv['kafka_bootstrap_servers'] = '${KAFKA_BOOTSTRAP_SERVERS}'
    
    # Save the updated configuration
    try:
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        pattern = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {str(server_group_name).title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        sg_yaml = yaml.dump(server_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
        print_success(f"✓ Changed Kafka topology from '{current_topology}' to '{new_topology}'")
        
        # Show updated bootstrap servers
        print_info("\nUpdated kafka_bootstrap_servers:")
        for srv_name, srv_config in servers.items():
            srv = cast(Dict[str, Any], srv_config)
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
        cdc manage-server-group --set-extraction-pattern default '^(?P<service>\\w+)_(?P<env>\\w+)$'
    """
    server_name, pattern = args.set_extraction_pattern
    server_name = server_name.lower()
    
    # Load existing configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found. Run 'cdc scaffold' first.")
        return 1
    
    # Get single server group
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return 1
    
    server_group_name = server_group.get('name', 'unknown')
    servers = server_group.get('servers', {})
    
    # Validate server exists
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found in server group.")
        print_info(f"Available servers: {', '.join(servers.keys())}")
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
    
    server_config = cast(Dict[str, Any], servers[server_name])
    old_pattern = server_config.get('extraction_pattern', '')
    
    if old_pattern == pattern:
        print_warning(f"Server '{server_name}' already has this extraction pattern.")
        return 0
    
    server_config['extraction_pattern'] = pattern
    
    # Save configuration
    try:
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        pattern_type = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern_type)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {str(server_group_name).title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        sg_yaml = yaml.dump(server_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
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
        cdc manage-server-group --add-extraction-pattern prod '^(?P<service>\\w+)_db_prod_adcuris$' \\
            --env prod_adcuris --strip-patterns '_db$' \\
            --description 'Service with _db suffix and prod_adcuris environment'
        
        # Pattern for adopus_db_{service}_prod_adcuris databases (strip _db anywhere)
        cdc manage-server-group --add-extraction-pattern prod '^(?P<service>adopus_db_\\w+)_prod_adcuris$' \\
            --env prod_adcuris --strip-patterns '_db' \\
            --description 'AdOpus service with _db infix and prod_adcuris environment'
        
        # Pattern for {service}_{env} databases
        cdc manage-server-group --add-extraction-pattern default '^(?P<service>\\w+)_(?P<env>\\w+)$' \\
            --description 'Standard service_env pattern'
        
        # Pattern for single-word databases (implicit prod env)
        cdc manage-server-group --add-extraction-pattern prod '^(?P<service>\\w+)$' \\
            --env prod --description 'Single word service name (implicit prod)'
    """
    server_name, pattern = args.add_extraction_pattern
    server_name = server_name.lower()
    
    # Load existing configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found. Run 'cdc scaffold' first.")
        return 1
    
    # Get single server group
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return 1
    
    server_group_name = server_group.get('name', 'unknown')
    servers = server_group.get('servers', {})
    
    # Validate server exists
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found in server group.")
        print_info(f"Available servers: {', '.join(servers.keys())}")
        return 1
    
    # Build pattern config
    pattern_config: Dict[str, Any] = {'pattern': pattern}
    
    if hasattr(args, 'env') and args.env:
        pattern_config['env'] = args.env
    
    if hasattr(args, 'strip_patterns') and args.strip_patterns:
        patterns = [s.strip() for s in args.strip_patterns.split(',') if s.strip()]
        if patterns:
            pattern_config['strip_patterns'] = patterns
    
    if hasattr(args, 'env_mapping') and args.env_mapping:
        # Parse env_mapping list in format ['from:to', 'from2:to2']
        env_map: Dict[str, str] = {}
        for mapping_str in args.env_mapping:
            if ':' in mapping_str:
                from_env, to_env = mapping_str.split(':', 1)
                env_map[from_env.strip()] = to_env.strip()
        if env_map:
            pattern_config['env_mapping'] = env_map
    
    if hasattr(args, 'description') and args.description:
        pattern_config['description'] = args.description
    
    # Get server config and current patterns
    server_config = cast(Dict[str, Any], servers[server_name])
    current_patterns = server_config.get('extraction_patterns', [])
    
    # Add new pattern to the list
    current_patterns.append(pattern_config)
    server_config['extraction_patterns'] = current_patterns
    
    # Save configuration
    try:
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        pattern_type = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern_type)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {str(server_group_name).title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        sg_yaml = yaml.dump(server_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
        print_success(f"✓ Added extraction pattern to server '{server_name}'")
        print_info(f"  Pattern: {pattern}")
        if pattern_config.get('env'):
            print_info(f"  Fixed env: {pattern_config['env']}")
        if pattern_config.get('strip_suffixes'):
            print_info(f"  Strip suffixes: {', '.join(pattern_config['strip_suffixes'])}")
        if pattern_config.get('description'):
            print_info(f"  Description: {pattern_config['description']}")
        
        print_info(f"\n  Total patterns for '{server_name}': {len(current_patterns)}")
        print_info("\n💡 Tip: Use 'cdc manage-server-group --list-extraction-patterns' to view all patterns")
        print_info("💡 Tip: Use 'cdc manage-server-group --update' to re-scan databases with new patterns")
        
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
    server_filter = args.list_extraction_patterns if args.list_extraction_patterns else None
    
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found. Run 'cdc scaffold' first.")
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
    
    has_any = False
    for server_name in sorted(servers.keys()):
        server_config = cast(Dict[str, Any], servers[server_name])
        patterns = server_config.get('extraction_patterns', [])
        
        print()
        print_info(f"📍 Server: {server_name}")
        
        if patterns:
            has_any = True
            for idx, pattern_config in enumerate(patterns):
                print_info(f"  [{idx}] Pattern: {pattern_config.get('pattern', '(missing)')}")
                if pattern_config.get('env'):
                    print_info(f"      Fixed env: {pattern_config['env']}")
                if pattern_config.get('strip_patterns'):
                    print_info(f"      Strip patterns: {', '.join(pattern_config['strip_patterns'])}")
                if pattern_config.get('env_mapping'):
                    mappings = ', '.join([f"{k}→{v}" for k, v in pattern_config['env_mapping'].items()])
                    print_info(f"      Env mapping: {mappings}")
                if pattern_config.get('description'):
                    print_info(f"      Description: {pattern_config['description']}")
        else:
            print_warning(f"  (no extraction patterns configured)")
            # Check for old single pattern
            single_pattern = server_config.get('extraction_pattern')
            if single_pattern:
                print_info(f"  Legacy single pattern: {single_pattern}")
    
    if not has_any:
        print()
        print_info("💡 Add extraction patterns for a server:")
        print_info("   cdc manage-server-group --add-extraction-pattern default '^(?P<service>\\w+)_(?P<env>\\w+)$'")
        print_info("   cdc manage-server-group --add-extraction-pattern prod '^(?P<service>\\w+)_db_prod_adcuris$' \\")
        print_info("       --env prod_adcuris --strip-suffixes '_db'")
    
    return 0


def handle_remove_extraction_pattern(args: Namespace) -> int:
    """Remove an extraction pattern from a specific server by index.
    
    Args:
        args: Parsed arguments with:
            - remove_extraction_pattern: [server_name, pattern_index]
    
    Returns:
        Exit code (0 for success, 1 for error)
        
    Example:
        cdc manage-server-group --list-extraction-patterns prod
        cdc manage-server-group --remove-extraction-pattern prod 2
    """
    server_name, index_str = args.remove_extraction_pattern
    server_name = server_name.lower()
    
    try:
        index = int(index_str)
    except ValueError:
        print_error(f"Invalid index '{index_str}'. Must be an integer.")
        return 1
    
    # Load existing configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("server_group.yaml not found. Run 'cdc scaffold' first.")
        return 1
    
    # Get single server group
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in configuration")
        return 1
    
    server_group_name = server_group.get('name', 'unknown')
    servers = server_group.get('servers', {})
    
    # Validate server exists
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found in server group.")
        print_info(f"Available servers: {', '.join(servers.keys())}")
        return 1
    
    # Get server config and current patterns
    server_config = cast(Dict[str, Any], servers[server_name])
    patterns = server_config.get('extraction_patterns', [])
    
    if not patterns:
        print_error(f"No extraction patterns configured for server '{server_name}'")
        return 1
    
    if index < 0 or index >= len(patterns):
        print_error(f"Invalid index {index}. Must be between 0 and {len(patterns) - 1}")
        return 1
    
    # Remove pattern
    removed_pattern = patterns.pop(index)
    server_config['extraction_patterns'] = patterns
    
    # Save configuration
    try:
        output_lines: List[str] = []
        header_comments = get_file_header_comments()
        output_lines.extend(header_comments)
        output_lines.append("")
        
        pattern_type = server_group.get('pattern', 'db-per-tenant')
        pattern_label = str(pattern_type)
        output_lines.append("# ============================================================================")
        output_lines.append(f"# {str(server_group_name).title()} Server Group ({pattern_label})")
        output_lines.append("# ============================================================================")
        output_lines.append(f"{server_group_name}:")
        
        sg_yaml = yaml.dump(server_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
        
        print_success(f"✓ Removed extraction pattern [{index}] from server '{server_name}'")
        print_info(f"  Removed: {removed_pattern.get('pattern', '(missing)')}")
        print_info(f"  Remaining patterns: {len(patterns)}")
        
        return 0
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        return 1
