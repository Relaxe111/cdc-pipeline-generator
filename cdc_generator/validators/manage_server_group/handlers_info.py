"""CLI handler for displaying server group information."""

from argparse import Namespace
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error

from .config import (
    get_single_server_group,
    load_server_groups,
)


def handle_info(args: Namespace) -> int:
    """Display detailed server group information with colors and formatting.
    
    Args:
        args: Parsed command-line arguments (not used, but required for handler signature)
        
    Returns:
        Exit code (0 for success, 1 for error)
        
    Example:
        >>> args = Namespace()
        >>> handle_info(args)
        0  # Displays formatted server group info
    """
    from cdc_generator.helpers.helpers_logging import Colors

    config = load_server_groups()

    # Use get_single_server_group for flat format
    sg_config = get_single_server_group(config)

    if not sg_config:
        print_error("No server group found in server_group.yaml")
        return 1

    sg_name = sg_config.get('name', 'unnamed')
    pattern = sg_config.get('pattern', 'unknown')
    sg_type = sg_config.get('type', 'unknown')  # Type at group level
    servers = sg_config.get('servers', {})
    sources = sg_config.get('sources', {})
    database_ref = sg_config.get('database_ref')
    db_exclude = sg_config.get('database_exclude_patterns', [])
    schema_exclude = sg_config.get('schema_exclude_patterns', [])
    description = sg_config.get('description', '')
    include_pattern = sg_config.get('include_pattern')
    environment_aware = sg_config.get('environment_aware', False)
    kafka_topology = sg_config.get('kafka_topology', 'shared')

    # Header
    print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}")
    print(f"{Colors.BLUE}🔧  Server Group: {Colors.GREEN}{sg_name}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*80}{Colors.RESET}\n")

    # Basic info
    print(f"    {Colors.YELLOW}Pattern:{Colors.RESET}         {pattern}")
    print(f"    {Colors.YELLOW}Type:{Colors.RESET}            {sg_type}")
    if description:
        print(f"    {Colors.YELLOW}Description:{Colors.RESET}     {description}")
    if database_ref:
        print(f"    {Colors.YELLOW}Database Ref:{Colors.RESET}    {database_ref}")
    if include_pattern:
        print(f"    {Colors.YELLOW}Include Pattern:{Colors.RESET} {include_pattern}")
    print(f"    {Colors.YELLOW}Environment Aware:{Colors.RESET} {environment_aware}")
    print(f"    {Colors.YELLOW}Kafka Topology:{Colors.RESET}  {kafka_topology}")

    # Server configurations (multiple servers)
    servers_dict = cast(dict[str, Any], servers)
    print(f"\n    {Colors.BLUE}📡 Servers ({len(servers_dict)}):{Colors.RESET}")
    for srv_name, srv_config in servers_dict.items():
        srv = cast(dict[str, Any], srv_config)
        print(f"        {Colors.GREEN}▶{Colors.RESET} {srv_name}")
        print(f"            {Colors.DIM}Host:{Colors.RESET} {srv.get('host', 'N/A')}")
        print(f"            {Colors.DIM}Port:{Colors.RESET} {srv.get('port', 'N/A')}")
        print(f"            {Colors.DIM}User:{Colors.RESET} {srv.get('user', 'N/A')}")
        print(f"            {Colors.DIM}Kafka:{Colors.RESET} {srv.get('kafka_bootstrap_servers', 'N/A')}")

    # Exclude patterns
    if db_exclude or schema_exclude:
        print(f"\n    {Colors.BLUE}🚫 Exclude Patterns:{Colors.RESET}")
        if db_exclude:
            print(f"        {Colors.DIM}Databases:{Colors.RESET}")
            for p in db_exclude:
                print(f"            • {p}")
        if schema_exclude:
            print(f"        {Colors.DIM}Schemas:{Colors.RESET}")
            for p in schema_exclude:
                print(f"            • {p}")

    # Sources (unified structure for both patterns)
    sources_dict = cast(dict[str, Any], sources)
    print(f"\n    {Colors.BLUE}📦 Sources ({len(sources_dict)}):{Colors.RESET}")
    if sources_dict:
        for source_name, source_config in sources_dict.items():
            src = cast(dict[str, Any], source_config)
            schemas = src.get('schemas', [])

            print(f"        {Colors.GREEN}▶{Colors.RESET} {source_name}")
            if schemas:
                print(f"            {Colors.DIM}Schemas:{Colors.RESET} {', '.join(schemas)}")

            # Show environment entries with their server references
            for key, value in src.items():
                if key == 'schemas':
                    continue
                if isinstance(value, dict) and 'database' in value:
                    env_entry = cast(dict[str, Any], value)
                    env = key
                    server_ref = str(env_entry.get('server', 'default'))
                    database = str(env_entry.get('database', 'N/A'))
                    table_count = int(env_entry.get('table_count', 0))
                    print(f"            {Colors.DIM}{env}:{Colors.RESET} {database} (server: {server_ref}, tables: {table_count})")
    else:
        print(f"        {Colors.DIM}No sources configured yet. Run --update to discover databases.{Colors.RESET}")

    print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}\n")

    return 0
