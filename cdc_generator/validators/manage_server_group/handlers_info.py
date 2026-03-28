"""CLI handler for displaying server group information."""

from argparse import Namespace
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error

from .config import (
    get_single_server_group,
    load_server_groups,
)
from cdc_generator.helpers.topology_runtime import (
    resolve_broker_topology,
    resolve_runtime_mode,
    resolve_topology,
    resolve_runtime_engine,
    resolve_topology_kind,
    topology_uses_broker,
)


def handle_info(args: Namespace) -> int:  # noqa: ARG001, PLR0915
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

    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("source-groups.yaml not found. Run 'cdc scaffold' first.")
        return 1

    # Use get_single_server_group for flat format
    sg_config = get_single_server_group(config)

    if not sg_config:
        print_error("No server group found in source-groups.yaml")
        return 1

    sg_name = sg_config.get('name', 'unnamed')
    pattern = sg_config.get('pattern', 'unknown')
    sg_type = sg_config.get('type', 'unknown')  # Type at group level
    servers = sg_config.get('servers', {})
    sources = sg_config.get('sources', {})
    database_ref = sg_config.get('database_ref')
    db_exclude = sg_config.get('database_exclude_patterns', [])
    schema_exclude = sg_config.get('schema_exclude_patterns', [])
    table_include = sg_config.get('table_include_patterns', [])
    table_exclude = sg_config.get('table_exclude_patterns', [])
    description = sg_config.get('description', '')
    include_pattern = sg_config.get('include_pattern')
    environment_aware = sg_config.get('environment_aware', False)
    topology = resolve_topology(cast(dict[str, Any], sg_config), source_type=str(sg_type))
    broker_topology = resolve_broker_topology(sg_config, topology=topology)
    runtime_mode = resolve_runtime_mode(
        cast(dict[str, Any], sg_config),
        topology=topology,
        source_type=str(sg_type),
    )
    topology_kind = resolve_topology_kind(cast(dict[str, Any], sg_config))
    runtime_engine = resolve_runtime_engine(
        cast(dict[str, Any], sg_config),
        topology_kind=topology_kind,
    )

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
    print(f"    {Colors.YELLOW}Topology:{Colors.RESET}        {topology or 'unknown'}")
    print(f"    {Colors.YELLOW}Runtime Mode:{Colors.RESET}    {runtime_mode}")
    if broker_topology is not None:
        print(f"    {Colors.YELLOW}Broker Topology:{Colors.RESET} {broker_topology}")
    print(f"    {Colors.YELLOW}Topology Kind:{Colors.RESET}   {topology_kind}")
    print(f"    {Colors.YELLOW}Runtime Engine:{Colors.RESET}  {runtime_engine}")

    # Server configurations (multiple servers)
    servers_dict = cast(dict[str, Any], servers)
    print(f"\n    {Colors.BLUE}📡 Servers ({len(servers_dict)}):{Colors.RESET}")
    for srv_name, srv_config in servers_dict.items():
        srv = cast(dict[str, Any], srv_config)
        print(f"        {Colors.GREEN}▶{Colors.RESET} {srv_name}")
        print(f"            {Colors.DIM}Host:{Colors.RESET} {srv.get('host', 'N/A')}")
        print(f"            {Colors.DIM}Port:{Colors.RESET} {srv.get('port', 'N/A')}")
        print(f"            {Colors.DIM}User:{Colors.RESET} {srv.get('user', 'N/A')}")
        if topology_uses_broker(topology):
            print(f"            {Colors.DIM}Kafka:{Colors.RESET} {srv.get('kafka_bootstrap_servers', 'N/A')}")

    # Include / exclude patterns
    if db_exclude or schema_exclude or table_include or table_exclude:
        if table_include:
            print(f"\n    {Colors.BLUE}✅ Include Patterns:{Colors.RESET}")
            print(f"        {Colors.DIM}Tables:{Colors.RESET}")
            for p in table_include:
                print(f"            • {p}")

        print(f"\n    {Colors.BLUE}🚫 Exclude Patterns:{Colors.RESET}")
        if db_exclude:
            print(f"        {Colors.DIM}Databases:{Colors.RESET}")
            for p in db_exclude:
                print(f"            • {p}")
        if schema_exclude:
            print(f"        {Colors.DIM}Schemas:{Colors.RESET}")
            for p in schema_exclude:
                print(f"            • {p}")
        if table_exclude:
            print(f"        {Colors.DIM}Tables:{Colors.RESET}")
            for p in table_exclude:
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
