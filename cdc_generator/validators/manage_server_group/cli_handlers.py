"""CLI command handlers for server group management."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import Dict, Any, List
from argparse import Namespace

from .config import (
    SERVER_GROUPS_FILE,
    load_server_groups,
    load_database_exclude_patterns,
    load_schema_exclude_patterns,
    save_database_exclude_patterns,
    save_schema_exclude_patterns
)
from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases
)
from .yaml_writer import update_server_group_yaml
from .utils import regenerate_all_validation_schemas, update_vscode_schema, update_completions
from cdc_generator.helpers.helpers_logging import (
    print_header, 
    print_info, 
    print_success, 
    print_warning, 
    print_error
)


def list_server_groups() -> None:
    """List all server groups."""
    config = load_server_groups()
    print_header("Server Groups")
    
    for group in config.get('server_groups', []):
        name = group.get('name', 'unknown')
        group_type = group.get('server_group_type', 'unknown')
        db_type = group.get('server', {}).get('type', 'unknown')
        db_count = len(group.get('databases', []))
        service = group.get('service', 'N/A')
        
        print(f"\n{name}")
        print(f"  Type: {group_type}")
        print(f"  DB Type: {db_type}")
        print(f"  Databases: {db_count}")
        if group_type == 'db-per-tenant':
            print(f"  Service: {service}")


def handle_add_group(args: Namespace) -> int:
    """Handle adding a new server group."""
    if not all([args.type, args.host, args.port, args.user, args.password]):
        print_error("--add-group requires: --type, --host, --port, --user, --password")
        return 1
    
    config = load_server_groups()
    
    # Check if group already exists
    for group in config.get('server_groups', []):
        if group.get('name') == args.add_group:
            print_error(f"Server group '{args.add_group}' already exists")
            return 1
    
    # Create new server group
    new_group: Dict[str, Any] = {
        'name': args.add_group,
        'pattern': args.mode,
        'description': f"{'Multi-tenant' if args.mode == 'db-per-tenant' else 'Shared'} {args.type.upper()} server",
        'server': {
            'type': args.type,
            'host': args.host,
            'port': args.port,
            'user': args.user,
            'password': args.password
        },
        'databases': []
    }
    
    config.setdefault('server_group', {})[server_group_name] = new_group  # type: ignore[misc]
    
    # Save configuration
    with open(SERVER_GROUPS_FILE, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
    
    print_success(f"✓ Added server group '{args.add_group}' ({args.mode})")
    print_info(f"\nNext steps:")
    print_info(f"  1. Run --update to populate databases: cdc manage-server-group --update --server-group {args.add_group}")
    if args.mode == 'db-per-tenant':
        print_info(f"  2. Update service field in server_group.yaml")
    print_info(f"  3. Regenerate validation schema: cdc manage-service --service <service> --generate-validation --all")
    
    return 0


def handle_add_ignore_pattern(args: Namespace) -> int:
    """Handle adding pattern(s) to the database exclude list."""
    if not args.add_to_ignore_list:
        print_error("No pattern specified")
        return 1
    
    patterns = load_database_exclude_patterns()
    
    # Support comma-separated patterns
    input_patterns = [p.strip() for p in args.add_to_ignore_list.split(',')]
    
    added: List[str] = []
    skipped: List[str] = []
    
    for pattern in input_patterns:
        if not pattern:
            continue
        
        if pattern in patterns:
            skipped.append(pattern)
            continue
        
        patterns.append(pattern)
        added.append(pattern)
    
    if added:
        save_database_exclude_patterns(patterns)
        print_success(f"✓ Added {len(added)} pattern(s) to database exclude list:")
        for p in added:
            print_info(f"  • {p}")
    
    if skipped:
        print_warning(f"Already in list ({len(skipped)}): {', '.join(skipped)}")
    
    if not added and not skipped:
        print_error("No valid patterns provided")
        return 1
    
    print_info(f"\nCurrent database exclude patterns: {patterns}")
    
    return 0


def handle_add_schema_exclude(args: Namespace) -> int:
    """Handle adding pattern(s) to the schema exclude list."""
    if not args.add_to_schema_excludes:
        print_error("No pattern specified")
        return 1
    
    patterns = load_schema_exclude_patterns()
    
    # Support comma-separated patterns
    input_patterns = [p.strip() for p in args.add_to_schema_excludes.split(',')]
    
    added: List[str] = []
    skipped: List[str] = []
    
    for pattern in input_patterns:
        if not pattern:
            continue
        
        if pattern in patterns:
            skipped.append(pattern)
            continue
        
        patterns.append(pattern)
        added.append(pattern)
    
    if added:
        save_schema_exclude_patterns(patterns)
        print_success(f"✓ Added {len(added)} pattern(s) to schema exclude list:")
        for p in added:
            print_info(f"  • {p}")
    
    if skipped:
        print_warning(f"Already in list ({len(skipped)}): {', '.join(skipped)}")
    
    if not added and not skipped:
        print_error("No valid patterns provided")
        return 1
    
    print_info(f"\nCurrent schema exclude patterns: {patterns}")
    
    return 0


def handle_update(args: Namespace) -> int:
    """Handle updating server group from database inspection.
    
    Since each implementation has only one server group, we update it directly.
    """
    config = load_server_groups()
    server_group_dict = config.get('server_group', {})
    
    if not server_group_dict:
        print_error("No server group found in server_group.yaml")
        return 1
    
    # Get the single server group (there should only be one)
    if len(server_group_dict) > 1:
        print_warning(f"Found {len(server_group_dict)} server groups, expected 1")
        print_info("Server groups: " + ", ".join(server_group_dict.keys()))
    
    # Process the first (and should be only) server group
    server_group = None
    for sg_name, sg_config in server_group_dict.items():
        sg_config['name'] = sg_name  # Add name for compatibility
        server_group = sg_config
        break
    
    if not server_group:
        print_error("Failed to load server group configuration")
        return 1
    
    sg_name = server_group.get('name')
    sg_type = server_group.get('server', {}).get('type')
    include_pattern = server_group.get('include_pattern')
    
    print_header(f"Updating Server Group: {sg_name}")
    print_info(f"Type: {sg_type}")
    if include_pattern:
        print_info(f"Include Pattern: {include_pattern}")
    print_info(f"{'='*80}\n")
    
    try:
        # Get server config from server group
        server_config = server_group.get('server', {})
        
        # Get per-group exclude patterns (or use defaults)
        database_exclude_patterns = server_group.get('database_exclude_patterns', [])
        schema_exclude_patterns = server_group.get('schema_exclude_patterns', [])
        
        # Inspect databases based on server type
        if sg_type == 'mssql':
            databases = list_mssql_databases(
                server_config, 
                include_pattern,
                database_exclude_patterns, 
                schema_exclude_patterns
            )
        elif sg_type == 'postgres':
            databases = list_postgres_databases(
                server_config, 
                include_pattern,
                database_exclude_patterns, 
                schema_exclude_patterns
            )
        else:
            print_error(f"Unknown server type: {sg_type}")
            return 1
        
        if not databases:
            print_warning(f"No databases found for server group '{sg_name}'")
            return 0
        
        print_success(f"\nFound {len(databases)} database(s)")
        
        # Update the YAML file
        if update_server_group_yaml(sg_name, databases):  # type: ignore[arg-type]
            print_success(f"✓ Updated server group '{sg_name}' with {len(databases)} databases")
            
            # Update VS Code schema
            update_vscode_schema(databases)
            
            # Update Fish completions
            update_completions()
            
            # Regenerate validation schemas
            regenerate_all_validation_schemas([sg_name])  # type: ignore[list-item]
            
            return 0
        else:
            print_error(f"Failed to update server group '{sg_name}'")
            return 1
            
    except Exception as e:
        print_error(f"Error updating server group '{sg_name}': {e}")
        import traceback
        traceback.print_exc()
        return 1


def handle_info(args: Namespace) -> int:
    """Display detailed server group information with colors and formatting."""
    from cdc_generator.helpers.helpers_logging import Colors
    
    config = load_server_groups()
    server_group_dict = config.get('server_group', {})
    
    if not server_group_dict:
        print_error("No server group found in server_group.yaml")
        return 1
    
    for sg_name, sg_config in server_group_dict.items():
        pattern = sg_config.get('pattern', 'unknown')
        server = sg_config.get('server', {})
        databases = sg_config.get('databases', [])
        database_ref = sg_config.get('database_ref')
        db_exclude = sg_config.get('database_exclude_patterns', [])
        schema_exclude = sg_config.get('schema_exclude_patterns', [])
        description = sg_config.get('description', '')
        include_pattern = sg_config.get('include_pattern')
        
        # Header
        print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}")
        print(f"{Colors.BLUE}🔧  Server Group: {Colors.GREEN}{sg_name}{Colors.RESET}")
        print(f"{Colors.BLUE}{'='*80}{Colors.RESET}\n")
        
        # Basic info
        print(f"    {Colors.YELLOW}Pattern:{Colors.RESET}         {pattern}")
        if description:
            print(f"    {Colors.YELLOW}Description:{Colors.RESET}     {description}")
        if database_ref:
            print(f"    {Colors.YELLOW}Database Ref:{Colors.RESET}    {database_ref}")
        if include_pattern:
            print(f"    {Colors.YELLOW}Include Pattern:{Colors.RESET} {include_pattern}")
        
        # Server configuration
        print(f"\n    {Colors.BLUE}📡 Server Configuration:{Colors.RESET}")
        print(f"        {Colors.DIM}Type:{Colors.RESET}     {server.get('type', 'N/A')}")
        print(f"        {Colors.DIM}Host:{Colors.RESET}     {server.get('host', 'N/A')}")
        print(f"        {Colors.DIM}Port:{Colors.RESET}     {server.get('port', 'N/A')}")
        print(f"        {Colors.DIM}User:{Colors.RESET}     {server.get('user', 'N/A')}")
        
        # Exclude patterns
        if db_exclude or schema_exclude:
            print(f"\n    {Colors.BLUE}🚫 Exclude Patterns:{Colors.RESET}")
            if db_exclude:
                print(f"        {Colors.DIM}Databases:{Colors.RESET}")
                for pattern in db_exclude:
                    print(f"            • {pattern}")
            if schema_exclude:
                print(f"        {Colors.DIM}Schemas:{Colors.RESET}")
                for pattern in schema_exclude:
                    print(f"            • {pattern}")
        
        # Databases
        print(f"\n    {Colors.BLUE}💾 Databases ({len(databases)}):{Colors.RESET}")
        if databases:
            for db in databases:
                db_name = db.get('name', 'unnamed')
                schemas = db.get('schemas', [])
                db_service = db.get('service')
                
                print(f"        {Colors.GREEN}▶{Colors.RESET} {db_name}")
                if db_service and pattern == 'db-shared':
                    print(f"            {Colors.DIM}Service:{Colors.RESET} {db_service}")
                if schemas:
                    print(f"            {Colors.DIM}Schemas:{Colors.RESET} {', '.join(schemas)}")
        else:
            print(f"        {Colors.DIM}No databases configured yet{Colors.RESET}")
        
        print(f"\n{Colors.BLUE}{'='*80}{Colors.RESET}\n")
    
    return 0
