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
    get_server_group_by_name,
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
        'server_group_type': args.mode,
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
    
    # Add service field based on mode
    if args.mode == 'db-per-tenant':
        new_group['service'] = args.add_group  # Use group name as service name
    
    config.setdefault('server_groups', []).append(new_group)  # type: ignore[misc]
    
    # Save configuration
    with open(SERVER_GROUPS_FILE, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
    
    print_success(f"✓ Added server group '{args.add_group}' ({args.mode})")
    print_info(f"\nNext steps:")
    print_info(f"  1. Run --update to populate databases: cdc manage-server-group --update --server-group {args.add_group}")
    if args.mode == 'db-per-tenant':
        print_info(f"  2. Update service field in server-groups.yaml")
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
    """Handle updating server group(s) from database inspection."""
    config = load_server_groups()
    
    # Get list of server groups to update
    if args.all:
        server_groups = config.get('server_groups', [])
        print_header(f"Updating All Server Groups ({len(server_groups)} groups)")
    else:
        server_group_name = args.server_group or 'asma'
        server_group = get_server_group_by_name(config, server_group_name)
        
        if not server_group:
            print_error(f"Server group '{server_group_name}' not found")
            print_info("Available server groups:")
            for sg in config.get('server_groups', []):
                print_info(f"  • {sg.get('name')}")
            return 1
        
        server_groups = [server_group]
        print_header(f"Updating Server Group: {server_group_name}")
    
    # Process each server group
    all_success = True
    for server_group in server_groups:
        sg_name = server_group.get('name')
        sg_type = server_group.get('server', {}).get('type')
        include_pattern = server_group.get('include_pattern')
        
        print_info(f"\n{'='*80}")
        print_info(f"Server Group: {sg_name}")
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
                all_success = False
                continue
            
            if not databases:
                print_warning(f"No databases found for server group '{sg_name}'")
                continue
            
            print_success(f"\nFound {len(databases)} database(s)")
            
            # Update the YAML file
            if update_server_group_yaml(sg_name, databases):  # type: ignore[arg-type]
                print_success(f"✓ Updated server group '{sg_name}' with {len(databases)} databases")
                
                # Update VS Code schema
                update_vscode_schema(databases)
                
                # Update Fish completions
                update_completions()
            else:
                print_error(f"Failed to update server group '{sg_name}'")
                all_success = False
                
        except Exception as e:
            print_error(f"Error updating server group '{sg_name}': {e}")
            import traceback
            traceback.print_exc()
            all_success = False
    
    # Regenerate validation schemas once after all server groups are updated
    if all_success:
        # Only regenerate schemas for the updated server group(s)
        updated_group_names = [sg.get('name') for sg in server_groups]
        regenerate_all_validation_schemas(updated_group_names)  # type: ignore[arg-type]
    
    return 0 if all_success else 1
