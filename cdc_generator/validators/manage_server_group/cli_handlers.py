"""CLI command handlers for server group management."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import Dict, Any, List, cast
from argparse import Namespace

from .config import (
    SERVER_GROUPS_FILE,
    PROJECT_ROOT,
    load_server_groups,
    load_database_exclude_patterns,
    load_schema_exclude_patterns,
    save_database_exclude_patterns,
    save_schema_exclude_patterns
)
from .scaffolding import scaffold_project_structure
from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases,
    MissingEnvironmentVariableError
)
from .yaml_writer import update_server_group_yaml
from .utils import regenerate_all_validation_schemas, update_vscode_schema, update_completions
from .metadata_comments import (
    get_file_header_comments,
    ensure_file_header_exists,
    validate_output_has_metadata
)
from cdc_generator.helpers.helpers_logging import (
    print_header, 
    print_info, 
    print_success, 
    print_warning, 
    print_error
)


def _ensure_project_structure(server_group_name: str, server_group_config: Dict[str, Any]) -> None:
    """Ensure basic directory structure exists, creating missing directories quietly.
    
    This runs on --update to make sure critical directories exist.
    """
    critical_dirs = [
        "services",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        ".vscode",
    ]
    
    missing_dirs: List[str] = []
    for dir_path in critical_dirs:
        full_path = PROJECT_ROOT / dir_path
        if not full_path.exists():
            missing_dirs.append(dir_path)
            full_path.mkdir(parents=True, exist_ok=True)
    
    if missing_dirs:
        print_info(f"📂 Created {len(missing_dirs)} missing director{'y' if len(missing_dirs) == 1 else 'ies'}")
        
        # Check if we should scaffold the full project
        needs_full_scaffold = not (PROJECT_ROOT / ".env.example").exists()
        
        if needs_full_scaffold:
            print_info("⚠️  Missing core files detected. Consider running scaffolding:")
            print_info(f"   cdc manage-server-group --create {server_group_name} --pattern {server_group_config.get('pattern', 'db-shared')} \\")
            print_info(f"       --source-type {server_group_config.get('server', {}).get('type', 'postgres')}")


def list_server_groups() -> None:
    """List all server groups."""
    config: Dict[str, Any]
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_warning("server_group.yaml not found – creating a new one from scratch.")
        config = {'server_group': {}}
    print_header("Server Groups")
    
    server_groups_obj = config.get('server_groups')
    server_groups: List[Dict[str, Any]] = []
    if isinstance(server_groups_obj, list):
        server_groups_list = cast(List[Any], server_groups_obj)
        for candidate in server_groups_list:
            if isinstance(candidate, dict):
                server_groups.append(cast(Dict[str, Any], candidate))
    
    for group in server_groups:
        name = str(group.get('name', 'unknown'))
        group_type = str(group.get('server_group_type', 'unknown'))
        server_obj = group.get('server')
        if isinstance(server_obj, dict):
            server_data = cast(Dict[str, Any], server_obj)
        else:
            server_data = {}
        db_type = str(server_data.get('type', 'unknown'))
        databases_obj = group.get('databases')
        if isinstance(databases_obj, list):
            databases = cast(List[Any], databases_obj)
        else:
            databases = []
        db_count = len(databases)
        service = str(group.get('service', 'N/A'))
        
        print(f"\n{name}")
        print(f"  Type: {group_type}")
        print(f"  DB Type: {db_type}")
        print(f"  Databases: {db_count}")
        if group_type == 'db-per-tenant':
            print(f"  Service: {service}")


def handle_add_group(args: Namespace) -> int:
    """Handle adding a new server group."""
    if not all([args.source_type, args.host, args.port, args.user, args.password]):
        print_error("--add-group requires: --source-type, --host, --port, --user, --password")
        return 1
    
    config: Dict[str, Any]
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_warning("server_group.yaml not found. Creating a fresh configuration.")
        config = {'server_group': {}}
    
    # Check if group already exists
    raw_groups = config.get('server_group')
    existing_groups: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_groups, dict):
        raw_groups_dict = cast(Dict[Any, Any], raw_groups)
        for name, group in raw_groups_dict.items():
            if isinstance(name, str) and isinstance(group, dict):
                existing_groups[name] = cast(Dict[str, Any], group)
    if args.add_group in existing_groups:
        print_error(f"Server group '{args.add_group}' already exists")
        return 1
    for group in existing_groups.values():
        if group.get('name') == args.add_group:
            print_error(f"Server group '{args.add_group}' already exists")
            return 1
    
    # Create new server group
    new_group: Dict[str, Any] = {
        'pattern': args.mode,
        'description': f"{'Multi-tenant' if args.mode == 'db-per-tenant' else 'Shared'} {args.source_type.upper()} server",
        'server': {
            'type': args.source_type,
            'host': args.host,
            'port': args.port,
            'user': args.user,
            'password': args.password
        },
        'databases': []
    }
    
    # Add extraction pattern (unified key for both db-per-tenant and db-shared)
    extraction_pattern = getattr(args, 'extraction_pattern', '')
    if extraction_pattern:
        new_group['extraction_pattern'] = extraction_pattern
    # If empty or not provided, don't add the key (will use fallback matching)
    
    # Add environment_aware for db-shared
    if args.mode == 'db-shared':
        new_group['environment_aware'] = getattr(args, 'environment_aware', True)
    
    server_group_name = args.add_group
    server_group_block = config.get('server_group')
    if not isinstance(server_group_block, dict):
        server_group_block = {}
        config['server_group'] = server_group_block
    server_group_block[server_group_name] = new_group
    
    # Build output with metadata comments
    output_lines: List[str] = []
    
    # Add file header comments
    header_comments = get_file_header_comments()
    output_lines.extend(header_comments)
    output_lines.append("")  # Blank line after header
    
    # Add server_group: key
    output_lines.append("server_group:")
    
    # Add server group separator
    output_lines.append("  # ============================================================================")
    if server_group_name == 'adopus':
        output_lines.append("  # AdOpus Server Group (db-per-tenant)")
    elif server_group_name == 'asma':
        output_lines.append("  # ASMA Server Group (db-shared)")
    else:
        pattern_label = "db-per-tenant" if args.mode == "db-per-tenant" else "db-shared"
        output_lines.append(f"  # {server_group_name.title()} Server Group ({pattern_label})")
    output_lines.append("  # ============================================================================")
    output_lines.append(f"  {server_group_name}:")
    
    # Dump the server group YAML
    sg_yaml = yaml.dump(new_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
    sg_lines = sg_yaml.strip().split('\n')
    for line in sg_lines:
        output_lines.append(f"    {line}")  # 4 spaces indent
    
    # Validate before writing
    validate_output_has_metadata(output_lines)
    
    # Save configuration
    with open(SERVER_GROUPS_FILE, 'w') as f:
        f.write('\n'.join(output_lines))
        f.write('\n')
    
    print_success(f"✓ Added server group '{args.add_group}' ({args.mode})")
    
    # Scaffold project structure
    print_info(f"\n📂 Scaffolding project structure...")
    try:
        scaffold_project_structure(
            server_group_name=server_group_name,
            pattern=args.mode,
            source_type=args.source_type,
            project_root=PROJECT_ROOT
        )
    except Exception as e:
        print_warning(f"⚠️  Scaffolding encountered issues: {e}")
        print_info("You may need to create some directories/files manually")
    
    # Update docker-compose.yml with database services
    print_info(f"\n🐳 Updating docker-compose.yml...")
    try:
        from cdc_generator.helpers.update_compose import update_docker_compose
        update_docker_compose(
            source_type=args.source_type,
            project_root=PROJECT_ROOT
        )
    except Exception as e:
        print_warning(f"⚠️  Could not update docker-compose.yml: {e}")
        print_info("You may need to add database services manually")
    
    print_info(f"\n📋 Next steps:")
    print_info(f"  1. cp .env.example .env")
    print_info(f"  2. Edit .env with your database credentials")
    print_info(f"  3. cdc manage-server-group --update")
    if args.mode == 'db-per-tenant':
        print_info(f"  4. Update service field in server_group.yaml")
    print_info(f"  5. docker compose up -d")
    
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
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_error("File 'server_group.yaml' not found in the project root.")
        print_info("\n💡 To get started, create a 'server_group.yaml' file in your repository root.")
        print_info("   Here is an example for a PostgreSQL 'db-shared' setup:")
        print_info(
            "\n"
            "    server_group:\n"
            "      asma:  # Or your implementation name\n"
            "        pattern: db-shared\n"
            "        server:\n"
            "          type: postgres\n"
            "          host: '${POSTGRES_SOURCE_HOST}'\n"
            "          port: '${POSTGRES_SOURCE_PORT}'\n"
            "          user: '${POSTGRES_SOURCE_USER}'\n"
            "          password: '${POSTGRES_SOURCE_PASSWORD}'\n"
            "        databases: [] # This will be auto-populated by --update"
        )
        return 1
        
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
    server_group_name = None
    for sg_name, sg_config in server_group_dict.items():
        sg_config['name'] = sg_name  # Add name for compatibility
        server_group = sg_config
        server_group_name = sg_name
        break
    
    if not server_group or not server_group_name:
        print_error("Failed to load server group configuration")
        return 1
    
    # Ensure directory structure exists (scaffold if missing)
    _ensure_project_structure(server_group_name, server_group)
    
    sg_name = server_group.get('name')
    sg_type = server_group.get('server', {}).get('type')
    sg_pattern = server_group.get('pattern', 'db-shared')
    include_pattern = server_group.get('include_pattern')
    
    print_header(f"Updating Server Group: {sg_name}")
    print_info(f"Type: {sg_type}")
    print_info(f"Pattern: {sg_pattern}")
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
                server_group,  # Pass full server group config for extraction patterns
                include_pattern,
                database_exclude_patterns, 
                schema_exclude_patterns
            )
        elif sg_type == 'postgres':
            databases = list_postgres_databases(
                server_config,
                server_group,  # Pass full server group config for extraction patterns
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
            
    except MissingEnvironmentVariableError as env_error:
        print_error(str(env_error))
        print_info(
            "Export the missing variable inside the dev container (e.g. `set -x NAME value`) "
            "or replace the placeholder in server_group.yaml before running --update."
        )
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
