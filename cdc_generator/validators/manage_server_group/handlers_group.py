"""CLI handlers for server group creation and listing."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from argparse import Namespace
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)

from .config import (
    PROJECT_ROOT,
    SERVER_GROUPS_FILE,
    load_server_groups,
)
from .metadata_comments import get_file_header_comments, validate_output_has_metadata
from .scaffolding import scaffold_project_structure
from .types import ServerGroupConfig


def ensure_project_structure(server_group_name: str, server_group_config: ServerGroupConfig) -> None:
    """Ensure basic directory structure exists, creating missing directories quietly.

    This runs on --update to make sure critical directories exist.

    Example:
        >>> ensure_project_structure('mygroup', {'pattern': 'db-shared', 'type': 'postgres'})
        # Creates: services/, pipeline-templates/, generated/pipelines/, etc.
    """
    critical_dirs = [
        "services",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        ".vscode",
    ]

    missing_dirs: list[str] = []
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
            # Get type from group level (new structure)
            sg_type = server_group_config.get('type', 'postgres')
            print_info("⚠️  Missing core files detected. Consider running scaffolding:")
            print_info(f"   cdc manage-source-groups --create {server_group_name} --pattern {server_group_config.get('pattern', 'db-shared')} \\")
            print_info(f"       --source-type {sg_type}")


def validate_multi_server_config(server_group: dict[str, Any]) -> list[str]:
    """Validate multi-server configuration.

    Checks:
    1. 'type' is at group level (not per-server)
    2. All sources reference valid server names in their environment entries
    3. kafka_bootstrap_servers is present on each server
    4. Each source.{env}.server references a valid server

    Args:
        server_group: The server group configuration dict

    Returns:
        List of validation error messages (empty if valid)

    Example:
        >>> errors = validate_multi_server_config({'type': 'postgres', 'servers': {'default': {}}})
        >>> print(errors)  # List of any validation issues
    """
    errors: list[str] = []

    servers = server_group.get('servers', {})
    sources = server_group.get('sources', {})

    if not isinstance(servers, dict):
        errors.append("'servers' must be a dictionary")
        return errors

    if not servers:
        errors.append("No servers configured")
        return errors

    # Check that 'default' server exists
    if 'default' not in servers:
        errors.append("Missing required 'default' server")

    # Check that 'type' is at group level (not per-server)
    group_type = server_group.get('type')
    if not group_type:
        errors.append("Missing 'type' at group level (must be 'postgres' or 'mssql')")
    elif group_type not in ('postgres', 'mssql'):
        errors.append(f"Invalid 'type': {group_type}. Must be 'postgres' or 'mssql'")

    # Check individual servers (should NOT have 'type')
    servers_dict = cast(dict[str, Any], servers)
    for srv_name, srv_config in servers_dict.items():
        srv_config_dict = cast(dict[str, Any], srv_config)
        # Warn if 'type' is at server level (should be at group level)
        if srv_config_dict.get('type'):
            errors.append(f"Server '{srv_name}' has 'type' - move 'type' to group level")

        if not srv_config_dict.get('kafka_bootstrap_servers'):
            errors.append(f"Server '{srv_name}' is missing 'kafka_bootstrap_servers'")

    # Check sources reference valid servers in their environment entries
    server_names: set[str] = set(servers_dict.keys())
    sources_dict = cast(dict[str, Any], sources)
    for source_name, source_config in sources_dict.items():
        source_config_dict = cast(dict[str, Any], source_config)
        for key, value in source_config_dict.items():
            # Skip non-environment keys
            if key == 'schemas':
                continue
            # Check environment entries for server reference
            if isinstance(value, dict) and 'database' in value:
                value_dict = cast(dict[str, Any], value)
                source_server = str(value_dict.get('server', 'default'))
                if source_server not in server_names:
                    errors.append(f"Source '{source_name}.{key}' references unknown server '{source_server}'")

    return errors


def list_server_groups() -> None:
    """List all server groups with their details.

    Example:
        >>> list_server_groups()
        # Prints formatted list of all server groups
    """
    config: dict[str, Any]
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_warning("source-groups.yaml not found – creating a new one from scratch.")
        config = {'server_group': {}}
    print_header("Server Groups")

    server_groups_obj = config.get('server_groups')
    server_groups: list[dict[str, Any]] = []
    if isinstance(server_groups_obj, list):
        server_groups_list = cast(list[Any], server_groups_obj)
        for candidate in server_groups_list:
            if isinstance(candidate, dict):
                server_groups.append(cast(dict[str, Any], candidate))

    for group in server_groups:
        name = str(group.get('name', 'unknown'))
        group_type = str(group.get('server_group_type', 'unknown'))
        server_obj = group.get('server')
        if isinstance(server_obj, dict):
            server_data = cast(dict[str, Any], server_obj)
        else:
            server_data = {}
        db_type = str(server_data.get('type', 'unknown'))
        databases_obj = group.get('databases')
        if isinstance(databases_obj, list):
            databases = cast(list[Any], databases_obj)
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
    """Handle adding a new server group with multi-server support.

    Structure created:
    - type: At group level (enforced for all servers)
    - servers: Dict of server configs (without type)
    - sources: Dict of source configs (instead of services)

    Args:
        args: Parsed command-line arguments with:
            - add_group: Server group name
            - source_type: 'postgres' or 'mssql'
            - mode: 'db-shared' or 'db-per-tenant'
            - host, port, user, password: Connection details

    Returns:
        Exit code (0 for success, 1 for error)
    """
    if not all([args.source_type, args.host, args.port, args.user, args.password]):
        print_error("--add-group requires: --source-type, --host, --port, --user, --password")
        return 1

    config: dict[str, Any]
    try:
        config = load_server_groups()
    except FileNotFoundError:
        print_warning("source-groups.yaml not found. Creating a fresh configuration.")
        config = {}

    # Check if group already exists (flat structure: name is root key with 'pattern' field)
    for name, group_data in config.items():
        if isinstance(group_data, dict) and 'pattern' in group_data:
            if name == args.add_group:
                print_error(f"Server group '{args.add_group}' already exists")
                return 1

    # Get kafka_topology (default to 'shared' if not provided)
    kafka_topology = getattr(args, 'kafka_topology', 'shared')
    kafka_bootstrap = getattr(args, 'kafka_bootstrap_servers', '${KAFKA_BOOTSTRAP_SERVERS}')

    # Create new server group with multi-server structure
    # NOTE: 'type' is at group level (enforced for all servers), not per-server
    # NOTE: 'sources' instead of 'services' for unified terminology
    new_group: dict[str, Any] = {
        'pattern': args.mode,
        'type': args.source_type,  # Database type at group level
        'description': f"{'Multi-tenant' if args.mode == 'db-per-tenant' else 'Shared'} {args.source_type.upper()} server",
        'kafka_topology': kafka_topology,
        'servers': {
            'default': {
                # No 'type' here - it's at group level
                'host': args.host,
                'port': args.port,
                'user': args.user,
                'password': args.password,
                'kafka_bootstrap_servers': kafka_bootstrap,
            }
        },
        'sources': {}  # 'sources' instead of 'services'
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

    # Build output with metadata comments
    output_lines: list[str] = []

    # Add file header comments
    header_comments = get_file_header_comments()
    output_lines.extend(header_comments)
    output_lines.append("")  # Blank line after header

    # Add server group separator
    output_lines.append("# ============================================================================")
    if server_group_name == 'adopus':
        output_lines.append("# AdOpus Server Group (db-per-tenant)")
    elif server_group_name == 'asma':
        output_lines.append("# ASMA Server Group (db-shared)")
    else:
        pattern_label = "db-per-tenant" if args.mode == "db-per-tenant" else "db-shared"
        output_lines.append(f"# {server_group_name.title()} Server Group ({pattern_label})")
    output_lines.append("# ============================================================================")
    output_lines.append(f"{server_group_name}:")

    # Dump the server group YAML
    sg_yaml = yaml.dump(new_group, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
    sg_lines = sg_yaml.strip().split('\n')
    for line in sg_lines:
        output_lines.append(f"  {line}")  # 2 spaces indent (flat structure)

    # Validate before writing
    validate_output_has_metadata(output_lines)

    # Save configuration
    with open(SERVER_GROUPS_FILE, 'w') as f:
        f.write('\n'.join(output_lines))
        f.write('\n')

    print_success(f"✓ Added server group '{args.add_group}' ({args.mode})")

    # Scaffold project structure
    print_info("\n📂 Scaffolding project structure...")
    try:
        # Prepare servers dict for env.example generation
        servers_for_scaffold = {
            'default': {'type': args.source_type}
        }
        scaffold_project_structure(
            server_group_name=server_group_name,
            pattern=args.mode,
            source_type=args.source_type,
            project_root=PROJECT_ROOT,
            kafka_topology=kafka_topology,
            servers=servers_for_scaffold,
        )
    except Exception as e:
        print_warning(f"⚠️  Scaffolding encountered issues: {e}")
        print_info("You may need to create some directories/files manually")

    # Update docker-compose.yml with database services
    print_info("\n🐳 Updating docker-compose.yml...")
    try:
        from cdc_generator.helpers.update_compose import update_docker_compose
        update_docker_compose(
            source_type=args.source_type,
            project_root=PROJECT_ROOT
        )
    except Exception as e:
        print_warning(f"⚠️  Could not update docker-compose.yml: {e}")
        print_info("You may need to add database services manually")

    print_info("\n📋 Next steps:")
    print_info("  1. cp .env.example .env")
    print_info("  2. Edit .env with your database credentials")
    print_info("  3. cdc manage-source-groups --update")
    if args.mode == 'db-per-tenant':
        print_info("  4. Update service field in source-groups.yaml")
    print_info("  5. docker compose up -d")

    return 0
