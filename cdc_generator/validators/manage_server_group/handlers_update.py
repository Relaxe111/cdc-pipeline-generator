"""CLI handler for updating server group from database inspection."""

from typing import List, cast, Optional
from argparse import Namespace

from .config import (
    load_server_groups,
    get_single_server_group,
)
from .types import DatabaseInfo, ServerGroupConfig, ServerConfig
from .handlers_group import ensure_project_structure
from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases,
    MissingEnvironmentVariableError,
    PostgresConnectionError
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


def _show_config_file_help() -> None:
    """Display help message for missing server_group.yaml file.
    
    Example:
        >>> _show_config_file_help()
        # Prints example YAML configuration
    """
    print_error("File 'server_group.yaml' not found in the project root.")
    print_info("\n💡 To get started, create a 'server_group.yaml' file in your repository root.")
    print_info("   Here is an example for a PostgreSQL 'db-shared' setup:")
    print_info(
        "\n"
        "    asma:  # Your implementation name\n"
        "      pattern: db-shared\n"
        "      type: postgres  # Database type at group level\n"
        "      servers:\n"
        "        default:\n"
        "          host: '${POSTGRES_SOURCE_HOST}'\n"
        "          port: '${POSTGRES_SOURCE_PORT}'\n"
        "          user: '${POSTGRES_SOURCE_USER}'\n"
        "          password: '${POSTGRES_SOURCE_PASSWORD}'\n"
        "          kafka_bootstrap_servers: '${KAFKA_BOOTSTRAP_SERVERS}'\n"
        "      sources: {} # This will be auto-populated by --update"
    )


def _print_update_header(
    sg_name: str, 
    sg_type: Optional[str], 
    sg_pattern: str, 
    include_pattern: Optional[str]
) -> None:
    """Print header information for the update operation.
    
    Args:
        sg_name: Server group name
        sg_type: Database type (postgres/mssql)
        sg_pattern: Pattern (db-shared/db-per-tenant)
        include_pattern: Optional regex to filter databases
        
    Example:
        >>> _print_update_header('mygroup', 'postgres', 'db-shared', None)
        # Prints formatted header
    """
    print_header(f"Updating Server Group: {sg_name}")
    print_info(f"Type: {sg_type}")
    print_info(f"Pattern: {sg_pattern}")
    if include_pattern:
        print_info(f"Include Pattern: {include_pattern}")
    print_info(f"{'='*80}\n")


def _inspect_server_databases(
    server_name: str,
    server_config: ServerConfig,
    server_group: ServerGroupConfig,
    sg_type: str,
    include_pattern: Optional[str],
    database_exclude_patterns: List[str],
    schema_exclude_patterns: List[str],
) -> Optional[List[DatabaseInfo]]:
    """Inspect a single server and return its databases.
    
    Args:
        server_name: Name of the server being inspected
        server_config: Connection config for this server
        server_group: Full server group configuration
        sg_type: Database type ('postgres' or 'mssql')
        include_pattern: Optional regex to filter databases
        database_exclude_patterns: Patterns to exclude databases
        schema_exclude_patterns: Patterns to exclude schemas
        
    Returns:
        List of DatabaseInfo objects, or None on error
        
    Example:
        >>> databases = _inspect_server_databases('default', config, group, 'postgres', None, [], [])
        >>> len(databases)
        5
    """
    print_info(f"\n📡 Inspecting server: {server_name}")
    
    if sg_type == 'mssql':
        databases = list_mssql_databases(
            server_config,
            server_group,
            include_pattern,
            database_exclude_patterns, 
            schema_exclude_patterns,
            server_name=server_name,
        )
    elif sg_type == 'postgres':
        databases = list_postgres_databases(
            server_config,
            server_group,
            include_pattern,
            database_exclude_patterns, 
            schema_exclude_patterns,
            server_name=server_name,
        )
    else:
        print_error(f"Unknown server type: {sg_type}")
        return None
    
    print_info(f"   Found {len(databases)} database(s) on {server_name}")
    return databases


def _apply_updates(sg_name: str, all_databases: List[DatabaseInfo]) -> bool:
    """Apply database updates to YAML, schemas, and completions.
    
    Args:
        sg_name: Server group name
        all_databases: All databases discovered across servers
        
    Returns:
        True if all updates succeeded, False otherwise
        
    Example:
        >>> _apply_updates('mygroup', databases)
        True
    """
    if not update_server_group_yaml(sg_name, all_databases):  # type: ignore[arg-type]
        print_error(f"Failed to update server group '{sg_name}'")
        return False
    
    print_success(f"✓ Updated server group '{sg_name}' with {len(all_databases)} databases")
    
    # Update VS Code schema
    update_vscode_schema(all_databases)
    
    # Update Fish completions
    update_completions()
    
    # Regenerate validation schemas
    regenerate_all_validation_schemas([sg_name])  # type: ignore[list-item]
    
    return True


def _handle_connection_error(error: Exception, sg_name: str) -> int:
    """Handle connection and environment errors during update.
    
    Args:
        error: The exception that occurred
        sg_name: Server group name for error messages
        
    Returns:
        Exit code (always 1 for errors)
        
    Example:
        >>> _handle_connection_error(MissingEnvironmentVariableError('VAR'), 'mygroup')
        1
    """
    if isinstance(error, MissingEnvironmentVariableError):
        print_error(str(error))
        print_info(
            "Export the missing variable inside the dev container (e.g. `set -x NAME value`) "
            "or replace the placeholder in server_group.yaml before running --update."
        )
    elif isinstance(error, PostgresConnectionError):
        print_error("PostgreSQL Connection Failed")
        print_error(f"   {error}")
        if error.hint:
            print("")
            print_info("💡 " + error.hint.replace("\n", "\n   "))
        print("")
        print_info(f"🔌 Target: {error.host}:{error.port}")
    else:
        print_error(f"Error updating server group '{sg_name}': {error}")
        import traceback
        traceback.print_exc()
    
    return 1


def handle_update(args: Namespace) -> int:
    """Handle updating server group from database inspection.
    
    Since each implementation has only one server group, we update it directly.
    Iterates over all servers in the 'servers' dict and inspects each one.
    
    Args:
        args: Parsed command-line arguments (not used, but required for handler signature)
        
    Returns:
        Exit code (0 for success, 1 for error)
        
    Example:
        >>> args = Namespace()
        >>> handle_update(args)
        0  # if successful
    """
    # Load configuration
    try:
        config = load_server_groups()
    except FileNotFoundError:
        _show_config_file_help()
        return 1
    
    # Get the single server group
    server_group = get_single_server_group(config)
    if not server_group:
        print_error("No server group found in server_group.yaml")
        return 1
    
    # Extract configuration
    server_group_name = cast(str, server_group.get('name'))
    ensure_project_structure(server_group_name, server_group)
    
    sg_name = server_group_name
    sg_type = server_group.get('type')
    sg_pattern = server_group.get('pattern', 'db-shared')
    include_pattern = server_group.get('include_pattern')
    
    # Print header
    _print_update_header(sg_name, sg_type, sg_pattern, include_pattern)
    
    # Validate type
    if not sg_type:
        print_error("No 'type' found at group level. Add 'type: postgres' or 'type: mssql' to your server_group.yaml")
        return 1
    
    try:
        # Validate servers exist
        servers = server_group.get('servers', {})
        if not servers:
            print_error("No servers configured. Add a 'servers' section with at least a 'default' server.")
            return 1
        
        # Get exclude patterns (typed in ServerGroupConfig)
        database_exclude_patterns = server_group.get('database_exclude_patterns', [])
        schema_exclude_patterns = server_group.get('schema_exclude_patterns', [])
        
        # Collect databases from ALL servers
        all_databases: List[DatabaseInfo] = []
        
        for server_name, server_config in servers.items():
            databases = _inspect_server_databases(
                server_name,
                server_config,
                server_group,
                sg_type,
                include_pattern,
                database_exclude_patterns,
                schema_exclude_patterns,
            )
            if databases is None:
                return 1  # Error already printed
            all_databases.extend(databases)
        
        # Handle empty results
        if not all_databases:
            print_warning(f"No databases found for server group '{sg_name}'")
            return 0
        
        print_success(f"\nTotal: {len(all_databases)} database(s) across {len(servers)} server(s)")
        
        # Apply updates
        if _apply_updates(sg_name, all_databases):
            return 0
        return 1
            
    except (MissingEnvironmentVariableError, PostgresConnectionError, Exception) as e:
        return _handle_connection_error(e, sg_name)
