#!/usr/bin/env python3
"""
Shell autocompletion helpers for CDC CLI.

Provides dynamic completion data for Fish shell and other shells.
All extraction logic is centralized here for easy maintenance.
"""

import sys
from pathlib import Path
from typing import List, Set, Dict, Any, cast

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]


def find_file_upward(filename: str, max_depth: int = 3) -> Path | None:
    """Search for a file by walking up the directory tree."""
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / filename
        if candidate.exists():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def find_directory_upward(dirname: str, max_depth: int = 3) -> Path | None:
    """Search for a directory by walking up the directory tree."""
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / dirname
        if candidate.is_dir():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def list_existing_services() -> List[str]:
    """
    List existing service files from services/*.yaml.
    
    Used for --service flag autocompletion (shows created services).
    
    Returns:
        List of service names (without .yaml extension)
    """
    services_dir = find_directory_upward('services')
    if not services_dir:
        return []
    
    services: List[str] = []
    for yaml_file in services_dir.glob('*.yaml'):
        if yaml_file.is_file():
            services.append(yaml_file.stem)
    
    return sorted(services)


def list_available_services_from_server_group() -> List[str]:
    """
    List sources defined in server_group.yaml (sources: section).
    
    Used for --create-service flag autocompletion (shows sources that can be created).
    
    Returns:
        List of source names from server_group.yaml
    """
    if yaml is None:
        return []
    
    server_group_file = find_file_upward('server_group.yaml')
    if not server_group_file:
        return []
    
    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)
        
        if not config:
            return []
        
        # Extract sources from server_group structure (flat format)
        # Look for root key with 'pattern' field (server group marker)
        sources_obj: object = {}
        
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(Dict[str, Any], group_data)
                # Found server group - check for 'sources' key
                sources_obj = group_dict.get('sources', {})
                if not sources_obj:
                    # Fallback to legacy 'services' key
                    sources_obj = group_dict.get('services', {})
                break
        
        if isinstance(sources_obj, dict):
            sources_dict = cast(Dict[str, Any], sources_obj)
            return sorted(sources_dict.keys())
        
        return []
    
    except Exception:
        return []


def list_servers_from_server_group() -> List[str]:
    """
    List server names defined in server_group.yaml (servers: section).
    
    Used for --update server selection autocompletion.
    
    Returns:
        List of server names from server_group.yaml
    """
    if yaml is None:
        return []
    
    server_group_file = find_file_upward('server_group.yaml')
    if not server_group_file:
        return []
    
    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)
        
        if not config:
            return []
        
        servers_obj: object = {}
        
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(Dict[str, Any], group_data)
                servers_obj = group_dict.get('servers', {})
                break
        
        if isinstance(servers_obj, dict):
            servers_dict = cast(Dict[str, Any], servers_obj)
            return sorted(servers_dict.keys())
        
        return []
    
    except Exception:
        return []


def list_databases_from_server_group() -> List[str]:
    """
    List all databases from server_group.yaml.
    
    Used for database-related autocompletions.
    
    Returns:
        List of database names
    """
    if yaml is None:
        return []
    
    server_group_file = find_file_upward('server_group.yaml')
    if not server_group_file:
        return []
    
    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)
        
        if not config:
            return []
        
        databases: Set[str] = set()
        
        # Extract from server_group structure
        server_group = config.get('server_group', {})
        if isinstance(server_group, dict):
            server_group_dict = cast(Dict[str, Any], server_group)
            for group_data in server_group_dict.values():
                if isinstance(group_data, dict):
                    group_dict = cast(Dict[str, Any], group_data)
                    # Get databases list
                    dbs = group_dict.get('databases', [])
                    if isinstance(dbs, list):
                        dbs_list = cast(List[Any], dbs)
                        for db in dbs_list:
                            if isinstance(db, str):
                                databases.add(db)
                            elif isinstance(db, dict):
                                db_dict = cast(Dict[str, Any], db)
                                db_name = db_dict.get('name')
                                if db_name:
                                    databases.add(str(db_name))
        
        return sorted(databases)
    
    except Exception:
        return []


def list_schemas_for_service(service_name: str) -> List[str]:
    """
    List available schemas for a specific service from service-schemas directory.
    
    Args:
        service_name: Name of the service
    
    Returns:
        List of schema names
    """
    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []
    
    service_dir = schemas_dir / service_name
    if not service_dir.is_dir():
        return []
    
    schemas: List[str] = []
    for schema_dir in service_dir.iterdir():
        if schema_dir.is_dir():
            schemas.append(schema_dir.name)
    
    return sorted(schemas)


def list_tables_for_service(service_name: str) -> List[str]:
    """
    List available tables for a specific service from service-schemas directory.
    
    Format: schema.TableName
    
    Args:
        service_name: Name of the service
    
    Returns:
        List of tables in format "schema.TableName"
    """
    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []
    
    service_dir = schemas_dir / service_name
    if not service_dir.is_dir():
        return []
    
    tables: List[str] = []
    for schema_dir in service_dir.iterdir():
        if schema_dir.is_dir():
            schema_name = schema_dir.name
            for table_file in schema_dir.glob('*.yaml'):
                if table_file.is_file():
                    table_name = table_file.stem
                    tables.append(f"{schema_name}.{table_name}")
    
    return sorted(tables)


def scaffold_flag_completions(flag: str) -> List[str]:
    """
    Return appropriate completions for scaffold subcommand flags.
    
    Args:
        flag: The flag name (--pattern, --source-type, etc.)
    
    Returns:
        List of "value\tDescription" formatted completions
    """
    completions = {
        '--pattern': [
            'db-per-tenant\tOne database per tenant',
            'db-shared\tShared database for all tenants',
        ],
        '--source-type': [
            'postgres\tPostgreSQL database',
            'mssql\tMicrosoft SQL Server',
        ],
    }
    
    return completions.get(flag, [])


def main() -> int:
    """CLI entry point for autocompletion queries."""
    if len(sys.argv) < 2:
        print("Usage: python -m cdc_generator.helpers.autocompletions <command>", file=sys.stderr)
        return 1
    
    command = sys.argv[1]
    
    if command == '--list-existing-services':
        services = list_existing_services()
        for service in services:
            print(service)
    
    elif command == '--list-available-services':
        services = list_available_services_from_server_group()
        for service in services:
            print(service)
    
    elif command == '--list-databases':
        databases = list_databases_from_server_group()
        for db in databases:
            print(db)

    elif command == '--list-server-names':
        servers = list_servers_from_server_group()
        for server in servers:
            print(server)
    
    elif command == '--list-schemas':
        if len(sys.argv) < 3:
            print("Error: --list-schemas requires service name", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        schemas = list_schemas_for_service(service_name)
        for schema in schemas:
            print(schema)
    
    elif command == '--list-tables':
        if len(sys.argv) < 3:
            print("Error: --list-tables requires service name", file=sys.stderr)
            return 1
        service_name = sys.argv[2]
        tables = list_tables_for_service(service_name)
        for table in tables:
            print(table)
    
    elif command == '--scaffold-flag-values':
        if len(sys.argv) < 3:
            print("Error: --scaffold-flag-values requires flag name", file=sys.stderr)
            return 1
        flag = sys.argv[2]
        completions = scaffold_flag_completions(flag)
        for completion in completions:
            print(completion)
    
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
