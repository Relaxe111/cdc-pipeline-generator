#!/usr/bin/env python3
"""
Manage the server group configuration file (server_group.yaml).

This command helps you keep your server_group.yaml file up-to-date by
inspecting the source database and populating it with the correct database
and schema information based on your configuration.

Usage:
    # Inspect the database and update server_group.yaml
    cdc manage-server-group --update

    # Show information about the configured server group
    cdc manage-server-group --info

    # List all server groups (there should only be one)
    cdc manage-server-group --list

    # Manage database/schema exclude patterns
    cdc manage-server-group --list-ignore-patterns
    cdc manage-server-group --add-to-ignore-list "pattern_to_ignore"
    cdc manage-server-group --list-schema-excludes
    cdc manage-server-group --add-to-schema-excludes "schema_to_exclude"

Note:
To create a new server group, run `cdc manage-server-group --create <name>`
inside your implementation repository (e.g., adopus-cdc-pipeline). The command
will scaffold `server_group.yaml` with environment-variable placeholders such as
${MSSQL_HOST}. Afterwards, use '--update' to inspect the database and populate
the database list automatically.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List

# When executed directly (python cdc_generator/cli/server_group.py), ensure the
# project root is on sys.path so package imports succeed.
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.helpers.helpers_logging import print_header, print_info, print_error, print_warning

# Import from modular package
from cdc_generator.validators.manage_server_group import (
    load_schema_exclude_patterns,
    load_database_exclude_patterns,
    list_server_groups,
    handle_add_group,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_update,
    handle_info,
)


def _default_connection_placeholders(source_type: str) -> Dict[str, str]:
    """Return default environment variable placeholders for the source database."""
    prefix = 'POSTGRES_SOURCE' if source_type == 'postgres' else 'MSSQL_SOURCE'
    return {
        'host': f"${{{prefix}_HOST}}",
        'port': f"${{{prefix}_PORT}}",
        'user': f"${{{prefix}_USER}}",
        'password': f"${{{prefix}_PASSWORD}}",
    }


def main() -> int:
    # Note: .env loading handled by implementations, not generator library
    
    parser = argparse.ArgumentParser(
        description="Manage the server_group.yaml file for your implementation.",
        prog="cdc manage-server-group",  # Use the alias in help messages
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Primary actions
    parser.add_argument("--create", metavar="NAME", help="Scaffold a new server_group.yaml with the given name.")
    parser.add_argument("--update", action="store_true", help="Update the server group by inspecting the source database.")
    parser.add_argument("--list", action="store_true", help="List the configured server group.")
    parser.add_argument("--info", action="store_true", help="Show detailed information for the server group.")
    parser.add_argument("--view-services", action="store_true", help="View environment-grouped services (db-shared mode).")

    # Arguments for --create
    parser.add_argument("--pattern", choices=["db-per-tenant", "db-shared"],
                       help="Server group pattern (required for --create).")
    parser.add_argument("--source-type", choices=["postgres", "mssql"],
                       help="Source database type (required for --create).")
    parser.add_argument("--host", help="Database host (default: ${POSTGRES_SOURCE_HOST}/${MSSQL_SOURCE_HOST}).")
    parser.add_argument("--port", help="Database port (default: ${POSTGRES_SOURCE_PORT}/${MSSQL_SOURCE_PORT}).")
    parser.add_argument("--user", help="Database user (default: ${POSTGRES_SOURCE_USER}/${MSSQL_SOURCE_USER}).")
    parser.add_argument("--password", help="Database password (default: ${POSTGRES_SOURCE_PASSWORD}/${MSSQL_SOURCE_PASSWORD}).")
    
    # Pattern extraction configuration
    parser.add_argument("--extraction-pattern", 
                       help="Regex pattern with named groups to extract identifiers from database names (required for --create). "
                            "For db-per-tenant: use 'customer' group. For db-shared: use 'service', 'env', 'suffix' groups. "
                            "Leave empty to use simple fallback matching.")
    parser.add_argument("--environment-aware", action="store_true",
                       help="Enable environment-aware grouping (required for db-shared with --create).")
    
    # Exclude patterns management
    parser.add_argument("--add-to-ignore-list", help="Add a pattern to the database exclude list (persisted in server_group.yaml).")
    parser.add_argument("--list-ignore-patterns", action="store_true", 
                       help="List current database exclude patterns.")
    parser.add_argument("--add-to-schema-excludes", help="Add a pattern to the schema exclude list (persisted in server_group.yaml).")
    parser.add_argument("--list-schema-excludes", action="store_true",
                       help="List current schema exclude patterns.")
    
    args = parser.parse_args()

    # Handle --create
    if args.create:
        missing: List[str] = []
        if not args.pattern:
            missing.append("--pattern")
        if not args.source_type:
            missing.append("--source-type")
        
        # extraction-pattern is required for --create but can be empty string for fallback
        if not hasattr(args, 'extraction_pattern') or args.extraction_pattern is None:
            missing.append("--extraction-pattern")
        
        # db-shared specific validation
        if args.pattern == "db-shared":
            if not args.environment_aware:
                missing.append("--environment-aware")
        
        if missing:
            print_error("Missing required options for --create:")
            for flag in missing:
                print_info(f"  • {flag}")
            print_info("\nNotes:")
            print_info("  --extraction-pattern: Can be empty string '' to use simple fallback matching")
            if args.pattern == "db-shared":
                print_info("  --environment-aware: Required for db-shared pattern")
            return 1
        
        placeholders = _default_connection_placeholders(args.source_type)
        args.host = args.host or placeholders['host']
        args.port = args.port or placeholders['port']
        args.user = args.user or placeholders['user']
        args.password = args.password or placeholders['password']
        # For backwards compatibility with the handler, we map the new names to the old ones.
        args.add_group = args.create
        args.mode = args.pattern
        return handle_add_group(args)
    
    # Handle list schema exclude patterns
    if args.list_schema_excludes:
        patterns = load_schema_exclude_patterns()
        print_header("Schema Exclude Patterns")
        if patterns:
            print_info("Schemas matching these patterns will be excluded during '--update':")
            for pattern in patterns:
                print_info(f"  • {pattern}")
        else:
            print_warning("No schema exclude patterns defined.")
            print_info("You can add patterns to a comment in server_group.yaml, for example:")
            print_info("  # schema_exclude_patterns: ['hdb_catalog', 'hdb_views', 'sessions']")
        return 0
    
    # Handle list ignore patterns
    if args.list_ignore_patterns:
        patterns = load_database_exclude_patterns()
        print_header("Database Exclude Patterns")
        if patterns:
            print_info("Databases with names containing these patterns will be excluded during '--update':")
            for pattern in patterns:
                print_info(f"  • {pattern}")
        else:
            print_warning("No database exclude patterns defined.")
            print_info("You can add patterns to a comment in server_group.yaml, for example:")
            print_info("  # database_exclude_patterns: ['test', 'dev', 'backup']")
        return 0
    
    # Handle add to ignore list
    if args.add_to_ignore_list:
        return handle_add_ignore_pattern(args)
    
    # Handle add to schema excludes
    if args.add_to_schema_excludes:
        return handle_add_schema_exclude(args)
    
    # Handle info
    if args.info:
        return handle_info(args)
    
    # Handle view-services
    if args.view_services:
        from cdc_generator.validators.manage_server_group.config import load_server_groups, get_single_server_group
        try:
            config = load_server_groups()
            server_group = get_single_server_group(config)
            
            if not server_group:
                print_error("No server group found in configuration")
                return 1
            
            if 'services' in server_group:
                print_header("Environment-Grouped Services")
                for service_name, service_data in sorted(server_group['services'].items()):
                    schemas = service_data.get('schemas', [])
                    print_info(f"\n📦 Service: {service_name}")
                    print_info(f"   Schemas (shared): {', '.join(schemas)}")
                    
                    # Display each environment
                    for key, value in sorted(service_data.items()):
                        if key == 'schemas':
                            continue  # Already displayed
                        if isinstance(value, dict) and 'database' in value:
                            env = key
                            database = value.get('database', '')
                            table_count = value.get('table_count', 0)
                            print_info(f"   🌍 {env}:")
                            print_info(f"       Database: {database}")
                            print_info(f"       Tables: {table_count}")
            else:
                print_warning("Server group is not using environment-aware grouping.")
                print_info("Run 'cdc manage-server-group --update' to regenerate with grouping enabled.")
            return 0
        except Exception as e:
            print_error(f"Failed to view services: {e}")
            return 1
    
    # Handle list
    if args.list:
        list_server_groups()
        return 0
    
    # Handle update (the primary action)
    if args.update:
        return handle_update(args)
    
    # No action specified
    print_error("No action specified. Use --create, --update, --list, or --info.")
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

