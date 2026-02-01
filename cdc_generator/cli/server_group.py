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
To create a new server group, you should manually create the 'server_group.yaml'
file in the root of your implementation repository (e.g., adopus-cdc-pipeline).
The '--update' command will then populate it based on the connection details
you provide in that file. Environment variables like ${MSSQL_HOST} are supported.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
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

    # Arguments for --create
    parser.add_argument("--pattern", choices=["db-per-tenant", "db-shared"],
                       help="Server group pattern (required for --create).")
    parser.add_argument("--source-type", choices=["postgres", "mssql"],
                       help="Source database type (required for --create).")
    parser.add_argument("--host", help="Database host (default: ${POSTGRES_SOURCE_HOST}/${MSSQL_SOURCE_HOST}).")
    parser.add_argument("--port", help="Database port (default: ${POSTGRES_SOURCE_PORT}/${MSSQL_SOURCE_PORT}).")
    parser.add_argument("--user", help="Database user (default: ${POSTGRES_SOURCE_USER}/${MSSQL_SOURCE_USER}).")
    parser.add_argument("--password", help="Database password (default: ${POSTGRES_SOURCE_PASSWORD}/${MSSQL_SOURCE_PASSWORD}).")
    
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
        missing = []
        if not args.pattern:
            missing.append("--pattern")
        if not args.source_type:
            missing.append("--source-type")
        if missing:
            print_error("Missing required options for --create:")
            for flag in missing:
                print_info(f"  • {flag}")
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

