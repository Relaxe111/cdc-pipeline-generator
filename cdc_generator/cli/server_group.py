#!/usr/bin/env python3
"""
Manage server groups - inspect databases and update server-groups.yaml.

Usage:
    cdc manage-server-group --update                          # Update asma (default)
    cdc manage-server-group --update --server-group adopus    # Update adopus
    cdc manage-server-group --update --server-group asma      # Update asma
    cdc manage-server-group --list                            # List all server groups
    cdc manage-server-group --add-group example --type postgres --host '${POSTGRES_HOST}' --port 5432 --user '${POSTGRES_USER}' --password '${POSTGRES_PASSWORD}'
    cdc manage-server-group --list-ignore-patterns            # List databases to ignore
    cdc manage-server-group --add-to-ignore-list "pattern"    # Add pattern to ignore list
    
Note: Service names are automatically inferred from database names using pattern: {service}_db_{environment}
      Databases containing ignore patterns will be excluded from server group updates.
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from helpers import load_env_file
from helpers_logging import print_header, print_info, print_error

# Import from modular package
from manage_server_group import (
    load_schema_exclude_patterns,
    load_database_exclude_patterns,
    list_server_groups,
    handle_add_group,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_update,
    regenerate_all_validation_schemas,
)


def main():
    # Load environment variables first
    load_env_file()
    
    parser = argparse.ArgumentParser(description="Manage server groups")
    parser.add_argument("--update", action="store_true", help="Update server group from database inspection")
    parser.add_argument("--server-group", default="asma", help="Server group name (default: asma)")
    parser.add_argument("--all", action="store_true", help="Update all server groups")
    parser.add_argument("--list", action="store_true", help="List all server groups")
    
    # Add server group
    parser.add_argument("--add-group", help="Add new server group")
    parser.add_argument("--type", choices=["postgres", "mssql"], help="Database type for new server group")
    parser.add_argument("--host", help="Database host (use ${VAR} for environment variables)")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--user", help="Database user (use ${VAR} for environment variables)")
    parser.add_argument("--password", help="Database password (use ${VAR} for environment variables)")
    parser.add_argument("--mode", choices=["db-per-tenant", "db-shared"], default="db-per-tenant",
                       help="Server group type (default: db-per-tenant)")
    
    # Exclude patterns management
    parser.add_argument("--add-to-ignore-list", help="Add pattern(s) to database exclude list (comma-separated)")
    parser.add_argument("--list-ignore-patterns", action="store_true", 
                       help="List current database exclude patterns")
    parser.add_argument("--add-to-schema-excludes", help="Add pattern(s) to schema exclude list (comma-separated)")
    parser.add_argument("--list-schema-excludes", action="store_true",
                       help="List current schema exclude patterns")
    
    args = parser.parse_args()
    
    # Handle list schema exclude patterns
    if args.list_schema_excludes:
        patterns = load_schema_exclude_patterns()
        print_header("Schema Exclude Patterns")
        if patterns:
            print_info("Schemas matching these patterns will be excluded:")
            for pattern in patterns:
                print_info(f"  • {pattern}")
        else:
            from helpers_logging import print_warning
            print_warning("No schema exclude patterns defined")
            print_info("Add patterns to the comment in server-groups.yaml:")
            print_info("  # schema_exclude_patterns: ['hdb_catalog', 'hdb_views', 'sessions']")
        return 0
    
    # Handle list ignore patterns
    if args.list_ignore_patterns:
        patterns = load_database_exclude_patterns()
        print_header("Database Exclude Patterns")
        print_info("Databases containing these patterns will be excluded:")
        for pattern in patterns:
            print_info(f"  • {pattern}")
        return 0
    
    # Handle add to ignore list
    if args.add_to_ignore_list:
        return handle_add_ignore_pattern(args)
    
    # Handle add to schema excludes
    if args.add_to_schema_excludes:
        return handle_add_schema_exclude(args)
    
    # Handle add group
    if args.add_group:
        result = handle_add_group(args)
        if result == 0:
            regenerate_all_validation_schemas()
        return result
    
    # Handle list
    if args.list:
        list_server_groups()
        return 0
    
    # Handle update
    if args.update:
        return handle_update(args)
    
    # No action specified
    print_error("No action specified. Use --update, --list, or --add-group")
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

