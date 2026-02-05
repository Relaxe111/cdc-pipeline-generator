#!/usr/bin/env python3
"""
Manage the server group configuration file (server_group.yaml).

This command helps you keep your server_group.yaml file up-to-date by
inspecting the source database and populating it with the correct database
and schema information based on your configuration.

Usage:
    # Inspect the database and update server_group.yaml
    cdc manage-server-group --update

    # Inspect a specific server
    cdc manage-server-group --update default
    cdc manage-server-group --update prod

    # Inspect all servers
    cdc manage-server-group --update --all

    # Show information about the configured server group
    cdc manage-server-group --info

    # Manage database/schema exclude patterns
    cdc manage-server-group --list-ignore-patterns
    cdc manage-server-group --add-to-ignore-list "pattern_to_ignore"
    cdc manage-server-group --list-schema-excludes
    cdc manage-server-group --add-to-schema-excludes "schema_to_exclude"

Note:
To create a new server group, use 'cdc scaffold <name>' command.

Example:
    cdc scaffold myproject --pattern db-shared --source-type postgres \\
        --extraction-pattern "" --environment-aware
"""

import argparse
import sys
from pathlib import Path
from typing import cast

# When executed directly (python cdc_generator/cli/server_group.py), ensure the
# project root is on sys.path so package imports succeed.
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.helpers.helpers_logging import print_header, print_info, print_error, print_warning
from cdc_generator.helpers.yaml_loader import ConfigDict

# Import from modular package
from cdc_generator.validators.manage_server_group import (
    load_schema_exclude_patterns,
    load_database_exclude_patterns,
    load_env_mappings,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_add_env_mapping,
    handle_update,
    handle_info,
    handle_add_server,
    handle_list_servers,
    handle_remove_server,
    handle_set_kafka_topology,
    handle_set_extraction_pattern,
)

# Import flag validator
from cdc_generator.validators.flag_validator import validate_manage_server_group_flags


def main() -> int:
    # Note: .env loading handled by implementations, not generator library
    
    parser = argparse.ArgumentParser(
        description="Manage the server_group.yaml file for your implementation.",
        prog="cdc manage-server-group",  # Use the alias in help messages
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Primary actions
    parser.add_argument(
        "--update",
        nargs="?",
        const="default",
        metavar="SERVER",
        help=(
            "Update the server group by inspecting the source database. "
            "Optionally provide a server name (default: 'default')."
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Update all servers (use with --update).",
    )
    parser.add_argument("--info", action="store_true", help="Show detailed information for the server group.")
    parser.add_argument("--view-services", action="store_true", help="View environment-grouped services (db-shared mode).")

    # Exclude patterns management
    parser.add_argument("--add-to-ignore-list", help="Add a pattern to the database exclude list (persisted in server_group.yaml).")
    parser.add_argument("--list-ignore-patterns", action="store_true", 
                       help="List current database exclude patterns.")
    parser.add_argument("--add-to-schema-excludes", help="Add a pattern to the schema exclude list (persisted in server_group.yaml).")
    parser.add_argument("--list-schema-excludes", action="store_true",
                       help="List current schema exclude patterns.")
    
    # Environment mappings management
    parser.add_argument("--add-env-mapping", 
                       help="Add environment mapping(s) in format 'from:to,from:to' (e.g., 'staging:stage,production:prod').")
    parser.add_argument("--list-env-mappings", action="store_true",
                       help="List current environment mappings.")
    
    # Multi-server management
    parser.add_argument("--add-server", metavar="NAME",
                       help="Add a new server configuration (e.g., 'analytics', 'reporting'). "
                            "Use with --source-type, --host, --port, --user, --password.")
    parser.add_argument("--list-servers", action="store_true",
                       help="List all configured servers in the server group.")
    parser.add_argument("--remove-server", metavar="NAME",
                       help="Remove a server configuration. Cannot remove 'default' or servers with services.")
    parser.add_argument("--set-kafka-topology", choices=["shared", "per-server"],
                       help="Change the Kafka topology. 'shared' = same Kafka for all servers, "
                            "'per-server' = isolated Kafka per server.")
    parser.add_argument("--set-extraction-pattern", nargs=2, metavar=("SERVER", "PATTERN"),
                       help="Set extraction pattern for a specific server. "
                            "Pattern is a regex with named groups: (?P<service>...), (?P<env>...), (?P<customer>...). "
                            "Example: --set-extraction-pattern default '^(?P<service>\\w+)_(?P<env>\\w+)$'")
    
    args = parser.parse_args()

    # Validate flag combinations (Python-based validation)
    validation_result = validate_manage_server_group_flags(args)
    
    if validation_result.level == 'error':
        print_error(validation_result.message or "Invalid flag combination")
        if validation_result.suggestion:
            print(validation_result.suggestion)
        return 1
    
    if validation_result.level == 'warning':
        print(validation_result.message or "")
        print()  # Blank line before proceeding

    
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
    
    # Handle list env mappings
    if args.list_env_mappings:
        mappings = load_env_mappings()
        print_header("Environment Mappings")
        if mappings:
            print_info("Database environment suffixes will be mapped as follows:")
            for from_env, to_env in sorted(mappings.items()):
                print_info(f"  • {from_env} → {to_env}")
            print_info("\nThese mappings are applied when extracting environment from database names.")
        else:
            print_warning("No environment mappings defined.")
            print_info("Add mappings to normalize environment suffixes:")
            print_info("  cdc manage-server-group --add-env-mapping 'staging:stage,production:prod'")
        return 0
    
    # Handle add env mapping
    if args.add_env_mapping:
        return handle_add_env_mapping(args)
    
    # Handle multi-server management
    if args.add_server:
        return handle_add_server(args)
    
    if args.list_servers:
        return handle_list_servers(args)
    
    if args.remove_server:
        return handle_remove_server(args)
    
    if args.set_kafka_topology:
        return handle_set_kafka_topology(args)
    
    if args.set_extraction_pattern:
        return handle_set_extraction_pattern(args)
    
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
            
            # Check for 'sources' key (new structure) or fallback to 'services' (legacy)
            sources = server_group.get('sources', server_group.get('services', {}))
            
            if sources:
                print_header("Environment-Grouped Sources")
                for source_name, source_data in sorted(sources.items()):
                    # Type is already dict from YAML structure
                    src = cast(ConfigDict, source_data)
                    schemas_raw = src.get('schemas', [])
                    # Runtime validation: schemas must be a list of strings
                    if isinstance(schemas_raw, list):
                        schemas = [str(s) for s in schemas_raw]
                    else:
                        schemas = []
                    print_info(f"\n📦 Source: {source_name}")
                    print_info(f"   Schemas (shared): {', '.join(schemas)}")
                    
                    # Display each environment with server reference
                    for key, value in sorted(src.items()):
                        if key == 'schemas':
                            continue  # Already displayed
                        if isinstance(value, dict) and 'database' in value:
                            env = key
                            env_data = value  # Type is already Dict[str, ConfigValue]
                            # Extract with defaults and explicit type conversion
                            server_raw = env_data.get('server', 'default')
                            database_raw = env_data.get('database', '')
                            table_count_raw = env_data.get('table_count', 0)
                            server = str(server_raw)
                            database = str(database_raw)
                            table_count = int(table_count_raw) if isinstance(table_count_raw, (int, str)) else 0
                            print_info(f"   🌍 {env}:")
                            print_info(f"       Server: {server}")
                            print_info(f"       Database: {database}")
                            print_info(f"       Tables: {table_count}")
            else:
                print_warning("Server group has no sources configured.")
                print_info("Run 'cdc manage-server-group --update' to discover databases.")
            return 0
        except Exception as e:
            print_error(f"Failed to view services: {e}")
            return 1
    
    
    # Handle update (the primary action)
    if args.all and args.update is None:
        print_error("'--all' requires '--update'.")
        print_info("Example: cdc manage-server-group --update --all")
        return 1

    if args.update is not None:
        return handle_update(args)
    
    # No action specified
    print_error("No action specified. Use --update or --info.")
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

