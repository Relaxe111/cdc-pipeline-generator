#!/usr/bin/env python3
"""
Interactive service management tool for CDC pipeline.

Usage:
    cdc manage-service                    # Interactive mode
    cdc manage-service --service adopus --inspect-mssql --all     # List all MSSQL tables (all schemas)
    cdc manage-service --service adopus --inspect-mssql --schema dbo  # List tables in specific schema
    cdc manage-service --service adopus --add-table Actor         # Add table to service
    cdc manage-service --service adopus --remove-table Test       # Remove table from service
    cdc manage-service --service adopus --generate-validation --all  # Generate JSON schema
"""

import argparse
import sys
from pathlib import Path
from typing import Any

try:
    import yaml 
except ImportError:
    yaml = None  

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_header,
    print_info,
    print_success,
    print_warning,
    print_error,
)
from cdc_generator.validators.manage_service.mssql_inspector import inspect_mssql_schema  
from cdc_generator.validators.manage_service.postgres_inspector import inspect_postgres_schema  
from cdc_generator.validators.manage_service.table_operations import (
    add_table_to_service,
    remove_table_from_service,
)
from cdc_generator.validators.manage_service.validation import (
    validate_service_config,
    validate_hierarchy_no_duplicates,
)
from cdc_generator.validators.manage_service.service_creator import create_service
from cdc_generator.validators.manage_service.schema_generator import generate_service_validation_schema
from cdc_generator.validators.manage_service.interactive_mode import run_interactive_mode  
from cdc_generator.validators.manage_service.schema_saver import save_detailed_schema  

PROJECT_ROOT = Path(__file__).parent.parent
SERVICE_SCHEMAS_DIR = PROJECT_ROOT / "service-schemas"


def main() -> int:
    parser = argparse.ArgumentParser(description="Interactive CDC table mapping manager")
    parser.add_argument("--service", help="Service name")
    parser.add_argument("--create-service", action="store_true", help="Create a new service configuration file")
    parser.add_argument("--add-source-table", help="Add single table to service (format: schema.table)")
    parser.add_argument("--add-source-tables", nargs='+', help="Add multiple tables to service (space-separated, format: schema.table schema.table)")
    parser.add_argument("--remove-table", help="Remove table from service (format: schema.table or just table)")
    parser.add_argument("--inspect", action="store_true", help="Inspect database schema and list available tables (auto-detects MSSQL/PostgreSQL from service)")

    parser.add_argument("--schema", help="Database schema to inspect or filter (for --generate-validation: required if --all not used)")
    parser.add_argument("--save", action="store_true", help="Save detailed table schemas to YAML")
    parser.add_argument("--generate-validation", action="store_true", help="Generate JSON Schema for service YAML validation from database")
    parser.add_argument("--validate-hierarchy", action="store_true", help="Validate hierarchical inheritance (no duplicate values)")
    parser.add_argument("--validate-config", action="store_true", help="Comprehensive validation: required fields + hierarchy + pipeline readiness")
    parser.add_argument("--all", action="store_true", help="Process all schemas (required with --generate-validation if --schema not specified)")
    parser.add_argument("--env", default="nonprod", help="Environment for MSSQL inspection (default: nonprod)")
    parser.add_argument("--primary-key", help="Primary key column name (optional - auto-detected from schema)")
    parser.add_argument("--ignore-columns", action="append", help="Column to ignore (format: schema.table.column, can be specified multiple times)")
    parser.add_argument("--track-columns", action="append", help="Column to explicitly track (format: schema.table.column, can be specified multiple times)")
    
    # Legacy args (for backward compatibility)
    parser.add_argument("--source", help="Source service name (legacy)")
    parser.add_argument("--sink", help="Sink service name (legacy)")
    parser.add_argument("--source-schema", help="Source schema name (legacy)")
    parser.add_argument("--sink-schema", help="Sink schema name (legacy)")
    parser.add_argument("--source-table", help="Source table name (legacy)")
    parser.add_argument("--sink-table", help="Sink table name (legacy)")
    
    args = parser.parse_args()
    
    # Handle create-service operation
    if args.create_service:
        if not args.service:
            print("❌ Error: --service is required for --create-service")
            return 1
        
        # Auto-detect server-group from server-groups.yaml
        server_group = None
        server_groups_file = Path(__file__).parent.parent / 'server-groups.yaml'
        if server_groups_file.exists() and yaml is not None:
            with open(server_groups_file) as f:
                server_groups_data = yaml.safe_load(f)
                for sg in server_groups_data.get('server_groups', []):
                    # Check server group level service field
                    if sg.get('service') == args.service:
                        server_group = sg.get('name')
                        print_info(f"Auto-detected server group: {server_group}")
                        break
                    
                    # Also check database-level service fields
                    for db in sg.get('databases', []):
                        if db.get('service') == args.service:
                            server_group = sg.get('name')
                            print_info(f"Auto-detected server group: {server_group} (from database {db.get('name')})")
                            break
                    
                    if server_group:
                        break
        
        if not server_group:
            print_error(f"❌ Could not find server group for service '{args.service}'")
            print_error(f"Add service mapping to server-groups.yaml")
            return 1
            
        create_service(args.service, server_group)
        return 0
    
    # Handle validate-config operation (comprehensive)
    if args.service and args.validate_config:
        if validate_service_config(args.service):
            return 0
        return 1
    
    # Handle validate-hierarchy operation
    if args.service and args.validate_hierarchy:
        if validate_hierarchy_no_duplicates(args.service):
            return 0
        return 1
    
    # Handle generate-validation operation
    if args.service and args.generate_validation:
        # Require either --all or explicit --schema
        if not args.all and not args.schema:
            print_error("Error: --generate-validation requires either --all (for all schemas) or --schema <name> (for specific schema)")
            return 1
        
        schema_filter = None if args.all else args.schema
        if generate_service_validation_schema(args.service, args.env, schema_filter):
            return 0
        return 1
    
    # Handle inspect operation (auto-detect database type)
    if args.service and args.inspect:
        # Load service config to detect database type
        from cdc_generator.helpers.service_config import load_service_config  
        config: dict[str, Any] = load_service_config(args.service)  
        server_group: str | None = config.get('server_group')  
        
        # Determine database type from server group
        db_type = None
        if server_group:
            server_groups_file = Path(__file__).parent.parent / 'server-groups.yaml'
            if server_groups_file.exists() and yaml is not None:
                with open(server_groups_file) as f:
                    server_groups_data = yaml.safe_load(f)
                    for sg in server_groups_data.get('server_groups', []):
                        if sg.get('name') == server_group:
                            db_type = sg.get('server', {}).get('type')
                            break
        
        if not db_type:
            print_error(f"Could not determine database type for service '{args.service}'")
            print_error(f"Check that server_group is set in service config and exists in server-groups.yaml")
            return 1
        
        # Require either --all or explicit --schema
        if not args.all and not args.schema:
            print_error(f"Error: --inspect requires either --all (for all schemas) or --schema <name> (for specific schema)")
            return 1
        
        # Get allowed schemas from server-groups.yaml
        server_groups_file = Path(__file__).parent.parent / 'server-groups.yaml'
        allowed_schemas = None
        
        if server_groups_file.exists() and yaml is not None:
            with open(server_groups_file) as f:
                server_groups_data = yaml.safe_load(f)
                for sg in server_groups_data.get('server_groups', []):
                    if sg.get('name') == server_group:
                        # For db-per-tenant: get schemas from database_ref
                        if sg.get('server_group_type') == 'db-per-tenant':
                            database_ref = sg.get('database_ref')
                            for db in sg.get('databases', []):
                                if db.get('name') == database_ref:
                                    allowed_schemas = db.get('schemas', [])
                                    break
                        # For db-shared: find database matching service name
                        elif sg.get('server_group_type') == 'db-shared':
                            for db in sg.get('databases', []):
                                if db.get('service') == args.service:
                                    allowed_schemas = db.get('schemas', [])
                                    break
                        break
        
        if not allowed_schemas:
            print_error(f"No schemas defined for service '{args.service}' in server-groups.yaml")
            print_error(f"Please add schemas list to the database entry for this service")
            return 1
        
        schema = args.schema  # None if --all, specific schema otherwise
        
        # Validate schema if explicitly provided
        if schema and schema not in allowed_schemas:
            print_error(f"Schema '{schema}' not allowed for service '{args.service}'")
            print_error(f"Allowed schemas: {', '.join(allowed_schemas)}")
            return 1
        
        schema_msg = f"schemas: {', '.join(allowed_schemas)}" if args.all else f"schema: {schema}"
        print_header(f"Inspecting {db_type.upper()} schema for {args.service} ({schema_msg})")
        
        # Call appropriate inspector based on database type
        if db_type == 'mssql':
            tables = inspect_mssql_schema(args.service, args.env)  
        elif db_type == 'postgres':
            tables = inspect_postgres_schema(args.service, args.env)  
        else:
            print_error(f"Unsupported database type: {db_type}")
            return 1
        
        if tables:
            # Filter by allowed schemas from server-groups.yaml
            if args.all:
                # When --all, filter to only allowed schemas
                tables = [t for t in tables if t['TABLE_SCHEMA'] in allowed_schemas]  
            else:
                # When specific schema, filter to that schema only
                tables = [t for t in tables if t['TABLE_SCHEMA'] == schema]  
            
            if not tables:
                if args.all:
                    print_warning(f"No tables found in allowed schemas: {', '.join(allowed_schemas)}")
                else:
                    print_warning(f"No tables found in schema '{schema}'")
                return 1
            
            if args.save:
                # Save detailed schema using modular function
                if save_detailed_schema(args.service, args.env, schema, tables, db_type):  
                    return 0
                return 1
            else:
                # Just list tables
                print_success(f"Found {len(tables)} tables:\n")  
                current_schema = None
                for table in tables:  # type: ignore[union-attr]
                    tbl_schema = table['TABLE_SCHEMA']  
                    if tbl_schema != current_schema:
                        print(f"\n{Colors.CYAN}[{tbl_schema}]{Colors.RESET}")
                        current_schema = tbl_schema  
                    print(f"  {table['TABLE_NAME']} ({table['COLUMN_COUNT']} columns)")  
        return 0 if tables else 1
    
    # Handle add-source-tables (bulk) operation
    if args.service and args.add_source_tables:
        success_count = 0
        failed_count = 0
        
        for table_spec in args.add_source_tables:
            table_spec = table_spec.strip()
            if not table_spec:
                continue
            
            # Parse schema.table format
            if '.' in table_spec:
                schema, table = table_spec.split('.', 1)
            else:
                schema = args.schema if args.schema else 'dbo'
                table = table_spec
            
            # Parse column specs in format schema.table.column
            ignore_cols = None
            track_cols = None
            
            if args.ignore_columns:
                # Filter columns for this specific table
                table_prefix = f"{schema}.{table}."
                ignore_cols = [col.replace(table_prefix, '') for col in args.ignore_columns if col.startswith(table_prefix)]
            
            if args.track_columns:
                # Filter columns for this specific table
                table_prefix = f"{schema}.{table}."
                track_cols = [col.replace(table_prefix, '') for col in args.track_columns if col.startswith(table_prefix)]
            
            if add_table_to_service(args.service, schema, table, args.primary_key, ignore_cols or None, track_cols or None):
                success_count += 1
            else:
                failed_count += 1
        
        if success_count > 0:
            print_success(f"\\nAdded {success_count} table(s)")
            if failed_count > 0:
                print_warning(f"Failed to add {failed_count} table(s)")
            print_info("Run 'cdc generate' to update pipelines")
            return 0
        return 1
    
    # Handle add-source-table operation
    if args.service and args.add_source_table:
        # Parse schema.table format
        if '.' in args.add_source_table:
            schema, table = args.add_source_table.split('.', 1)
        else:
            # Use --schema flag or default to 'dbo'
            schema = args.schema if args.schema else 'dbo'
            table = args.add_source_table
        
        # Parse column specs in format schema.table.column
        ignore_cols = None
        track_cols = None
        
        if args.ignore_columns:
            table_prefix = f"{schema}.{table}."
            ignore_cols = [col.replace(table_prefix, '') for col in args.ignore_columns if col.startswith(table_prefix)]
        
        if args.track_columns:
            table_prefix = f"{schema}.{table}."
            track_cols = [col.replace(table_prefix, '') for col in args.track_columns if col.startswith(table_prefix)]
        
        if add_table_to_service(args.service, schema, table, args.primary_key, ignore_cols or None, track_cols or None):
            print_info("\nRun 'cdc generate' to update pipelines")
            return 0
        return 1
    
    # Handle remove-table operation
    if args.service and args.remove_table:
        # Parse schema.table format
        if '.' in args.remove_table:
            schema, table = args.remove_table.split('.', 1)
        else:
            # Use --schema flag or default to 'dbo'
            schema = args.schema if args.schema else 'dbo'
            table = args.remove_table
        
        if remove_table_from_service(args.service, schema, table):
            print_info("\nRun 'cdc generate' to update pipelines")
            return 0
        return 1
    
    # Interactive mode (legacy workflow - delegates to interactive_mode module)
    return run_interactive_mode(args)


if __name__ == "__main__":
    sys.exit(main())