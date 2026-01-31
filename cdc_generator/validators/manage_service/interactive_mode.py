"""Interactive workflow for legacy table mapping functionality."""

from pathlib import Path
from typing import Optional
from helpers_logging import print_header, print_info, print_success, print_error, print_warning, Colors
from .config import (
    get_available_services,
    load_service_schema_tables,
    get_table_schema_definition,
    detect_service_mode
)
from .interactive import (
    prompt_select,
    prompt_multiselect,
    prompt_mappings,
    validate_table_compatibility
)

PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVICE_SCHEMAS_DIR = PROJECT_ROOT / "service-schemas"


def run_interactive_mode(args) -> int:
    """Run interactive table mapping workflow.
    
    This is legacy functionality for creating table mappings between source and sink.
    Most users should use the command-line flags instead.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    print_header("CDC Table Mapping Manager")
    
    # Step 1: Select source service
    available_services = get_available_services()
    if not available_services:
        print_error("No services found in 2-services/")
        return 1
    
    source_service = args.source or prompt_select("Select source service:", available_services)
    if not source_service:
        return 1
    
    source_mode = detect_service_mode(source_service)
    print_info(f"Source service '{source_service}' mode: {source_mode}")
    
    # Step 2: Select sink service (optional)
    sink_service = args.sink or prompt_select("Select sink service (optional):", available_services, allow_empty=True)
    if sink_service:
        sink_mode = detect_service_mode(sink_service)
        print_info(f"Sink service '{sink_service}' mode: {sink_mode}")
    
    # Step 3: Select source schema
    source_schemas = [d.name for d in (SERVICE_SCHEMAS_DIR / source_service).iterdir() if d.is_dir()] if (SERVICE_SCHEMAS_DIR / source_service).exists() else []
    
    if not source_schemas:
        print_error(f"No schemas found in service-schemas/{source_service}/")
        return 1
    
    source_schema = args.source_schema or prompt_select("Select source schema:", source_schemas)
    if not source_schema:
        return 1
    
    # Step 4: Select source table
    source_tables = load_service_schema_tables(source_service, source_schema)
    if not source_tables:
        print_error(f"No tables found in service-schemas/{source_service}/{source_schema}/")
        return 1
    
    source_table = args.source_table or prompt_select("Select source table:", source_tables)
    if not source_table:
        return 1
    
    # Load source table definition
    source_table_def = get_table_schema_definition(source_service, source_schema, source_table)
    if not source_table_def:
        print_error(f"Table definition not found: {source_table}")
        return 1
    
    source_columns = [col['name'] for col in source_table_def.get('columns', [])]
    print_success(f"Source table '{source_table}' has {len(source_columns)} columns")
    
    # Step 5: Ignore/track columns
    print_info("\nConfigure column tracking:")
    ignore_columns = prompt_multiselect("Select columns to IGNORE:", source_columns)
    
    # Step 6: If sink is specified, configure mappings
    if sink_service:
        # Select sink schema
        sink_schemas = [d.name for d in (SERVICE_SCHEMAS_DIR / sink_service).iterdir() if d.is_dir()] if (SERVICE_SCHEMAS_DIR / sink_service).exists() else []
        
        if not sink_schemas:
            print_error(f"No schemas found in service-schemas/{sink_service}/")
            return 1
        
        sink_schema = args.sink_schema or prompt_select("Select sink schema:", sink_schemas)
        if not sink_schema:
            return 1
        
        # Select sink table
        sink_tables = load_service_schema_tables(sink_service, sink_schema)
        if not sink_tables:
            print_error(f"No tables found in service-schemas/{sink_service}/{sink_schema}/")
            return 1
        
        sink_table = args.sink_table or prompt_select("Select sink table:", sink_tables)
        if not sink_table:
            return 1
        
        # Load sink table definition
        sink_table_def = get_table_schema_definition(sink_service, sink_schema, sink_table)
        if not sink_table_def:
            print_error(f"Table definition not found: {sink_table}")
            return 1
        
        sink_columns = [col['name'] for col in sink_table_def.get('columns', [])]
        print_success(f"Sink table '{sink_table}' has {len(sink_columns)} columns")
        
        # Create column mappings
        tracked_columns = [col for col in source_columns if col not in ignore_columns]
        mappings = prompt_mappings(tracked_columns, sink_columns)
        
        # Validate compatibility
        if not validate_table_compatibility(source_table_def, sink_table_def, mappings):
            print_error("Table compatibility validation failed")
            return 1
        
        # Display summary
        print_header("Configuration Summary")
        print(f"{Colors.CYAN}Source:{Colors.RESET} {source_service}.{source_schema}.{source_table}")
        print(f"{Colors.CYAN}Sink:{Colors.RESET} {sink_service}.{sink_schema}.{sink_table}")
        print(f"{Colors.CYAN}Ignored columns:{Colors.RESET} {', '.join(ignore_columns) if ignore_columns else 'None'}")
        print(f"{Colors.CYAN}Mappings:{Colors.RESET}")
        for src, snk in mappings.items():
            print(f"  {src} → {snk}")
        
        # TODO: Save configuration to service YAML
        print_warning("\n⚠️  Configuration saving not yet implemented")
    else:
        # Just source configuration
        print_header("Configuration Summary")
        print(f"{Colors.CYAN}Source:{Colors.RESET} {source_service}.{source_schema}.{source_table}")
        print(f"{Colors.CYAN}Ignored columns:{Colors.RESET} {', '.join(ignore_columns) if ignore_columns else 'None'}")
        
        # TODO: Save configuration to service YAML
        print_warning("\n⚠️  Configuration saving not yet implemented")
    
    print_success("\n✓ Table configuration completed!")
    return 0
