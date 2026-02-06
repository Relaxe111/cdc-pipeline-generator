"""Schema generator package - Main entry point for JSON Schema validation generation.

This module provides the public API for generating JSON Schema validation files
for CDC service YAML configurations. The schema generation is based on YAML files
from the service-schemas/ directory.

Main functions:
    - generate_service_validation_schema(): Generate comprehensive validation schema
    - save_detailed_schema(): Legacy function for MSSQL introspection (kept for compatibility)

The refactored structure:
    - yaml_loader.py: Load schemas from YAML files
    - mini_schema_generators.py: Generate mini schemas for keys (service, database, table, etc.)
    - validation_schema_builder.py: Build comprehensive JSON Schema structure
    - legacy_db_inspector.py: Legacy MSSQL database introspection (not used in main flow)
"""

import json
from pathlib import Path
from typing import Optional

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.service_config import load_service_config

from .legacy_db_inspector import save_detailed_schema
from .mini_schema_generators import (
    generate_database_name_schemas,
    generate_schema_name_schemas,
    generate_server_group_enum_schema,
    generate_service_enum_schema,
    generate_table_names_enum_schema,  # type: ignore[misc]
)
from .validation_schema_builder import build_json_schema_structure, update_service_yaml_header
from .yaml_loader import (
    extract_database_names_by_group,
    load_schemas_from_yaml,
    load_server_groups_config,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def generate_service_validation_schema(
    service: str,
    env: str = 'nonprod',
    schema_filter: str | None = None
) -> bool:
    """Generate JSON Schema for service YAML validation based on database schema.
    
    This is the main entry point for schema generation. It:
    1. Loads schemas from service-schemas/{service}/ YAML files
    2. Generates mini schemas for keys (service, server_group, database_name, etc.)
    3. Builds comprehensive JSON Schema with table definitions
    4. Updates service YAML file with schema validation comment
    
    Args:
        service: Service name (e.g., 'adopus', 'directory')
        env: Environment name (default: nonprod) - currently unused but kept for API compatibility
        schema_filter: Optional schema name to filter (None = all schemas)
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        config = load_service_config(service)
        server_group = config.get('server_group', '')

        # Load server groups configuration
        server_groups_data = load_server_groups_config()
        extract_database_names_by_group(server_groups_data)

        # Load schemas from YAML files
        schema_msg = f"schema: {schema_filter}" if schema_filter else "all schemas"
        print_header(f"Generating validation schema from service-schemas/{service} ({schema_msg})")

        schemas_data = load_schemas_from_yaml(service, schema_filter)

        if not schemas_data:
            print_error(f"No schemas found in service-schemas/{service}" + (f" matching '{schema_filter}'" if schema_filter else ""))
            print_info(f"Make sure YAML files exist in service-schemas/{service}/{{schema}}/{{table}}.yaml")
            return False

        # Display loaded schemas
        for schema_name in sorted(schemas_data.keys()):
            print_info(f"Processing schema: {schema_name}")
            total_tables = len(schemas_data[schema_name])
            for idx, table_name in enumerate(sorted(schemas_data[schema_name].keys()), 1):
                print(f"  {Colors.CYAN}[{idx}/{total_tables}]{Colors.RESET} {table_name}")

        # Get schema names and database for validation
        schema_names = sorted(schemas_data.keys())
        database = config.get('source', {}).get('validation_database', service)

        # Generate mini schemas for keys (BEFORE determining schema_ref so shared schemas exist)
        generate_service_enum_schema()
        generate_server_group_enum_schema()
        generate_database_name_schemas()
        generate_schema_name_schemas()
        generate_table_names_enum_schema(service, schemas_data)

        # Determine schema reference (use shared if schema list has shared version)
        if len(schema_names) == 1:
            shared_name = schema_names[0]
        else:
            shared_name = '_'.join(schema_names)

        shared_schema_file = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys' / 'schema_name' / 'shared' / f'{shared_name}.schema.json'

        if shared_schema_file.exists():
            schema_ref = f"keys/schema_name/shared/{shared_name}.schema.json"
        else:
            schema_ref = f"keys/schema_name/{database}.schema.json"

        # Build comprehensive JSON Schema
        json_schema = build_json_schema_structure(
            service=service,
            database=database,
            server_group=server_group,
            schema_ref=schema_ref,
            schemas_data=schemas_data
        )

        # Save JSON Schema
        output_dir = PROJECT_ROOT / '.vscode' / 'schemas'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f'{database}.service-validation.schema.json'

        with open(output_file, 'w') as f:
            json.dump(json_schema, f, indent=2)

        print_success(f"\nGenerated validation schema: {output_file}")
        print_info(f"  {len(schemas_data)} schemas: {', '.join(schemas_data.keys())}")
        print_info(f"  Total tables: {sum(len(t) for t in schemas_data.values())}")

        # Update service YAML header with schema validation comment
        update_service_yaml_header(service, database, schemas_data)

        return True

    except Exception as e:
        print_error(f"Failed to generate validation schema: {e}")
        import traceback
        traceback.print_exc()
        return False


# Re-export public API
__all__ = [
    'generate_database_name_schemas',
    'generate_schema_name_schemas',
    'generate_server_group_enum_schema',
    'generate_service_enum_schema',
    'generate_service_validation_schema',
    'generate_table_names_enum_schema',
    'save_detailed_schema',  # Legacy function kept for compatibility
]
