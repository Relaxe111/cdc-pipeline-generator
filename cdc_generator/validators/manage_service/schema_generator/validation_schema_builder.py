"""JSON Schema builder for service validation (main schema assembly)."""

from pathlib import Path
from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_success
from cdc_generator.helpers.service_config import get_project_root

from .schema_properties import (
    add_table_definitions,
    build_conditional_requirements,
    build_customers_schema,
    build_environments_schema,
    build_single_environment_schema,
)


def build_json_schema_structure(
    service: str,
    database: str,
    server_group: str,
    schema_ref: str,
    schemas_data: dict[str, Any]
) -> dict[str, Any]:
    """Build the comprehensive JSON Schema structure for service validation.

    Args:
        service: Service name
        database: Database name for validation
        server_group: Server group name
        schema_ref: Reference to schema_name mini schema
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]

    Returns:
        Complete JSON Schema dict
    """
    json_schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"{service.title()} Service Validation Schema",
        "description": f"Comprehensive validation schema for {service} service: structure + table content validation (from service-schemas/{service})",
        "type": "object",
        "required": [],
        "definitions": {},
        "properties": {
            "service": {
                "$ref": "keys/service.schema.json",
                "description": "⚠️ DEPRECATED: Service name is now the root key in YAML (e.g., adopus:, directory:). This field is kept for backward compatibility."
            },
            "server_group": {
                "$ref": "keys/server_group.schema.json"
            },
            "reference": {
                "type": "string",
                "description": "Reference customer/database for schema validation (db-per-tenant only)"
            },
            "mode": {
                "type": "string",
                "enum": ["db-per-tenant", "shared-db"],
                "description": "⚠️ LEGACY: Service mode - use server_group instead. db-per-tenant (database per customer) or shared-db (customer_id column)"
            },
            "source": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["mssql", "postgres"],
                        "description": "⚠️ LEGACY: Source database type (auto-detected from server_group)"
                    },
                    "validation_database": {
                        "$ref": f"keys/database_name/{server_group}.schema.json",
                        "description": f"Database name for schema validation (from server_group: {server_group})"
                    },
                    "validation_env": {
                        "type": "string",
                        "enum": ["local", "nonprod", "prod", "prod-fretex"],
                        "description": "Environment for schema validation"
                    }
                }
            },
            "source_tables": {
                "type": "array",
                "description": "Source table definitions grouped by schema",
                "items": {
                    "type": "object",
                    "required": ["schema", "tables"],
                    "properties": {
                        "schema": {
                            "$ref": schema_ref,
                            "description": f"Database schema name (from validation_database: {database})"
                        },
                        "tables": {
                            "type": "array",
                            "description": "List of tables in this schema",
                            "items": {
                                "anyOf": []
                            }
                        }
                    }
                }
            },
            "environments": build_environments_schema(),
            "environment": build_single_environment_schema(),
            "customers": build_customers_schema()
        },
        "allOf": build_conditional_requirements()
    }

    # Add table definitions
    add_table_definitions(json_schema, schemas_data)

    return json_schema


def update_service_yaml_header(service: str, database: str, schemas_data: dict[str, Any]) -> bool:
    """Update service YAML file with schema validation comment and informational header.

    Args:
        service: Service name
        database: Database name
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]

    Returns:
        True if update succeeded
    """
    try:
        services_dir = get_project_root() / 'services'
        service_yaml_path = services_dir / f'{service}.yaml'
        schema_comment = f"# yaml-language-server: $schema=../.vscode/schemas/{database}.service-validation.schema.json"

        # Detect case-variant column names
        case_variants: dict[str, set[str]] = {}
        for _schema_name, tables in schemas_data.items():
            for _table_name, table_info in tables.items():
                for col in table_info['columns']:
                    col_name = col['name']
                    col_lower = col_name.lower()
                    if col_lower not in case_variants:
                        case_variants[col_lower] = set()
                    case_variants[col_lower].add(col_name)

        overlapping_columns = {col_lower: sorted(variants) for col_lower, variants in case_variants.items() if len(variants) > 1}
        has_case_variants = len(overlapping_columns) > 0

        # Build informational header
        info_header = f"""# The redhat.vscode-yaml extension shows ALL possible column names from ALL tables
# in autocomplete suggestions, not just columns for the specific table being edited.
# This is a known limitation of the extension's JSON Schema support.
#
# ✅ NOTE: Despite this limitation, the schema DOES validate that column names
#    you specify actually exist in the specific table. Invalid columns will be
#    marked as errors.
#
# 💡 HIERARCHICAL CONFIGURATION
# Properties set at higher levels are inherited by lower levels unless overridden.
# Inheritance chain: environments (root) → environments.<env> → customers[].environments.<env>
# To validate hierarchical configuration:
#   cdc manage-tables --service {service} --validate-hierarchy
# To validate complete configuration for pipeline generation:
#   cdc manage-tables --service {service} --validate-config
#
# 🛡️  existing_mssql FLAG
# Controls database initialization behavior:
#   • false: Fresh setup - can create database and tables (for local development)
#   • true:  Existing DB - only enable CDC, never modify database (nonprod/prod)
#
"""

        # Build warning comment if case variants exist
        warning_comment = ""
        if has_case_variants:
            overlap_list: list[str] = []
            for col_lower, variants in sorted(overlapping_columns.items()):
                overlap_list.append(f"#   - {', '.join(variants)}")
            overlap_section = '\n'.join(overlap_list)

            warning_comment = f"""#
# ⚠️  CASE SENSITIVITY WARNING
# The following columns exist with different casing across tables:
{overlap_section}
#
# Always verify the exact column name casing for your specific table.
"""

        final_comment = f"# 📝 To verify column names for a specific table:\n#    cdc manage-service --service {service} --inspect --schema {{schema_name}}\n# ==================================================================================\n"

        full_header = schema_comment + '\n' + info_header + warning_comment + final_comment

        with open(service_yaml_path) as f:
            content = f.read()

        # Remove ALL old header comments (everything before first non-comment line)
        lines = content.split('\n')
        first_non_comment_idx = 0
        for i, line in enumerate(lines):
            if line and not line.startswith('#'):
                first_non_comment_idx = i
                break

        # Keep only the actual YAML content
        yaml_content = '\n'.join(lines[first_non_comment_idx:])

        # Write new header + content
        with open(service_yaml_path, 'w') as f:
            f.write(full_header + '\n' + yaml_content)

        print_success(f"\n✓ Updated schema validation in {service}.yaml")
        return True

    except Exception as e:
        print_error(f"Failed to update service YAML header: {e}")
        return False
