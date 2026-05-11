"""JSON Schema builder for service validation (main schema assembly)."""

from pathlib import Path

from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_success
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.validators.manage_service.config import (
    build_service_validation_header_section,
    render_service_file_header_comment,
)

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
    schemas_data: dict[str, Any],
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
        "description": (
            f"Comprehensive validation schema for {service} service: " + "structure + table content validation " + f"(from service-schemas/{service})"
        ),
        "type": "object",
        "required": [],
        "definitions": {},
        "properties": {
            "service": {
                "$ref": "keys/service.schema.json",
                "description": (
                    "⚠️ DEPRECATED: Service name is now the root key in YAML "
                    + "(e.g., adopus:, directory:). "
                    + "This field is kept for backward compatibility."
                ),
            },
            "server_group": {
                "$ref": "keys/server_group.schema.json",
            },
            "reference": {
                "type": "string",
                "description": ("Reference customer/database for schema validation " + "(db-per-tenant only)"),
            },
            "mode": {
                "type": "string",
                "enum": ["db-per-tenant", "shared-db"],
                "description": (
                    "⚠️ LEGACY: Service mode - use server_group instead. "
                    + "db-per-tenant (database per customer) "
                    + "or shared-db (customer_id column)"
                ),
            },
            "source": {
                "type": "object",
                "required": ["validation_database"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["mssql", "postgres"],
                        "description": ("⚠️ LEGACY: Source database type " + "(auto-detected from server_group)"),
                    },
                    "validation_database": {
                        "$ref": f"keys/database_name/{server_group}.schema.json",
                        "description": ("Database name for schema validation " + f"(from server_group: {server_group})"),
                    },
                },
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
                            "description": ("Database schema name " + f"(from validation_database: {database})"),
                        },
                        "tables": {
                            "type": "array",
                            "description": "List of tables in this schema",
                            "items": {
                                "anyOf": [],
                            },
                        },
                    },
                },
            },
            "environments": build_environments_schema(),
            "environment": build_single_environment_schema(),
            "customers": build_customers_schema(),
        },
        "allOf": build_conditional_requirements(),
    }

    # Add table definitions
    add_table_definitions(json_schema, schemas_data)

    return json_schema


def update_service_yaml_header(
    service: str,
    database: str,
    schemas_data: dict[str, Any],
) -> bool:
    """Update service YAML file with schema validation comment and informational header.

    Args:
        service: Service name
        database: Database name
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]

    Returns:
        True if update succeeded
    """
    try:
        del database
        services_dir = get_project_root() / "services"
        service_yaml_path = services_dir / f"{service}.yaml"

        report_path = _write_service_awareness_report(service, schemas_data)
        validation_section = build_service_validation_header_section(
            service,
            report_path.name,
        )

        content = service_yaml_path.read_text(encoding="utf-8")
        header_lines_end = 0
        for idx, line in enumerate(content.splitlines(keepends=True)):
            stripped = line.strip()
            if stripped.startswith("#") or (header_lines_end == idx and not stripped):
                header_lines_end += len(line)
                continue
            break

        existing_header = content[:header_lines_end] or None
        yaml_content = content[header_lines_end:]
        full_header = render_service_file_header_comment(
            service,
            existing_header,
            validation_section,
        )

        service_yaml_path.write_text(full_header + yaml_content, encoding="utf-8")

        print_success(f"\n✓ Updated schema validation in {service}.yaml")
        return True

    except Exception as e:
        print_error(f"Failed to update service YAML header: {e}")
        return False


def _collect_case_variant_columns(
    schemas_data: dict[str, Any],
) -> dict[str, list[str]]:
    """Collect columns that appear with multiple casing variants across schemas."""
    case_variants: dict[str, set[str]] = {}
    for tables in schemas_data.values():
        for table_info in tables.values():
            columns = table_info.get("columns", [])
            if not isinstance(columns, list):
                continue
            for column in columns:
                if not isinstance(column, dict):
                    continue
                col_name = column.get("name")
                if not isinstance(col_name, str):
                    continue
                col_lower = col_name.lower()
                if col_lower not in case_variants:
                    case_variants[col_lower] = set()
                case_variants[col_lower].add(col_name)

    return {col_lower: sorted(variants) for col_lower, variants in case_variants.items() if len(variants) > 1}


def _build_service_awareness_report(
    service: str,
    schemas_data: dict[str, Any],
    overlapping_columns: dict[str, list[str]],
) -> str:
    """Build the service-specific warnings and awareness report content."""
    schema_names = sorted(schemas_data.keys())
    table_count = sum(len(tables) for tables in schemas_data.values())
    lines = [
        f"# {service} Warnings and Awareness",
        "",
        "This file is auto-generated by:",
        f"`cdc manage-services config --service {service} --generate-validation --all`",
        "",
        "It tracks service-specific observations, warnings, and recommendations",
        "that developers should be aware of when working on this service.",
        "",
        "## Summary",
        "",
        f"- Schemas inspected: {', '.join(schema_names)}",
        f"- Tables inspected: {table_count}",
        f"- Global case-variant column groups: {len(overlapping_columns)}",
        "",
        "## Global Observations",
        "",
    ]

    if overlapping_columns:
        lines.extend(
            [
                "### Case-variant columns across tables",
                "",
                "These are cross-table observations from the inspected service schemas.",
                "They do not automatically mean a runtime failure, but they increase the",
                "risk of using the wrong column casing when authoring mappings, includes,",
                "ignores, or transform outputs by hand.",
                "",
            ]
        )
        for variants in overlapping_columns.values():
            lines.append(f"- {', '.join(variants)}")
    else:
        lines.extend(
            [
                "No global service-level warnings were detected during validation generation.",
            ]
        )

    lines.extend(
        [
            "",
            "## Recommendations",
            "",
            "- When mapping columns manually, inspect the exact table schema before choosing a column name.",
            f"- Use `cdc manage-services config --service {service} --inspect --schema <schema_name>`",
            "  to verify column names for a specific table.",
            "- Treat this file as generated awareness information; rerun validation generation after schema refreshes.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_service_awareness_report(
    service: str,
    schemas_data: dict[str, Any],
) -> Path:
    """Write the service-specific warnings and awareness report next to the service YAML."""
    services_dir = get_project_root() / "services"
    report_path = services_dir / f"{service}-warnings.md"
    overlapping_columns = _collect_case_variant_columns(schemas_data)
    report_content = _build_service_awareness_report(
        service,
        schemas_data,
        overlapping_columns,
    )
    report_path.write_text(report_content + "\n", encoding="utf-8")
    return report_path
