"""Output/help-text builders for custom sink table handlers."""

from __future__ import annotations


def build_table_exists_messages(
    table_key: str,
    target_service: str,
    schema_name: str,
) -> tuple[str, str]:
    """Build messages shown when custom table already exists in schema files."""
    error_message = (
        f"Table '{table_key}' already exists in "
        + f"service-schemas/{target_service}/{schema_name}/"
    )
    info_message = "Use --add-sink-table instead for existing schema tables"
    return error_message, info_message


def build_source_table_not_found_messages(
    table_ref: str,
    service: str,
    table_keys: list[str],
) -> tuple[str, str | None]:
    """Build messages when --from source table is not declared in source.tables."""
    error_message = (
        f"Source table '{table_ref}' not found in service '{service}'"
    )
    info_message = None
    if table_keys:
        info_message = "Available source tables: " + ", ".join(table_keys)
    return error_message, info_message


def build_source_schema_missing_messages(
    table_ref: str,
    service: str,
) -> tuple[str, str]:
    """Build messages when schema for --from source table is missing on disk."""
    error_message = (
        f"Schema for source table '{table_ref}' not found in service-schemas"
    )
    info_message = (
        "Run: cdc manage-services config --service "
        + f"{service} --inspect --all --save"
    )
    return error_message, info_message


def build_created_custom_table_messages(
    table_key: str,
    sink_key: str,
    col_names: list[str],
    target_service: str,
    schema_name: str,
    table_name: str,
) -> tuple[str, str, str]:
    """Build success/info output after creating a custom sink table."""
    success_message = f"Created custom table '{table_key}' in sink '{sink_key}'"
    columns_message = f"Columns: {', '.join(col_names)}"
    schema_saved_message = (
        f"Schema saved to: service-schemas/{target_service}/"
        + f"{schema_name}/{table_name}.yaml"
    )
    return success_message, columns_message, schema_saved_message


def build_custom_table_disabled_messages(
    table_key: str,
) -> tuple[str, str]:
    """Build messages when table is not custom and cannot be modified via CLI."""
    error_message = (
        f"Table '{table_key}' is not a custom table "
        + "- it was inferred from source schemas"
    )
    info_message = "Only tables with 'custom: true' can be modified via CLI"
    return error_message, info_message


def build_custom_table_unmanaged_messages(
    table_key: str,
) -> tuple[str, str]:
    """Build messages when custom table has managed=false."""
    error_message = (
        f"Table '{table_key}' has custom=true but managed=false "
        + "- CLI modifications are disabled"
    )
    info_message = "Set 'managed: true' in the YAML to enable CLI edits"
    return error_message, info_message


def build_no_column_definitions_messages(table_key: str) -> tuple[str, str]:
    """Build messages when no columns can be resolved for custom table."""
    error_message = f"No column definitions available for '{table_key}'"
    info_message = (
        "Ensure schema exists under services/_schemas/<target>/<schema>/<table>.yaml"
    )
    return error_message, info_message


def build_column_not_found_messages(
    table_key: str,
    column_name: str,
    available: list[str],
) -> tuple[str, str | None]:
    """Build messages when trying to remove a missing column."""
    error_message = f"Column '{column_name}' not found in '{table_key}'"
    info_message = None
    if available:
        info_message = "Available columns: " + ", ".join(available)
    return error_message, info_message
