"""Service schema autocompletion functions.

Provides completions for the ``manage-services resources`` command,
including service names from ``service-schemas/`` and custom table
references.
"""

from cdc_generator.helpers.autocompletions.utils import (
    find_service_schemas_dir_upward,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_schema_services() -> list[str]:
    """List services that have service-schemas directories.

    Used for ``--service`` flag autocompletion in
    ``manage-services resources`` command.

    Returns:
        Sorted list of service directory names.
    """
    schemas_dir = find_service_schemas_dir_upward()
    if not schemas_dir:
        return []

    services: list[str] = []
    for d in schemas_dir.iterdir():
        if (
            d.is_dir()
            and not d.name.startswith((".", "_"))
            and d.name not in ("_definitions", "_bloblang")
        ):
            services.append(d.name)

    return sorted(services)


def list_custom_tables_for_schema_service(
    service: str,
) -> list[str]:
    """List custom tables for a service from service-schemas.

    Used for ``--show`` and ``--remove-custom-table`` autocompletion.

    Args:
        service: Service name.

    Returns:
        List of table references (schema.table).
    """
    schemas_dir = find_service_schemas_dir_upward()
    if not schemas_dir:
        return []

    custom_dir = schemas_dir / service / "custom-tables"
    if not custom_dir.exists():
        return []

    tables: list[str] = []
    for f in sorted(custom_dir.glob("*.yaml")):
        # Filename: schema.table.yaml → schema.table
        tables.append(f.stem)

    return tables


def list_custom_table_columns_for_mapping(
    service: str,
    table_ref: str,
) -> list[str]:
    """List column names from a custom table definition.

    Used for ``--map-column`` RIGHT-side autocompletion
    (target columns from custom table).

    Args:
        service: Service name (target service).
        table_ref: Table reference (schema.table).

    Returns:
        List of column names.
    """
    schemas_dir = find_service_schemas_dir_upward()
    if not schemas_dir:
        return []

    custom_file = (
        schemas_dir / service / "custom-tables"
        / f"{table_ref}.yaml"
    )
    if not custom_file.is_file():
        return []

    try:
        data = load_yaml_file(custom_file)
        if not data:
            return []

        columns = data.get("columns", [])
        if not isinstance(columns, list):
            return []

        return sorted(
            str(col["name"])
            for col in columns
            if isinstance(col, dict) and col.get("name")
        )
    except Exception:
        return []
