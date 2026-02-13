"""Custom table CRUD operations for service schemas.

Manages custom table YAML files under
``service-schemas/{service}/custom-tables/{schema}.{table}.yaml``.

Custom tables use the same format as inspected tables, with an
additional ``custom: true`` marker.
"""

from pathlib import Path
from typing import Any

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.service_schema_paths import (
    get_schema_roots,
    get_service_schema_read_dirs,
    get_service_schema_write_dir,
)

try:
    from cdc_generator.helpers.yaml_loader import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------

_CUSTOM_TABLES_DIR = "custom-tables"

# Minimum parts in a column spec (name:type)
_MIN_COLUMN_PARTS = 2

# Expected parts in a table ref (schema.table)
_TABLE_REF_PARTS = 2

# Known PostgreSQL types (for validation warnings)
_PG_TYPES = frozenset({
    "smallint", "integer", "bigint", "serial", "bigserial",
    "numeric", "real", "double precision",
    "text", "varchar", "char", "citext",
    "boolean",
    "date", "time", "timestamp", "timestamptz", "interval",
    "uuid",
    "json", "jsonb",
    "bytea",
    "inet", "cidr", "macaddr",
    "text[]", "integer[]", "uuid[]", "jsonb[]",
})

# Default expression aliases
_DEFAULT_EXPRESSIONS: dict[str, str] = {
    "now": "now()",
    "default_now": "now()",
    "current_timestamp": "CURRENT_TIMESTAMP",
    "current_date": "CURRENT_DATE",
    "gen_random_uuid": "gen_random_uuid()",
    "uuid": "gen_random_uuid()",
    "default_gen_random_uuid": "gen_random_uuid()",
    "default_0": "0",
    "default_false": "false",
    "default_true": "true",
    "default_empty": "''",
}


# -------------------------------------------------------------------
# Path helpers
# -------------------------------------------------------------------


def _get_custom_tables_dir(service: str) -> Path:
    """Return canonical custom-tables/ dir for a service."""
    return get_service_schema_write_dir(service, get_project_root()) / _CUSTOM_TABLES_DIR


def _custom_table_path(
    service: str,
    schema: str,
    table: str,
) -> Path:
    """Return path to a custom table YAML file."""
    return _get_custom_tables_dir(service) / f"{schema}.{table}.yaml"


def _custom_table_read_path(
    service: str,
    schema: str,
    table: str,
) -> Path | None:
    """Return existing custom-table path from preferred/legacy roots."""
    for service_dir in get_service_schema_read_dirs(service, get_project_root()):
        candidate = service_dir / _CUSTOM_TABLES_DIR / f"{schema}.{table}.yaml"
        if candidate.exists():
            return candidate
    return None


# -------------------------------------------------------------------
# Column spec parsing
# -------------------------------------------------------------------


def parse_column_spec(
    spec: str,
) -> dict[str, Any] | None:
    """Parse a column specification string.

    Format: ``name:type[:modifier[:modifier...]]``

    Modifiers:
        pk          — primary key
        not_null    — NOT NULL constraint
        nullable    — allow NULL (explicit)
        default_X   — default expression

    Args:
        spec: Column specification string.

    Returns:
        Column dict or None on parse error.

    Examples:
        >>> parse_column_spec("id:uuid:pk")
        {'name': 'id', 'type': 'uuid', ...}
    """
    parts = spec.split(":")
    if len(parts) < _MIN_COLUMN_PARTS:
        print_error(
            f"Invalid column spec '{spec}'. "
            + "Format: name:type[:pk][:not_null][:default_X]"
        )
        return None

    col_name = parts[0].strip()
    col_type = parts[1].strip().lower()

    if not col_name:
        print_error("Column name cannot be empty")
        return None
    if not col_type:
        print_error("Column type cannot be empty")
        return None

    # Warn if unknown type (don't fail)
    if col_type not in _PG_TYPES:
        _try_type_definitions_check(col_type)

    # Parse modifiers
    is_pk = False
    is_nullable = True
    default_expr: str | None = None
    has_nullable_modifier = False
    has_not_null_modifier = False

    for mod in parts[2:]:
        mod_lower = mod.strip().lower()
        if mod_lower == "pk":
            if has_nullable_modifier:
                print_error(
                    "Invalid column spec: 'pk' cannot be combined with 'nullable'"
                )
                return None
            is_pk = True
            is_nullable = False
        elif mod_lower == "not_null":
            if has_nullable_modifier:
                print_error(
                    "Invalid column spec: 'not_null' cannot be combined with 'nullable'"
                )
                return None
            has_not_null_modifier = True
            is_nullable = False
        elif mod_lower == "nullable":
            if is_pk:
                print_error(
                    "Invalid column spec: 'nullable' cannot be combined with 'pk'"
                )
                return None
            if has_not_null_modifier:
                print_error(
                    "Invalid column spec: 'nullable' cannot be combined with 'not_null'"
                )
                return None
            has_nullable_modifier = True
            is_nullable = True
        elif mod_lower.startswith("default_"):
            key = mod_lower.removeprefix("default_")
            default_expr = _DEFAULT_EXPRESSIONS.get(
                key, key,
            )
        elif mod_lower in _DEFAULT_EXPRESSIONS:
            default_expr = _DEFAULT_EXPRESSIONS[mod_lower]
        else:
            print_warning(f"Unknown modifier '{mod}' — ignoring")

    column: dict[str, Any] = {
        "name": col_name,
        "type": col_type,
        "nullable": is_nullable,
        "primary_key": is_pk,
    }
    if default_expr:
        column["default"] = default_expr

    return column


def _try_type_definitions_check(col_type: str) -> None:
    """Check type against definitions if available."""
    try:
        from .type_definitions import get_all_type_names

        pgsql_types = get_all_type_names("pgsql")
        if pgsql_types and col_type not in pgsql_types:
            print_warning(
                f"Unknown column type '{col_type}'. "
                + "Run --inspect --save to update type "
                + "definitions, or check spelling."
            )
        elif not pgsql_types:
            print_warning(
                f"Type '{col_type}' not in built-in list. "
                + "Run --inspect --save to generate "
                + "type definitions for validation."
            )
    except ImportError:
        print_warning(
            f"Type '{col_type}' not in built-in types list"
        )


# -------------------------------------------------------------------
# CRUD operations
# -------------------------------------------------------------------


def create_custom_table(
    service: str,
    table_ref: str,
    column_specs: list[str],
) -> bool:
    """Create a custom table schema YAML file.

    Args:
        service: Service name (directory under service-schemas/).
        table_ref: Table reference as ``schema.table``.
        column_specs: List of column specs (name:type[:mods]).

    Returns:
        True on success.
    """
    if yaml is None:
        print_error("PyYAML is required")
        return False

    # Parse table reference
    parsed = _parse_table_ref(table_ref)
    if not parsed:
        return False
    schema, table = parsed

    # Check if already exists
    existing_file_path = _custom_table_read_path(service, schema, table)
    file_path = _custom_table_path(service, schema, table)
    if existing_file_path is not None:
        print_error(
            f"Custom table '{table_ref}' already exists "
            + f"for service '{service}'"
        )
        print_info(f"File: {existing_file_path}")
        return False

    # Parse all column specs
    columns: list[dict[str, Any]] = []
    for spec in column_specs:
        col = parse_column_spec(spec)
        if col is None:
            return False
        columns.append(col)

    if not columns:
        print_error("At least one --column is required")
        return False

    # Build primary keys list
    primary_keys = [
        c["name"] for c in columns if c.get("primary_key")
    ]

    # Build YAML structure (same format as inspected tables)
    table_data: dict[str, Any] = {
        "database": None,
        "schema": schema,
        "service": service,
        "table": table,
        "custom": True,
        "columns": columns,
        "primary_key": (
            primary_keys[0]
            if len(primary_keys) == 1
            else primary_keys
        ),
    }

    # Save
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        yaml.dump(table_data, f)

    print_success(
        f"Created custom table: {service}/{_CUSTOM_TABLES_DIR}"
        + f"/{schema}.{table}.yaml"
    )
    _print_table_summary(columns, primary_keys)

    if not primary_keys:
        print_warning(
            "Table created without primary key. "
            + "This table is incomplete and may not be usable in sinks."
        )
        print_info(
            "Suggested fix: recreate with a PK column, e.g. "
            + "--column id:uuid:pk"
        )
        print_info(
            "Or edit the table file and add primary_key to one or more columns."
        )

    return True


def remove_custom_table(
    service: str,
    table_ref: str,
) -> bool:
    """Remove a custom table schema YAML file.

    Args:
        service: Service name.
        table_ref: Table reference as ``schema.table``.

    Returns:
        True on success.
    """
    parsed = _parse_table_ref(table_ref)
    if not parsed:
        return False
    schema, table = parsed

    file_path = _custom_table_read_path(service, schema, table)
    if file_path is None:
        print_error(
            f"Custom table '{table_ref}' not found "
            + f"for service '{service}'"
        )
        return False

    file_path.unlink()
    print_success(f"Removed custom table '{table_ref}'")

    # Clean up empty directory
    parent = file_path.parent
    if parent.exists() and not any(parent.iterdir()):
        parent.rmdir()
        print_info("Removed empty custom-tables/ directory")

    return True


def list_custom_tables(service: str) -> list[str]:
    """List all custom tables for a service.

    Args:
        service: Service name.

    Returns:
        List of table references (schema.table).
    """
    table_refs: set[str] = set()
    for service_dir in get_service_schema_read_dirs(service, get_project_root()):
        custom_dir = service_dir / _CUSTOM_TABLES_DIR
        if not custom_dir.exists():
            continue
        for file_path in custom_dir.glob("*.yaml"):
            table_refs.add(file_path.stem)

    return sorted(table_refs)


def show_custom_table(
    service: str,
    table_ref: str,
) -> dict[str, Any] | None:
    """Load and return a custom table's schema data.

    Args:
        service: Service name.
        table_ref: Table reference as ``schema.table``.

    Returns:
        Table schema dict or None if not found.
    """
    if yaml is None:
        print_error("PyYAML is required")
        return None

    parsed = _parse_table_ref(table_ref)
    if not parsed:
        return None
    schema, table = parsed

    file_path = _custom_table_read_path(service, schema, table)
    if file_path is None:
        print_error(
            f"Custom table '{table_ref}' not found "
            + f"for service '{service}'"
        )
        return None

    with file_path.open(encoding="utf-8") as f:
        data = yaml.load(f)
        if isinstance(data, dict):
            return data
        return None


def get_custom_table_columns(
    service: str,
    table_ref: str,
) -> list[str]:
    """Get column names from a custom table.

    Args:
        service: Service name.
        table_ref: Table reference as ``schema.table``.

    Returns:
        List of column names, or empty list.
    """
    data = show_custom_table(service, table_ref)
    if not data or "columns" not in data:
        return []

    columns: list[dict[str, Any]] = data["columns"]
    return [
        str(c["name"])
        for c in columns
        if "name" in c
    ]


def list_services_with_schemas() -> list[str]:
    """List all services that have service-schemas/ dirs.

    Returns:
        List of service directory names.
    """
    service_names: set[str] = set()
    for schemas_root in get_schema_roots(get_project_root()):
        if not schemas_root.exists():
            continue
        for service_dir in schemas_root.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith((".", "_")):
                if service_dir.name in ("_definitions", "_bloblang", "adapters", "types"):
                    continue
                service_names.add(service_dir.name)

    return sorted(service_names)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _parse_table_ref(table_ref: str) -> tuple[str, str] | None:
    """Parse ``schema.table`` reference.

    Returns:
        (schema, table) or None on error.
    """
    parts = table_ref.split(".", 1)
    if len(parts) != _TABLE_REF_PARTS or not parts[0] or not parts[1]:
        print_error(
            f"Invalid table reference '{table_ref}'. "
            + "Expected format: schema.table"
        )
        return None
    return parts[0], parts[1]


def _print_table_summary(
    columns: list[dict[str, Any]],
    primary_keys: list[str],
) -> None:
    """Print a summary of the created table."""
    print_info(f"  Columns: {len(columns)}")
    pk_str = ", ".join(primary_keys) if primary_keys else "(none)"
    print_info(f"  Primary key: {pk_str}")
    for col in columns:
        nullable = "NULL" if col.get("nullable") else "NOT NULL"
        pk_marker = " [PK]" if col.get("primary_key") else ""
        default = (
            f" DEFAULT {col['default']}"
            if col.get("default")
            else ""
        )
        print_info(
            f"    {col['name']}: {col['type']} "
            + f"{nullable}{pk_marker}{default}"
        )
