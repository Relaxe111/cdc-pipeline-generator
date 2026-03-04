"""Parsing and normalization helpers for custom sink table handlers."""

from __future__ import annotations

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_warning,
)

# Valid PostgreSQL types for quick validation
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

# Default expressions mapping (shorthand → SQL)
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

# Minimum parts in a column spec (name:type)
_MIN_SPEC_PARTS = 2

# Expected parts when splitting schema.table
_TABLE_KEY_PARTS = 2


def _split_table_key(table_key: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema_name, table_name)."""
    parts = table_key.split(".", 1)
    if len(parts) == _TABLE_KEY_PARTS:
        return parts[0], parts[1]
    return "public", parts[0]


def _parse_column_spec(spec: str) -> dict[str, object] | None:
    """Parse a column specification string into a column definition dict."""
    parts = spec.split(":")
    if len(parts) < _MIN_SPEC_PARTS:
        print_error(
            f"Invalid column spec '{spec}'. "
            + "Format: name:type[:pk][:not_null][:default_X]",
        )
        return None

    col_name = parts[0].strip()
    col_type = parts[1].strip().lower()

    if not col_name:
        print_error("Column name cannot be empty")
        return None

    if col_type not in _PG_TYPES:
        print_warning(
            f"Type '{col_type}' is not a standard PostgreSQL type. "
            + "Proceeding anyway.",
        )

    col_def: dict[str, object] = {"type": col_type}

    for modifier in parts[2:]:
        mod = modifier.strip().lower()
        if mod == "pk":
            col_def["primary_key"] = True
            col_def["nullable"] = False
        elif mod == "not_null":
            col_def["nullable"] = False
        elif mod == "nullable":
            col_def["nullable"] = True
        elif mod in _DEFAULT_EXPRESSIONS:
            col_def["default"] = _DEFAULT_EXPRESSIONS[mod]
        elif mod.startswith("default_"):
            default_key = mod
            if default_key in _DEFAULT_EXPRESSIONS:
                col_def["default"] = _DEFAULT_EXPRESSIONS[default_key]
            else:
                col_def["default"] = mod.removeprefix("default_")
        elif mod:
            print_warning(f"Unknown modifier '{mod}' — ignoring")

    return {"name": col_name, **col_def}


def _parse_multiple_columns(
    column_specs: list[str],
) -> list[dict[str, object]] | None:
    """Parse multiple column specs into column definitions."""
    columns: list[dict[str, object]] = []
    names_seen: set[str] = set()

    for spec in column_specs:
        col = _parse_column_spec(spec)
        if col is None:
            return None
        name = str(col["name"])
        if name in names_seen:
            print_error(f"Duplicate column name: '{name}'")
            return None
        names_seen.add(name)
        columns.append(col)

    if not columns:
        print_error("At least one --column is required")
        return None

    has_pk = any(col.get("primary_key") for col in columns)
    if not has_pk:
        print_warning(
            "No primary key defined. Consider adding :pk to a column.",
        )

    return columns


def _build_schema_yaml(
    table_key: str,
    columns: list[dict[str, object]],
    target_service: str,
) -> dict[str, object]:
    """Build a service-schemas YAML structure from column definitions."""
    schema_name, table_name = _split_table_key(table_key)

    pk_cols = [str(c["name"]) for c in columns if c.get("primary_key")]
    primary_key = pk_cols[0] if len(pk_cols) == 1 else None

    schema_columns: list[dict[str, object]] = []
    for col in columns:
        schema_col: dict[str, object] = {
            "name": col["name"],
            "type": col["type"],
            "nullable": col.get("nullable", True),
            "primary_key": bool(col.get("primary_key", False)),
        }
        if "default" in col:
            schema_col["default"] = col["default"]
        schema_columns.append(schema_col)

    result: dict[str, object] = {
        "schema": schema_name,
        "service": target_service,
        "table": table_name,
        "columns": schema_columns,
    }
    if primary_key:
        result["primary_key"] = primary_key

    return result


def _build_custom_table_config(
    from_table: str | None = None,
) -> dict[str, object]:
    """Build per-table config dict for a custom table."""
    cfg: dict[str, object] = {
        "target_exists": False,
        "custom": True,
        "managed": True,
    }
    if from_table is not None:
        cfg["from"] = from_table
    return cfg


def split_table_key(table_key: str) -> tuple[str, str]:
    """Public wrapper around table-key splitting."""
    return _split_table_key(table_key)


def parse_column_spec(spec: str) -> dict[str, object] | None:
    """Public wrapper around single-column spec parsing."""
    return _parse_column_spec(spec)


def parse_multiple_columns(column_specs: list[str]) -> list[dict[str, object]] | None:
    """Public wrapper around multi-column spec parsing."""
    return _parse_multiple_columns(column_specs)


def build_schema_yaml(
    table_key: str,
    columns: list[dict[str, object]],
    target_service: str,
) -> dict[str, object]:
    """Public wrapper around schema YAML structure generation."""
    return _build_schema_yaml(table_key, columns, target_service)


def build_custom_table_config(from_table: str | None = None) -> dict[str, object]:
    """Public wrapper around custom table config builder."""
    return _build_custom_table_config(from_table)
