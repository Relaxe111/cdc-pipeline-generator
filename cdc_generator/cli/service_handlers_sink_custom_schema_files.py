"""Schema file helpers for custom sink table handlers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from cdc_generator.cli.service_handlers_sink_custom_parsing import (
    split_table_key,
)


def load_columns_map_from_schema(
    schemas_dir: Path,
    yaml_module: object | None,
    target_service: str,
    table_key: str,
) -> dict[str, object] | None:
    """Load column definitions from a service-schemas YAML file as mapping."""
    if yaml_module is None:
        return None

    schema_name, table_name = split_table_key(table_key)
    schema_file = schemas_dir / target_service / schema_name / f"{table_name}.yaml"
    if not schema_file.exists():
        return None

    with schema_file.open(encoding="utf-8") as f:
        data = yaml_module.load(f)

    if not isinstance(data, dict):
        return None

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns")
    if not isinstance(columns_raw, list):
        return None

    cols: dict[str, object] = {}
    for col_raw in cast(list[object], columns_raw):
        if not isinstance(col_raw, dict):
            continue
        col = cast(dict[str, object], col_raw)
        name = col.get("name")
        col_type = col.get("type")
        if not isinstance(name, str) or not isinstance(col_type, str):
            continue
        col_cfg: dict[str, object] = {"type": col_type}
        if bool(col.get("primary_key")):
            col_cfg["primary_key"] = True
        nullable = col.get("nullable")
        if nullable is not None and nullable is False:
            col_cfg["nullable"] = False
        default_value = col.get("default")
        if default_value is not None:
            col_cfg["default"] = default_value
        cols[name] = col_cfg

    return cols


def update_schema_file_add_column(
    schemas_dir: Path,
    yaml_module: object | None,
    target_service: str,
    table_key: str,
    col: dict[str, object],
) -> None:
    """Add a column to the service-schemas YAML file."""
    if yaml_module is None:
        return

    schema_name, table_name = split_table_key(table_key)

    schema_file = (
        schemas_dir / target_service / schema_name
        / f"{table_name}.yaml"
    )
    if not schema_file.exists():
        return

    with schema_file.open(encoding="utf-8") as f:
        data = yaml_module.load(f)

    if not isinstance(data, dict):
        return

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns", [])
    if not isinstance(columns_raw, list):
        return

    columns_list = cast(list[dict[str, object]], columns_raw)

    new_col: dict[str, object] = {
        "name": col["name"],
        "type": col["type"],
        "nullable": col.get("nullable", True),
        "primary_key": bool(col.get("primary_key", False)),
    }
    if "default" in col:
        new_col["default"] = col["default"]

    columns_list.append(new_col)

    with schema_file.open("w", encoding="utf-8") as f:
        yaml_module.dump(schema_data, f)


def update_schema_file_remove_column(
    schemas_dir: Path,
    yaml_module: object | None,
    target_service: str,
    table_key: str,
    column_name: str,
) -> None:
    """Remove a column from the service-schemas YAML file."""
    if yaml_module is None:
        return

    schema_name, table_name = split_table_key(table_key)

    schema_file = (
        schemas_dir / target_service / schema_name
        / f"{table_name}.yaml"
    )
    if not schema_file.exists():
        return

    with schema_file.open(encoding="utf-8") as f:
        data = yaml_module.load(f)

    if not isinstance(data, dict):
        return

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns", [])
    if not isinstance(columns_raw, list):
        return

    columns_list = cast(list[dict[str, object]], columns_raw)
    schema_data["columns"] = [
        c for c in columns_list
        if c.get("name") != column_name
    ]

    with schema_file.open("w", encoding="utf-8") as f:
        yaml_module.dump(schema_data, f)
