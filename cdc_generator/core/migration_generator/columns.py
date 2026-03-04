"""Column building utilities for migration generation."""

from __future__ import annotations

from typing import Any, cast

from cdc_generator.core.column_template_operations import (
    resolve_column_templates,
    resolve_transforms,
)
from cdc_generator.helpers.type_mapper import TypeMapper

from .data_structures import MigrationColumn
from .service_parsing import get_source_table_config

CDC_METADATA_COLUMNS: list[dict[str, str | bool]] = [
    {"name": "__sync_timestamp", "type": "TIMESTAMP", "nullable": False, "default": "CURRENT_TIMESTAMP"},
    {"name": "__source", "type": "VARCHAR(255)", "nullable": True},
    {"name": "__source_db", "type": "VARCHAR(255)", "nullable": True},
    {"name": "__source_table", "type": "VARCHAR(255)", "nullable": True},
    {"name": "__source_ts_ms", "type": "BIGINT", "nullable": True},
    {"name": "__cdc_operation", "type": "VARCHAR(10)", "nullable": True},
]


def build_columns_from_table_def(
    table_def: dict[str, Any],
    ignore_columns: list[str] | None = None,
    type_mapper: TypeMapper | None = None,
) -> tuple[list[MigrationColumn], list[str]]:
    """Build column list from a service-schema YAML."""
    ignore_set = {c.casefold() for c in (ignore_columns or [])}

    columns_raw = table_def.get("columns", table_def.get("fields", []))
    if not isinstance(columns_raw, list):
        return [], []

    is_schemas_format = "columns" in table_def

    columns: list[MigrationColumn] = []
    primary_keys: list[str] = []
    seen_column_names: set[str] = set()

    for field_entry in cast(list[object], columns_raw):
        if not isinstance(field_entry, dict):
            continue
        f = cast(dict[str, Any], field_entry)

        pg_name = str(f.get("name", f.get("postgres", "")))
        if not pg_name:
            continue
        if pg_name.casefold() in ignore_set:
            continue
        column_key = pg_name.casefold()
        if column_key in seen_column_names:
            continue
        seen_column_names.add(column_key)

        raw_type = str(f.get("type", "TEXT"))
        pg_type = (
            type_mapper.map_type(raw_type)
            if is_schemas_format and type_mapper is not None
            else raw_type
        )

        col = MigrationColumn(
            name=pg_name,
            type=pg_type,
            nullable=bool(f.get("nullable", True)),
            primary_key=bool(f.get("primary_key", False)),
        )
        columns.append(col)
        if col.primary_key:
            primary_keys.append(pg_name)

    return columns, dedupe_names_case_insensitive(primary_keys)


def add_column_template_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add columns from column_templates to the column list."""
    resolved = resolve_column_templates(table_cfg)
    existing_names = {c.name for c in columns}

    for r in resolved:
        if r.name in existing_names:
            continue
        col = MigrationColumn(
            name=r.name,
            type=r.template.column_type.upper(),
            nullable=not r.template.not_null,
            default=r.template.default,
        )
        columns.append(col)

    return columns


def add_transform_output_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add columns produced by transforms without altering configured names."""
    from cdc_generator.validators.bloblang_parser import extract_root_assignments

    existing_names = {c.name for c in columns}

    transforms_raw = table_cfg.get("transforms")
    if isinstance(transforms_raw, list):
        for item in cast(list[object], transforms_raw):
            if not isinstance(item, dict):
                continue
            entry = cast(dict[str, object], item)
            expected_output = entry.get("expected_output_column")
            if isinstance(expected_output, str) and expected_output and expected_output not in existing_names:
                columns.append(MigrationColumn(name=expected_output, type="TEXT"))
                existing_names.add(expected_output)

    for transform in resolve_transforms(table_cfg):
        output_columns = sorted(extract_root_assignments(transform.bloblang))
        for output_name in output_columns:
            if output_name in existing_names:
                continue
            columns.append(MigrationColumn(name=output_name, type="TEXT"))
            existing_names.add(output_name)

    return columns


def add_cdc_metadata_columns(
    columns: list[MigrationColumn],
) -> list[MigrationColumn]:
    """Append standard CDC metadata columns."""
    existing_names = {c.name for c in columns}
    for meta in CDC_METADATA_COLUMNS:
        name = str(meta["name"])
        if name in existing_names:
            continue
        columns.append(MigrationColumn(
            name=name,
            type=str(meta["type"]),
            nullable=bool(meta.get("nullable", True)),
            default=str(meta["default"]) if meta.get("default") else None,
        ))
    return columns


def build_full_column_list(
    table_def: dict[str, Any],
    sink_cfg: dict[str, object],
    service_config: dict[str, object],
    source_key: str,
    type_mapper: TypeMapper | None = None,
) -> tuple[list[MigrationColumn], list[str]]:
    """Build the complete column list using the full generation pipeline."""
    source_cfg = get_source_table_config(service_config, source_key)
    ignore_raw = source_cfg.get("ignore_columns")
    ignore_cols = (
        [str(c) for c in cast(list[object], ignore_raw)]
        if isinstance(ignore_raw, list) else None
    )

    columns, primary_keys = build_columns_from_table_def(
        table_def, ignore_cols, type_mapper,
    )

    columns = add_column_template_columns(columns, sink_cfg)
    columns = add_transform_output_columns(columns, sink_cfg)
    columns = add_cdc_metadata_columns(columns)

    return columns, primary_keys


def dedupe_names_case_insensitive(names: list[str]) -> list[str]:
    """Return unique names preserving order with case-insensitive matching."""
    seen: set[str] = set()
    deduped: list[str] = []
    for name in names:
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped
