"""Column mapping helpers for sink operations."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error, print_info, print_warning
from cdc_generator.helpers.service_config import load_service_config
from cdc_generator.helpers.yaml_loader import load_yaml_file

from .config import SERVICE_SCHEMAS_DIR
from .sink_operations_helpers import (
    _get_sink_tables,
    _get_sinks_dict,
    _get_source_table_keys,
    _get_target_service_from_sink_key,
    _resolve_sink_config,
)


@dataclass
class MappingContext:
    """Resolved context needed for column mapping."""

    config: dict[str, object]
    tables: dict[str, object]
    source_table: str
    source_columns: list[dict[str, Any]]
    sink_columns: list[dict[str, Any]]


def load_table_columns(
    service: str,
    table_key: str,
) -> list[dict[str, Any]] | None:
    """Load table columns from services/_schemas/{service}/{schema}/{table}.yaml."""
    if "." not in table_key:
        return None

    schema, table = table_key.split(".", 1)
    schema_file = SERVICE_SCHEMAS_DIR / service / schema / f"{table}.yaml"
    if not schema_file.exists():
        return None

    try:
        data = load_yaml_file(schema_file)
        columns = data.get("columns", [])
        if isinstance(columns, list):
            return cast(list[dict[str, Any]], columns)
    except (FileNotFoundError, ValueError):
        pass
    return None


def _get_column_type(
    columns: list[dict[str, Any]],
    column_name: str,
) -> str | None:
    """Find a column type by name from column definitions."""
    for col in columns:
        if col.get("name") == column_name:
            raw_type = col.get("type")
            return str(raw_type) if raw_type is not None else None
    return None


def _is_text_like_type(
    type_name: str,
    normalize_type_name: Callable[[str], str],
) -> bool:
    """Return True for text-like SQL sink types."""
    normalized = normalize_type_name(type_name)
    return any(
        marker in normalized
        for marker in (
            "char",
            "text",
            "string",
            "varchar",
            "nvarchar",
            "nchar",
            "citext",
            "user-defined",
        )
    )


def _is_uuid_like_type(
    type_name: str,
    normalize_type_name: Callable[[str], str],
) -> bool:
    """Return True for UUID-like SQL types."""
    normalized = normalize_type_name(type_name)
    return "uuid" in normalized or "uniqueidentifier" in normalized


def _is_numeric_like_type(
    type_name: str,
    normalize_type_name: Callable[[str], str],
) -> bool:
    """Return True for numeric SQL types."""
    normalized = normalize_type_name(type_name)
    return any(
        marker in normalized
        for marker in (
            "int",
            "integer",
            "bigint",
            "smallint",
            "tinyint",
            "numeric",
            "decimal",
            "float",
            "double",
            "real",
            "money",
        )
    )


def validate_column_mappings(
    column_mappings: list[tuple[str, str]],
    source_columns: list[dict[str, Any]],
    sink_columns: list[dict[str, Any]],
    source_table: str,
    table_key: str,
    *,
    check_type_compatibility: Callable[..., bool],
    normalize_type_name: Callable[[str], str],
) -> list[str]:
    """Validate mapping pairs against source/sink schemas."""
    source_col_names = {col["name"] for col in source_columns if "name" in col}
    sink_col_names = {col["name"] for col in sink_columns if "name" in col}

    errors: list[str] = []
    for src_col, tgt_col in column_mappings:
        if src_col not in source_col_names:
            errors.append(
                f"Source column '{src_col}' not found in "
                + f"'{source_table}' (available: "
                + f"{', '.join(sorted(source_col_names))})"
            )
            continue

        if tgt_col not in sink_col_names:
            errors.append(
                f"Sink column '{tgt_col}' not found in "
                + f"'{table_key}' (available: "
                + f"{', '.join(sorted(sink_col_names))})"
            )
            continue

        src_type = _get_column_type(source_columns, src_col)
        tgt_type = _get_column_type(sink_columns, tgt_col)
        if src_type and tgt_type:
            if _is_text_like_type(tgt_type, normalize_type_name) and (
                _is_numeric_like_type(src_type, normalize_type_name)
                or _is_uuid_like_type(src_type, normalize_type_name)
            ):
                continue

            try:
                is_compatible = check_type_compatibility(
                    src_type,
                    tgt_type,
                    source_table=source_table,
                    source_column=src_col,
                )
            except ValueError as exc:
                errors.append("Type compatibility map error: " + str(exc))
                continue

            if not is_compatible:
                errors.append(
                    f"Type mismatch: '{src_col}' ({src_type}) "
                    + f"→ '{tgt_col}' ({tgt_type})"
                )

    return errors


def apply_column_mappings(
    tables: dict[str, object],
    table_key: str,
    column_mappings: list[tuple[str, str]],
) -> dict[str, str]:
    """Write mapping pairs into table config and return resulting columns dict."""
    tbl_raw = tables[table_key]
    if not isinstance(tbl_raw, dict):
        tbl_raw = {}
        tables[table_key] = tbl_raw
    tbl_cfg = cast(dict[str, object], tbl_raw)
    tbl_cfg["target_exists"] = True

    cols_raw = tbl_cfg.get("columns")
    if not isinstance(cols_raw, dict):
        tbl_cfg["columns"] = {}
        cols_raw = tbl_cfg["columns"]
    cols = cast(dict[str, str], cols_raw)

    for src_col, tgt_col in column_mappings:
        cols[src_col] = tgt_col

    return cols


def warn_unmapped_required(
    sink_columns: list[dict[str, Any]],
    cols: dict[str, str],
    source_col_names: set[str],
    sink_col_names: set[str],
) -> None:
    """Warn about required sink columns left unmapped."""
    mapped_sink_cols = set(cols.values())
    identity_mapped = source_col_names & sink_col_names
    all_covered = mapped_sink_cols | identity_mapped

    unmapped_required: list[str] = []
    for col in sink_columns:
        col_name = col.get("name", "")
        nullable = col.get("nullable", True)
        is_pk = col.get("primary_key", False)
        if (not nullable or is_pk) and col_name not in all_covered:
            unmapped_required.append(col_name)

    if unmapped_required:
        print_warning(
            "Unmapped required sink columns (non-nullable or PK): "
            + ", ".join(sorted(unmapped_required))
        )


def load_schemas_for_mapping(
    source_service: str,
    source_table: str,
    target_service: str,
    table_key: str,
    service: str,
    sink_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Load source and sink schemas required for mapping validation."""
    source_columns = load_table_columns(source_service, source_table)
    sink_columns = load_table_columns(target_service, table_key)

    if source_columns is None:
        src_path = source_table.replace(".", "/")
        print_error(
            "Source table schema not found: "
            + f"service-schemas/{source_service}/{src_path}.yaml"
        )
        print_info(
            "Run: cdc manage-services config --service "
            + f"{service} --inspect --all --save"
        )
        return None

    if sink_columns is None:
        tgt_path = table_key.replace(".", "/")
        print_error(
            "Sink table schema not found: "
            + f"service-schemas/{target_service}/{tgt_path}.yaml"
        )
        print_info(
            f"Run: cdc manage-services config --service {service}"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return None

    return source_columns, sink_columns


def resolve_source_table_from_sink(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
) -> str | None:
    """Resolve source table from sink entry via `from` or identity fallback."""
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        return None

    tbl_cfg = cast(dict[str, object], tbl_raw)
    from_table = tbl_cfg.get("from")
    if isinstance(from_table, str):
        return from_table

    source_tables = _get_source_table_keys(config)
    if table_key in source_tables:
        return table_key

    return None


def resolve_mapping_context(
    service: str,
    sink_key: str,
    table_key: str,
) -> MappingContext | None:
    """Resolve all required context for mapping operations."""
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        available = [str(k) for k in tables]
        if available:
            print_info(f"Available tables: {', '.join(available)}")
        return None

    source_table = resolve_source_table_from_sink(config, sink_key, table_key)
    if source_table is None:
        print_error(
            f"Cannot determine source table for '{table_key}' in sink '{sink_key}'"
        )
        print_info(
            "Ensure the sink table has a 'from' field or matches a source table"
        )
        return None

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        print_error(f"Invalid sink key format: '{sink_key}'")
        return None

    schemas = load_schemas_for_mapping(
        service,
        source_table,
        target_service,
        table_key,
        service,
        sink_key,
    )
    if schemas is None:
        return None
    source_columns, sink_columns = schemas

    return MappingContext(
        config=config,
        tables=tables,
        source_table=source_table,
        source_columns=source_columns,
        sink_columns=sink_columns,
    )
