"""Custom-table access and listing helpers for sink custom handlers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from cdc_generator.cli.service_handlers_sink_custom_config_nav import (
    get_sink_tables,
    get_sinks_dict,
    resolve_sink_config,
)


@dataclass(frozen=True)
class CustomTableAccessDeps:
    """Dependencies for custom-table access and list operations."""

    load_service_config_fn: Callable[[str], dict[str, object]]
    extract_target_service_fn: Callable[[str], str | None]
    load_columns_map_from_schema_fn: Callable[[str, str], dict[str, object] | None]
    build_custom_table_disabled_messages_fn: Callable[[str], tuple[str, str]]
    build_custom_table_unmanaged_messages_fn: Callable[[str], tuple[str, str]]
    build_no_column_definitions_messages_fn: Callable[[str], tuple[str, str]]
    print_error_fn: Callable[[str], None]
    print_info_fn: Callable[[str], None]


def load_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
    deps: CustomTableAccessDeps,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], str] | None:
    """Load and validate a custom+managed table for modification."""
    try:
        config = deps.load_service_config_fn(service)
    except FileNotFoundError as exc:
        deps.print_error_fn(f"Service not found: {exc}")
        return None

    sinks = get_sinks_dict(config)
    sink_cfg = resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        deps.print_error_fn(f"Sink '{sink_key}' not found or invalid")
        return None

    tables = get_sink_tables(sink_cfg)
    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        deps.print_error_fn(f"Table '{table_key}' not found in sink '{sink_key}'")
        return None

    tbl_cfg = cast(dict[str, object], tbl_raw)

    if not tbl_cfg.get("custom"):
        error_message, info_message = deps.build_custom_table_disabled_messages_fn(
            table_key,
        )
        deps.print_error_fn(error_message)
        deps.print_info_fn(info_message)
        return None

    if not tbl_cfg.get("managed"):
        error_message, info_message = deps.build_custom_table_unmanaged_messages_fn(
            table_key,
        )
        deps.print_error_fn(error_message)
        deps.print_info_fn(info_message)
        return None

    target_service = deps.extract_target_service_fn(sink_key)
    if target_service is None:
        return None

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        resolved_columns = cast(dict[str, object], cols_raw)
    else:
        schema_columns = deps.load_columns_map_from_schema_fn(target_service, table_key)
        if schema_columns is None:
            error_message, info_message = (
                deps.build_no_column_definitions_messages_fn(table_key)
            )
            deps.print_error_fn(error_message)
            deps.print_info_fn(info_message)
            return None
        resolved_columns = schema_columns

    return config, tbl_cfg, resolved_columns, target_service


def get_tables_dict_from_config(
    service: str,
    sink_key: str,
    *,
    load_service_config_fn: Callable[[str], dict[str, object]],
) -> dict[str, object] | None:
    """Load config and navigate to the tables dict for a sink."""
    try:
        config = load_service_config_fn(service)
    except FileNotFoundError:
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return None

    sink_raw = cast(dict[str, object], sinks_raw).get(sink_key)
    if not isinstance(sink_raw, dict):
        return None

    tables_raw = cast(dict[str, object], sink_raw).get("tables")
    if not isinstance(tables_raw, dict):
        return None

    return cast(dict[str, object], tables_raw)


def list_custom_table_columns(
    service: str,
    sink_key: str,
    table_key: str,
    deps: CustomTableAccessDeps,
) -> list[str]:
    """Return column names for a custom table in a sink."""
    tables = get_tables_dict_from_config(
        service,
        sink_key,
        load_service_config_fn=deps.load_service_config_fn,
    )
    if tables is None:
        return []

    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        return []

    tbl_cfg = cast(dict[str, object], tbl_raw)
    if not tbl_cfg.get("custom"):
        return []

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        cols = cast(dict[str, object], cols_raw)
        return sorted(str(k) for k in cols)

    target_service = deps.extract_target_service_fn(sink_key)
    if target_service is None:
        return []

    schema_cols = deps.load_columns_map_from_schema_fn(target_service, table_key)
    if schema_cols is None:
        return []

    return sorted(str(k) for k in schema_cols)


def list_custom_tables_for_sink(
    service: str,
    sink_key: str,
    *,
    load_service_config_fn: Callable[[str], dict[str, object]],
) -> list[str]:
    """Return table keys where custom=true for a given sink."""
    tables = get_tables_dict_from_config(
        service,
        sink_key,
        load_service_config_fn=load_service_config_fn,
    )
    if tables is None:
        return []

    result: list[str] = []
    for tbl_key_raw, tbl_raw in tables.items():
        if isinstance(tbl_raw, dict):
            tbl_cfg = cast(dict[str, object], tbl_raw)
            if tbl_cfg.get("custom"):
                result.append(str(tbl_key_raw))

    return sorted(result)
