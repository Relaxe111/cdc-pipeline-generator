"""Column mutation handlers for custom sink tables."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnMutationDeps:
    """Dependencies for add/remove custom-table column handlers."""

    load_custom_table_fn: Callable[
        [str, str, str],
        tuple[dict[str, object], dict[str, object], dict[str, object], str] | None,
    ]
    parse_column_spec_fn: Callable[[str], dict[str, object] | None]
    save_service_config_fn: Callable[[str, dict[str, object]], bool]
    update_schema_file_add_column_fn: Callable[[str, str, dict[str, object]], None]
    update_schema_file_remove_column_fn: Callable[[str, str, str], None]
    build_column_not_found_messages_fn: Callable[[str, str, list[str]], tuple[str, str | None]]
    print_error_fn: Callable[[str], None]
    print_info_fn: Callable[[str], None]
    print_success_fn: Callable[[str], None]
    print_warning_fn: Callable[[str], None]


def add_column_to_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
    column_spec: str,
    deps: ColumnMutationDeps,
) -> bool:
    """Add a column to a custom table."""
    result = deps.load_custom_table_fn(service, sink_key, table_key)
    if result is None:
        return False

    config, tbl_cfg, columns, target_service = result

    col = deps.parse_column_spec_fn(column_spec)
    if col is None:
        return False

    col_name = str(col["name"])
    if col_name in columns:
        deps.print_warning_fn(f"Column '{col_name}' already exists in '{table_key}'")
        return False

    col_entry: dict[str, object] = {"type": col["type"]}
    if col.get("primary_key"):
        col_entry["primary_key"] = True
    if col.get("nullable") is not None and not col.get("nullable"):
        col_entry["nullable"] = False
    if "default" in col:
        col_entry["default"] = col["default"]

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        columns[col_name] = col_entry
        if not deps.save_service_config_fn(service, config):
            return False

    deps.update_schema_file_add_column_fn(target_service, table_key, col)

    deps.print_success_fn(
        f"Added column '{col_name}' ({col['type']}) "
        + f"to custom table '{table_key}'"
    )
    return True


def remove_column_from_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
    column_name: str,
    deps: ColumnMutationDeps,
) -> bool:
    """Remove a column from a custom table."""
    result = deps.load_custom_table_fn(service, sink_key, table_key)
    if result is None:
        return False

    config, tbl_cfg, columns, target_service = result

    if column_name not in columns:
        available = [str(k) for k in columns]
        error_message, info_message = deps.build_column_not_found_messages_fn(
            table_key,
            column_name,
            available,
        )
        deps.print_error_fn(error_message)
        if info_message:
            deps.print_info_fn(info_message)
        return False

    if len(columns) <= 1:
        deps.print_error_fn(
            "Cannot remove last column — "
            + "use --remove-sink-table to remove the entire table"
        )
        return False

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        del columns[column_name]
        if not deps.save_service_config_fn(service, config):
            return False

    deps.update_schema_file_remove_column_fn(
        target_service,
        table_key,
        column_name,
    )

    deps.print_success_fn(
        f"Removed column '{column_name}' from custom table '{table_key}'"
    )
    return True
