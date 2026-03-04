"""Source-table schema loading helpers for custom sink table handlers."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


@dataclass(frozen=True)
class SourceLoaderDeps:
    """Injected dependencies for source-table schema loading."""

    load_service_config_fn: Callable[[str], dict[str, object]]
    get_service_schema_read_dirs_fn: Callable[[str], Iterable[Path]]
    load_yaml_file_fn: Callable[[Path], object]
    build_source_table_not_found_messages_fn: Callable[
        [str, str, list[str]], tuple[str, str | None]
    ]
    build_source_schema_missing_messages_fn: Callable[[str, str], tuple[str, str]]
    print_error_fn: Callable[[str], None]
    print_info_fn: Callable[[str], None]


def load_columns_from_source_table(
    service: str,
    table_ref: str,
    deps: SourceLoaderDeps,
) -> list[dict[str, object]] | None:
    """Load column definitions from a source table schema.

    Validates that ``table_ref`` exists in ``source.tables`` of the service,
    then reads from preferred/legacy schema paths for that same service.
    """
    try:
        config = deps.load_service_config_fn(service)
    except FileNotFoundError as exc:
        deps.print_error_fn(f"Service not found: {exc}")
        return None

    source = config.get("source")
    source_dict = cast(dict[str, object], source) if isinstance(source, dict) else None
    tables = source_dict.get("tables") if source_dict is not None else None
    if not isinstance(tables, dict) or table_ref not in tables:
        table_keys = (
            sorted(str(k) for k in cast(dict[str, object], tables))
            if isinstance(tables, dict) else []
        )
        error_message, info_message = deps.build_source_table_not_found_messages_fn(
            table_ref,
            service,
            table_keys,
        )
        deps.print_error_fn(error_message)
        if info_message:
            deps.print_info_fn(info_message)
        return None

    if "." not in table_ref:
        deps.print_error_fn(
            f"Invalid source table reference '{table_ref}'. Expected schema.table"
        )
        return None

    schema_name, table_name = table_ref.split(".", 1)

    table_schema: dict[str, Any] | None = None
    for service_dir in deps.get_service_schema_read_dirs_fn(service):
        schema_file = service_dir / schema_name / f"{table_name}.yaml"
        if not schema_file.exists():
            continue
        loaded = deps.load_yaml_file_fn(schema_file)
        table_schema = cast(dict[str, Any], loaded)
        break

    if table_schema is None:
        error_message, info_message = deps.build_source_schema_missing_messages_fn(
            table_ref,
            service,
        )
        deps.print_error_fn(error_message)
        deps.print_info_fn(info_message)
        return None

    columns_raw: list[dict[str, Any]] = table_schema.get("columns", [])
    if not columns_raw:
        deps.print_error_fn(
            f"Source table '{table_ref}' has no columns"
        )
        return None

    columns: list[dict[str, object]] = []
    for col in columns_raw:
        col_def: dict[str, object] = {
            "name": col.get("name", ""),
            "type": col.get("type", "text"),
        }
        if col.get("primary_key"):
            col_def["primary_key"] = True
            col_def["nullable"] = False
        elif col.get("nullable") is not None:
            col_def["nullable"] = col["nullable"]
        if col.get("default"):
            col_def["default"] = col["default"]
        columns.append(col_def)

    return columns
