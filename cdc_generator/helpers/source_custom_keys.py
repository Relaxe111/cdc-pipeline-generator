"""Helpers for SQL-based source custom key management and execution."""

from __future__ import annotations

from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_warning


def normalize_source_custom_keys(raw_value: object) -> dict[str, str]:
    """Normalize custom key definitions into ``key -> SQL`` mapping."""
    normalized: dict[str, str] = {}
    if not isinstance(raw_value, dict):
        return normalized

    for key_raw, entry_raw in cast(dict[str, object], raw_value).items():
        key = str(key_raw).strip()
        if not key:
            continue

        sql_query: str | None = None
        if isinstance(entry_raw, str):
            sql_query = entry_raw.strip()
        elif isinstance(entry_raw, dict):
            entry_dict = cast(dict[str, object], entry_raw)
            value = entry_dict.get("value")
            if isinstance(value, str):
                sql_query = value.strip()

        if sql_query:
            normalized[key] = sql_query

    return normalized


def execute_source_custom_keys(
    databases: list[dict[str, Any]],
    *,
    db_type: str,
    server_name: str,
    server_config: dict[str, object],
    source_custom_keys: dict[str, str],
    context_label: str,
) -> list[str]:
    """Execute configured SQL custom keys for each discovered database.

    Mutates ``databases`` by adding ``source_custom_values`` map.
    Missing values are stored as ``None`` (YAML null).
    Returns warning messages for missing/invalid key values and execution errors.
    """
    if not source_custom_keys:
        return []

    warnings: list[str] = []
    for db in databases:
        database_name = str(db.get("name", "")).strip()
        if not database_name:
            continue

        resolved_values: dict[str, str | None] = {}
        for key_name, sql_query in source_custom_keys.items():
            query_value, warning_message = _execute_single_custom_key(
                db_type=db_type,
                server_config=server_config,
                database_name=database_name,
                sql_query=sql_query,
                key_name=key_name,
                server_name=server_name,
                context_label=context_label,
            )
            if warning_message:
                warnings.append(warning_message)
            resolved_values[key_name] = query_value

        db["source_custom_values"] = resolved_values

    for message in warnings:
        print_warning(message)
    return warnings


def _execute_single_custom_key(
    *,
    db_type: str,
    server_config: dict[str, object],
    database_name: str,
    sql_query: str,
    key_name: str,
    server_name: str,
    context_label: str,
) -> tuple[str | None, str | None]:
    try:
        if db_type == "postgres":
            value = _run_sql_postgres(server_config, database_name, sql_query)
        elif db_type == "mssql":
            value = _run_sql_mssql(server_config, database_name, sql_query)
        else:
            return None, (
                f"[{context_label}] Unsupported source_custom_keys execution type "
                + f"for database type '{db_type}'"
            )
    except Exception as exc:
        return None, (
            f"[{context_label}] Failed custom key '{key_name}' on "
            + f"server '{server_name}', database '{database_name}': {exc}"
        )

    if value is None:
        return None, (
            f"[{context_label}] Missing value for custom key '{key_name}' on "
            + f"server '{server_name}', database '{database_name}'"
        )
    return value, None


def _run_sql_postgres(
    server_config: dict[str, object],
    database_name: str,
    sql_query: str,
) -> str | None:
    from cdc_generator.validators.manage_server_group.db_inspector import (
        get_postgres_connection,
    )

    with get_postgres_connection(server_config, database=database_name) as conn, conn.cursor() as cursor:
        cursor.execute(sql_query)
        row = cursor.fetchone()
    return _normalize_row_value(row)


def _run_sql_mssql(
    server_config: dict[str, object],
    database_name: str,
    sql_query: str,
) -> str | None:
    from cdc_generator.validators.manage_server_group.db_inspector import (
        get_mssql_connection,
    )

    with get_mssql_connection(server_config, database=database_name) as conn, conn.cursor() as cursor:
        cursor.execute(sql_query)
        row = cursor.fetchone()
    return _normalize_row_value(row)


def _normalize_row_value(row: object) -> str | None:
    if not isinstance(row, (list, tuple)) or not row:
        return None
    first_value = row[0]
    if first_value is None:
        return None
    resolved = str(first_value).strip()
    if not resolved:
        return None
    return resolved
