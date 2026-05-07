"""Shared helpers for deterministic FDW object naming."""

from __future__ import annotations

import re

_DBO_SCHEMA_NAME = "dbo"
_FDW_CT_SUFFIX = "_CT"
_FDW_BASE_SUFFIX = "_base"
_IDENTIFIER_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_]+")


def sanitize_fdw_identifier(value: str) -> str:
    """Return a deterministic ASCII-safe identifier fragment."""
    sanitized = _IDENTIFIER_SANITIZE_PATTERN.sub("_", value).strip("_")
    return sanitized or "unnamed"


def build_foreign_table_name(
    source_schema_name: str,
    source_table_name: str,
    *,
    duplicate_table_name_count: int,
) -> str:
    """Build the local PostgreSQL foreign table name for one CDC table."""
    sanitized_table_name = sanitize_fdw_identifier(source_table_name)
    if (
        duplicate_table_name_count > 1
        or source_schema_name.casefold() != _DBO_SCHEMA_NAME
    ):
        sanitized_schema_name = sanitize_fdw_identifier(source_schema_name)
        return f"{sanitized_schema_name}_{sanitized_table_name}{_FDW_CT_SUFFIX}"
    return f"{sanitized_table_name}{_FDW_CT_SUFFIX}"


def build_min_lsn_table_name(foreign_table_name: str) -> str:
    """Build the helper foreign-table name used for retention-gap checks."""
    base_name = (
        foreign_table_name[: -len(_FDW_CT_SUFFIX)]
        if foreign_table_name.endswith(_FDW_CT_SUFFIX)
        else foreign_table_name
    )
    return f"cdc_min_lsn_{sanitize_fdw_identifier(base_name)}"


def build_base_foreign_table_name(
    source_schema_name: str,
    source_table_name: str,
    *,
    duplicate_table_name_count: int,
) -> str:
    """Build the local PostgreSQL foreign table name for one live source table."""
    sanitized_table_name = sanitize_fdw_identifier(source_table_name)
    if (
        duplicate_table_name_count > 1
        or source_schema_name.casefold() != _DBO_SCHEMA_NAME
    ):
        sanitized_schema_name = sanitize_fdw_identifier(source_schema_name)
        return f"{sanitized_schema_name}_{sanitized_table_name}{_FDW_BASE_SUFFIX}"
    return f"{sanitized_table_name}{_FDW_BASE_SUFFIX}"