#!/usr/bin/env python3
"""Generate YAML schema documentation from source MSSQL tables.

Usage:
    cdc manage-migrations schema-docs
    cdc manage-migrations schema-docs --env local
    cdc manage-migrations schema-docs --env nonprod
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_info, print_success, print_warning
from cdc_generator.helpers.helpers_mssql import get_mssql_connection
from cdc_generator.helpers.yaml_loader import yaml


def _get_all_tables(cursor: Any) -> list[str]:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _get_table_columns(cursor: Any, table_name: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID('dbo.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = 'dbo'
          AND c.TABLE_NAME = %s
        ORDER BY c.ORDINAL_POSITION
        """,
        (table_name,),
    )

    columns: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        col_name, data_type, max_length, is_nullable, default, is_identity = row

        if max_length and data_type in ("varchar", "nvarchar", "char", "nchar"):
            suffix = "MAX" if max_length == -1 else str(max_length)
            type_str = f"{data_type}({suffix})"
        else:
            type_str = str(data_type)

        column: dict[str, Any] = {
            "name": str(col_name),
            "type": type_str,
            "nullable": is_nullable == "YES",
        }
        if is_identity == 1:
            column["identity"] = True
        if default:
            column["default"] = str(default).strip("()")

        columns.append(column)

    return columns


def _get_primary_keys(cursor: Any, table_name: str) -> list[str]:
    cursor.execute(
        """
        SELECT ku.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
          ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
         AND tc.TABLE_NAME = ku.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND tc.TABLE_SCHEMA = 'dbo'
          AND tc.TABLE_NAME = %s
        ORDER BY ku.ORDINAL_POSITION
        """,
        (table_name,),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _get_foreign_keys(cursor: Any, table_name: str) -> list[dict[str, str]]:
    cursor.execute(
        """
        SELECT
            fk.name AS FK_NAME,
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS COLUMN_NAME,
            OBJECT_NAME(fc.referenced_object_id) AS REFERENCED_TABLE,
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS REFERENCED_COLUMN
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fc
            ON fk.object_id = fc.constraint_object_id
        WHERE OBJECT_NAME(fk.parent_object_id) = %s
        ORDER BY fk.name, fc.constraint_column_id
        """,
        (table_name,),
    )

    fks: list[dict[str, str]] = []
    for fk_name, col_name, ref_table, ref_col in cursor.fetchall():
        fks.append(
            {
                "column": str(col_name),
                "references": f"{ref_table}.{ref_col}",
                "constraint_name": str(fk_name),
            },
        )
    return fks


def _build_table_schema(cursor: Any, table_name: str) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "table": table_name,
        "schema": "dbo",
        "columns": _get_table_columns(cursor, table_name),
    }

    pk_cols = _get_primary_keys(cursor, table_name)
    if pk_cols:
        schema["primary_key"] = pk_cols[0] if len(pk_cols) == 1 else pk_cols

    foreign_keys = _get_foreign_keys(cursor, table_name)
    if foreign_keys:
        schema["foreign_keys"] = foreign_keys

    return schema


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations schema-docs",
        description="Generate YAML schema documentation for MSSQL tables",
    )
    parser.add_argument(
        "--env",
        default="nonprod",
        help="Environment name for MSSQL connection (default: nonprod)",
    )
    args = parser.parse_args()

    output_dir = Path("generated/schemas")
    output_dir.mkdir(exist_ok=True)

    print_info(f"Connecting to environment: {args.env}")
    try:
        conn, database = get_mssql_connection(args.env)
    except Exception as exc:
        print_error(f"Failed to connect MSSQL: {exc}")
        return 1

    print_success(f"Connected to database: {database}")
    cursor = conn.cursor()

    tables = _get_all_tables(cursor)
    print_info(f"Found {len(tables)} dbo tables")

    success_count = 0
    error_count = 0
    for idx, table_name in enumerate(tables, start=1):
        print_info(f"[{idx}/{len(tables)}] Processing {table_name}")
        try:
            table_schema = _build_table_schema(cursor, table_name)
            output_file = output_dir / f"{table_name}.yaml"
            with output_file.open("w", encoding="utf-8") as file_obj:
                yaml.dump(
                    table_schema,
                    file_obj,
                    default_flow_style=False,
                    sort_keys=False,
                    allow_unicode=True,
                )
            success_count += 1
        except Exception as exc:
            print_warning(f"Failed table {table_name}: {exc}")
            error_count += 1

    conn.close()

    if error_count == 0:
        print_success(f"Generated {success_count} schema files in {output_dir}/")
    else:
        print_warning(f"Generated {success_count} files, {error_count} errors")

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
