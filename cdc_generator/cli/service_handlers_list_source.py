"""Source table listing handlers for manage-services config."""

import argparse
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_warning,
)
from cdc_generator.helpers.yaml_loader import (
    ConfigDict,
    ConfigValue,
)


def _print_old_format_tables(
    old_tables: dict[str, ConfigValue],
) -> int:
    """Print tables in old flat format (source.tables)."""
    schema_tables: dict[str, list[tuple[str, ConfigValue]]] = {}
    for table_key, table_props in sorted(old_tables.items()):
        key_str = str(table_key)
        if "." in key_str:
            schema, table = key_str.split(".", 1)
        else:
            schema = "dbo"
            table = key_str

        if schema not in schema_tables:
            schema_tables[schema] = []
        schema_tables[schema].append((table, table_props))

    table_count = 0
    for schema_name in sorted(schema_tables):
        print(
            f"{Colors.BOLD}{Colors.BLUE}"
            + f"{schema_name}{Colors.RESET}"
        )
        for table_name, table_properties in schema_tables[schema_name]:
            table_count += 1
            old_pk: str = ""
            if isinstance(table_properties, dict):
                pk_value = cast(
                    str,
                    table_properties.get("primary_key", ""),
                )
                old_pk = pk_value if pk_value else ""

            if old_pk:
                pk_str = (
                    f"  {Colors.CYAN}{table_name}"
                    + f"{Colors.RESET} "
                    + f"{Colors.DIM}(PK: {old_pk})"
                    + f"{Colors.RESET}"
                )
                print(pk_str)
            else:
                print(f"  {Colors.CYAN}{table_name}{Colors.RESET}")

        print()

    schema_count = len(schema_tables)
    total_msg = (
        f"{Colors.DIM}Total: {table_count} table(s) "
        + f"across {schema_count} schema(s){Colors.RESET}"
    )
    print(total_msg + "\n")
    return 0


def _print_table_detail(table: ConfigDict) -> None:
    """Print a single table entry (new hierarchical format)."""
    table_name = str(table.get("name", "Unknown"))
    pk_raw = table.get("primary_key")
    pk: str | None = str(pk_raw) if pk_raw else None
    ignore_cols_raw = table.get("ignore_columns", [])
    ignore_cols = (
        [str(c) for c in ignore_cols_raw]
        if isinstance(ignore_cols_raw, list)
        else []
    )
    track_cols_raw = table.get("track_columns", [])
    track_cols = (
        [str(c) for c in track_cols_raw]
        if isinstance(track_cols_raw, list)
        else []
    )

    if pk:
        pk_line = (
            f"  {Colors.CYAN}{table_name}{Colors.RESET} "
            + f"{Colors.DIM}(PK: {pk}){Colors.RESET}"
        )
        print(pk_line)
    else:
        print(f"  {Colors.CYAN}{table_name}{Colors.RESET}")

    if ignore_cols:
        print(f"    {Colors.DIM}ignore:{Colors.RESET}")
        for col in ignore_cols:
            print(f"      {Colors.RED}{col}{Colors.RESET}")

    if track_cols:
        print(f"    {Colors.DIM}track:{Colors.RESET}")
        for col in track_cols:
            print(f"      {Colors.OKGREEN}{col}{Colors.RESET}")


def handle_list_source_tables(args: argparse.Namespace) -> int:
    """List all source tables configured in a service."""
    if not args.service:
        print_error(
            "❌ Error: --service is required for --list-source-tables"
        )
        return 1

    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(args.service)

        # Try new hierarchical format first (shared.source_tables)
        shared_raw = config.get("shared", {})
        shared = (
            cast(ConfigDict, shared_raw)
            if isinstance(shared_raw, dict)
            else {}
        )
        source_tables_raw = shared.get("source_tables", []) if shared else []
        source_tables = (
            cast(list[ConfigDict], source_tables_raw)
            if isinstance(source_tables_raw, list)
            else []
        )

        # Try old flat format (source.tables)
        if not source_tables:
            source_raw = config.get("source", {})
            source = (
                cast(ConfigDict, source_raw)
                if isinstance(source_raw, dict)
                else {}
            )
            old_tables_val = source.get("tables", {})
            old_tables: dict[str, ConfigValue] = (
                cast(dict[str, ConfigValue], old_tables_val)
                if isinstance(old_tables_val, dict)
                else {}
            )
            if old_tables:
                print_header(f"Source Tables in '{args.service}'")
                return _print_old_format_tables(old_tables)

        if not source_tables:
            print_warning(
                "No source tables configured in service "
                + f"'{args.service}'"
            )
            print_info(
                "Add tables with: cdc manage-services config "
                + f"--service {args.service} "
                + "--add-source-table <schema.table>"
            )
            return 0

        print_header(f"Source Tables in '{args.service}'")

        table_count = 0
        for schema_group in source_tables:
            schema_name = schema_group.get("schema")
            tables_raw = schema_group.get("tables", [])
            if not isinstance(tables_raw, list):
                continue

            if tables_raw:
                print(
                    f"{Colors.BOLD}{Colors.BLUE}"
                    + f"{schema_name}{Colors.RESET}"
                )
                for tbl in tables_raw:
                    table_count += 1
                    if isinstance(tbl, str):
                        print(
                            f"  {Colors.CYAN}{tbl}{Colors.RESET}"
                        )
                    elif isinstance(tbl, dict):
                        _print_table_detail(tbl)
                print()

        total_msg = (
            f"{Colors.DIM}Total: {table_count} table(s) "
            + f"across {len(source_tables)} schema(s)"
            + f"{Colors.RESET}"
        )
        print(total_msg + "\n")
        return 0

    except FileNotFoundError as e:
        print_error(f"Service not found: {e}")
        return 1
    except Exception as e:
        print_error(f"Failed to list source tables: {e}")
        return 1
