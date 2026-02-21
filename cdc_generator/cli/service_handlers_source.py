"""Source table CLI operations for manage-service."""

import argparse
from typing import Any

from cdc_generator.helpers.helpers_logging import (
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.validators.manage_service.table_operations import (
    add_table_to_service,
    remove_table_from_service,
)


def _parse_column_specs(
    args: argparse.Namespace,
    schema: str,
    table: str,
) -> tuple[list[str] | None, list[str] | None]:
    """Parse ignore/track column specs for a table.

    Returns:
        (ignore_cols, track_cols) — None when empty.
    """
    ignore_cols: list[str] | None = None
    track_cols: list[str] | None = None
    table_prefix = f"{schema}.{table}."

    def _flatten_columns(raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []

        flattened: list[str] = []
        for item in raw:
            if isinstance(item, str):
                flattened.append(item)
            elif isinstance(item, list):
                for nested in item:
                    if isinstance(nested, str):
                        flattened.append(nested)
        return flattened

    if args.ignore_columns:
        cols = [
            col.replace(table_prefix, "")
            for col in _flatten_columns(args.ignore_columns)
            if col.startswith(table_prefix)
        ]
        ignore_cols = cols or None

    if args.track_columns:
        cols = [
            col.replace(table_prefix, "")
            for col in _flatten_columns(args.track_columns)
            if col.startswith(table_prefix)
        ]
        track_cols = cols or None

    return ignore_cols, track_cols


def handle_add_source_tables(args: argparse.Namespace) -> int:
    """Add multiple tables to service (bulk operation)."""
    table_specs = getattr(args, "add_source_tables", None)
    if not isinstance(table_specs, list):
        return 1

    return _handle_add_source_table_specs(args, table_specs)


def _handle_add_source_table_specs(
    args: argparse.Namespace,
    table_specs: list[str],
) -> int:
    """Add one or more source table specs to a service."""
    success_count = 0
    failed_count = 0

    for raw_spec in table_specs:
        spec = raw_spec.strip()
        if not spec:
            continue

        if "." in spec:
            schema, table = spec.split(".", 1)
        else:
            schema = args.schema if args.schema else "dbo"
            table = spec

        ignore_cols, track_cols = _parse_column_specs(
            args, schema, table,
        )

        if add_table_to_service(
            args.service, schema, table,
            args.primary_key, ignore_cols, track_cols,
        ):
            success_count += 1
        else:
            failed_count += 1

    if success_count > 0:
        print_success(f"\nAdded {success_count} table(s)")
        if failed_count > 0:
            print_warning(
                f"Failed to add {failed_count} table(s)"
            )
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_add_source_table(args: argparse.Namespace) -> int:
    """Add one or more tables from --add-source-table (repeatable option)."""
    raw_value = getattr(args, "add_source_table", None)
    if raw_value is None:
        return 1

    if isinstance(raw_value, list):
        table_specs = [str(spec) for spec in raw_value]
    else:
        table_specs = [str(raw_value)]

    return _handle_add_source_table_specs(args, table_specs)


def handle_update_source_table(args: argparse.Namespace) -> int:
    """Update an existing source table (track/ignore columns)."""
    spec = args.source_table
    if "." in spec:
        schema, table = spec.split(".", 1)
    else:
        schema = args.schema if args.schema else "dbo"
        table = spec

    ignore_cols, track_cols = _parse_column_specs(
        args, schema, table,
    )

    if not ignore_cols and not track_cols:
        print_warning(
            f"No columns specified for {spec}."
            + " Use --track-columns or --ignore-columns."
        )
        return 1

    if add_table_to_service(
        args.service, schema, table,
        None, ignore_cols, track_cols,
    ):
        print_info("\nRun 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_remove_table(args: argparse.Namespace) -> int:
    """Remove a table from service."""
    if "." in args.remove_table:
        schema, table = args.remove_table.split(".", 1)
    else:
        schema = args.schema if args.schema else "dbo"
        table = args.remove_table

    if remove_table_from_service(args.service, schema, table):
        print_info("\nRun 'cdc generate' to update pipelines")
        return 0
    return 1
