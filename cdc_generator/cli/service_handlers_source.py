"""Source table CLI operations for manage-service."""

import argparse

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

    if args.ignore_columns:
        cols = [
            col.replace(table_prefix, "")
            for col in args.ignore_columns
            if col.startswith(table_prefix)
        ]
        ignore_cols = cols or None

    if args.track_columns:
        cols = [
            col.replace(table_prefix, "")
            for col in args.track_columns
            if col.startswith(table_prefix)
        ]
        track_cols = cols or None

    return ignore_cols, track_cols


def handle_add_source_tables(args: argparse.Namespace) -> int:
    """Add multiple tables to service (bulk operation)."""
    success_count = 0
    failed_count = 0

    for raw_spec in args.add_source_tables:
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
    """Add a single table to service."""
    if "." in args.add_source_table:
        schema, table = args.add_source_table.split(".", 1)
    else:
        schema = args.schema if args.schema else "dbo"
        table = args.add_source_table

    ignore_cols, track_cols = _parse_column_specs(
        args, schema, table,
    )

    if add_table_to_service(
        args.service, schema, table,
        args.primary_key, ignore_cols, track_cols,
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
