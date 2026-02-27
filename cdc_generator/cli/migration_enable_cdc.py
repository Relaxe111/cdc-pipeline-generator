#!/usr/bin/env python3
"""CLI entry point for enabling CDC tracking on MSSQL source tables.

Usage:
    cdc manage-migrations enable-cdc --env nonprod
    cdc manage-migrations enable-cdc --env nonprod --table Actor
    cdc manage-migrations enable-cdc --env nonprod --dry-run
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.core.migration_ops import enable_cdc_tables
from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run enable-cdc from CLI arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations enable-cdc",
        description="Enable CDC tracking on MSSQL source tables",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--env",
        required=True,
        help="MSSQL environment (local, nonprod, prod)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Filter: only enable CDC for tables matching this name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables without enabling CDC",
    )
    args = parser.parse_args()

    result = enable_cdc_tables(
        service_name=args.service,
        env=args.env,
        table_filter=args.table,
        dry_run=args.dry_run,
    )

    if result.errors:
        for err in result.errors:
            print_error(err)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
