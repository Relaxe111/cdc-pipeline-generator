#!/usr/bin/env python3
"""CLI entry point for cleaning old CDC change tracking data from MSSQL.

Usage:
    cdc manage-migrations clean-cdc --env nonprod
    cdc manage-migrations clean-cdc --env nonprod --days 60
    cdc manage-migrations clean-cdc --env nonprod --dry-run
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.core.migration_ops import clean_cdc_data
from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run clean-cdc from CLI arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations clean-cdc",
        description="Clean old CDC change tracking data from MSSQL source tables",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--env",
        default="nonprod",
        help="MSSQL environment (local, nonprod, prod) (default: nonprod)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Purge CDC entries older than this many days (default: 30)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Filter: only clean CDC data for tables matching this name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables without cleaning CDC data",
    )
    args = parser.parse_args()

    result = clean_cdc_data(
        service_name=args.service,
        env=args.env,
        days=args.days,
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
