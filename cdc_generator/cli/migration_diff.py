#!/usr/bin/env python3
"""CLI entry point for migration schema diff.

Usage:
    cdc manage-migrations diff
    cdc manage-migrations diff --service adopus
    cdc manage-migrations diff --table Actor
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.core.migration_diff import diff_migrations
from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run migration diff from CLI arguments.

    Returns:
        Exit code: 0 = no changes, 1 = changes detected, 2 = error.
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations diff",
        description="Compare service-schema definitions against generated migrations",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Filter: only compare tables matching this name",
    )
    parser.add_argument(
        "--migrations-dir",
        default=None,
        help="Override migrations directory (default: migrations/)",
    )
    args = parser.parse_args()

    from pathlib import Path

    migrations_dir = Path(args.migrations_dir) if args.migrations_dir else None

    result = diff_migrations(
        service_name=args.service,
        migrations_dir=migrations_dir,
        table_filter=args.table,
    )

    if result.errors:
        for err in result.errors:
            print_error(err)
        return 2

    return 1 if result.has_changes else 0


if __name__ == "__main__":
    sys.exit(main())
