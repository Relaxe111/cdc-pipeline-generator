#!/usr/bin/env python3
"""CLI entry point for migration SQL generation.

Usage:
    cdc manage-migrations generate
    cdc manage-migrations generate --service adopus
    cdc manage-migrations generate --table Actor
    cdc manage-migrations generate --dry-run
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.core.migration_generator import generate_migrations
from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run migration generation from CLI arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations generate",
        description="Generate PostgreSQL migration SQL files from service config + table definitions",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Filter: only generate for tables matching this name",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be generated without writing files",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override output directory (default: migrations/)",
    )
    args = parser.parse_args()

    from pathlib import Path

    output_dir = Path(args.output_dir) if args.output_dir else None

    result = generate_migrations(
        service_name=args.service,
        table_filter=args.table,
        dry_run=args.dry_run,
        output_dir=output_dir,
    )

    if result.errors:
        for err in result.errors:
            print_error(err)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
