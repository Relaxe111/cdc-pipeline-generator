#!/usr/bin/env python3
"""CLI entry point for applying migrations to a target PostgreSQL database.

Usage:
    cdc manage-migrations apply --env dev
    cdc manage-migrations apply --env stage --dry-run
    cdc manage-migrations apply --env prod --sink sink_asma.directory
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.core.migration_apply import apply_migrations
from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run migration apply from CLI arguments.

    Returns:
        Exit code (0 = success, 1 = error).
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations apply",
        description="Apply pending migration SQL files to a target PostgreSQL database",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--env",
        required=True,
        help="Target environment (dev, stage, prod, test, etc.)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List pending migrations without applying them",
    )
    parser.add_argument(
        "--sink",
        default=None,
        help="Only apply for this sink target (e.g., sink_asma.directory)",
    )
    parser.add_argument(
        "--migrations-dir",
        default=None,
        help="Override migrations directory (default: migrations/)",
    )
    args = parser.parse_args()

    from pathlib import Path

    migrations_dir = Path(args.migrations_dir) if args.migrations_dir else None

    result = apply_migrations(
        service_name=args.service,
        env=args.env,
        dry_run=args.dry_run,
        migrations_dir=migrations_dir,
        sink_filter=args.sink,
    )

    if result.errors:
        for err in result.errors:
            print_error(err)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
