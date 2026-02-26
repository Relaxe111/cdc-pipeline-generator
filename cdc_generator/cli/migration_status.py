#!/usr/bin/env python3
"""CLI entry point for migration status reporting.

Usage:
    cdc manage-migrations status --env dev
    cdc manage-migrations status --env prod --sink sink_asma.directory
    cdc manage-migrations status --offline
"""

from __future__ import annotations

import argparse
import sys

from cdc_generator.helpers.helpers_logging import print_error


def main() -> int:
    """Run migration status check from CLI arguments.

    Returns:
        Exit code: 0 = all applied, 1 = pending, 2 = error.
    """
    parser = argparse.ArgumentParser(
        prog="cdc manage-migrations status",
        description="Show which migrations have been applied vs pending",
    )
    parser.add_argument(
        "--service",
        default="adopus",
        help="Service name (default: adopus)",
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Target environment (dev, stage, prod). Omit for offline mode.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Show file listing without connecting to database",
    )
    parser.add_argument(
        "--sink",
        default=None,
        help="Only check this sink target (e.g., sink_asma.directory)",
    )
    parser.add_argument(
        "--migrations-dir",
        default=None,
        help="Override migrations directory (default: migrations/)",
    )
    args = parser.parse_args()

    from pathlib import Path

    migrations_dir = Path(args.migrations_dir) if args.migrations_dir else None

    if args.offline or not args.env:
        from cdc_generator.core.migration_status import check_migration_status_offline

        result = check_migration_status_offline(
            migrations_dir=migrations_dir,
            sink_filter=args.sink,
        )
        if result.errors:
            for err in result.errors:
                print_error(err)
            return 2

        from cdc_generator.helpers.helpers_logging import print_info

        for f in result.files:
            print_info(f"  ○ {f.file_name}")
        print_info(f"\n  {len(result.files)} file(s) — connect with --env to check status")
        return 0

    from cdc_generator.core.migration_status import check_migration_status

    result = check_migration_status(
        service_name=args.service,
        env=args.env,
        migrations_dir=migrations_dir,
        sink_filter=args.sink,
    )

    if result.errors:
        for err in result.errors:
            print_error(err)
        return 2

    return 1 if result.pending_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
