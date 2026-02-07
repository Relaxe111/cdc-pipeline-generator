#!/usr/bin/env python3
"""
Interactive service management tool for CDC pipeline.

Usage:
    cdc manage-service
    cdc manage-service --service adopus --inspect --all
    cdc manage-service --service adopus --add-table Actor
    cdc manage-service --service adopus --remove-table Test
"""

import argparse
import sys

from cdc_generator.cli.service_handlers import (
    handle_add_source_table,
    handle_add_source_tables,
    handle_create_service,
    handle_generate_validation,
    handle_inspect,
    handle_inspect_sink,
    handle_interactive,
    handle_list_source_tables,
    handle_no_service,
    handle_remove_table,
    handle_sink_add,
    handle_sink_add_table,
    handle_sink_list,
    handle_sink_map_column_error,
    handle_sink_remove,
    handle_sink_remove_table,
    handle_sink_validate,
    handle_validate_config,
    handle_validate_hierarchy,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)

_EPILOG = """\
Examples:
  # Create a new service
  cdc manage-service --service myservice --create-service

  # Inspect database tables
  cdc manage-service --service adopus --inspect --schema dbo
  cdc manage-service --service adopus --inspect --all

  # Add tables to service
  cdc manage-service --service adopus --add-source-table dbo.Actor
  cdc manage-service --service adopus \\
      --add-source-tables dbo.Actor dbo.Fraver

  # Remove table from service
  cdc manage-service --service adopus --remove-table dbo.Actor

  # Validate service configuration
  cdc manage-service --service adopus --validate-config

  # Inspect sink database tables
  cdc manage-service --service directory --inspect-sink sink_asma.calendar --all
  cdc manage-service --service directory --inspect-sink sink_asma.calendar --schema public

  # Sink management
  cdc manage-service --service directory --add-sink sink_asma.chat
  cdc manage-service --service directory \\
      --sink sink_asma.chat \\
      --add-sink-table public.customer_user
  cdc manage-service --service directory \\
      --sink sink_asma.chat \\
      --add-sink-table public.attachments \\
      --target public.chat_attachments \\
      --map-column id attachment_id \\
      --map-column name file_name
  cdc manage-service --service directory --list-sinks
  cdc manage-service --service directory --validate-sinks
  cdc manage-service --service directory \\
      --remove-sink sink_asma.chat
"""


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for manage-service."""
    parser = argparse.ArgumentParser(
        description="Interactive CDC service management tool",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Core args
    parser.add_argument(
        "--service",
        required=False,
        help="Service name from services/*.yaml",
    )
    parser.add_argument(
        "--create-service",
        metavar="SERVICE_NAME",
        help="Create a new service configuration file",
    )
    parser.add_argument(
        "--server",
        help="Server name for multi-server setups",
    )

    # Source table args
    parser.add_argument(
        "--list-source-tables",
        action="store_true",
        help="List all source tables in this service",
    )
    parser.add_argument(
        "--add-source-table",
        help="Add single table (format: schema.table)",
    )
    parser.add_argument(
        "--add-source-tables",
        nargs="+",
        help="Add multiple tables (space-separated)",
    )
    parser.add_argument(
        "--remove-table",
        help="Remove table (format: schema.table)",
    )

    # Inspect / validation args
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Inspect database schema (auto-detects DB)",
    )
    parser.add_argument(
        "--inspect-sink",
        metavar="SINK_KEY",
        help="Inspect sink database schema (e.g., sink_asma.calendar)",
    )
    parser.add_argument(
        "--schema",
        help="Database schema to inspect or filter",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save detailed table schemas to YAML",
    )
    parser.add_argument(
        "--generate-validation",
        action="store_true",
        help="Generate JSON Schema for validation",
    )
    parser.add_argument(
        "--validate-hierarchy",
        action="store_true",
        help="Validate hierarchical inheritance",
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Comprehensive validation of config",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all schemas",
    )
    parser.add_argument(
        "--env",
        default="nonprod",
        help="Environment for inspection (default: nonprod)",
    )
    parser.add_argument(
        "--primary-key",
        help="Primary key column name (optional)",
    )
    parser.add_argument(
        "--ignore-columns",
        action="append",
        help="Column to ignore (schema.table.column)",
    )
    parser.add_argument(
        "--track-columns",
        action="append",
        help="Column to track (schema.table.column)",
    )

    # Sink management args
    parser.add_argument(
        "--add-sink",
        metavar="SINK_KEY",
        help="Add sink (sink_group.target_service)",
    )
    parser.add_argument(
        "--remove-sink",
        metavar="SINK_KEY",
        help="Remove sink destination",
    )
    parser.add_argument(
        "--sink",
        metavar="SINK_KEY",
        help="Target sink for table operations",
    )
    parser.add_argument(
        "--add-sink-table",
        metavar="TABLE",
        help="Add table to sink (requires --sink)",
    )
    parser.add_argument(
        "--remove-sink-table",
        metavar="TABLE",
        help="Remove table from sink (requires --sink)",
    )
    parser.add_argument(
        "--target-exists",
        choices=["true", "false"],
        help=(
            "REQUIRED for --add-sink-table: "
            + "true (map to existing table) or false (autocreate)"
        ),
    )
    parser.add_argument(
        "--target",
        metavar="TARGET",
        help="Target table for mapped sink table",
    )
    parser.add_argument(
        "--target-schema",
        metavar="SCHEMA",
        help="Override target schema for cloned table",
    )
    parser.add_argument(
        "--map-column",
        nargs=2,
        metavar=("SOURCE_COL", "TARGET_COL"),
        action="append",
        help="Map source column to target column",
    )
    parser.add_argument(
        "--include-sink-columns",
        nargs="+",
        metavar="COL",
        help="Only sync these columns to sink",
    )
    parser.add_argument(
        "--list-sinks",
        action="store_true",
        help="List all sink configurations",
    )
    parser.add_argument(
        "--validate-sinks",
        action="store_true",
        help="Validate sink configuration",
    )

    # Legacy args (backward compatibility)
    parser.add_argument("--source", help=argparse.SUPPRESS)
    parser.add_argument(
        "--source-schema", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--sink-schema", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--source-table", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--sink-table", help=argparse.SUPPRESS,
    )

    return parser


def _auto_detect_service(
    args: argparse.Namespace,
) -> argparse.Namespace | None:
    """Auto-detect service if only one exists.

    Returns:
        Updated args, or None if error should cause exit(1).
    """
    if args.service:
        return args

    from cdc_generator.helpers.service_config import (
        get_project_root,
    )

    services_dir = get_project_root() / "services"
    if not services_dir.exists():
        return args

    service_files = list(services_dir.glob("*.yaml"))
    if len(service_files) == 1:
        args.service = service_files[0].stem
        print_info(f"Auto-detected service: {args.service}")
    elif len(service_files) > 1 and not args.create_service:
        available = ", ".join(
            f.stem for f in sorted(service_files)
        )
        print_error(
            f"Multiple services found: {available}"
        )
        print_error("Please specify --service <name>")
        return None

    return args


def _dispatch_validation(args: argparse.Namespace) -> int | None:
    """Handle validation-related commands. None = not handled."""
    if args.list_source_tables:
        return handle_list_source_tables(args)

    if args.create_service:
        return handle_create_service(args)

    if not args.service:
        return None

    validators: dict[str, bool] = {
        "validate_config": args.validate_config,
        "validate_hierarchy": args.validate_hierarchy,
        "generate_validation": args.generate_validation,
    }
    handlers = {
        "validate_config": handle_validate_config,
        "validate_hierarchy": handle_validate_hierarchy,
        "generate_validation": handle_generate_validation,
    }
    for key, active in validators.items():
        if active:
            return handlers[key](args)

    return _dispatch_inspect(args)


def _dispatch_inspect(args: argparse.Namespace) -> int | None:
    """Handle inspect-related commands. None = not handled."""
    if args.inspect:
        return handle_inspect(args)
    if args.inspect_sink:
        return handle_inspect_sink(args)
    return None


def _dispatch_sink(args: argparse.Namespace) -> int | None:
    """Handle sink-related commands. None = not handled."""
    if not args.service:
        return None

    simple_sink_cmds = {
        "list_sinks": handle_sink_list,
        "validate_sinks": handle_sink_validate,
        "add_sink": handle_sink_add,
        "remove_sink": handle_sink_remove,
        "add_sink_table": handle_sink_add_table,
    }
    for attr, handler in simple_sink_cmds.items():
        if getattr(args, attr, False):
            return handler(args)

    if args.remove_sink_table and args.sink:
        return handle_sink_remove_table(args)

    if args.map_column and args.sink and not args.add_sink_table:
        return handle_sink_map_column_error()

    return None


def _dispatch_source(args: argparse.Namespace) -> int | None:
    """Handle source table commands. None = not handled."""
    if not args.service:
        return None

    if args.add_source_tables:
        return handle_add_source_tables(args)

    if args.add_source_table:
        return handle_add_source_table(args)

    if args.remove_table:
        return handle_remove_table(args)

    return None


def _dispatch(args: argparse.Namespace) -> int:
    """Route parsed args to the appropriate handler."""
    result = _dispatch_validation(args)
    if result is not None:
        return result

    result = _dispatch_sink(args)
    if result is not None:
        return result

    result = _dispatch_source(args)
    if result is not None:
        return result

    if not args.service:
        return handle_no_service()

    return handle_interactive(args)


def main() -> int:
    """Entry point for `cdc manage-service`."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.create_service:
        args.service = args.create_service

    result = _auto_detect_service(args)
    if result is None:
        return 1
    args = result

    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
