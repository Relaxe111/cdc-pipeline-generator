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
from collections.abc import Callable
from typing import NoReturn

from cdc_generator.cli.service_handlers import (
    handle_add_extra_column,
    handle_add_source_table,
    handle_add_source_tables,
    handle_add_transform,
    handle_create_service,
    handle_generate_validation,
    handle_inspect,
    handle_inspect_sink,
    handle_interactive,
    handle_list_column_templates,
    handle_list_extra_columns,
    handle_list_source_tables,
    handle_list_transform_rules,
    handle_list_transforms,
    handle_modify_custom_table,
    handle_no_service,
    handle_remove_extra_column,
    handle_remove_table,
    handle_remove_transform,
    handle_sink_add,
    handle_sink_add_custom_table,
    handle_sink_add_table,
    handle_sink_list,
    handle_sink_map_column_error,
    handle_sink_remove,
    handle_sink_remove_table,
    handle_sink_update_schema,
    handle_sink_validate,
    handle_validate_config,
    handle_validate_hierarchy,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)

# flag → (description, example)
_FLAG_HINTS: dict[str, tuple[str, str]] = {
    "--service": (
        "Service name from services/*.yaml",
        "cdc manage-service --service adopus --inspect --all",
    ),
    "--create-service": (
        "Name for the new service to create",
        "cdc manage-service --create-service myservice",
    ),
    "--add-source-table": (
        "Table name to add (format: schema.table)",
        "cdc manage-service --service adopus --add-source-table dbo.Actor",
    ),
    "--remove-table": (
        "Table name to remove (format: schema.table)",
        "cdc manage-service --service adopus --remove-table dbo.Actor",
    ),
    "--inspect-sink": (
        "Sink key to inspect (format: sink_group.target_service)",
        (
            "cdc manage-service --service directory"
            " --inspect-sink sink_asma.calendar --all"
        ),
    ),
    "--schema": (
        "Database schema to inspect or filter",
        "cdc manage-service --service adopus --inspect --schema dbo",
    ),
    "--add-sink": (
        "Sink key to add (format: sink_group.target_service)",
        "cdc manage-service --service directory --add-sink sink_asma.chat",
    ),
    "--remove-sink": (
        "Sink key to remove",
        "cdc manage-service --service directory --remove-sink sink_asma.chat",
    ),
    "--sink": (
        "Target sink for table operations",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --add-sink-table public.users"
        ),
    ),
    "--add-sink-table": (
        "Table name to add to sink (requires --sink)",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --add-sink-table public.users"
        ),
    ),
    "--from": (
        "Source table reference for sink (defaults to sink table name)",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.proxy --add-sink-table other.manage_audits"
            " --from logs.audit_queue --replicate-structure"
        ),
    ),
    "--replicate-structure": (
        "Auto-generate sink table DDL from source schema",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --add-sink-table public.customer_user"
            " --replicate-structure --target-exists false"
        ),
    ),
    "--remove-sink-table": (
        "Table name to remove from sink (requires --sink)",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --remove-sink-table public.users"
        ),
    ),
    "--target": (
        "Target table name for mapped sink table",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --add-sink-table public.attachments"
            " --target public.chat_attachments"
        ),
    ),
    "--target-schema": (
        "Override target schema for cloned table",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.chat --add-sink-table public.users"
            " --target-schema custom_schema"
        ),
    ),
    "--env": (
        "Environment for inspection (default: nonprod)",
        "cdc manage-service --service adopus --inspect --env prod",
    ),
    "--primary-key": (
        "Primary key column name",
        (
            "cdc manage-service --service adopus"
            " --add-source-table dbo.Actor --primary-key actno"
        ),
    ),
    "--add-custom-sink-table": (
        "Table name for custom table (format: schema.table)",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.proxy"
            " --add-custom-sink-table public.audit_log"
            " --column id:uuid:pk"
            " --column name:text:not_null"
        ),
    ),
    "--modify-custom-table": (
        "Custom table to modify (format: schema.table)",
        (
            "cdc manage-service --service directory"
            " --sink sink_asma.proxy"
            " --modify-custom-table public.audit_log"
            " --add-column updated_at:timestamptz:default_now"
        ),
    ),
    "--add-column": (
        "Column spec to add (format: name:type[:pk][:not_null][:default_X])",
        "--add-column updated_at:timestamptz:not_null:default_now",
    ),
    "--remove-column": (
        "Column name to remove from custom table",
        "--remove-column payload",
    ),
    "--column": (
        "Column definition (format: name:type[:pk][:not_null][:default_X])",
        "--column id:uuid:pk --column name:text:not_null",
    ),
}


class ServiceArgumentParser(argparse.ArgumentParser):
    """Custom parser with user-friendly error messages."""

    def error(self, message: str) -> NoReturn:
        """Override to show friendly errors with examples."""
        for flag, (desc, example) in _FLAG_HINTS.items():
            if flag in message and "expected" in message:
                print_error(f"{flag} requires a value: {desc}")
                print_info(f"Example: {example}")
                raise SystemExit(1)

        # Fall back to a clean error (no usage dump)
        print_error(message)
        raise SystemExit(1)

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

  # Custom sink tables (auto-created in sink database)
  cdc manage-service --service directory \\
      --sink sink_asma.proxy \\
      --add-custom-sink-table public.audit_log \\
      --column id:uuid:pk \\
      --column event_type:text:not_null \\
      --column payload:jsonb \\
      --column created_at:timestamptz:not_null:default_now

  # Modify custom table columns
  cdc manage-service --service directory \\
      --sink sink_asma.proxy \\
      --modify-custom-table public.audit_log \\
      --add-column updated_at:timestamptz:default_now
  cdc manage-service --service directory \\
      --sink sink_asma.proxy \\
      --modify-custom-table public.audit_log \\
      --remove-column payload
"""


def _add_column_template_args(parser: ServiceArgumentParser) -> None:
    """Add column templates & transforms arguments to the parser."""
    parser.add_argument(
        "--add-column-template",
        metavar="TEMPLATE",
        help=(
            "Add column template to sink table "
            + "(requires --sink-table)"
        ),
    )
    parser.add_argument(
        "--remove-column-template",
        metavar="TEMPLATE",
        help=(
            "Remove column template from sink table "
            + "(requires --sink-table)"
        ),
    )
    parser.add_argument(
        "--list-column-templates",
        action="store_true",
        help="List column templates on a sink table (requires --sink-table)",
    )
    parser.add_argument(
        "--column-name",
        metavar="NAME",
        help=(
            "Override column name when adding column template "
            + "(default: template's name)"
        ),
    )
    parser.add_argument(
        "--add-transform",
        metavar="RULE",
        help=(
            "Add transform rule to sink table "
            + "(requires --sink-table)"
        ),
    )
    parser.add_argument(
        "--remove-transform",
        metavar="RULE",
        help=(
            "Remove transform rule from sink table "
            + "(requires --sink-table)"
        ),
    )
    parser.add_argument(
        "--list-transforms",
        action="store_true",
        help="List transforms on a sink table (requires --sink-table)",
    )
    parser.add_argument(
        "--list-column-templates",
        action="store_true",
        help="List all available column templates",
    )
    parser.add_argument(
        "--list-transform-rules",
        action="store_true",
        help="List all available transform rules",
    )


def _build_parser() -> ServiceArgumentParser:
    """Build the argument parser for manage-service."""
    parser = ServiceArgumentParser(
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
        action="append",
        help="Add sink (sink_group.target_service). Can be used multiple times.",
    )
    parser.add_argument(
        "--remove-sink",
        metavar="SINK_KEY",
        action="append",
        help="Remove sink destination. Can be used multiple times.",
    )
    parser.add_argument(
        "--sink",
        metavar="SINK_KEY",
        help="Target sink for table operations",
    )
    parser.add_argument(
        "--add-sink-table",
        metavar="TABLE",
        nargs="?",
        help="Add table to sink (requires --sink). If omitted, uses --from value as table name.",
    )
    parser.add_argument(
        "--remove-sink-table",
        metavar="TABLE",
        help="Remove table from sink (requires --sink)",
    )
    parser.add_argument(
        "--update-schema",
        metavar="NEW_SCHEMA",
        help="Update schema of a sink table (requires --sink and --sink-table)",
    )
    parser.add_argument(
        "--from",
        dest="from_table",
        metavar="SOURCE_TABLE",
        help=(
            "Source table reference for sink table "
            + "(format: schema.table). Defaults to same name as sink table."
        ),
    )
    parser.add_argument(
        "--replicate-structure",
        action="store_true",
        help=(
            "Auto-generate sink table DDL from source schema "
            + "(requires --from if sink table name differs from source)"
        ),
    )
    parser.add_argument(
        "--sink-schema",
        metavar="SCHEMA",
        help=(
            "Override sink table schema (default: use source schema). "
            + "Structure saved to service-schemas/{target}/custom-tables/{schema}.{table}.yaml"
        ),
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

    # Column templates & transforms args
    _add_column_template_args(parser)

    # Custom sink table args
    parser.add_argument(
        "--add-custom-sink-table",
        metavar="TABLE",
        help="Create custom table in sink (schema.table)",
    )
    parser.add_argument(
        "--column",
        action="append",
        metavar="SPEC",
        help="Column def: name:type[:pk][:not_null][:default_X]",
    )
    parser.add_argument(
        "--modify-custom-table",
        metavar="TABLE",
        help="Modify custom table (requires --add-column or --remove-column)",
    )
    parser.add_argument(
        "--add-column",
        metavar="SPEC",
        help="Add column: name:type[:pk][:not_null][:default_X]",
    )
    parser.add_argument(
        "--remove-column",
        metavar="COL",
        help="Remove column from custom table",
    )

    # Legacy args (backward compatibility)
    parser.add_argument("--source", help=argparse.SUPPRESS)
    parser.add_argument(
        "--source-schema", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--source-table", help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--sink-table",
        metavar="TABLE",
        help="Target sink table for update operations (schema.table)",
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


def _dispatch_extra_columns(args: argparse.Namespace) -> int | None:
    """Handle extra column and transform commands. None = not handled."""
    # Global listing commands (no --service required)
    if args.list_column_templates:
        return handle_list_column_templates(args)
    if args.list_transform_rules:
        return handle_list_transform_rules(args)

    if not args.service:
        return None

    extra_handlers: dict[str, Callable[[argparse.Namespace], int]] = {
        "add_column_template": handle_add_extra_column,
        "remove_column_template": handle_remove_extra_column,
        "list_column_templates": handle_list_extra_columns,
        "add_transform": handle_add_transform,
        "remove_transform": handle_remove_transform,
        "list_transforms": handle_list_transforms,
    }
    for attr, handler in extra_handlers.items():
        if getattr(args, attr, False):
            return handler(args)

    return None


def _dispatch_sink(args: argparse.Namespace) -> int | None:
    """Handle sink-related commands. None = not handled."""
    if not args.service:
        return None

    sink_cmds: dict[str, Callable[[argparse.Namespace], int]] = {
        "list_sinks": handle_sink_list,
        "validate_sinks": handle_sink_validate,
        "add_sink": handle_sink_add,
        "remove_sink": handle_sink_remove,
        "add_sink_table": handle_sink_add_table,
        "add_custom_sink_table": handle_sink_add_custom_table,
        "modify_custom_table": handle_modify_custom_table,
    }
    for attr, handler in sink_cmds.items():
        if getattr(args, attr, False):
            return handler(args)

    result = _dispatch_sink_conditional(args)
    if result is not None:
        return result

    return None


def _dispatch_sink_conditional(args: argparse.Namespace) -> int | None:
    """Handle sink commands that need conditional checks."""
    if args.remove_sink_table and args.sink:
        return handle_sink_remove_table(args)

    if args.update_schema and args.sink:
        return handle_sink_update_schema(args)

    # --add-sink-table can be optional if --from is provided
    has_from = hasattr(args, "from_table") and args.from_table
    has_add_table = args.add_sink_table is not None or has_from
    if has_add_table and args.sink:
        return handle_sink_add_table(args)

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

    result = _dispatch_extra_columns(args)
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
