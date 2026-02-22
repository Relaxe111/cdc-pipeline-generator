#!/usr/bin/env python3
"""CLI for managing service schema definitions.

Manages custom table schemas independently from service YAML,
stored under ``services/_schemas/{service}/custom-tables/``
(legacy read compatibility: ``service-schemas/{service}/custom-tables/``).

Usage:
    cdc manage-services schema custom-tables --service chat --list
    cdc manage-services schema custom-tables --service chat \
        --add-custom-table public.audit_log \\
        --column id:uuid:pk \\
        --column event_type:text:not_null \\
        --column payload:jsonb
    cdc manage-services schema custom-tables --service chat \
        --show public.audit_log
    cdc manage-services schema custom-tables --service chat \
        --remove-custom-table public.audit_log
    cdc manage-services schema custom-tables --list-services
"""

import argparse
import sys
from typing import Any, NoReturn

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_warning,
)

# ---------------------------------------------------------------------------
# Flag hints for friendly error messages
# ---------------------------------------------------------------------------

_FLAG_HINTS: dict[str, tuple[str, str]] = {
    "--service": (
        "Service name from services/_schemas/",
        (
            "cdc manage-services schema custom-tables"
            " --service chat --list"
        ),
    ),
    "--add-custom-table": (
        "Table reference as schema.table",
        (
            "cdc manage-services schema custom-tables --service chat"
            " --add-custom-table public.audit_log"
            " --column id:uuid:pk"
            " --column event_type:text:not_null"
        ),
    ),
    "--show": (
        "Table reference as schema.table",
        (
            "cdc manage-services schema custom-tables"
            " --service chat --show public.audit_log"
        ),
    ),
    "--remove-custom-table": (
        "Table reference as schema.table",
        (
            "cdc manage-services schema custom-tables"
            " --service chat"
            " --remove-custom-table public.audit_log"
        ),
    ),
    "--column": (
        "Column spec: name:type[:pk][:not_null][:default_X]",
        "--column id:uuid:pk --column name:text:not_null",
    ),
}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

_EPILOG = """\
Examples:
  # List services with schemas
    cdc manage-services schema custom-tables --list-services

  # List custom tables for a service
    cdc manage-services schema custom-tables --service chat --list

  # Create a custom table
    cdc manage-services schema custom-tables --service chat \
      --add-custom-table public.audit_log \\
      --column id:uuid:pk \\
      --column event_type:text:not_null \\
      --column payload:jsonb \\
      --column created_at:timestamptz:not_null:default_now

  # Show custom table details
    cdc manage-services schema custom-tables --service chat \
      --show public.audit_log

  # Remove a custom table
    cdc manage-services schema custom-tables --service chat \
      --remove-custom-table public.audit_log

Column specification format:
  name:type[:pk][:not_null][:default_X]

  Modifiers:
    pk          Mark as primary key (implies NOT NULL)
    not_null    NOT NULL constraint
    nullable    Allow NULL (explicit, default)
    default_X   Default expression (now, uuid, current_timestamp)

  Examples:
    id:uuid:pk
    name:text:not_null
    created_at:timestamptz:not_null:default_now
    metadata:jsonb
    status:text:not_null:default_active
"""


class SchemaArgumentParser(argparse.ArgumentParser):
    """Custom parser with friendly error messages."""

    def error(self, message: str) -> NoReturn:
        """Override to show friendly errors with examples."""
        for flag, (desc, example) in _FLAG_HINTS.items():
            if flag in message and "expected" in message:
                print_error(
                    f"{flag} requires a value: {desc}"
                )
                print_info(f"Example: {example}")
                raise SystemExit(1)

        print_error(message)
        raise SystemExit(1)


def _build_parser() -> SchemaArgumentParser:
    """Build argument parser for manage-services schema custom-tables."""
    parser = SchemaArgumentParser(
        description="Manage custom table schema definitions",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--service",
        help="Service name (directory under services/_schemas/)",
    )

    # List operations
    parser.add_argument(
        "--list-services",
        action="store_true",
        help="List all services with schema directories",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help=(
            "List custom tables for a service"
            " (requires --service)"
        ),
    )

    # CRUD operations
    parser.add_argument(
        "--add-custom-table",
        metavar="TABLE",
        help=(
            "Create custom table schema"
            " (format: schema.table, requires --column)"
        ),
    )
    parser.add_argument(
        "--show",
        metavar="TABLE",
        help="Show custom table details (format: schema.table)",
    )
    parser.add_argument(
        "--remove-custom-table",
        metavar="TABLE",
        help=(
            "Remove custom table schema"
            " (format: schema.table)"
        ),
    )

    # Column definitions
    parser.add_argument(
        "--column",
        action="append",
        metavar="SPEC",
        help=(
            "Column definition: "
            "name:type[:pk][:not_null][:default_X]"
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _handle_list_services() -> int:
    """List all services with schema directories."""
    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        list_services_with_schemas,
    )

    services = list_services_with_schemas()
    if not services:
        print_warning("No service schema directories found")
        return 0

    print_info("Services with schemas:")
    for svc in services:
        print(f"  {svc}")
    return 0


def _handle_list(service: str) -> int:
    """List custom tables for a service."""
    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        list_custom_tables,
    )

    tables = list_custom_tables(service)
    if not tables:
        print_info(
            f"No custom tables for service '{service}'"
        )
        return 0

    print_info(
        f"Custom tables for '{service}'"
        + f" ({len(tables)} total):"
    )
    for ref in tables:
        print(f"  {ref}")
    return 0


def _handle_add_custom_table(
    service: str,
    table_ref: str,
    column_specs: list[str] | None,
) -> int:
    """Create a new custom table schema."""
    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        create_custom_table,
    )

    if not column_specs:
        print_error(
            "--add-custom-table requires at least one --column"
        )
        print_info(
            "Example: cdc manage-services schema custom-tables"
            + " --service chat"
            + " --add-custom-table public.audit_log"
            + " --column id:uuid:pk"
            + " --column event_type:text:not_null"
        )
        return 1

    if create_custom_table(service, table_ref, column_specs):
        return 0
    return 1


def _handle_show(service: str, table_ref: str) -> int:
    """Show details of a custom table."""
    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        show_custom_table,
    )

    data = show_custom_table(service, table_ref)
    if data is None:
        return 1

    _print_table_details(data)
    return 0


def _handle_remove(service: str, table_ref: str) -> int:
    """Remove a custom table."""
    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        remove_custom_table,
    )

    if remove_custom_table(service, table_ref):
        return 0
    return 1


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _print_table_details(data: dict[str, Any]) -> None:
    """Pretty-print custom table schema details."""
    print_info(f"Custom table: {data.get('schema')}.{data.get('table')}")
    print_info(f"  Service: {data.get('service')}")
    print_info(f"  Database: {data.get('database', '(any)')}")

    pk: str | list[str] | None = data.get("primary_key")
    if isinstance(pk, list):
        pk_str = ", ".join(pk)
    elif pk:
        pk_str = str(pk)
    else:
        pk_str = "(none)"
    print_info(f"  Primary key: {pk_str}")

    columns: list[dict[str, Any]] = data.get("columns", [])
    print_info(f"  Columns ({len(columns)}):")
    for col in columns:
        nullable = "NULL" if col.get("nullable") else "NOT NULL"
        pk_marker = " [PK]" if col.get("primary_key") else ""
        default = (
            f" DEFAULT {col['default']}"
            if col.get("default")
            else ""
        )
        print(
            f"    {col['name']}: {col['type']}"
            + f" {nullable}{pk_marker}{default}"
        )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _dispatch(args: argparse.Namespace) -> int:
    """Route parsed args to the appropriate handler."""
    if args.list_services:
        return _handle_list_services()

    if args.list and args.service:
        return _handle_list(args.service)

    if not args.service:
        msg = (
            "--list requires --service"
            if args.list
            else "--service is required"
            + " (or use --list-services)"
        )
        print_error(msg)
        return 1

    return _dispatch_service_action(args)


def _dispatch_service_action(
    args: argparse.Namespace,
) -> int:
    """Dispatch service-scoped actions."""
    if args.add_custom_table:
        return _handle_add_custom_table(
            args.service,
            args.add_custom_table,
            args.column,
        )

    if args.show:
        return _handle_show(args.service, args.show)

    if args.remove_custom_table:
        return _handle_remove(
            args.service,
            args.remove_custom_table,
        )

    # No action specified — show available custom tables
    return _handle_list(args.service)


def main() -> int:
    """Entry point for ``cdc manage-services schema custom-tables``."""
    parser = _build_parser()
    args = parser.parse_args()
    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
