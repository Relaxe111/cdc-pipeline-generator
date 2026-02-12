#!/usr/bin/env python3
"""CLI for managing column template definitions in column-templates.yaml.

Manages the template library — adding, removing, editing, and listing
reusable column definitions that can be referenced in service YAML.

Usage:
    cdc manage-column-templates --list
    cdc manage-column-templates --show tenant_id
    cdc manage-column-templates --add tenant_id \\
        --type text --not-null \\
        --value '${TENANT_ID}' \\
        --description "Tenant identifier"
    cdc manage-column-templates --edit tenant_id \\
        --value '{asma.sources.*.customer_id}'
    cdc manage-column-templates --remove tenant_id
"""

import argparse
import sys
from typing import NoReturn

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)

# ---------------------------------------------------------------------------
# Flag hints for friendly error messages
# ---------------------------------------------------------------------------

_FLAG_HINTS: dict[str, tuple[str, str]] = {
    "--add": (
        "Template key (unique identifier)",
        (
            "cdc manage-column-templates --add tenant_id"
            " --type text --not-null"
            " --value '${TENANT_ID}'"
            " --description 'Tenant identifier'"
        ),
    ),
    "--remove": (
        "Template key to remove",
        "cdc manage-column-templates --remove tenant_id",
    ),
    "--show": (
        "Template key to show details for",
        "cdc manage-column-templates --show tenant_id",
    ),
    "--edit": (
        "Template key to edit",
        (
            "cdc manage-column-templates --edit tenant_id"
            " --value '{asma.sources.*.customer_id}'"
        ),
    ),
    "--type": (
        "PostgreSQL column type",
        "--type text",
    ),
    "--value": (
        "Bloblang expression or env var reference",
        "--value '${TENANT_ID}'",
    ),
    "--name": (
        "Column name (defaults to _<key>)",
        "--name _tenant_id",
    ),
}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

_EPILOG = """\
Examples:
  # List all template definitions
  cdc manage-column-templates --list

  # Show details of a template
  cdc manage-column-templates --show tenant_id

  # Add a new template
  cdc manage-column-templates --add tenant_id \\
      --type text --not-null \\
      --value '${TENANT_ID}' \\
      --description "Tenant identifier"

  # Add template with SQL default
  cdc manage-column-templates --add sync_timestamp \\
      --type timestamptz --not-null \\
      --value 'now()' \\
      --default 'now()' \\
      --description "Sync timestamp"

  # Edit an existing template
  cdc manage-column-templates --edit tenant_id \\
      --value '{asma.sources.*.customer_id}'

  # Remove a template
  cdc manage-column-templates --remove tenant_id

Template value expressions:
  Bloblang:     meta("table"), this.field, now(), uuid_v4()
  Environment:  ${VARIABLE_NAME}
  Source ref:   {group.sources.*.key} (resolved at generation time)
"""


class TemplateArgumentParser(argparse.ArgumentParser):
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


def _build_parser() -> TemplateArgumentParser:
    """Build argument parser for manage-column-templates."""
    parser = TemplateArgumentParser(
        description="Manage column template definitions (column-templates.yaml)",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # List / show
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all column template definitions",
    )
    parser.add_argument(
        "--show",
        metavar="KEY",
        help="Show detailed info about a template",
    )

    # Add / edit / remove
    parser.add_argument(
        "--add",
        metavar="KEY",
        help="Add a new template definition",
    )
    parser.add_argument(
        "--edit",
        metavar="KEY",
        help="Edit an existing template definition",
    )
    parser.add_argument(
        "--remove",
        metavar="KEY",
        help="Remove a template definition",
    )

    # Template field arguments (used with --add and --edit)
    parser.add_argument(
        "--name",
        metavar="NAME",
        help="Column name (default: _<key> for --add)",
    )
    parser.add_argument(
        "--type",
        metavar="TYPE",
        dest="col_type",
        help="PostgreSQL column type (e.g., text, integer, timestamptz)",
    )
    parser.add_argument(
        "--value",
        metavar="EXPR",
        help=(
            "Bloblang expression, env var, or source-group reference "
            + "(e.g., '${TENANT_ID}', '{asma.sources.*.customer_id}')"
        ),
    )
    parser.add_argument(
        "--description",
        metavar="DESC",
        help="Human-readable description",
    )
    parser.add_argument(
        "--not-null",
        action="store_true",
        default=None,
        help="Mark column as NOT NULL",
    )
    parser.add_argument(
        "--nullable",
        action="store_true",
        default=None,
        help="Mark column as nullable (for --edit to undo NOT NULL)",
    )
    parser.add_argument(
        "--default",
        metavar="EXPR",
        dest="sql_default",
        help="SQL default expression for DDL (e.g., now(), gen_random_uuid())",
    )
    parser.add_argument(
        "--applies-to",
        metavar="PATTERN",
        action="append",
        help="Table glob pattern restriction (e.g., '*.users', 'public.*')",
    )

    return parser


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _handle_list() -> int:
    """List all template definitions."""
    from cdc_generator.core.column_template_definitions import (
        list_template_definitions,
    )

    templates = list_template_definitions()
    if not templates:
        print_info("No column templates defined")
        print_info(
            "Add one with: cdc manage-column-templates"
            " --add <key> --type <type> --value <expr>"
        )
        return 0

    print_info(f"Column templates ({len(templates)} total):")
    for t in templates:
        null_str = "NOT NULL" if t.not_null else "NULL"
        default_str = f", default: {t.default}" if t.default else ""
        applies_str = ""
        if t.applies_to:
            applies_str = f", applies_to: {', '.join(t.applies_to)}"
        print(
            f"  {t.key}: {t.name} ({t.column_type}, {null_str}{default_str}{applies_str})"
        )
        if t.description:
            print(f"    {t.description}")
        print(f"    value: {t.value}")
    return 0


def _handle_show(key: str) -> int:
    """Show detailed info about a template."""
    from cdc_generator.core.column_template_definitions import (
        show_template_definition,
    )

    template = show_template_definition(key)
    if template is None:
        return 1

    print_info(f"Template: {template.key}")
    print(f"  Column name:  {template.name}")
    print(f"  Type:         {template.column_type}")
    print(f"  NOT NULL:     {template.not_null}")
    print(f"  Value:        {template.value}")
    if template.default:
        print(f"  SQL default:  {template.default}")
    if template.description:
        print(f"  Description:  {template.description}")
    if template.applies_to:
        print(f"  Applies to:   {', '.join(template.applies_to)}")
    return 0


def _handle_add(args: argparse.Namespace) -> int:
    """Handle --add: create a new template definition."""
    from cdc_generator.core.column_template_definitions import (
        add_template_definition,
    )

    key = args.add

    # Validate required fields for --add
    if args.col_type is None:
        print_error("--add requires --type")
        print_info(
            "Example: cdc manage-column-templates"
            + f" --add {key} --type text --value '${{VARIABLE}}'"
        )
        return 1

    if args.value is None:
        print_error("--add requires --value")
        print_info(
            "Example: cdc manage-column-templates"
            + f" --add {key} --type {args.col_type}"
            + " --value '${VARIABLE}'"
        )
        return 1

    # Default column name: _{key}
    name = args.name if args.name is not None else f"_{key}"
    not_null = bool(args.not_null) if args.not_null is not None else False
    description = args.description or ""

    if add_template_definition(
        key=key,
        name=name,
        col_type=args.col_type,
        value=args.value,
        description=description,
        not_null=not_null,
        default=args.sql_default,
        applies_to=args.applies_to,
    ):
        return 0
    return 1


def _handle_edit(args: argparse.Namespace) -> int:
    """Handle --edit: update an existing template definition."""
    from cdc_generator.core.column_template_definitions import (
        edit_template_definition,
    )

    key = args.edit

    # Determine not_null value
    not_null: bool | None = None
    if args.not_null:
        not_null = True
    elif args.nullable:
        not_null = False

    if edit_template_definition(
        key=key,
        name=args.name,
        col_type=args.col_type,
        value=args.value,
        description=args.description,
        not_null=not_null,
        default=args.sql_default,
    ):
        return 0
    return 1


def _handle_remove(key: str) -> int:
    """Handle --remove: delete a template definition."""
    from cdc_generator.core.column_template_definitions import (
        remove_template_definition,
    )

    if remove_template_definition(key):
        return 0
    return 1


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _dispatch(args: argparse.Namespace) -> int:
    """Route parsed args to the appropriate handler."""
    if args.list:
        return _handle_list()

    if args.show:
        return _handle_show(args.show)

    if args.add:
        return _handle_add(args)

    if args.edit:
        return _handle_edit(args)

    if args.remove:
        return _handle_remove(args.remove)

    # No action specified — default to list
    return _handle_list()


def main() -> int:
    """Entry point for ``cdc manage-column-templates``."""
    parser = _build_parser()
    args = parser.parse_args()
    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
