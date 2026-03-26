"""Argparse parser definition for ``manage-sink-groups`` CLI."""

from __future__ import annotations

import argparse
from typing import NoReturn

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_info,
)

# Mapping from flag name to (description, example)
_FLAG_HINTS: dict[str, tuple[str, str]] = {
    "--add-server": (
        "Server name to add",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --add-server nonprod"
        ),
    ),
    "--remove-server": (
        "Server name to remove",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --remove-server nonprod"
        ),
    ),
    "--sink-group": (
        "Sink group to operate on",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --add-server nonprod"
        ),
    ),
    "--server": (
        "Existing server name in sink group",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --server default --extraction-patterns '^AdOpus(?P<customer>.+)$'"
        ),
    ),
    "--list-server-extraction-patterns": (
        "List extraction patterns for sink-group servers",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --list-server-extraction-patterns default"
        ),
    ),
    "--add-new-sink-group": (
        "Name for the new sink group (auto-prefixed with 'sink_')",
        (
            "cdc manage-sink-groups --add-new-sink-group analytics"
            " --pattern db-shared"
        ),
    ),
    "--source-group": (
        "Source group name to inherit from",
        "cdc manage-sink-groups --create --source-group asma",
    ),
    "--info": (
        "Sink group name to show info for",
        "cdc manage-sink-groups --info sink_asma",
    ),
    "--introspect-types": (
        "Requires --sink-group to identify the database engine",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --introspect-types"
        ),
    ),
    "--db-definitions": (
        "Requires --sink-group to identify sink DB engine",
        (
            "cdc manage-sink-groups --sink-group sink_asma"
            " --db-definitions"
        ),
    ),
    "--update": (
        "Inspect sink databases and update sources",
        (
            "cdc manage-sink-groups --update --sink-group sink_asma"
            " --server default"
        ),
    ),
    "--remove": (
        "Sink group name to remove",
        "cdc manage-sink-groups --remove sink_test",
    ),
    "--add-to-ignore-list": (
        "Add database exclude pattern(s) to sink group",
        "cdc manage-sink-groups --add-to-ignore-list temp_%",
    ),
    "--add-to-schema-excludes": (
        "Add schema exclude pattern(s) to sink group",
        "cdc manage-sink-groups --add-to-schema-excludes hdb_catalog",
    ),
    "--add-to-table-excludes": (
        "Add table exclude pattern(s) to sink group",
        "cdc manage-sink-groups --add-to-table-excludes '^log'",
    ),
    "--add-source-custom-key": (
        "Add or update SQL-based custom key",
        (
            "cdc manage-sink-groups --sink-group sink_asma "
            "--add-source-custom-key customer_id "
            "--custom-key-value 'SELECT ...' --custom-key-exec-type sql"
        ),
    ),
}


class SinkGroupArgumentParser(argparse.ArgumentParser):
    """Custom parser with user-friendly error messages."""

    def error(self, message: str) -> NoReturn:
        """Override to show friendly errors with examples."""
        # Match "argument --flag: expected one argument"
        for flag, (desc, example) in _FLAG_HINTS.items():
            if flag in message and "expected" in message:
                print_error(f"{flag} requires a value: {desc}")
                print_info(f"Example: {example}")
                raise SystemExit(1)

        # Fall back to a clean error (no usage dump)
        print_error(message)
        raise SystemExit(1)


def build_parser() -> SinkGroupArgumentParser:
    """Build and return the argparse parser for manage-sink-groups."""
    header = (
        f"{Colors.CYAN}{Colors.BOLD}"
        "Manage sink server group configuration (sink-groups.yaml)"
        f"{Colors.RESET}"
    )
    description = f"""{header}

{Colors.DIM}This command helps manage sink destinations for CDC pipelines.
Sink groups can either inherit from source groups (db-shared pattern)
or be standalone (analytics warehouse, webhooks, etc.).{Colors.RESET}

{Colors.YELLOW}Usage Examples:{Colors.RESET}
    {Colors.GREEN}Auto-scaffold all sink groups{Colors.RESET}
    $ cdc manage-sink-groups --create

    {Colors.BLUE}Create inherited sink group{Colors.RESET}
    $ cdc manage-sink-groups --create --source-group foo

    {Colors.GREEN}Add new standalone sink group{Colors.RESET}
    $ cdc manage-sink-groups --add-new-sink-group analytics --type postgres

    {Colors.CYAN}List all sink groups{Colors.RESET}
    $ cdc manage-sink-groups --list

    {Colors.BLUE}Show sink group information{Colors.RESET}
    $ cdc manage-sink-groups --info sink_analytics

    {Colors.GREEN}Add a server{Colors.RESET}
    $ cdc manage-sink-groups --sink-group sink_analytics --add-server default \\
        --host localhost --port 5432 --user postgres --password secret

    {Colors.RED}Remove a server{Colors.RESET}
    $ cdc manage-sink-groups --sink-group sink_analytics --remove-server default

    {Colors.RED}Remove a sink group{Colors.RESET}
    $ cdc manage-sink-groups --remove sink_analytics

    {Colors.GREEN}Validate configuration{Colors.RESET}
    $ cdc manage-sink-groups --validate

    {Colors.CYAN}Introspect column types from database{Colors.RESET}
    $ cdc manage-sink-groups --sink-group sink_analytics --introspect-types
"""

    parser = SinkGroupArgumentParser(
        description=description,
        prog="cdc manage-sink-groups",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Create actions
    parser.add_argument(
        "--create",
        action="store_true",
        help=f"{Colors.GREEN}🏗️  Create a new sink group{Colors.RESET}",
    )
    parser.add_argument(
        "--source-group",
        metavar="NAME",
        help=(
            f"{Colors.BLUE}Source group to inherit from"
            f" (for inherited sink groups){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--add-new-sink-group",
        metavar="NAME",
        help=(
            f"{Colors.GREEN}+ Add new standalone sink group"
            f" (auto-prefixes with 'sink_'; default source_group is first"
            f" entry in source-groups.yaml if --for-source-group is omitted){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--type",
        choices=["postgres", "mssql", "http_client", "http_server"],
        default="postgres",
        help=f"{Colors.YELLOW}🗂️  Type of sink (default: postgres){Colors.RESET}",
    )
    parser.add_argument(
        "--pattern",
        choices=["db-shared", "db-per-tenant"],
        default="db-shared",
        help=f"{Colors.CYAN}🏗️  Pattern for sink group (default: db-shared){Colors.RESET}",
    )
    parser.add_argument(
        "--environment-aware",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Environment-aware grouping (default: enabled, use --no-environment-aware to disable)",
    )
    parser.add_argument(
        "--database-exclude-patterns",
        nargs="+",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Regex patterns for excluding"
            f" databases (space-separated){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--schema-exclude-patterns",
        nargs="+",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Regex patterns for excluding"
            f" schemas (space-separated){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--table-exclude-patterns",
        nargs="+",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Regex patterns for excluding"
            f" tables (space-separated){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--for-source-group",
        metavar="NAME",
        help=(
            f"{Colors.CYAN}📡 Source group this standalone sink consumes from"
            f" (recommended to set explicitly){Colors.RESET}"
        ),
    )

    # List/Info actions
    parser.add_argument(
        "--list",
        action="store_true",
        help=f"{Colors.CYAN}📋 List all sink groups{Colors.RESET}",
    )
    parser.add_argument(
        "--info",
        metavar="NAME",
        help=(
            f"{Colors.BLUE}(i) Show detailed information"
            f" about a sink group{Colors.RESET}"
        ),
    )

    # Inspection actions (standalone sink groups only)
    parser.add_argument(
        "--inspect",
        action="store_true",
        help=(
            f"{Colors.CYAN}Inspect databases on sink server"
            f" (standalone sink groups only){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--update",
        nargs="?",
        const="__AUTO__",
        metavar="SINK_GROUP",
        help=(
            f"{Colors.CYAN}Inspect sink server and update sink-group sources"
            f" (optionally pass sink group name){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--server",
        metavar="NAME",
        help=(
            f"{Colors.BLUE}🖥️  Server name to inspect"
            f" or update with --extraction-patterns (default inspect/update: 'default'){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--include-pattern",
        metavar="PATTERN",
        help=f"{Colors.GREEN}✅ Only include databases matching regex pattern{Colors.RESET}",
    )

    # Type introspection
    parser.add_argument(
        "--introspect-types",
        action="store_true",
        help=(
            f"{Colors.CYAN}🔍 Introspect column types from"
            f" database server (requires --sink-group){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--db-definitions",
        action="store_true",
        help=(
            f"{Colors.CYAN}Generate services/_schemas/_definitions/{{pgsql|mssql}}.yaml"
            f" once from sink DB metadata (requires --sink-group){Colors.RESET}"
        ),
    )

    # Validation
    parser.add_argument(
        "--validate",
        action="store_true",
        help=f"{Colors.GREEN}✅ Validate sink group configuration{Colors.RESET}",
    )
    parser.add_argument(
        "--add-to-ignore-list",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Add pattern(s) to database_exclude_patterns"
            f" (comma-separated supported){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--add-to-schema-excludes",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Add pattern(s) to schema_exclude_patterns"
            f" (comma-separated supported){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--add-to-table-excludes",
        metavar="PATTERN",
        help=(
            f"{Colors.YELLOW}Add pattern(s) to table_exclude_patterns"
            f" (comma-separated supported){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--list-table-excludes",
        action="store_true",
        help=(
            f"{Colors.YELLOW}List table_exclude_patterns"
            f" for sink group{Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--add-source-custom-key",
        metavar="KEY",
        help=(
            f"{Colors.YELLOW}Add/update source custom key resolved during --update"
            f"{Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--custom-key-value",
        metavar="SQL",
        help=(
            f"{Colors.YELLOW}SQL expression/query used to resolve custom key value"
            f"{Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--custom-key-exec-type",
        choices=["sql"],
        default="sql",
        help=(
            f"{Colors.YELLOW}Execution type for custom key (currently: sql)"
            f"{Colors.RESET}"
        ),
    )

    # Server management
    parser.add_argument(
        "--sink-group",
        metavar="NAME",
        help=(
            f"{Colors.CYAN}Sink group to operate on"
            f" (for --add-server, --remove-server, --server updates){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--add-server",
        metavar="NAME",
        help=f"{Colors.GREEN}🖥️  Add a server to a sink group (requires --sink-group){Colors.RESET}",
    )
    parser.add_argument(
        "--remove-server",
        metavar="NAME",
        help=(
            f"{Colors.RED}Remove a server from a sink group"
            f" (requires --sink-group){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--host",
        metavar="HOST",
        help=f"{Colors.BLUE}🌐 Server host{Colors.RESET}",
    )
    parser.add_argument(
        "--port",
        metavar="PORT",
        help=f"{Colors.BLUE}🔌 Server port{Colors.RESET}",
    )
    parser.add_argument(
        "--user",
        metavar="USER",
        help=f"{Colors.BLUE}👤 Server user{Colors.RESET}",
    )
    parser.add_argument(
        "--password",
        metavar="PASSWORD",
        help=f"{Colors.YELLOW}🔑 Server password{Colors.RESET}",
    )
    parser.add_argument(
        "--extraction-patterns",
        nargs="+",
        metavar="PATTERN",
        help=(
            f"{Colors.CYAN}Regex extraction patterns for server"
            f" (space-separated, use quotes). Supports --env, --strip-patterns,"
            f" --env-mapping, --description metadata.{Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--env",
        type=str,
        help=(
            f"{Colors.CYAN}Fixed environment for extraction patterns"
            f" (overrides captured (?P<env>) group){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--strip-patterns",
        type=str,
        help=(
            f"{Colors.CYAN}Comma-separated regex patterns to remove from"
            f" extracted service name (e.g., '_db,_legacy$'){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--env-mapping",
        type=str,
        action="append",
        help=(
            f"{Colors.CYAN}Environment mapping in format from:to"
            f" (can be repeated){Colors.RESET}"
        ),
    )
    parser.add_argument(
        "--description",
        type=str,
        help=f"{Colors.CYAN}Description for extraction pattern entries{Colors.RESET}",
    )
    parser.add_argument(
        "--list-server-extraction-patterns",
        nargs="?",
        const="",
        metavar="SERVER",
        help=(
            f"{Colors.CYAN}List extraction patterns for sink-group servers"
            f" (optionally filter by server){Colors.RESET}"
        ),
    )

    # Sink group management
    parser.add_argument(
        "--remove",
        metavar="NAME",
        help=f"{Colors.RED}❌ Remove a sink group{Colors.RESET}",
    )

    return parser
