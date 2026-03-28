#!/usr/bin/env python3
"""CLI entry point for metadata-driven MSSQL FDW bootstrap.

Usage:
    cdc fdw plan --service adopus
    cdc fdw sql --service adopus --output fdw-bootstrap.sql
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapRequest,
    build_fdw_bootstrap_plan,
    render_fdw_bootstrap_sql,
    render_fdw_plan_summary,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc fdw",
        description=(
            "Plan and generate metadata-driven tds_fdw bootstrap SQL for "
            + "db-per-tenant MSSQL sources"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  cdc fdw plan --service adopus\n"
            "  cdc fdw plan --service adopus --source-env prod --customer Test --customer FretexDev\n"
            "  cdc fdw sql --service adopus --table Actor --table Soknad\n"
            "  cdc fdw sql --service adopus --metadata-only --output migrations/fdw-bootstrap.sql\n"
        ),
    )

    subparsers = parser.add_subparsers(dest="subcommand")

    plan_parser = subparsers.add_parser(
        "plan",
        help="Preview derived FDW source and table registrations",
    )
    _add_common_arguments(plan_parser)

    sql_parser = subparsers.add_parser(
        "sql",
        help="Render idempotent SQL for metadata and FDW objects",
    )
    _add_common_arguments(sql_parser)
    sql_parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Render only cdc_management metadata registration SQL",
    )
    sql_parser.add_argument(
        "--output",
        default=None,
        help="Write SQL to this file instead of stdout",
    )

    return parser


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--service",
        required=True,
        help="Service name from services/<service>.yaml",
    )
    parser.add_argument(
        "--source-env",
        default="default",
        help="Source environment key from source-groups.yaml (default: default)",
    )
    parser.add_argument(
        "--customer",
        dest="customers",
        action="append",
        default=None,
        help="Limit to one source/customer name; repeat to include multiple",
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        default=None,
        help="Limit to one tracked source table; repeat to include multiple",
    )
    parser.add_argument(
        "--target-schema",
        default=None,
        help="Override target schema name stored in source_table_registration",
    )
    parser.add_argument(
        "--runner-role",
        default="cdc_runner",
        help="PostgreSQL role name for CREATE USER MAPPING (default: cdc_runner)",
    )
    parser.add_argument(
        "--fdw-server-prefix",
        default="mssql",
        help="Prefix for generated FDW server names (default: mssql)",
    )
    parser.add_argument(
        "--fdw-schema-prefix",
        default="fdw",
        help="Prefix for generated FDW schema names (default: fdw)",
    )
    parser.add_argument(
        "--keep-placeholders",
        action="store_true",
        help=(
            "Do not resolve ${VAR} placeholders from .env or process env. "
            + "Useful when generating templated SQL instead of immediately applying it."
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.subcommand:
        parser.print_help()
        return 1

    try:
        plan = build_fdw_bootstrap_plan(
            service_name=args.service,
            source_env=args.source_env,
            request=FdwBootstrapRequest(
                customers=tuple(args.customers or []),
                tables=tuple(args.tables or []),
                target_schema_name=args.target_schema,
                runner_role=args.runner_role,
                fdw_server_prefix=args.fdw_server_prefix,
                fdw_schema_prefix=args.fdw_schema_prefix,
                resolve_env_values=not args.keep_placeholders,
            ),
        )
    except (FileNotFoundError, ValueError) as exc:
        print_error(str(exc))
        return 1

    if args.subcommand == "plan":
        for line in render_fdw_plan_summary(plan):
            print_info(line)

        if plan.warnings:
            print_warning("")
            print_warning("Warnings:")
            for warning in plan.warnings:
                print_warning(f"  {warning}")

        print_success(
            f"Planned {len(plan.source_plans)} source instance(s) and "
            + f"{len(plan.table_plans)} tracked table(s)"
        )
        return 0

    if args.subcommand == "sql":
        sql_text = render_fdw_bootstrap_sql(
            plan,
            metadata_only=bool(args.metadata_only),
        )
        output_path_raw = getattr(args, "output", None)
        if output_path_raw:
            output_path = Path(str(output_path_raw))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(sql_text, encoding="utf-8")
            print_success(f"Wrote FDW bootstrap SQL to {output_path}")
        else:
            sys.stdout.write(sql_text)
        return 0

    print_error(f"Unknown fdw subcommand: {args.subcommand}")
    return 1


if __name__ == "__main__":
    sys.exit(main())