#!/usr/bin/env python3
"""CLI entry point for metadata-driven MSSQL FDW bootstrap.

Usage:
    cdc fdw plan --service adopus
    cdc fdw sql --service adopus --target-sink-env dev
    cdc fdw apply --service adopus --target-sink-env dev
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapPlan,
    FdwBootstrapRequest,
    build_fdw_bootstrap_plan,
    render_fdw_bootstrap_sql,
    render_fdw_plan_summary,
)
from cdc_generator.helpers.fdw_sink_target import (
    FdwSinkTarget,
    resolve_psql_bin,
    resolve_service_name,
    resolve_sink_target,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)

_DEFAULT_FDW_OUTPUT_DIR = Path("generated") / "fdw"
_DEFAULT_TARGET_SINK_ENV_LABEL = "any"

# Backward-compatible alias for external consumers
FdwApplyTarget = FdwSinkTarget


def _slugify_filename_component(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").lower()
    return slug or "unnamed"


def _build_source_label(plan: FdwBootstrapPlan) -> str:
    if plan.resolved_server_names:
        return "_".join(_slugify_filename_component(name) for name in plan.resolved_server_names)
    if plan.resolved_source_envs:
        return "_".join(_slugify_filename_component(name) for name in plan.resolved_source_envs)
    return "unknown"


def _build_default_output_path(
    plan: FdwBootstrapPlan,
    *,
    metadata_only: bool,
) -> Path:
    service_name = _slugify_filename_component(plan.service_name)
    source_label = _build_source_label(plan)
    target_sink_env = _slugify_filename_component(plan.target_sink_env or _DEFAULT_TARGET_SINK_ENV_LABEL)
    suffix = "metadata.sql" if metadata_only else "fdw.sql"
    file_name = f"{service_name}-{source_label}-{target_sink_env}-{suffix}"
    return _DEFAULT_FDW_OUTPUT_DIR / file_name


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc fdw",
        description=("Plan, generate, and apply metadata-driven tds_fdw bootstrap SQL for " + "db-per-tenant MSSQL sources"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  cdc fdw plan --service adopus\n"
            "  cdc fdw plan --service adopus --target-sink-env dev\n"
            "  cdc fdw plan --service adopus --source-env prod --target-sink-env prod\n"
            "  cdc fdw sql --service adopus --target-sink-env dev --table Actor\n"
            "  cdc fdw sql --service adopus --metadata-only --target-sink-env stage\n"
            "  cdc fdw apply --service adopus --target-sink-env dev\n"
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

    apply_parser = subparsers.add_parser(
        "apply",
        help="Apply a generated FDW SQL file to the target PostgreSQL sink environment",
    )
    _add_common_arguments(apply_parser)
    apply_parser.add_argument(
        "--sink",
        default=None,
        help="Sink target key in the form <sink-group>.<sink-service>; inferred when the service has exactly one sink",
    )
    apply_parser.add_argument(
        "--sql-path",
        default=None,
        help="Override the SQL file to apply; defaults to the generated FDW SQL path for the resolved plan",
    )
    apply_parser.add_argument(
        "--psql-bin",
        default=None,
        help="Override the psql executable path; defaults to the first psql found in PATH",
    )
    apply_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved target and psql command without applying the SQL",
    )

    return parser


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--service",
        required=False,
        help="Service name from services/<service>.yaml; inferred when exactly one service exists",
    )
    parser.add_argument(
        "--source-env",
        default=None,
        help=(
            "Optional source environment key from source-groups.yaml. "
            + "When omitted, matching source routes are inferred from the selected customers and target sink env."
        ),
    )
    parser.add_argument(
        "--target-sink-env",
        default=None,
        help=("Only include source routes whose target_sink_env matches this sink env " + "(for example: dev, stage, prod)"),
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
        dest="runner_roles",
        action="append",
        default=None,
        help="PostgreSQL role name for CREATE USER MAPPING; repeat to include multiple (default: cdc_runner)",
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


def _resolve_apply_sql_path(plan: FdwBootstrapPlan, sql_path_override: str | None) -> Path:
    if sql_path_override is not None and sql_path_override.strip():
        return Path(sql_path_override.strip())
    return _build_default_output_path(plan, metadata_only=False)


def _build_psql_apply_command(
    *,
    psql_bin: str,
    target: FdwSinkTarget,
    sql_path: Path,
) -> list[str]:
    return [
        psql_bin,
        "-h",
        target.host,
        "-p",
        target.port,
        "-U",
        target.username,
        "-d",
        target.database,
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        str(sql_path),
    ]


def _build_plan_from_args(args: argparse.Namespace) -> FdwBootstrapPlan:
    resolved_service_name = resolve_service_name(getattr(args, "service", None))
    return build_fdw_bootstrap_plan(
        service_name=resolved_service_name,
        source_env=args.source_env,
        request=FdwBootstrapRequest(
            customers=tuple(args.customers or []),
            tables=tuple(args.tables or []),
            target_sink_env=args.target_sink_env,
            target_schema_name=args.target_schema,
            runner_roles=tuple(args.runner_roles or []),
            fdw_server_prefix=args.fdw_server_prefix,
            fdw_schema_prefix=args.fdw_schema_prefix,
            resolve_env_values=not args.keep_placeholders,
        ),
    )


def _run_plan_subcommand(plan: FdwBootstrapPlan) -> int:
    for line in render_fdw_plan_summary(plan):
        print_info(line)

    if plan.warnings:
        print_warning("")
        print_warning("Warnings:")
        for warning in plan.warnings:
            print_warning(f"  {warning}")

    print_success(f"Planned {len(plan.source_plans)} source instance(s) and " + f"{len(plan.table_plans)} tracked table(s)")
    return 0


def _run_sql_subcommand(
    plan: FdwBootstrapPlan,
    *,
    metadata_only: bool,
) -> int:
    output_path = _write_fdw_sql_output(plan, metadata_only=metadata_only)
    print_success(f"Wrote FDW bootstrap SQL to {output_path}")
    return 0


def _write_fdw_sql_output(
    plan: FdwBootstrapPlan,
    *,
    metadata_only: bool,
    output_path: Path | None = None,
) -> Path:
    resolved_output_path = output_path or _build_default_output_path(
        plan,
        metadata_only=metadata_only,
    )
    sql_text = render_fdw_bootstrap_sql(
        plan,
        metadata_only=metadata_only,
    )
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(sql_text, encoding="utf-8")
    return resolved_output_path


def _run_apply_subcommand(
    args: argparse.Namespace,
    plan: FdwBootstrapPlan,
) -> int:
    if args.keep_placeholders:
        print_error("fdw apply does not support --keep-placeholders because it needs resolved connection values")
        return 1

    try:
        target = resolve_sink_target(
            resolve_service_name(getattr(args, "service", None)),
            target_sink_env=args.target_sink_env,
            sink_key_override=args.sink,
            resolve_env_values=True,
        )
        sql_path = _resolve_apply_sql_path(plan, args.sql_path)
        psql_bin = resolve_psql_bin(args.psql_bin)
    except (FileNotFoundError, ValueError) as exc:
        print_error(str(exc))
        return 1

    sql_path_override = args.sql_path.strip() if isinstance(args.sql_path, str) else ""
    if not sql_path_override:
        sql_path = _write_fdw_sql_output(plan, metadata_only=False, output_path=sql_path)
    elif not sql_path.exists():
        print_error(f"FDW SQL file not found: {sql_path}")
        print_info("Run 'cdc fdw sql ...' first or pass --sql-path to an existing SQL file")
        return 1

    command = _build_psql_apply_command(
        psql_bin=psql_bin,
        target=target,
        sql_path=sql_path,
    )
    print_info("Resolved target sink: " + f"{target.sink_key} ({args.target_sink_env} -> {target.database}@{target.host}:{target.port})")

    if bool(args.dry_run):
        print_info("psql command: " + shlex.join(command))
        return 0

    run_env = dict(os.environ)
    run_env["PGPASSWORD"] = target.password
    result = subprocess.run(command, check=False, env=run_env)
    if result.returncode != 0:
        print_error(f"psql exited with status {result.returncode}")
        return result.returncode or 1

    print_success(f"Applied FDW SQL from {sql_path} to {target.database}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.subcommand:
        parser.print_help()
        return 1

    try:
        plan = _build_plan_from_args(args)
    except (FileNotFoundError, ValueError) as exc:
        print_error(str(exc))
        return 1

    if args.subcommand == "plan":
        return _run_plan_subcommand(plan)

    if args.subcommand == "sql":
        return _run_sql_subcommand(plan, metadata_only=bool(args.metadata_only))

    if args.subcommand == "apply":
        return _run_apply_subcommand(args, plan)

    print_error(f"Unknown fdw subcommand: {args.subcommand}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
