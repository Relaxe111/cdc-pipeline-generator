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
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.autocompletions.services import list_existing_services
from cdc_generator.helpers.env_resolution import build_env_lookup, resolve_config_value
from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapPlan,
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
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.yaml_loader import load_yaml_file

_DEFAULT_FDW_OUTPUT_DIR = Path("generated") / "fdw"
_DEFAULT_TARGET_SINK_ENV_LABEL = "any"


@dataclass(frozen=True)
class FdwApplyTarget:
    """Resolved PostgreSQL sink target for an FDW apply operation."""

    sink_key: str
    sink_group_name: str
    sink_service_name: str
    sink_server_name: str
    host: str
    port: str
    username: str
    password: str
    database: str


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


def _resolve_sink_key(
    service_config: dict[str, object],
    sink_key_override: str | None,
) -> str:
    sinks_raw = service_config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        raise ValueError("Service config does not define any sink targets")

    sinks = cast(dict[str, object], sinks_raw)
    sink_keys = [key.strip() for key in sinks if key.strip()]
    if sink_key_override is not None:
        normalized_override = sink_key_override.strip()
        if normalized_override in sink_keys:
            return normalized_override
        available = ", ".join(sorted(sink_keys))
        raise ValueError(f"Sink '{normalized_override}' is not configured for this service. Available sinks: {available}")

    if len(sink_keys) == 1:
        return sink_keys[0]

    available = ", ".join(sorted(sink_keys))
    raise ValueError("Service config defines multiple sink targets; pass --sink <sink-group>.<sink-service>. " + f"Available sinks: {available}")


def _resolve_service_name(service_name: str | None) -> str:
    if service_name is not None and service_name.strip():
        return service_name.strip()

    existing_services = list_existing_services()
    if len(existing_services) == 1:
        return existing_services[0]
    if not existing_services:
        raise ValueError("No service configs were found under services/. Pass --service <name>.")

    available = ", ".join(existing_services)
    raise ValueError("Multiple service configs are available; pass --service <name>. " + f"Available services: {available}")


def _parse_sink_key(sink_key: str) -> tuple[str, str]:
    if "." not in sink_key:
        raise ValueError("Sink keys must use the form <sink-group>.<sink-service>")

    sink_group_name, sink_service_name = sink_key.split(".", 1)
    normalized_group = sink_group_name.strip()
    normalized_service = sink_service_name.strip()
    if not normalized_group or not normalized_service:
        raise ValueError("Sink keys must use the form <sink-group>.<sink-service>")
    return normalized_group, normalized_service


def _resolve_apply_target(
    service_name: str,
    *,
    target_sink_env: str | None,
    sink_key_override: str | None,
    resolve_env_values: bool,
) -> FdwApplyTarget:
    if target_sink_env is None or not target_sink_env.strip():
        raise ValueError("fdw apply requires --target-sink-env to resolve the target PostgreSQL database")

    project_root = get_project_root()
    service_config = load_service_config(service_name)
    sink_key = _resolve_sink_key(service_config, sink_key_override)
    sink_group_name, sink_service_name = _parse_sink_key(sink_key)

    sink_groups_path = project_root / "sink-groups.yaml"
    if not sink_groups_path.exists():
        raise FileNotFoundError(f"sink-groups.yaml not found at {sink_groups_path}")

    sink_groups = load_yaml_file(sink_groups_path)
    sink_group_raw = sink_groups.get(sink_group_name)
    if not isinstance(sink_group_raw, dict):
        raise ValueError(f"Sink group '{sink_group_name}' not found in sink-groups.yaml")

    sink_group = cast(dict[str, Any], sink_group_raw)
    sources_raw = sink_group.get("sources")
    sources = cast(dict[str, Any], sources_raw) if isinstance(sources_raw, dict) else {}
    sink_service_raw = sources.get(sink_service_name)
    if not isinstance(sink_service_raw, dict):
        raise ValueError(f"Sink service '{sink_service_name}' not found under sink group '{sink_group_name}'")

    sink_service = cast(dict[str, Any], sink_service_raw)
    env_cfg_raw = sink_service.get(target_sink_env)
    if not isinstance(env_cfg_raw, dict):
        raise ValueError("Target sink env '" + target_sink_env + "' is not configured for sink '" + sink_key + "'")

    env_cfg = cast(dict[str, Any], env_cfg_raw)
    database_raw = env_cfg.get("database")
    database = str(database_raw).strip() if database_raw is not None else ""
    if not database:
        raise ValueError(f"database is missing for sink '{sink_key}' env '{target_sink_env}'")

    sink_server_name_raw = env_cfg.get("server", "default")
    sink_server_name = str(sink_server_name_raw).strip() if sink_server_name_raw is not None else "default"
    servers_raw = sink_group.get("servers")
    servers = cast(dict[str, Any], servers_raw) if isinstance(servers_raw, dict) else {}
    server_cfg_raw = servers.get(sink_server_name)
    if not isinstance(server_cfg_raw, dict):
        raise ValueError(f"Server '{sink_server_name}' is not defined in sink group '{sink_group_name}'")

    server_cfg = cast(dict[str, Any], server_cfg_raw)
    env_lookup = build_env_lookup(project_root)
    host = resolve_config_value(server_cfg.get("host"), env_lookup, resolve_env_values, "host")
    port = resolve_config_value(server_cfg.get("port"), env_lookup, resolve_env_values, "port")
    username = resolve_config_value(server_cfg.get("username", server_cfg.get("user")), env_lookup, resolve_env_values, "username")
    password = resolve_config_value(server_cfg.get("password"), env_lookup, resolve_env_values, "password")

    return FdwApplyTarget(
        sink_key=sink_key,
        sink_group_name=sink_group_name,
        sink_service_name=sink_service_name,
        sink_server_name=sink_server_name,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )


def _resolve_apply_sql_path(plan: FdwBootstrapPlan, sql_path_override: str | None) -> Path:
    if sql_path_override is not None and sql_path_override.strip():
        return Path(sql_path_override.strip())
    return _build_default_output_path(plan, metadata_only=False)


def _resolve_psql_bin(psql_bin_override: str | None) -> str:
    if psql_bin_override is not None and psql_bin_override.strip():
        return psql_bin_override.strip()

    resolved_path = shutil.which("psql")
    if resolved_path is None:
        raise ValueError("psql executable not found in PATH. Install libpq or pass --psql-bin /absolute/path/to/psql")
    return resolved_path


def _build_psql_apply_command(
    *,
    psql_bin: str,
    target: FdwApplyTarget,
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
    resolved_service_name = _resolve_service_name(getattr(args, "service", None))
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
        target = _resolve_apply_target(
            _resolve_service_name(getattr(args, "service", None)),
            target_sink_env=args.target_sink_env,
            sink_key_override=args.sink,
            resolve_env_values=True,
        )
        sql_path = _resolve_apply_sql_path(plan, args.sql_path)
        psql_bin = _resolve_psql_bin(args.psql_bin)
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
