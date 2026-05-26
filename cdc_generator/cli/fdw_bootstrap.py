#!/usr/bin/env python3
"""CLI entry point for native CDC table bootstrapping.

Usage:
    cdc fdw bootstrap status --target-sink-env dev
    cdc fdw bootstrap run --target-sink-env dev --source AdOpusTest
    cdc fdw bootstrap retry --target-sink-env dev --source AdOpusTest
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapPlan,
    FdwBootstrapRequest,
    FdwSourcePlan,
    build_fdw_bootstrap_plan,
)
from cdc_generator.helpers.fdw_bootstrap_state import (
    BootstrapStateRow,
    build_source_database_map,
    build_state_query,
    classify_result_rows,
    count_failed_results,
    render_result_summary,
    render_result_table,
    render_status_table,
    result_rows_to_json,
    parse_state_rows,
    status_rows_to_json,
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
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import load_yaml_file


def _list_source_databases(project_root: Path, server_group_name: str) -> list[str]:
    """List source identifiers from source-groups.yaml sources section.

    Returns source keys (the top-level keys under ``sources:``).
    These can be used as ``--source`` values.
    """
    source_groups_path = project_root / "source-groups.yaml"
    if not source_groups_path.exists():
        return []
    source_groups = load_yaml_file(source_groups_path)
    if not isinstance(source_groups, dict):
        return []
    group = source_groups.get(server_group_name)
    if not isinstance(group, dict):
        return []
    sources = group.get("sources")
    if not isinstance(sources, dict):
        return []
    return sorted(str(k).strip() for k in sources if str(k).strip())


def _resolve_source_instance_keys(
    source_plans: list[FdwSourcePlan],
    source_databases: list[str],
) -> list[str]:
    """Map source database names or source keys to source_instance_key values.

    Matches against both ``customer_name`` (source key) and ``source_database``
    (actual MSSQL database name).  source_instance_key = {source_env}_{customer_id}
    """
    if not source_databases:
        return []

    db_set = {db.casefold() for db in source_databases}
    keys: list[str] = []
    for plan in source_plans:
        if plan.customer_name.casefold() in db_set or plan.source_database.casefold() in db_set:
            keys.append(f"{plan.source_env}_{plan.customer_key}")
    return keys


def _build_psql_query_command(
    *,
    psql_bin: str,
    target: FdwSinkTarget,
    query: str,
) -> list[str]:
    """Build a psql command that runs a single query."""
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
        "-X",
        "-A",
        "-t",
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "pager=off",
        "-P",
        "footer=off",
        "-c",
        query,
    ]


def _run_psql_query(
    psql_bin: str,
    target: FdwSinkTarget,
    query: str,
) -> subprocess.CompletedProcess[str]:
    """Execute a single psql query and return the result."""
    command = _build_psql_query_command(
        psql_bin=psql_bin,
        target=target,
        query=query,
    )
    run_env = dict(os.environ)
    run_env["PGPASSWORD"] = target.password
    return subprocess.run(command, check=False, env=run_env, capture_output=True, text=True)


def _quote_sql_literal(value: str) -> str:
    """Quote a SQL string literal for ad-hoc CLI queries."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _available_source_labels(plan: FdwBootstrapPlan) -> list[str]:
    """Return sorted user-facing source database labels from a plan."""
    return sorted({source_plan.source_database for source_plan in plan.source_plans if source_plan.source_database})


def _resolve_optional_source_keys(
    plan: FdwBootstrapPlan,
    sources: list[str] | None,
) -> list[str]:
    """Resolve optional ``--source`` filters for non-mutating commands."""
    if not sources:
        return []

    source_keys = _resolve_source_instance_keys(plan.source_plans, sources)
    if source_keys:
        return source_keys

    available = _available_source_labels(plan)
    print_error("No matching source databases found.")
    if available:
        print_info(f"Available sources: {', '.join(available)}")
    raise SystemExit(1)


def _query_bootstrap_state_rows(
    psql_bin: str,
    target: FdwSinkTarget,
    plan: FdwBootstrapPlan,
    *,
    status_filter: str | None,
    source_keys: list[str],
    tables: list[str] | None,
) -> list[BootstrapStateRow]:
    """Query bootstrap state rows and parse them into typed records."""
    query = build_state_query(status_filter, source_keys, tables)
    result = _run_psql_query(psql_bin, target, query)
    if result.returncode != 0:
        message = result.stderr.strip() or "psql query failed"
        raise RuntimeError(message)
    return parse_state_rows(result.stdout, build_source_database_map(plan))


def _group_rows_by_source(
    rows: list[BootstrapStateRow],
) -> dict[str, list[BootstrapStateRow]]:
    """Group typed state rows by ``source_instance_key``."""
    grouped: dict[str, list[BootstrapStateRow]] = {}
    for row in rows:
        grouped.setdefault(row.source_instance_key, []).append(row)
    return grouped


def _target_table_names(rows: list[BootstrapStateRow]) -> list[str]:
    """Return stable logical table names from a set of state rows."""
    return sorted({row.logical_table_name for row in rows if row.logical_table_name})


def _print_dry_run_queries(
    target: FdwSinkTarget,
    plan: FdwBootstrapPlan,
    source_keys: list[str],
    tables: list[str] | None,
    *,
    failed_only: bool,
    enable_after: bool,
    output_json: bool,
) -> int:
    """Print the bootstrap queries that would run for the selected sources."""
    source_database_map = build_source_database_map(plan)
    queries: list[dict[str, object]] = []
    for source_key in source_keys:
        query = _build_bootstrap_query(
            [source_key],
            tables,
            enable_after,
        )
        queries.append(
            {
                "source_instance_key": source_key,
                "source_database": source_database_map.get(source_key, source_key),
                "tables": tables or [],
                "query": query,
            }
        )

    selection_query = ""
    if failed_only:
        selection_query = build_state_query("failed", source_keys, tables)

    if output_json:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "target": {
                        "database": target.database,
                        "host": target.host,
                        "port": target.port,
                    },
                    "selection_query": selection_query,
                    "queries": queries,
                },
                indent=2,
            )
        )
        return 0

    print_info(f"Target: {target.database}@{target.host}:{target.port}")
    if selection_query:
        print_info("Would select failed tables with:")
        print_info(f"  {selection_query}")
    print_info("SQL to execute:")
    for query_info in queries:
        print_info(f"  [{query_info['source_database']}] {query_info['query']}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc fdw bootstrap",
        description="Execute native CDC table bootstrapping against a PostgreSQL sink",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  cdc fdw bootstrap status --target-sink-env dev\n"
            "  cdc fdw bootstrap run --target-sink-env dev --source AdOpusTest\n"
            "  cdc fdw bootstrap run --target-sink-env dev --all-sources\n"
            "  cdc fdw bootstrap retry --target-sink-env dev --source AdOpusFretexDev\n"
        ),
    )

    subparsers = parser.add_subparsers(dest="subcommand")

    status_parser = subparsers.add_parser(
        "status",
        help="Show bootstrap state for tracked tables",
    )
    _add_common_bootstrap_arguments(status_parser)
    status_parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        default=None,
        help="Source database name from source-groups.yaml; repeat to include multiple",
    )
    status_parser.add_argument(
        "--pending",
        action="store_true",
        help="Show only pending tables",
    )
    status_parser.add_argument(
        "--failed",
        action="store_true",
        help="Show only failed tables",
    )
    status_parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Print machine-readable JSON output",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Execute bootstrap for pending tables",
    )
    _add_common_bootstrap_arguments(run_parser)
    run_parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        default=None,
        help="Source database name from source-groups.yaml; repeat to include multiple",
    )
    run_parser.add_argument(
        "--all-sources",
        action="store_true",
        help="Bootstrap all registered source databases",
    )
    run_parser.add_argument(
        "--failed",
        action="store_true",
        help="Retry only failed tables for the selected sources",
    )
    run_parser.add_argument(
        "--no-enable-after",
        action="store_true",
        help="Leave tables disabled after bootstrap (default: enable after)",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would run without executing",
    )
    run_parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Print machine-readable JSON output",
    )

    retry_parser = subparsers.add_parser(
        "retry",
        help="Retry bootstrap for previously failed tables",
    )
    _add_common_bootstrap_arguments(retry_parser)
    retry_parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        default=None,
        help="Source database name from source-groups.yaml; repeat to include multiple",
    )
    retry_parser.add_argument(
        "--all-sources",
        action="store_true",
        help="Retry all registered source databases",
    )
    retry_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would run without executing",
    )
    retry_parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Print machine-readable JSON output",
    )

    return parser


def _add_common_bootstrap_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--service",
        required=False,
        help="Service name from services/<service>.yaml; inferred when exactly one service exists",
    )
    parser.add_argument(
        "--target-sink-env",
        required=True,
        help="Target sink environment (dev, stage, prod)",
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        default=None,
        help="Limit to one tracked source table; repeat to include multiple",
    )
    parser.add_argument(
        "--psql-bin",
        default=None,
        help="Override the psql executable path",
    )


def _run_status_subcommand(args: argparse.Namespace) -> int:
    try:
        service_name = resolve_service_name(getattr(args, "service", None))
        target = resolve_sink_target(
            service_name,
            target_sink_env=args.target_sink_env,
            sink_key_override=None,
            resolve_env_values=True,
        )
        psql_bin = resolve_psql_bin(args.psql_bin)
        plan = build_fdw_bootstrap_plan(
            service_name,
            source_env=None,
            request=FdwBootstrapRequest(target_sink_env=args.target_sink_env),
        )
    except (FileNotFoundError, ValueError) as exc:
        print_error(str(exc))
        return 1

    if args.pending and args.failed:
        print_error("Use only one of --pending or --failed.")
        return 1

    status_filter = None
    if args.pending:
        status_filter = "pending"
    elif args.failed:
        status_filter = "failed"

    try:
        source_keys = _resolve_optional_source_keys(plan, list(args.sources) if args.sources else None)
        rows = _query_bootstrap_state_rows(
            psql_bin,
            target,
            plan,
            status_filter=status_filter,
            source_keys=source_keys,
            tables=list(args.tables) if args.tables else None,
        )
    except SystemExit:
        return 1
    except RuntimeError as exc:
        print_error(str(exc))
        return 1

    if args.output_json:
        print(status_rows_to_json(rows))
        return 0

    if not rows:
        print_info("No bootstrap state rows found.")
        return 0

    print(render_status_table(rows))
    return 0


def _resolve_sources_for_bootstrap(args: argparse.Namespace) -> tuple[str, FdwSinkTarget, str, list[str], FdwBootstrapPlan]:
    """Resolve common bootstrap prerequisites.

    Returns: (service_name, target, psql_bin, source_instance_keys, plan)
    """
    service_name = resolve_service_name(getattr(args, "service", None))
    target = resolve_sink_target(
        service_name,
        target_sink_env=args.target_sink_env,
        sink_key_override=None,
        resolve_env_values=True,
    )
    psql_bin = resolve_psql_bin(args.psql_bin)

    plan = build_fdw_bootstrap_plan(
        service_name,
        source_env=None,
        request=FdwBootstrapRequest(target_sink_env=args.target_sink_env),
    )

    if args.all_sources and args.sources:
        print_error("Use either --source or --all-sources, not both.")
        raise SystemExit(1)

    if args.all_sources:
        source_keys = [f"{source_plan.source_env}_{source_plan.customer_key}" for source_plan in plan.source_plans]
        return service_name, target, psql_bin, source_keys, plan

    if not args.sources:
        available = _available_source_labels(plan)
        print_error("Use --source or --all-sources for bootstrap execution.")
        if available:
            print_info(f"Available sources: {', '.join(available)}")
        raise SystemExit(1)

    source_keys = _resolve_source_instance_keys(plan.source_plans, list(args.sources))
    if source_keys:
        return service_name, target, psql_bin, source_keys, plan

    available = _available_source_labels(plan)
    print_error("No matching source databases found.")
    if available:
        print_info(f"Available sources: {', '.join(available)}")
    raise SystemExit(1)


def _select_pre_run_rows(
    psql_bin: str,
    target: FdwSinkTarget,
    plan: FdwBootstrapPlan,
    *,
    source_keys: list[str],
    tables: list[str] | None,
    failed_only: bool,
) -> list[BootstrapStateRow]:
    """Select the rows targeted by a run or retry operation."""
    status_filter = "failed" if failed_only else (None if tables else "pending")
    return _query_bootstrap_state_rows(
        psql_bin,
        target,
        plan,
        status_filter=status_filter,
        source_keys=source_keys,
        tables=tables,
    )


def _run_bootstrap_operation(
    args: argparse.Namespace,
    *,
    failed_only: bool,
    enable_after: bool,
) -> int:
    """Execute the selected bootstrap operation and render structured output."""
    try:
        _service_name, target, psql_bin, source_keys, plan = _resolve_sources_for_bootstrap(args)
        tables = list(args.tables) if args.tables else None
        if args.dry_run:
            return _print_dry_run_queries(
                target,
                plan,
                source_keys,
                tables,
                failed_only=failed_only,
                enable_after=enable_after,
                output_json=args.output_json,
            )
        selected_rows = _select_pre_run_rows(
            psql_bin,
            target,
            plan,
            source_keys=source_keys,
            tables=tables,
            failed_only=failed_only,
        )
    except SystemExit:
        return 1
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print_error(str(exc))
        return 1

    if not selected_rows:
        if args.output_json:
            print("[]")
            return 0
        if failed_only:
            print_info("No failed tables found.")
        elif args.tables:
            print_info("No matching tables found.")
        else:
            print_info("No pending tables found.")
        return 0

    grouped_rows = _group_rows_by_source(selected_rows)

    exit_code = 0
    for source_key, rows_for_source in grouped_rows.items():
        query = _build_bootstrap_query(
            [source_key],
            _target_table_names(rows_for_source),
            enable_after,
        )
        result = _run_psql_query(psql_bin, target, query)
        if result.returncode != 0:
            message = result.stderr.strip() or "Bootstrap query failed"
            print_error(f"Bootstrap failed for {rows_for_source[0].source_database}: {message}")
            exit_code = result.returncode or 1
        elif result.stdout.strip():
            print_info(result.stdout.rstrip())

    try:
        post_rows = _query_bootstrap_state_rows(
            psql_bin,
            target,
            plan,
            status_filter=None,
            source_keys=list(grouped_rows.keys()),
            tables=_target_table_names(selected_rows),
        )
    except RuntimeError as exc:
        print_error(str(exc))
        return exit_code or 1

    result_rows = classify_result_rows(
        selected_rows,
        post_rows,
        enable_after=enable_after,
    )
    if args.output_json:
        print(result_rows_to_json(result_rows))
    else:
        if result_rows:
            print(render_result_table(result_rows))
            print()
            for summary_line in render_result_summary(
                result_rows,
                enable_after=enable_after,
                target_sink_env=args.target_sink_env,
            ):
                print(summary_line)

    all_unchanged = all(
        row.result not in ("bootstrapped", "failed", "skipped")
        for row in result_rows
    )
    if all_unchanged and result_rows:
        print_warning(
            "No tables changed state. "
            + "Source instances must be disabled before bootstrap can load data. "
            + "Check the function output above for skip reasons "
            + "(e.g. 'skipped_active' means the registration is still enabled)."
        )

    if count_failed_results(result_rows):
        exit_code = 1

    return exit_code


def _build_bootstrap_query(
    source_keys: list[str],
    tables: list[str] | None,
    enable_after: bool,
) -> str:
    """Build the bootstrap_native_cdc_tables() SELECT query."""
    source_arg = "NULL"
    if len(source_keys) == 1:
        source_arg = _quote_sql_literal(source_keys[0])

    table_arg = "NULL"
    if tables:
        quoted = ", ".join(_quote_sql_literal(table_name) for table_name in tables)
        table_arg = f"ARRAY[{quoted}]"

    enable_str = "true" if enable_after else "false"

    return f"SELECT * FROM cdc_management.bootstrap_native_cdc_tables({source_arg}, {table_arg}, {enable_str})"


def _run_bootstrap_subcommand(args: argparse.Namespace) -> int:
    return _run_bootstrap_operation(
        args,
        failed_only=bool(getattr(args, "failed", False)),
        enable_after=not args.no_enable_after,
    )


def _run_retry_subcommand(args: argparse.Namespace) -> int:
    """Retry bootstrap for failed tables."""
    return _run_bootstrap_operation(
        args,
        failed_only=True,
        enable_after=True,
    )


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()

    # If no subcommand given, prepend "status" as the default
    args_list = list(argv)
    if args_list and not any(args_list[0] == sub for sub in ("status", "run", "retry", "-h", "--help")):
        if not any(a in ("status", "run", "retry") for a in args_list):
            args_list = ["status"] + args_list

    args = parser.parse_args(args_list)

    if args.subcommand == "status":
        return _run_status_subcommand(args)

    if args.subcommand == "run":
        return _run_bootstrap_subcommand(args)

    if args.subcommand == "retry":
        return _run_retry_subcommand(args)

    print_error(f"Unknown bootstrap subcommand: {args.subcommand}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
