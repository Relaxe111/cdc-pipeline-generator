"""Shell completion callbacks for ``cdc fdw bootstrap``.

--source completion reads from source planning data (local, fast).
--table completion queries native_cdc_bootstrap_state via psql (DB-backed, cached).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from click.shell_completion import CompletionItem

from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapRequest,
    build_fdw_bootstrap_plan,
)
from cdc_generator.helpers.fdw_sink_target import (
    resolve_psql_bin,
    resolve_service_name,
    resolve_sink_target,
)

_CACHE_TTL_SECONDS = 60
_CACHE_DIR_NAME = "cdc-bootstrap-completions"


def complete_bootstrap_sources(
    ctx: object,
    param: object,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete ``--source`` with source database names."""
    try:
        del param

        service_name = _get_service_from_ctx(ctx)
        if not service_name:
            return []

        target_sink_env = _get_target_sink_env_from_ctx(ctx)
        plan = build_fdw_bootstrap_plan(
            service_name,
            source_env=None,
            request=FdwBootstrapRequest(target_sink_env=target_sink_env or None),
        )
        source_databases = sorted({source_plan.source_database for source_plan in plan.source_plans if source_plan.source_database})
        normalized = incomplete.casefold()
        return [CompletionItem(source_database) for source_database in source_databases if source_database.casefold().startswith(normalized)]
    except Exception:
        return []


def complete_bootstrap_tables(
    ctx: object,
    param: object,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete ``--table`` from ``native_cdc_bootstrap_state``."""
    try:
        del param

        service_name = _get_service_from_ctx(ctx)
        target_sink_env = _get_target_sink_env_from_ctx(ctx)
        if not service_name or not target_sink_env:
            return []

        plan = build_fdw_bootstrap_plan(
            service_name,
            source_env=None,
            request=FdwBootstrapRequest(target_sink_env=target_sink_env),
        )
        source_values = _get_sources_from_ctx(ctx)
        source_keys = _resolve_source_keys_for_completion(plan, source_values)
        include_completed = _get_command_name(ctx) == "status"

        cache_path = _get_cache_file_path(
            service_name,
            target_sink_env,
            source_keys,
            include_completed,
        )
        values = _load_cached_values(cache_path)
        if values is None:
            target = resolve_sink_target(
                service_name,
                target_sink_env=target_sink_env,
                sink_key_override=None,
                resolve_env_values=True,
            )
            psql_bin = resolve_psql_bin(None)
            query = _build_table_completion_query(
                source_keys,
                include_completed=include_completed,
            )
            values = _query_bootstrap_table_names(
                psql_bin,
                host=target.host,
                port=target.port,
                username=target.username,
                password=target.password,
                database=target.database,
                query=query,
            )
            _store_cached_values(cache_path, values)

        normalized = incomplete.casefold()
        return [CompletionItem(value) for value in values if value.casefold().startswith(normalized)]
    except Exception:
        return []


def _get_command_name(ctx: object) -> str:
    """Return the current subcommand name, if available."""
    command = getattr(ctx, "command", None)
    command_name = getattr(command, "name", "")
    return str(command_name)


def _get_target_sink_env_from_ctx(ctx: object) -> str:
    """Extract ``--target-sink-env`` from the Click context chain."""
    value = _get_param_value(ctx, "target_sink_env")
    return str(value).strip() if value else ""


def _get_sources_from_ctx(ctx: object) -> list[str]:
    """Extract repeatable ``--source`` values from the Click context chain."""
    value = _get_param_value(ctx, "sources")
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _get_service_from_ctx(ctx: object) -> str:
    """Extract service name from Click context."""
    value = _get_param_value(ctx, "service")
    if value:
        return str(value)
    try:
        return resolve_service_name(None)
    except Exception:
        return ""


def _get_param_value(ctx: object, param_name: str) -> object:
    """Return the first non-empty parameter value from the context chain."""
    current_ctx: object | None = ctx
    while current_ctx is not None:
        ctx_params = getattr(current_ctx, "params", None)
        if isinstance(ctx_params, dict):
            value = ctx_params.get(param_name)
            if value not in (None, "", (), []):
                return value
        current_ctx = getattr(current_ctx, "parent", None)
    return None


def _resolve_source_keys_for_completion(
    plan: Any,
    sources: list[str],
) -> list[str]:
    """Resolve user-facing source filters to source instance keys."""
    if not sources:
        return []

    from cdc_generator.cli.fdw_bootstrap import _resolve_source_instance_keys

    return _resolve_source_instance_keys(plan.source_plans, sources)


def _build_table_completion_query(
    source_keys: list[str],
    *,
    include_completed: bool,
) -> str:
    """Build the SQL query used to fetch candidate bootstrap tables."""
    allowed_statuses = ["pending", "failed"]
    if include_completed:
        allowed_statuses.append("completed")

    statuses_sql = ", ".join(_quote_sql_literal(status) for status in allowed_statuses)
    source_filter = ""
    if source_keys:
        quoted_source_keys = ", ".join(_quote_sql_literal(source_key) for source_key in source_keys)
        source_filter = f" AND source_instance_key IN ({quoted_source_keys})"

    return (
        "SELECT DISTINCT logical_table_name "
        "FROM cdc_management.native_cdc_bootstrap_state "
        f"WHERE bootstrap_status IN ({statuses_sql}){source_filter} "
        "ORDER BY logical_table_name"
    )


def _quote_sql_literal(value: str) -> str:
    """Quote a SQL string literal for a simple ad-hoc query."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _query_bootstrap_table_names(
    psql_bin: str,
    *,
    host: str,
    port: str,
    username: str,
    password: str,
    database: str,
    query: str,
) -> list[str]:
    """Run a lightweight completion query and return sorted table names."""
    command = [
        psql_bin,
        "-h",
        host,
        "-p",
        port,
        "-U",
        username,
        "-d",
        database,
        "-X",
        "-A",
        "-t",
        "-P",
        "pager=off",
        "-P",
        "footer=off",
        "-c",
        query,
    ]
    run_env = dict(os.environ)
    run_env["PGPASSWORD"] = password
    result = subprocess.run(
        command,
        check=False,
        env=run_env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []

    rows = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return sorted(dict.fromkeys(rows))


def _get_cache_file_path(
    service_name: str,
    target_sink_env: str,
    source_keys: list[str],
    include_completed: bool,
) -> Path:
    """Return the cache path for one bootstrap completion query."""
    cache_root = Path(tempfile.gettempdir()) / _CACHE_DIR_NAME
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_key = "|".join(
        [
            service_name,
            target_sink_env,
            ",".join(sorted(source_keys)) or "all",
            "with-completed" if include_completed else "pending-and-failed",
        ]
    )
    digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
    return cache_root / f"{digest}.json"


def _load_cached_values(cache_path: Path) -> list[str] | None:
    """Load cached completion values when the TTL has not expired."""
    if not cache_path.exists():
        return None
    cache_age_seconds = time.time() - cache_path.stat().st_mtime
    if cache_age_seconds > _CACHE_TTL_SECONDS:
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, list):
        return None

    values = [str(value).strip() for value in payload if str(value).strip()]
    return sorted(dict.fromkeys(values))


def _store_cached_values(cache_path: Path, values: list[str]) -> None:
    """Persist completion values to the short-lived cache file."""
    try:
        cache_path.write_text(json.dumps(values), encoding="utf-8")
    except OSError:
        return
