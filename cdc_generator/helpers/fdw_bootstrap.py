"""Helpers for metadata-driven MSSQL FDW bootstrap planning and SQL rendering."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.type_mapper import TypeMapper
from cdc_generator.helpers.yaml_loader import load_yaml_file

_CDC_SCHEMA_NAME = "cdc"
_DEFAULT_TDS_VERSION = "7.4"
_MIN_QUOTED_VALUE_LENGTH = 2
_FDW_META_COLUMNS: tuple[tuple[str, str], ...] = (
    ("__$start_lsn", "bytea"),
    ("__$seqval", "bytea"),
    ("__$operation", "integer"),
    ("__$update_mask", "bytea"),
)
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SQL_TYPE_BASE_PATTERN = re.compile(r"^\s*([A-Za-z0-9_]+)")
_IDENTIFIER_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9_]+")


def _new_warning_list() -> list[str]:
    """Create a typed empty warning list."""
    return []


@dataclass(frozen=True)
class FdwTablePlan:
    """Static FDW object plan for one tracked source table."""

    source_schema_name: str
    source_table_name: str
    logical_table_name: str
    foreign_table_name: str
    min_lsn_table_name: str
    capture_instance_name: str
    remote_table_name: str
    target_schema_name: str
    target_table_name: str
    columns: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class FdwBootstrapRequest:
    """Input options for FDW bootstrap planning."""

    customers: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()
    target_schema_name: str | None = None
    runner_role: str = "cdc_runner"
    fdw_server_prefix: str = "mssql"
    fdw_schema_prefix: str = "fdw"
    resolve_env_values: bool = True


@dataclass
class FdwSourcePlan:
    """Per-customer FDW source bootstrap plan."""

    customer_key: str
    customer_name: str
    customer_id: str
    source_env: str
    environment_profile_name: str
    server_name: str
    source_database: str
    fdw_server_name: str
    fdw_schema_name: str
    host: str
    port: str
    username: str
    password: str


@dataclass
class FdwBootstrapPlan:
    """Full bootstrap plan for one service + source environment."""

    service_name: str
    server_group_name: str
    source_env: str
    target_schema_name: str
    runner_role: str
    resolve_env_values: bool
    table_plans: list[FdwTablePlan]
    source_plans: list[FdwSourcePlan]
    warnings: list[str] = field(default_factory=_new_warning_list)


def build_fdw_bootstrap_plan(
    service_name: str,
    source_env: str,
    request: FdwBootstrapRequest | None = None,
) -> FdwBootstrapPlan:
    """Build a metadata-driven FDW bootstrap plan from implementation YAML.

    The command currently targets MSSQL db-per-tenant services where source
    databases are defined in source-groups.yaml and tracked tables are defined
    in services/<service>.yaml plus services/_schemas/<service>/.
    """
    effective_request = request or FdwBootstrapRequest()
    project_root = get_project_root()
    service_config = load_service_config(service_name)
    server_group_name = _resolve_server_group_name(service_config, service_name)
    source_group = _load_source_group(project_root, server_group_name)
    _validate_source_group(source_group, server_group_name)

    normalized_service_name = str(service_config.get("service", service_name)).strip()
    normalized_target_schema = (
        effective_request.target_schema_name.strip()
        if effective_request.target_schema_name
        else normalized_service_name
    )
    env_lookup = _build_env_lookup(project_root)

    tracked_tables = _load_tracked_tables(service_config)
    filtered_tables = _filter_tracked_tables(
        tracked_tables,
        list(effective_request.tables),
    )
    if not filtered_tables:
        raise ValueError("No tracked source tables matched the requested filters")

    table_plans = _build_table_plans(
        project_root,
        normalized_service_name,
        filtered_tables,
        normalized_target_schema,
    )
    source_plans, warnings = _build_source_plans(
        source_group,
        source_env,
        list(effective_request.customers),
        fdw_server_prefix=effective_request.fdw_server_prefix,
        fdw_schema_prefix=effective_request.fdw_schema_prefix,
        resolve_env_values=effective_request.resolve_env_values,
        env_lookup=env_lookup,
    )

    if not source_plans:
        raise ValueError("No valid source instances matched the requested filters")

    _assign_environment_profile_names(source_plans)

    return FdwBootstrapPlan(
        service_name=normalized_service_name,
        server_group_name=server_group_name,
        source_env=source_env,
        target_schema_name=normalized_target_schema,
        runner_role=effective_request.runner_role,
        resolve_env_values=effective_request.resolve_env_values,
        table_plans=table_plans,
        source_plans=source_plans,
        warnings=warnings,
    )


def render_fdw_plan_summary(plan: FdwBootstrapPlan) -> list[str]:
    """Render a concise human-readable summary for ``cdc fdw plan``."""
    lines = [
        f"Service: {plan.service_name}",
        f"Server group: {plan.server_group_name}",
        f"Source env: {plan.source_env}",
        f"Target schema: {plan.target_schema_name}",
        f"Runner role: {plan.runner_role}",
        f"Tracked tables: {len(plan.table_plans)}",
        f"Customer sources: {len(plan.source_plans)}",
        f"Foreign tables to create: {len(plan.table_plans) * len(plan.source_plans)}",
        f"Gap helpers to create: {len(plan.table_plans) * len(plan.source_plans)}",
    ]

    lines.append("")
    lines.append("Source instances:")
    for source_plan in plan.source_plans:
        lines.append(
            "  - "
            + f"{source_plan.customer_name} ({source_plan.customer_id}) -> "
            + f"{source_plan.source_database} | "
            + f"server {source_plan.fdw_server_name} | "
            + f"schema {source_plan.fdw_schema_name}"
        )

    lines.append("")
    lines.append("Tracked tables:")
    for table_plan in plan.table_plans:
        lines.append(
            "  - "
            + f"{table_plan.source_schema_name}.{table_plan.source_table_name} -> "
            + f"{table_plan.target_schema_name}.{table_plan.target_table_name} | "
            + f"foreign {table_plan.foreign_table_name} | "
            + f"columns {len(table_plan.columns)}"
        )

    return lines


def render_fdw_bootstrap_sql(
    plan: FdwBootstrapPlan,
    *,
    metadata_only: bool = False,
) -> str:
    """Render idempotent SQL for metadata registration and FDW objects."""
    sections: list[str] = [
        "-- Generated by: cdc fdw sql",
        f"-- Service: {plan.service_name}",
        f"-- Source env: {plan.source_env}",
        f"-- Target schema: {plan.target_schema_name}",
        f"-- Runner role: {plan.runner_role}",
    ]
    if plan.warnings:
        sections.append("-- Warnings:")
        for warning in plan.warnings:
            sections.append(f"--   {warning}")
    sections.append("")

    if not metadata_only:
        sections.extend([
            "CREATE EXTENSION IF NOT EXISTS tds_fdw;",
            "CREATE SCHEMA IF NOT EXISTS \"cdc_management\";",
            "",
        ])
    else:
        sections.extend([
            "CREATE SCHEMA IF NOT EXISTS \"cdc_management\";",
            "",
        ])

    sections.append(_render_metadata_tables_sql())
    sections.append(_render_customer_registry_sql(plan))
    sections.append(_render_environment_profiles_sql(plan))
    sections.append(_render_source_instances_sql(plan))
    sections.append(_render_source_table_registrations_sql(plan))

    if metadata_only:
        return "\n".join(section for section in sections if section).rstrip() + "\n"

    for source_plan in plan.source_plans:
        sections.append(_render_schema_sql(source_plan))
        sections.append(_render_server_sql(source_plan))
        sections.append(_render_user_mapping_sql(plan.runner_role, source_plan))
        for table_plan in plan.table_plans:
            sections.append(_render_foreign_table_sql(source_plan, table_plan))
            sections.append(_render_gap_table_sql(source_plan, table_plan))

    return "\n".join(section for section in sections if section).rstrip() + "\n"


def _resolve_server_group_name(
    service_config: dict[str, object],
    service_name: str,
) -> str:
    server_group_name_raw = service_config.get("server_group") or service_name
    server_group_name = str(server_group_name_raw).strip()
    if not server_group_name:
        raise ValueError("Service config does not define a server group")
    return server_group_name


def _load_source_group(project_root: Path, server_group_name: str) -> dict[str, Any]:
    source_groups_path = project_root / "source-groups.yaml"
    if not source_groups_path.exists():
        raise FileNotFoundError(f"source-groups.yaml not found at {source_groups_path}")

    source_groups = load_yaml_file(source_groups_path)
    source_group_raw = source_groups.get(server_group_name)
    if not isinstance(source_group_raw, dict):
        raise ValueError(f"Source group '{server_group_name}' not found in source-groups.yaml")
    return cast(dict[str, Any], source_group_raw)


def _validate_source_group(source_group: dict[str, Any], server_group_name: str) -> None:
    pattern = str(source_group.get("pattern", "")).strip().lower()
    source_type = str(source_group.get("type", source_group.get("server_type", ""))).strip().lower()

    if pattern != "db-per-tenant":
        raise ValueError(
            "fdw bootstrap currently supports only db-per-tenant services; "
            + f"source group '{server_group_name}' uses '{pattern or 'unknown'}'"
        )
    if source_type != "mssql":
        raise ValueError(
            "fdw bootstrap currently supports only MSSQL sources; "
            + f"source group '{server_group_name}' uses '{source_type or 'unknown'}'"
        )


def _load_tracked_tables(service_config: dict[str, object]) -> list[dict[str, object]]:
    shared_raw = service_config.get("shared")
    shared = cast(dict[str, Any], shared_raw) if isinstance(shared_raw, dict) else {}
    source_tables_raw = shared.get("source_tables", [])
    ignore_tables_raw = shared.get("ignore_tables", [])

    source_tables = cast(list[object], source_tables_raw) if isinstance(source_tables_raw, list) else []
    ignore_tables = cast(list[object], ignore_tables_raw) if isinstance(ignore_tables_raw, list) else []

    tracked_tables: list[dict[str, object]] = []
    for schema_group_raw in source_tables:
        if not isinstance(schema_group_raw, dict):
            continue
        schema_group = cast(dict[str, Any], schema_group_raw)
        schema_name_raw = schema_group.get("schema")
        schema_name = str(schema_name_raw).strip() if schema_name_raw is not None else ""
        if not schema_name:
            continue

        tables_raw = schema_group.get("tables", [])
        if not isinstance(tables_raw, list):
            continue

        for table_raw in cast(list[object], tables_raw):
            if isinstance(table_raw, str):
                table_name = table_raw.strip()
                table_dict: dict[str, Any] = {"name": table_name}
            elif isinstance(table_raw, dict):
                table_dict = cast(dict[str, Any], table_raw)
                table_name_raw = table_dict.get("name")
                table_name = str(table_name_raw).strip() if table_name_raw is not None else ""
            else:
                continue

            if not table_name or _should_ignore_table(ignore_tables, schema_name, table_name):
                continue

            tracked_tables.append(
                {
                    "schema": schema_name,
                    "table": table_name,
                    "ignore_columns": table_dict.get("ignore_columns"),
                    "include_columns": table_dict.get("include_columns"),
                }
            )

    return tracked_tables


def _should_ignore_table(
    ignore_tables: list[object],
    schema_name: str,
    table_name: str,
) -> bool:
    normalized_schema = schema_name.casefold()
    normalized_table = table_name.casefold()

    for ignore_entry in ignore_tables:
        if isinstance(ignore_entry, str):
            if normalized_schema == "dbo" and ignore_entry.casefold() == normalized_table:
                return True
            continue

        if not isinstance(ignore_entry, dict):
            continue

        ignore_dict = cast(dict[str, Any], ignore_entry)
        ignore_schema_raw = ignore_dict.get("schema", "dbo")
        ignore_table_raw = ignore_dict.get("table")
        ignore_schema = str(ignore_schema_raw).casefold() if ignore_schema_raw is not None else "dbo"
        ignore_table = str(ignore_table_raw).casefold() if ignore_table_raw is not None else ""
        if ignore_schema == normalized_schema and ignore_table == normalized_table:
            return True

    return False


def _filter_tracked_tables(
    tracked_tables: list[dict[str, object]],
    selected_tables: list[str] | None,
) -> list[dict[str, object]]:
    if not selected_tables:
        return tracked_tables

    normalized_filters = {_normalize_table_filter(value) for value in selected_tables}
    filtered: list[dict[str, object]] = []
    for tracked_table in tracked_tables:
        schema_name = str(tracked_table.get("schema", "")).strip()
        table_name = str(tracked_table.get("table", "")).strip()
        if not schema_name or not table_name:
            continue

        full_name = f"{schema_name}.{table_name}".casefold()
        if full_name in normalized_filters or table_name.casefold() in normalized_filters:
            filtered.append(tracked_table)

    return filtered


def _normalize_table_filter(table_filter: str) -> str:
    return table_filter.strip().casefold()


def _build_table_plans(
    project_root: Path,
    service_name: str,
    tracked_tables: list[dict[str, object]],
    target_schema_name: str,
) -> list[FdwTablePlan]:
    table_name_counts: dict[str, int] = {}
    for tracked_table in tracked_tables:
        table_name = str(tracked_table.get("table", "")).strip().casefold()
        if table_name:
            table_name_counts[table_name] = table_name_counts.get(table_name, 0) + 1

    mapper = TypeMapper("mssql", "pgsql")
    table_plans: list[FdwTablePlan] = []

    for tracked_table in tracked_tables:
        schema_name = str(tracked_table.get("schema", "")).strip()
        table_name = str(tracked_table.get("table", "")).strip()
        schema_path = project_root / "services" / "_schemas" / service_name / schema_name / f"{table_name}.yaml"
        schema_data = load_yaml_file(schema_path)
        columns = _load_fdw_columns(
            schema_data,
            mapper,
            include_columns=tracked_table.get("include_columns"),
            ignore_columns=tracked_table.get("ignore_columns"),
        )
        foreign_table_name = _build_foreign_table_name(
            schema_name,
            table_name,
            duplicate_table_name_count=table_name_counts.get(table_name.casefold(), 0),
        )
        logical_table_name = table_name
        capture_instance_name = f"{schema_name}_{table_name}"
        table_plans.append(
            FdwTablePlan(
                source_schema_name=schema_name,
                source_table_name=table_name,
                logical_table_name=logical_table_name,
                foreign_table_name=foreign_table_name,
                min_lsn_table_name=f"cdc_min_lsn_{_sanitize_object_name(foreign_table_name.removesuffix('_CT'))}",
                capture_instance_name=capture_instance_name,
                remote_table_name=f"{schema_name}_{table_name}_CT",
                target_schema_name=target_schema_name,
                target_table_name=table_name,
                columns=columns,
            )
        )

    return table_plans


def _build_foreign_table_name(
    schema_name: str,
    table_name: str,
    *,
    duplicate_table_name_count: int,
) -> str:
    sanitized_table_name = _sanitize_object_name(table_name)
    if duplicate_table_name_count > 1 or schema_name.casefold() != "dbo":
        return f"{_sanitize_object_name(schema_name)}_{sanitized_table_name}_CT"
    return f"{sanitized_table_name}_CT"


def _load_fdw_columns(
    schema_data: Mapping[str, object],
    mapper: TypeMapper,
    *,
    include_columns: object,
    ignore_columns: object,
) -> tuple[tuple[str, str], ...]:
    include_names = _normalize_name_list(include_columns)
    ignore_names = _normalize_name_list(ignore_columns)

    columns_raw = schema_data.get("columns", [])
    if not isinstance(columns_raw, list):
        raise ValueError("Schema file does not contain a valid columns list")

    fdw_columns = list(_FDW_META_COLUMNS)
    for column_raw in cast(list[object], columns_raw):
        if not isinstance(column_raw, dict):
            continue

        column = cast(dict[str, Any], column_raw)
        name_raw = column.get("name")
        type_raw = column.get("type")
        column_name = str(name_raw).strip() if name_raw is not None else ""
        source_type = str(type_raw).strip() if type_raw is not None else ""
        if not column_name or not source_type:
            continue

        normalized_name = column_name.casefold()
        if include_names and normalized_name not in include_names:
            continue
        if normalized_name in ignore_names:
            continue

        fdw_columns.append((column_name, _map_mssql_column_type(source_type, mapper)))

    return tuple(fdw_columns)


def _normalize_name_list(values: object) -> set[str]:
    if not isinstance(values, list):
        return set()

    return {
        str(value).strip().casefold()
        for value in cast(list[object], values)
        if str(value).strip()
    }


def _map_mssql_column_type(source_type: str, mapper: TypeMapper) -> str:
    mapped = mapper.map_type(source_type)
    if mapped != mapper.fallback:
        return mapped

    type_match = _SQL_TYPE_BASE_PATTERN.match(source_type)
    if type_match is None:
        return mapped

    base_type = type_match.group(1)
    return mapper.map_type(base_type)


def _build_source_plans(
    source_group: dict[str, Any],
    source_env: str,
    customers: list[str] | None,
    *,
    fdw_server_prefix: str,
    fdw_schema_prefix: str,
    resolve_env_values: bool,
    env_lookup: dict[str, str],
) -> tuple[list[FdwSourcePlan], list[str]]:
    selected_customers = {customer.strip().casefold() for customer in customers or [] if customer.strip()}
    warnings: list[str] = []
    source_plans: list[FdwSourcePlan] = []

    sources_raw = source_group.get("sources", {})
    servers_raw = source_group.get("servers", {})
    sources = cast(dict[str, Any], sources_raw) if isinstance(sources_raw, dict) else {}
    servers = cast(dict[str, Any], servers_raw) if isinstance(servers_raw, dict) else {}

    for source_name_raw, source_entry_raw in sources.items():
        source_name = str(source_name_raw).strip()
        if not source_name or not isinstance(source_entry_raw, dict):
            continue

        customer_key = source_name.casefold()
        if selected_customers and customer_key not in selected_customers and source_name.casefold() not in selected_customers:
            continue

        source_entry = cast(dict[str, Any], source_entry_raw)
        env_cfg_raw = source_entry.get(source_env)
        if not isinstance(env_cfg_raw, dict):
            warnings.append(
                f"Skipping {source_name}: source env '{source_env}' is not configured"
            )
            continue

        env_cfg = cast(dict[str, Any], env_cfg_raw)
        customer_id = _resolve_customer_id(source_entry, env_cfg)
        if customer_id is None:
            warnings.append(
                f"Skipping {source_name}: customer_id is missing for env '{source_env}'"
            )
            continue

        source_database_raw = env_cfg.get("database")
        source_database = str(source_database_raw).strip() if source_database_raw is not None else ""
        if not source_database:
            warnings.append(
                f"Skipping {source_name}: database is missing for env '{source_env}'"
            )
            continue

        server_name_raw = env_cfg.get("server", "default")
        server_name = str(server_name_raw).strip() if server_name_raw is not None else "default"
        server_cfg_raw = servers.get(server_name)
        if not isinstance(server_cfg_raw, dict):
            warnings.append(
                f"Skipping {source_name}: server '{server_name}' is not defined"
            )
            continue

        server_cfg = cast(dict[str, Any], server_cfg_raw)
        try:
            host = _resolve_config_value(server_cfg.get("host"), env_lookup, resolve_env_values, "host")
            port = _resolve_config_value(server_cfg.get("port"), env_lookup, resolve_env_values, "port")
            username = _resolve_config_value(
                server_cfg.get("username", server_cfg.get("user")),
                env_lookup,
                resolve_env_values,
                "username",
            )
            password = _resolve_config_value(server_cfg.get("password"), env_lookup, resolve_env_values, "password")
        except ValueError as exc:
            warnings.append(f"Skipping {source_name}: {exc}")
            continue

        fdw_source_key = _sanitize_object_name(customer_key)
        source_plans.append(
            FdwSourcePlan(
                customer_key=customer_key,
                customer_name=source_name,
                customer_id=customer_id,
                source_env=source_env,
                environment_profile_name=source_env,
                server_name=server_name,
                source_database=source_database,
                fdw_server_name=f"{_sanitize_object_name(fdw_server_prefix)}_{_sanitize_object_name(source_env)}_{fdw_source_key}",
                fdw_schema_name=f"{_sanitize_object_name(fdw_schema_prefix)}_{_sanitize_object_name(source_env)}_{fdw_source_key}",
                host=host,
                port=port,
                username=username,
                password=password,
            )
        )

    source_plans.sort(key=lambda source_plan: source_plan.customer_key)
    return source_plans, warnings


def _resolve_customer_id(
    source_entry: dict[str, Any],
    env_cfg: dict[str, Any],
) -> str | None:
    env_customer_id = env_cfg.get("customer_id")
    if env_customer_id is not None and str(env_customer_id).strip():
        return str(env_customer_id).strip()

    top_level_customer_id = source_entry.get("customer_id")
    if top_level_customer_id is None or not str(top_level_customer_id).strip():
        return None
    return str(top_level_customer_id).strip()


def _assign_environment_profile_names(source_plans: list[FdwSourcePlan]) -> None:
    profile_groups: dict[tuple[str, str, str, str, str, str], list[FdwSourcePlan]] = {}
    for source_plan in source_plans:
        profile_key = (
            source_plan.source_env,
            source_plan.server_name,
            source_plan.host,
            source_plan.port,
            source_plan.username,
            source_plan.password,
        )
        profile_groups.setdefault(profile_key, []).append(source_plan)

    multiple_profiles = len(profile_groups) > 1
    for (source_env, server_name, _host, _port, _username, _password), grouped_plans in profile_groups.items():
        profile_name = (
            f"{_sanitize_object_name(source_env)}_{_sanitize_object_name(server_name)}"
            if multiple_profiles
            else source_env
        )
        for source_plan in grouped_plans:
            source_plan.environment_profile_name = profile_name


def _build_env_lookup(project_root: Path) -> dict[str, str]:
    env_lookup = dict(os.environ)
    env_path = project_root / ".env"
    if not env_path.exists():
        return env_lookup

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        env_lookup.setdefault(key.strip(), _strip_env_value(value.strip()))

    return env_lookup


def _strip_env_value(value: str) -> str:
    if (
        len(value) >= _MIN_QUOTED_VALUE_LENGTH
        and value[0] == value[-1]
        and value[0] in {'"', "'"}
    ):
        return value[1:-1]
    return value


def _resolve_config_value(
    raw_value: object,
    env_lookup: dict[str, str],
    resolve_env_values: bool,
    field_name: str,
) -> str:
    if raw_value is None:
        raise ValueError(f"{field_name} is missing")

    value = str(raw_value).strip()
    if not value:
        raise ValueError(f"{field_name} is empty")
    if not resolve_env_values:
        return value

    missing_vars = [
        match.group(1)
        for match in _ENV_VAR_PATTERN.finditer(value)
        if not env_lookup.get(match.group(1), "").strip()
    ]
    if missing_vars:
        missing_list = ", ".join(sorted(set(missing_vars)))
        raise ValueError(
            f"{field_name} uses missing environment variable(s): {missing_list}"
        )

    resolved_value = _ENV_VAR_PATTERN.sub(
        lambda match: env_lookup.get(match.group(1), ""),
        value,
    )
    if not resolved_value.strip():
        raise ValueError(f"{field_name} resolves to an empty value")
    return resolved_value


def _render_metadata_tables_sql() -> str:
    return "\n".join([
        "CREATE TABLE IF NOT EXISTS \"cdc_management\".\"customer_registry\" (",
        "    \"customer_key\" text PRIMARY KEY,",
        "    \"customer_id\" uuid NOT NULL UNIQUE,",
        "    \"customer_name\" text NOT NULL",
        ");",
        "",
        "CREATE TABLE IF NOT EXISTS \"cdc_management\".\"environment_profile\" (",
        "    \"environment_name\" text PRIMARY KEY,",
        "    \"mssql_host\" text NOT NULL,",
        "    \"mssql_port\" integer NOT NULL,",
        "    \"mssql_username\" text NOT NULL,",
        "    \"mssql_password\" text NOT NULL,",
        "    \"tds_version\" text NOT NULL DEFAULT '7.4',",
        "    \"enabled\" boolean NOT NULL DEFAULT true",
        ");",
        "",
        "CREATE TABLE IF NOT EXISTS \"cdc_management\".\"source_instance\" (",
        "    \"source_instance_key\" text PRIMARY KEY,",
        "    \"environment_name\" text NOT NULL REFERENCES \"cdc_management\".\"environment_profile\"(\"environment_name\"),",
        "    \"customer_key\" text NOT NULL REFERENCES \"cdc_management\".\"customer_registry\"(\"customer_key\"),",
        "    \"source_database\" text NOT NULL,",
        "    \"fdw_server_name\" text NOT NULL UNIQUE,",
        "    \"fdw_schema_name\" text NOT NULL UNIQUE,",
        "    \"enabled\" boolean NOT NULL DEFAULT true",
        ");",
        "",
        "CREATE TABLE IF NOT EXISTS \"cdc_management\".\"source_table_registration\" (",
        "    \"source_instance_key\" text NOT NULL REFERENCES \"cdc_management\".\"source_instance\"(\"source_instance_key\"),",
        "    \"logical_table_name\" text NOT NULL,",
        "    \"remote_schema_name\" text NOT NULL,",
        "    \"remote_table_name\" text NOT NULL,",
        "    \"target_schema_name\" text NOT NULL,",
        "    \"target_table_name\" text NOT NULL,",
        "    \"enabled\" boolean NOT NULL DEFAULT true,",
        "    PRIMARY KEY (\"source_instance_key\", \"logical_table_name\")",
        ");",
        "",
    ])


def _render_customer_registry_sql(plan: FdwBootstrapPlan) -> str:
    value_rows = [
        "    ("
        + ", ".join([
            _quote_literal(source_plan.customer_key),
            _quote_literal(source_plan.customer_id),
            _quote_literal(source_plan.customer_name),
        ])
        + ")"
        for source_plan in plan.source_plans
    ]

    return "\n".join([
        "INSERT INTO \"cdc_management\".\"customer_registry\" (",
        "    \"customer_key\",",
        "    \"customer_id\",",
        "    \"customer_name\"",
        ")",
        "VALUES",
        ",\n".join(value_rows),
        "ON CONFLICT (\"customer_key\") DO UPDATE",
        "SET",
        "    \"customer_id\" = EXCLUDED.\"customer_id\",",
        "    \"customer_name\" = EXCLUDED.\"customer_name\";",
        "",
    ])


def _render_environment_profiles_sql(plan: FdwBootstrapPlan) -> str:
    rendered_profiles: dict[str, FdwSourcePlan] = {}
    for source_plan in plan.source_plans:
        rendered_profiles.setdefault(source_plan.environment_profile_name, source_plan)

    value_rows = [
        "    ("
        + ", ".join([
            _quote_literal(profile_name),
            _quote_literal(source_plan.host),
            _quote_literal(source_plan.port),
            _quote_literal(source_plan.username),
            _quote_literal(source_plan.password),
            _quote_literal(_DEFAULT_TDS_VERSION),
            "true",
        ])
        + ")"
        for profile_name, source_plan in sorted(rendered_profiles.items())
    ]

    return "\n".join([
        "INSERT INTO \"cdc_management\".\"environment_profile\" (",
        "    \"environment_name\",",
        "    \"mssql_host\",",
        "    \"mssql_port\",",
        "    \"mssql_username\",",
        "    \"mssql_password\",",
        "    \"tds_version\",",
        "    \"enabled\"",
        ")",
        "VALUES",
        ",\n".join(value_rows),
        "ON CONFLICT (\"environment_name\") DO UPDATE",
        "SET",
        "    \"mssql_host\" = EXCLUDED.\"mssql_host\",",
        "    \"mssql_port\" = EXCLUDED.\"mssql_port\",",
        "    \"mssql_username\" = EXCLUDED.\"mssql_username\",",
        "    \"mssql_password\" = EXCLUDED.\"mssql_password\",",
        "    \"tds_version\" = EXCLUDED.\"tds_version\",",
        "    \"enabled\" = EXCLUDED.\"enabled\";",
        "",
    ])


def _render_source_instances_sql(plan: FdwBootstrapPlan) -> str:
    value_rows = [
        "    ("
        + ", ".join([
            _quote_literal(f"{source_plan.source_env}_{source_plan.customer_key}"),
            _quote_literal(source_plan.environment_profile_name),
            _quote_literal(source_plan.customer_key),
            _quote_literal(source_plan.source_database),
            _quote_literal(source_plan.fdw_server_name),
            _quote_literal(source_plan.fdw_schema_name),
            "true",
        ])
        + ")"
        for source_plan in plan.source_plans
    ]

    return "\n".join([
        "INSERT INTO \"cdc_management\".\"source_instance\" (",
        "    \"source_instance_key\",",
        "    \"environment_name\",",
        "    \"customer_key\",",
        "    \"source_database\",",
        "    \"fdw_server_name\",",
        "    \"fdw_schema_name\",",
        "    \"enabled\"",
        ")",
        "VALUES",
        ",\n".join(value_rows),
        "ON CONFLICT (\"source_instance_key\") DO UPDATE",
        "SET",
        "    \"environment_name\" = EXCLUDED.\"environment_name\",",
        "    \"customer_key\" = EXCLUDED.\"customer_key\",",
        "    \"source_database\" = EXCLUDED.\"source_database\",",
        "    \"fdw_server_name\" = EXCLUDED.\"fdw_server_name\",",
        "    \"fdw_schema_name\" = EXCLUDED.\"fdw_schema_name\",",
        "    \"enabled\" = EXCLUDED.\"enabled\";",
        "",
    ])


def _render_source_table_registrations_sql(plan: FdwBootstrapPlan) -> str:
    value_rows: list[str] = []
    for source_plan in plan.source_plans:
        source_instance_key = f"{source_plan.source_env}_{source_plan.customer_key}"
        for table_plan in plan.table_plans:
            value_rows.append(
                "    ("
                + ", ".join([
                    _quote_literal(source_instance_key),
                    _quote_literal(table_plan.logical_table_name),
                    _quote_literal(_CDC_SCHEMA_NAME),
                    _quote_literal(table_plan.remote_table_name),
                    _quote_literal(table_plan.target_schema_name),
                    _quote_literal(table_plan.target_table_name),
                    "true",
                ])
                + ")"
            )

    return "\n".join([
        "INSERT INTO \"cdc_management\".\"source_table_registration\" (",
        "    \"source_instance_key\",",
        "    \"logical_table_name\",",
        "    \"remote_schema_name\",",
        "    \"remote_table_name\",",
        "    \"target_schema_name\",",
        "    \"target_table_name\",",
        "    \"enabled\"",
        ")",
        "VALUES",
        ",\n".join(value_rows),
        "ON CONFLICT (\"source_instance_key\", \"logical_table_name\") DO UPDATE",
        "SET",
        "    \"remote_schema_name\" = EXCLUDED.\"remote_schema_name\",",
        "    \"remote_table_name\" = EXCLUDED.\"remote_table_name\",",
        "    \"target_schema_name\" = EXCLUDED.\"target_schema_name\",",
        "    \"target_table_name\" = EXCLUDED.\"target_table_name\",",
        "    \"enabled\" = EXCLUDED.\"enabled\";",
        "",
    ])


def _render_schema_sql(source_plan: FdwSourcePlan) -> str:
    return f'CREATE SCHEMA IF NOT EXISTS {_quote_ident(source_plan.fdw_schema_name)};\n'


def _render_server_sql(source_plan: FdwSourcePlan) -> str:
    server_sql = (
        f'CREATE SERVER {_quote_ident(source_plan.fdw_server_name)}\n'
        + 'FOREIGN DATA WRAPPER tds_fdw\n'
        + 'OPTIONS (\n'
        + f"    servername {_quote_literal(source_plan.host)},\n"
        + f"    port {_quote_literal(source_plan.port)},\n"
        + f"    database {_quote_literal(source_plan.source_database)},\n"
        + f"    tds_version {_quote_literal(_DEFAULT_TDS_VERSION)},\n"
        + "    dbuse '0',\n"
        + "    msg_handler 'notice'\n"
        + ');'
    )
    alter_sql = (
        f'ALTER SERVER {_quote_ident(source_plan.fdw_server_name)}\n'
        + 'OPTIONS (\n'
        + f"    SET servername {_quote_literal(source_plan.host)},\n"
        + f"    SET port {_quote_literal(source_plan.port)},\n"
        + f"    SET database {_quote_literal(source_plan.source_database)},\n"
        + f"    SET tds_version {_quote_literal(_DEFAULT_TDS_VERSION)},\n"
        + "    SET dbuse '0',\n"
        + "    SET msg_handler 'notice'\n"
        + ');'
    )

    return "\n".join([
        'DO $fdw$',
        'BEGIN',
        '    IF NOT EXISTS (',
        '        SELECT 1',
        '        FROM pg_foreign_server',
        f"        WHERE srvname = {_quote_literal(source_plan.fdw_server_name)}",
        '    ) THEN',
        f"        EXECUTE {_quote_literal(server_sql)};",
        '    ELSE',
        f"        EXECUTE {_quote_literal(alter_sql)};",
        '    END IF;',
        'END',
        '$fdw$;',
        '',
    ])


def _render_user_mapping_sql(runner_role: str, source_plan: FdwSourcePlan) -> str:
    create_sql = (
        f'CREATE USER MAPPING FOR {_quote_ident(runner_role)}\n'
        + f'SERVER {_quote_ident(source_plan.fdw_server_name)}\n'
        + 'OPTIONS (\n'
        + f"    username {_quote_literal(source_plan.username)},\n"
        + f"    password {_quote_literal(source_plan.password)}\n"
        + ');'
    )
    alter_sql = (
        f'ALTER USER MAPPING FOR {_quote_ident(runner_role)}\n'
        + f'SERVER {_quote_ident(source_plan.fdw_server_name)}\n'
        + 'OPTIONS (\n'
        + f"    SET username {_quote_literal(source_plan.username)},\n"
        + f"    SET password {_quote_literal(source_plan.password)}\n"
        + ');'
    )

    return "\n".join([
        'DO $fdw$',
        'BEGIN',
        '    IF NOT EXISTS (',
        '        SELECT 1',
        '        FROM pg_user_mappings um',
        '        JOIN pg_foreign_server s ON s.oid = um.srvid',
        '        JOIN pg_roles r ON r.oid = um.umuser',
        f"        WHERE s.srvname = {_quote_literal(source_plan.fdw_server_name)}",
        f"          AND r.rolname = {_quote_literal(runner_role)}",
        '    ) THEN',
        f"        EXECUTE {_quote_literal(create_sql)};",
        '    ELSE',
        f"        EXECUTE {_quote_literal(alter_sql)};",
        '    END IF;',
        'END',
        '$fdw$;',
        '',
    ])


def _render_foreign_table_sql(source_plan: FdwSourcePlan, table_plan: FdwTablePlan) -> str:
    column_lines = [
        f"    {_quote_ident(column_name)} {column_type}"
        for column_name, column_type in table_plan.columns
    ]
    return "\n".join([
        f'DROP FOREIGN TABLE IF EXISTS {_quote_ident(source_plan.fdw_schema_name)}.{_quote_ident(table_plan.foreign_table_name)};',
        f'CREATE FOREIGN TABLE {_quote_ident(source_plan.fdw_schema_name)}.{_quote_ident(table_plan.foreign_table_name)} (',
        ",\n".join(column_lines),
        ')',
        f'SERVER {_quote_ident(source_plan.fdw_server_name)}',
        'OPTIONS (',
        f"    schema_name {_quote_literal(_CDC_SCHEMA_NAME)},",
        f"    table_name {_quote_literal(table_plan.remote_table_name)},",
        "    match_column_names 'true',",
        "    row_estimate_method 'showplan_all'",
        ');',
        '',
    ])


def _render_gap_table_sql(source_plan: FdwSourcePlan, table_plan: FdwTablePlan) -> str:
    query = (
        "SELECT sys.fn_cdc_get_min_lsn("
        + _quote_literal(table_plan.capture_instance_name)
        + ") AS min_lsn"
    )
    return "\n".join([
        f'DROP FOREIGN TABLE IF EXISTS {_quote_ident(source_plan.fdw_schema_name)}.{_quote_ident(table_plan.min_lsn_table_name)};',
        f'CREATE FOREIGN TABLE {_quote_ident(source_plan.fdw_schema_name)}.{_quote_ident(table_plan.min_lsn_table_name)} (',
        '    "min_lsn" bytea',
        ')',
        f'SERVER {_quote_ident(source_plan.fdw_server_name)}',
        'OPTIONS (',
        f"    query {_quote_literal(query)},",
        "    row_estimate_method 'execute'",
        ');',
        '',
    ])


def _sanitize_object_name(value: str) -> str:
    sanitized = _IDENTIFIER_SANITIZE_PATTERN.sub("_", value).strip("_")
    return sanitized or "unnamed"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
