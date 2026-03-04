"""Shared constants and utility helpers for pipeline generation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

from cdc_generator.core.sink_env_routing import resolve_sink_env_key
from cdc_generator.helpers.service_config import (
    get_project_root,
    load_service_config,
)
from cdc_generator.helpers.yaml_loader import ConfigValue, load_yaml_file


def get_services_for_customers(customers: list[str]) -> set[str]:
    """Determine which services are used by the given customers."""
    services: set[str] = set()
    services_dir = get_project_root() / "services"

    if not services_dir.exists():
        return services

    target_customers = {customer.casefold() for customer in customers}

    for service_file in services_dir.glob("*.yaml"):
        try:
            service_config = load_service_config(service_file.stem)
            service_customers = service_config.get("customers", [])
            if not isinstance(service_customers, list):
                continue
            service_customers_list = cast(list[ConfigValue], service_customers)

            customer_names: set[str] = set()
            customer_value: ConfigValue
            for customer_value in service_customers_list:
                if not isinstance(customer_value, dict):
                    continue
                customer_dict = cast(dict[str, ConfigValue], customer_value)
                name = customer_dict.get("name")
                if isinstance(name, str):
                    customer_names.add(name.casefold())

            if target_customers.intersection(customer_names):
                services.add(service_file.stem)
        except Exception:
            continue

    return services


def normalize_for_comparison(content: str) -> str:
    """Remove timestamp line from content for comparison purposes."""
    lines = content.split("\n")
    normalized_lines = [
        line for line in lines
        if not line.startswith("# Generated:")
    ]
    return "\n".join(normalized_lines)


def should_write_file(file_path: Path, new_content: str) -> bool:
    """Check if file should be written by comparing content (ignoring timestamp)."""
    if not file_path.exists():
        return True

    existing_content = file_path.read_text()
    return normalize_for_comparison(existing_content) != normalize_for_comparison(new_content)


def preserve_env_vars(value: object) -> str:
    """Preserve environment variable placeholders for runtime resolution by Bento."""
    if isinstance(value, str):
        return re.sub(r"\$\{env:([A-Z_][A-Z0-9_]*)\}", r"${\1}", value)
    return str(value) if value is not None else ""


SINK_REF_PARTS_COUNT = 2


def load_template(template_name: str) -> str:
    """Load a template file as string."""
    templates_dir = get_project_root() / "pipelines" / "templates"
    template_path = templates_dir / template_name
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text()


def normalize_table_name(name: str) -> str:
    """Normalize Norwegian special characters only (keep original casing)."""
    replacements = {
        "å": "a", "Å": "A",
        "ø": "o", "Ø": "O",
        "æ": "ae", "Æ": "AE",
    }

    result = name
    for norwegian_char, replacement in replacements.items():
        result = result.replace(norwegian_char, replacement)

    return result


def load_generated_table_definitions() -> dict[str, Any]:
    """Load table definitions from canonical services/_schemas/ tree."""
    services_dir = get_project_root() / "services"
    generated_dir = services_dir / "_schemas"

    if not generated_dir.exists():
        return {}

    tables_by_name: dict[str, Any] = {}
    for yaml_file in sorted(generated_dir.rglob("*.yaml")):
        path_parts = set(yaml_file.parts)
        if "_definitions" in path_parts or "_bloblang" in path_parts or "adapters" in path_parts:
            continue

        table_def = load_yaml_file(yaml_file)
        if table_def and table_def:
            table_def_dict = cast(dict[str, object], table_def)
            table_name = table_def_dict.get("table")
            columns = table_def_dict.get("columns")
            if not isinstance(table_name, str) or not isinstance(columns, list):
                continue
            columns_list = cast(list[object], columns)

            fields: list[dict[str, str]] = []
            col_raw: object
            for col_raw in columns_list:
                if not isinstance(col_raw, dict):
                    continue
                col = cast(dict[str, Any], col_raw)
                col_name_raw = col.get("name")
                if not isinstance(col_name_raw, str) or not col_name_raw:
                    continue
                postgres_name = normalize_table_name(col_name_raw)
                fields.append(
                    {
                        "mssql": f"[{col_name_raw}]",
                        "postgres": postgres_name,
                    },
                )

            if fields:
                tables_by_name[table_name] = {
                    "name": table_name,
                    "fields": fields,
                }

    return tables_by_name


def get_primary_key_from_schema(
    service_name: str,
    schema_name: str,
    table_name: str,
) -> tuple[str | list[str] | None, str | None]:
    """Read primary_key from canonical service schema table definition."""
    services_dir = get_project_root() / "services"
    table_schema_path = services_dir / "_schemas" / service_name / schema_name / f"{table_name}.yaml"

    if not table_schema_path.exists():
        raise ValueError(
            "Missing table schema definition for primary key resolution: "
            + f"{table_schema_path}. "
            + "Generation requires services/_schemas/{service}/{schema}/{Table}.yaml "
            + "with a valid primary_key."
        )

    table_def_dict = cast(dict[str, Any], load_yaml_file(table_schema_path))

    top_level_pk = table_def_dict.get("primary_key")
    if isinstance(top_level_pk, str) and top_level_pk:
        return top_level_pk, "schema"
    if isinstance(top_level_pk, list):
        top_level_pk_list = cast(list[object], top_level_pk)
        pk_list = [str(pk) for pk in top_level_pk_list if str(pk)]
        if pk_list:
            return pk_list, "schema"

    columns = table_def_dict.get("columns", [])
    pk_columns: list[str] = []
    if isinstance(columns, list):
        columns_list = cast(list[object], columns)
        col: object
        for col in columns_list:
            if not isinstance(col, dict):
                continue
            col_dict = cast(dict[str, Any], col)
            if col_dict.get("primary_key") is True:
                col_name = col_dict.get("name")
                if isinstance(col_name, str) and col_name:
                    pk_columns.append(col_name)

    if pk_columns:
        if len(pk_columns) == 1:
            return pk_columns[0], "schema"
        return pk_columns, "schema"

    raise ValueError(
        "Missing primary key metadata in schema definition: "
        + f"{table_schema_path}. "
        + "Add top-level primary_key or mark columns with primary_key: true."
    )


def substitute_variables(template: str, variables: dict[str, Any]) -> str:
    """Replace {{VAR}} placeholders with values from variables dict."""
    result = template
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        result = result.replace(placeholder, str(value))
    return result


def resolve_postgres_url_from_sink_groups(
    service_name: str,
    env_name: str,
    target_sink_env: str | None = None,
) -> str:
    """Build consolidated sink PostgreSQL URL from sink-groups.yaml."""
    service_cfg = load_service_config(service_name)
    sinks_raw = service_cfg.get("sinks", {})
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        raise ValueError(
            f"Service '{service_name}' has no sinks configuration; cannot resolve sink DSN"
        )
    sinks = cast(dict[str, Any], sinks_raw)

    sink_ref = str(next(iter(sinks)))
    sink_ref_parts = sink_ref.split(".", 1)
    if len(sink_ref_parts) != SINK_REF_PARTS_COUNT:
        raise ValueError(
            f"Invalid sink reference '{sink_ref}' in service '{service_name}'. "
            + "Expected format '<sink_group>.<source>'"
        )

    sink_group_name, sink_source_name = sink_ref_parts

    sink_groups_path = get_project_root() / "sink-groups.yaml"
    if not sink_groups_path.exists():
        raise ValueError(f"Missing sink-groups.yaml at {sink_groups_path}")

    sink_groups_data = cast(dict[str, Any], load_yaml_file(sink_groups_path))
    sink_group_raw = sink_groups_data.get(sink_group_name)
    if not isinstance(sink_group_raw, dict):
        raise ValueError(
            f"Sink group '{sink_group_name}' not found in sink-groups.yaml"
        )

    sink_group = cast(dict[str, Any], sink_group_raw)
    sources_raw = sink_group.get("sources", {})
    if not isinstance(sources_raw, dict):
        raise ValueError(
            f"Sink group '{sink_group_name}' has invalid 'sources' section"
        )

    sources = cast(dict[str, Any], sources_raw)
    source_raw = sources.get(sink_source_name)
    if not isinstance(source_raw, dict):
        raise ValueError(
            f"Sink source '{sink_source_name}' not found in sink group '{sink_group_name}'"
        )

    source_cfg = cast(dict[str, Any], source_raw)
    source_env_key = resolve_sink_env_key(
        source_cfg,
        env_name,
        target_sink_env=target_sink_env,
    )
    source_env_raw = source_cfg.get(source_env_key)
    if not isinstance(source_env_raw, dict):
        raise ValueError(
            f"Invalid sink source env config for '{sink_group_name}.{sink_source_name}.{source_env_key}'"
        )

    source_env_cfg = cast(dict[str, Any], source_env_raw)
    server_name = source_env_cfg.get("server")
    database_name = source_env_cfg.get("database")
    if not isinstance(server_name, str) or not isinstance(database_name, str):
        raise ValueError(
            f"Missing server/database for sink source '{sink_group_name}.{sink_source_name}.{source_env_key}'"
        )

    servers_raw = sink_group.get("servers", {})
    if not isinstance(servers_raw, dict):
        raise ValueError(f"Sink group '{sink_group_name}' has invalid 'servers' section")
    servers = cast(dict[str, Any], servers_raw)
    server_cfg_raw = servers.get(server_name)
    if not isinstance(server_cfg_raw, dict):
        raise ValueError(
            f"Server '{server_name}' not found in sink group '{sink_group_name}'"
        )

    server_cfg = cast(dict[str, Any], server_cfg_raw)

    host_raw = server_cfg.get("host")
    port_raw = server_cfg.get("port")
    user_raw = server_cfg.get("user")
    password_raw = server_cfg.get("password")

    if host_raw in (None, "") or port_raw in (None, "") or user_raw in (None, "") or password_raw in (None, ""):
        raise ValueError(
            f"Missing sink server credentials for '{sink_group_name}.{server_name}'. "
            + "Required: host, port, user, password"
        )

    host = preserve_env_vars(host_raw)
    port = preserve_env_vars(port_raw)
    user = preserve_env_vars(user_raw)
    password = preserve_env_vars(password_raw)
    database = preserve_env_vars(database_name)

    return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=disable"


def load_source_groups_config() -> dict[str, Any]:
    source_groups_path = get_project_root() / "source-groups.yaml"
    if not source_groups_path.exists():
        return {}
    return cast(dict[str, Any], load_yaml_file(source_groups_path))


def find_source_name_case_insensitive(group_name: str, customer_name: str) -> str | None:
    source_groups = load_source_groups_config()
    group_raw = source_groups.get(group_name)
    if not isinstance(group_raw, dict):
        return None

    group_cfg = cast(dict[str, Any], group_raw)
    sources_raw = group_cfg.get("sources", {})
    if not isinstance(sources_raw, dict):
        return None

    sources = cast(dict[str, Any], sources_raw)
    target = customer_name.casefold()
    for source_name in sources:
        if source_name.casefold() == target:
            return source_name
    return None


def _load_source_groups_config() -> dict[str, Any]:
    """Backward-compatible alias for internal callers."""
    return load_source_groups_config()


def _find_source_name_case_insensitive(group_name: str, customer_name: str) -> str | None:
    """Backward-compatible alias for internal callers."""
    return find_source_name_case_insensitive(group_name, customer_name)
