"""Shared PostgreSQL sink target resolution for FDW operations.

Used by both ``cdc fdw apply`` and ``cdc fdw bootstrap`` to resolve
a target PostgreSQL connection from sink-groups.yaml + services config.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.autocompletions.services import list_existing_services
from cdc_generator.helpers.env_resolution import build_env_lookup, resolve_config_value
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.yaml_loader import load_yaml_file


@dataclass(frozen=True)
class FdwSinkTarget:
    """Resolved PostgreSQL sink target for FDW operations (apply, bootstrap, etc.)."""

    sink_key: str
    sink_group_name: str
    sink_service_name: str
    sink_server_name: str
    host: str
    port: str
    username: str
    password: str
    database: str


def resolve_service_name(service_name: str | None) -> str:
    """Resolve a service name, auto-detecting when exactly one exists."""
    if service_name is not None and service_name.strip():
        return service_name.strip()

    existing_services = list_existing_services()
    if len(existing_services) == 1:
        return existing_services[0]
    if not existing_services:
        raise ValueError("No service configs were found under services/. Pass --service <name>.")

    available = ", ".join(existing_services)
    raise ValueError("Multiple service configs are available; pass --service <name>. " + f"Available services: {available}")


def resolve_sink_key(
    service_config: dict[str, object],
    sink_key_override: str | None,
) -> str:
    """Resolve a sink key from service config, auto-detecting when exactly one exists."""
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
        raise ValueError(f"Sink '{normalized_override}' is not configured for this service. " + f"Available sinks: {available}")

    if len(sink_keys) == 1:
        return sink_keys[0]

    available = ", ".join(sorted(sink_keys))
    raise ValueError("Service config defines multiple sink targets; " + "pass --sink <sink-group>.<sink-service>. " + f"Available sinks: {available}")


def parse_sink_key(sink_key: str) -> tuple[str, str]:
    """Parse '<sink-group>.<sink-service>' into its two components."""
    if "." not in sink_key:
        raise ValueError("Sink keys must use the form <sink-group>.<sink-service>")

    sink_group_name, sink_service_name = sink_key.split(".", 1)
    normalized_group = sink_group_name.strip()
    normalized_service = sink_service_name.strip()
    if not normalized_group or not normalized_service:
        raise ValueError("Sink keys must use the form <sink-group>.<sink-service>")
    return normalized_group, normalized_service


def resolve_psql_bin(psql_bin_override: str | None) -> str:
    """Resolve the psql executable path, searching PATH when not overridden."""
    if psql_bin_override is not None and psql_bin_override.strip():
        return psql_bin_override.strip()

    resolved_path = shutil.which("psql")
    if resolved_path is None:
        raise ValueError("psql executable not found in PATH. " + "Install libpq or pass --psql-bin /absolute/path/to/psql")
    return resolved_path


def resolve_sink_target(
    service_name: str,
    *,
    target_sink_env: str | None,
    sink_key_override: str | None,
    resolve_env_values: bool,
) -> FdwSinkTarget:
    """Resolve a full PostgreSQL sink target from YAML configuration.

    Reads sink-groups.yaml and services/<service>.yaml to resolve host, port,
    database, username, and password for a given target sink environment.
    """
    if target_sink_env is None or not target_sink_env.strip():
        raise ValueError("--target-sink-env is required to resolve the target PostgreSQL database")

    project_root = get_project_root()
    service_config = load_service_config(service_name)
    sink_key = resolve_sink_key(service_config, sink_key_override)
    sink_group_name, sink_service_name = parse_sink_key(sink_key)

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
    username = resolve_config_value(
        server_cfg.get("username", server_cfg.get("user")),
        env_lookup,
        resolve_env_values,
        "username",
    )
    password = resolve_config_value(server_cfg.get("password"), env_lookup, resolve_env_values, "password")

    return FdwSinkTarget(
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
