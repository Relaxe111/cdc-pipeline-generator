"""Typed contracts for manage-service preflight validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypedDict, cast

from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import load_yaml_file


class ValidationConfig(TypedDict, total=False):
    """Subset of service config used by preflight checks."""

    service: str
    server_group: str
    source: dict[str, object]
    sinks: dict[str, object]


class SourceGroupContext(TypedDict):
    """Resolved source-group context for current service."""

    server_group_name: str | None
    source_group_cfg: dict[str, Any] | None


def load_source_group_context(
    service: str,
    config: ValidationConfig,
) -> SourceGroupContext:
    """Load source-group context for the current service configuration."""
    source_groups_path = get_project_root() / "source-groups.yaml"
    if not source_groups_path.exists():
        return {
            "server_group_name": None,
            "source_group_cfg": None,
        }

    source_groups = cast(dict[str, Any], load_yaml_file(source_groups_path))
    server_group_raw = config.get("server_group") or config.get("service") or service
    server_group_name = str(server_group_raw).strip()
    if not server_group_name:
        return {
            "server_group_name": None,
            "source_group_cfg": None,
        }

    server_group_cfg_raw = source_groups.get(server_group_name)
    if not isinstance(server_group_cfg_raw, dict):
        return {
            "server_group_name": server_group_name,
            "source_group_cfg": None,
        }

    return {
        "server_group_name": server_group_name,
        "source_group_cfg": cast(dict[str, Any], server_group_cfg_raw),
    }


def load_sink_group_config(sink_group_name: str) -> dict[str, Any] | None:
    """Load sink-group config from sink-groups.yaml if present."""
    sink_groups_path = get_project_root() / "sink-groups.yaml"
    if not sink_groups_path.exists():
        return None

    sink_groups = cast(dict[str, Any], load_yaml_file(sink_groups_path))
    sink_group_raw = sink_groups.get(sink_group_name)
    if not isinstance(sink_group_raw, dict):
        return None

    return cast(dict[str, Any], sink_group_raw)


def iter_source_env_entries(
    source_entry: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Iterate source env entries while skipping non-env metadata keys."""
    entries: list[tuple[str, dict[str, Any]]] = []
    for env_name, env_cfg_raw in source_entry.items():
        if env_name == "schemas" or not isinstance(env_cfg_raw, dict):
            continue
        entries.append((str(env_name), cast(dict[str, Any], env_cfg_raw)))
    return entries


def project_root() -> Path:
    """Get project root for preflight checks."""
    return get_project_root()
