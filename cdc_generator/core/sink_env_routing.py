"""Sink environment routing helpers.

Shared helpers for resolving sink target environment keys and validating
sink topology availability for service-level preflight checks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.yaml_loader import load_yaml_file

_ENV_ALIASES: dict[str, list[str]] = {
    "default": ["dev", "nonprod", "stage", "test"],
    "nonprod": ["dev", "stage", "test"],
    "local": ["dev", "test"],
    "prod": ["prod", "prod-adcuris"],
}

_RESERVED_SINK_SOURCE_KEYS = {"schemas"}
_SINK_KEY_PARTS_COUNT = 2


def list_sink_source_env_keys(source_cfg: dict[str, Any]) -> list[str]:
    """List sink source env keys (excluding non-env metadata keys)."""
    env_keys: list[str] = []
    for key, value in source_cfg.items():
        if key in _RESERVED_SINK_SOURCE_KEYS:
            continue
        if isinstance(value, dict):
            env_keys.append(str(key))
    return env_keys


def resolve_sink_env_key(
    source_cfg: dict[str, Any],
    env_name: str,
    target_sink_env: str | None = None,
) -> str:
    """Resolve sink-groups source environment key from runtime environment.

    If ``target_sink_env`` is provided it has strict precedence and must
    exist in the sink source topology.
    """
    available_envs = list_sink_source_env_keys(source_cfg)

    if target_sink_env is not None:
        requested = target_sink_env.strip()
        if requested and requested in source_cfg and isinstance(source_cfg.get(requested), dict):
            return requested
        raise ValueError(f"Invalid target_sink_env '{target_sink_env}'. " + f"Available sink envs: {available_envs}")

    if env_name in source_cfg and isinstance(source_cfg.get(env_name), dict):
        return env_name

    for candidate in _ENV_ALIASES.get(env_name, []):
        if candidate in source_cfg and isinstance(source_cfg.get(candidate), dict):
            return candidate

    raise ValueError(f"No sink-groups source environment mapping for env '{env_name}'. " + f"Available: {available_envs}")


def get_sink_target_env_keys(
    project_root: Path,
    sink_key: str,
) -> tuple[set[str] | None, str | None]:
    """Get available sink target env keys for a sink reference.

    Returns:
        tuple: (env_keys or None, warning message or None)
        - env_keys is None when sink topology is not available/initialized.
    """
    sink_parts = sink_key.split(".", 1)
    if len(sink_parts) != _SINK_KEY_PARTS_COUNT:
        return None, f"Invalid sink key '{sink_key}'"

    sink_group_name, sink_source_name = sink_parts
    sink_groups_path = project_root / "sink-groups.yaml"
    if not sink_groups_path.exists():
        return None, "sink-groups.yaml is missing"

    sink_groups_data = cast(dict[str, Any], load_yaml_file(sink_groups_path))
    sink_group_raw = sink_groups_data.get(sink_group_name)
    if not isinstance(sink_group_raw, dict):
        return None, f"Sink group '{sink_group_name}' not found in sink-groups.yaml"

    sink_group = cast(dict[str, Any], sink_group_raw)
    sources_raw = sink_group.get("sources")
    if not isinstance(sources_raw, dict):
        return None, f"Sink group '{sink_group_name}' has no sources topology"

    sources = cast(dict[str, Any], sources_raw)
    sink_source_raw = sources.get(sink_source_name)
    if not isinstance(sink_source_raw, dict):
        return None, (f"Sink source '{sink_source_name}' not found in sink group " + f"'{sink_group_name}'")

    sink_source = cast(dict[str, Any], sink_source_raw)
    env_keys = set(list_sink_source_env_keys(sink_source))
    if not env_keys:
        return None, (f"Sink source '{sink_group_name}.{sink_source_name}' has no environment entries")

    return env_keys, None


def get_all_sink_target_env_keys(project_root: Path) -> tuple[set[str] | None, str | None]:
    """Get the union of all sink target env keys defined in sink-groups.yaml."""
    sink_groups_path = project_root / "sink-groups.yaml"
    if not sink_groups_path.exists():
        return None, "sink-groups.yaml is missing"

    sink_groups_data = cast(dict[str, Any], load_yaml_file(sink_groups_path))
    env_keys: set[str] = set()

    for sink_group_raw in sink_groups_data.values():
        if not isinstance(sink_group_raw, dict):
            continue

        sink_group = cast(dict[str, Any], sink_group_raw)
        sources_raw = sink_group.get("sources")
        if not isinstance(sources_raw, dict):
            continue

        sources = cast(dict[str, Any], sources_raw)
        for sink_source_raw in sources.values():
            if not isinstance(sink_source_raw, dict):
                continue

            sink_source = cast(dict[str, Any], sink_source_raw)
            current_env_keys = set(list_sink_source_env_keys(sink_source))
            if current_env_keys:
                env_keys.update(current_env_keys)

    if not env_keys:
        return None, "No sink environment keys found in sink-groups.yaml"

    return env_keys, None
