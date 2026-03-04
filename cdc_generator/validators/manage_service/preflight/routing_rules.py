"""Sink routing preflight rule checks."""

from __future__ import annotations

from typing import Any, cast

from cdc_generator.core.sink_env_routing import get_sink_target_env_keys

from .types import (
    ValidationConfig,
    iter_source_env_entries,
    load_sink_group_config,
    load_source_group_context,
    project_root,
)

_SINK_KEY_PARTS_COUNT = 2


def collect_sink_routing_issues(
    service: str,
    config: ValidationConfig,
) -> tuple[list[str], list[str]]:
    """Collect sink routing preflight errors and warnings."""
    errors: list[str] = []
    warnings: list[str] = []

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        return errors, warnings

    source_context = load_source_group_context(service, config)
    server_group_name = source_context["server_group_name"]
    source_group_cfg = source_context["source_group_cfg"]
    if source_group_cfg is None:
        if server_group_name is not None:
            warnings.append(
                f"Source group '{server_group_name}' not found in source-groups.yaml; skipping sink routing preflight"
            )
        return errors, warnings

    source_env_aware = bool(source_group_cfg.get("environment_aware", False))
    sources_raw = source_group_cfg.get("sources")
    if not isinstance(sources_raw, dict):
        return errors, warnings

    sources = cast(dict[str, Any], sources_raw)
    for sink_key_raw in sinks_raw:
        sink_key = str(sink_key_raw)
        sink_parts = sink_key.split(".", 1)
        if len(sink_parts) != _SINK_KEY_PARTS_COUNT:
            continue

        sink_group_name = sink_parts[0]
        sink_group_cfg = load_sink_group_config(sink_group_name)
        if sink_group_cfg is None:
            warnings.append(
                f"Sink group '{sink_group_name}' not available in sink-groups.yaml; skipping sink routing preflight for '{sink_key}'"
            )
            continue

        sink_env_aware = bool(sink_group_cfg.get("environment_aware", False))
        if not sink_env_aware or source_env_aware:
            continue

        sink_target_envs, topology_warning = get_sink_target_env_keys(project_root(), sink_key)
        if sink_target_envs is None:
            warning_message = topology_warning if topology_warning is not None else "sink topology unavailable"
            warnings.append(f"Sink routing preflight warning for '{sink_key}': {warning_message}")
            continue

        for source_name, source_entry_raw in sources.items():
            if not isinstance(source_entry_raw, dict):
                continue
            source_entry = cast(dict[str, Any], source_entry_raw)
            for env_name, env_cfg in iter_source_env_entries(source_entry):
                raw_target_env = env_cfg.get("target_sink_env")
                target_env = str(raw_target_env).strip() if isinstance(raw_target_env, str) else ""
                route_label = f"{source_name}.{env_name}"
                if not target_env:
                    errors.append(
                        f"{sink_key}: missing required 'target_sink_env' for source route '{route_label}' "
                        + "(source group is not environment-aware, sink group is environment-aware)"
                    )
                    continue

                if target_env not in sink_target_envs:
                    errors.append(
                        f"{sink_key}: invalid target_sink_env '{target_env}' for source route '{route_label}'. "
                        + f"Available sink envs: {sorted(sink_target_envs)}"
                    )

    return errors, warnings
