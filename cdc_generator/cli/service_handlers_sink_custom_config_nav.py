"""Config navigation helpers for custom sink table handlers."""

from __future__ import annotations

from typing import cast


def get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return the sinks section, creating it if absent."""
    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        config["sinks"] = {}
        return cast(dict[str, object], config["sinks"])
    return cast(dict[str, object], sinks_raw)


def get_sink_tables(sink_cfg: dict[str, object]) -> dict[str, object]:
    """Return tables dict inside a sink config, creating if absent."""
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        sink_cfg["tables"] = {}
        return cast(dict[str, object], sink_cfg["tables"])
    return cast(dict[str, object], tables_raw)


def resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None if not found."""
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        return None
    return cast(dict[str, object], sink_raw)


def extract_target_service(
    sink_key: str,
    expected_parts: int = 2,
) -> str | None:
    """Extract target_service from sink key format sink_group.target_service."""
    parts = sink_key.split(".", 1)
    if len(parts) != expected_parts:
        return None
    return parts[1]
