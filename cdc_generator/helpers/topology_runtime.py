"""Normalized topology and runtime resolution helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, cast

BrokerTopology = Literal["shared", "per-server", "per-service"]
Topology = Literal["redpanda", "fdw", "pg_native"]
TopologyKind = Literal[
    "brokered_redpanda",
    "mssql_fdw_pull",
    "pg_logical",
    "unknown",
]
RuntimeMode = Literal["brokered", "native"]
RuntimeEngine = Literal[
    "bento",
    "redpanda_connect",
    "postgres_native",
    "bun_runner",
    "pg_cron",
    "unknown",
]

_BROKER_TOPOLOGY_VALUES = {"shared", "per-server", "per-service"}
_TOPOLOGY_VALUES = {"redpanda", "fdw", "pg_native"}
_TOPOLOGY_KIND_VALUES = {
    "brokered_redpanda",
    "mssql_fdw_pull",
    "pg_logical",
    "unknown",
}
_RUNTIME_MODE_VALUES = {"brokered", "native"}
_RUNTIME_ENGINE_VALUES = {
    "bento",
    "redpanda_connect",
    "postgres_native",
    "bun_runner",
    "pg_cron",
    "unknown",
}
_TOPOLOGY_TO_KIND: dict[Topology, TopologyKind] = {
    "redpanda": "brokered_redpanda",
    "fdw": "mssql_fdw_pull",
    "pg_native": "pg_logical",
}
_KIND_TO_TOPOLOGY: dict[TopologyKind, Topology | None] = {
    "brokered_redpanda": "redpanda",
    "mssql_fdw_pull": "fdw",
    "pg_logical": "pg_native",
    "unknown": None,
}
_SUPPORTED_TOPOLOGIES_BY_SOURCE_TYPE: dict[str, tuple[Topology, ...]] = {
    "mssql": ("redpanda", "fdw"),
    "postgres": ("redpanda", "pg_native"),
}


def resolve_broker_topology(
    config: Mapping[str, Any],
    *,
    topology: Topology | None = None,
    source_group: Mapping[str, Any] | None = None,
    default: BrokerTopology = "shared",
) -> BrokerTopology | None:
    """Resolve broker fan-out mode only when the effective topology is redpanda."""
    effective_topology = topology or resolve_topology(
        config,
        source_group=source_group,
    )
    if not topology_uses_broker(effective_topology):
        return None

    value = config.get("broker_topology")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _BROKER_TOPOLOGY_VALUES:
            return cast(BrokerTopology, normalized)

    if source_group is not None:
        source_value = source_group.get("broker_topology")
        if isinstance(source_value, str):
            normalized_source_value = source_value.strip().lower()
            if normalized_source_value in _BROKER_TOPOLOGY_VALUES:
                return cast(BrokerTopology, normalized_source_value)

    return default


def topology_uses_broker(topology: Topology | None) -> bool:
    """Return whether the selected topology uses broker-specific configuration."""
    return topology == "redpanda"


def resolve_topology(
    config: Mapping[str, Any],
    *,
    source_group: Mapping[str, Any] | None = None,
    runtime_mode: str | None = None,
    source_type: str | None = None,
) -> Topology | None:
    """Resolve the user-facing topology name.

    Resolution order:
    1. explicit ``topology`` name on config or source group
    2. explicit internal ``topology_kind`` mapping
    3. runtime mode + source type when provided
    4. brokered source-group defaults
    """
    explicit = _read_explicit_topology(config)
    if explicit is not None:
        return explicit

    explicit_source = _read_explicit_topology(source_group)
    if explicit_source is not None:
        return explicit_source

    explicit_kind = _read_explicit_topology_kind(config)
    if explicit_kind is not None:
        return _KIND_TO_TOPOLOGY.get(explicit_kind)

    explicit_source_kind = _read_explicit_topology_kind(source_group)
    if explicit_source_kind is not None:
        return _KIND_TO_TOPOLOGY.get(explicit_source_kind)

    normalized_runtime_mode = _normalize_text(runtime_mode)
    normalized_source_type = _resolve_source_type_name(
        config,
        source_group=source_group,
        source_type=source_type,
    )

    if normalized_runtime_mode == "native":
        if normalized_source_type == "mssql":
            return "fdw"
        if normalized_source_type == "postgres":
            return "pg_native"

    if normalized_runtime_mode == "brokered":
        return "redpanda"

    if _has_brokered_config(config) or _has_brokered_config(source_group):
        return "redpanda"

    return None


def resolve_runtime_mode(
    config: Mapping[str, Any],
    *,
    topology: Topology | None = None,
    source_group: Mapping[str, Any] | None = None,
    runtime_mode: str | None = None,
    source_type: str | None = None,
    default: RuntimeMode = "brokered",
) -> RuntimeMode:
    """Resolve the internal runtime mode from the user-facing topology."""
    normalized_runtime_mode = _normalize_text(runtime_mode)
    if normalized_runtime_mode in _RUNTIME_MODE_VALUES:
        return cast(RuntimeMode, normalized_runtime_mode)

    effective_topology = topology or resolve_topology(
        config,
        source_group=source_group,
        source_type=source_type,
    )
    if effective_topology == "redpanda":
        return "brokered"
    if effective_topology in {"fdw", "pg_native"}:
        return "native"
    return default


def supported_topologies_for_source_type(source_type: str) -> tuple[Topology, ...]:
    """Return valid user-facing topologies for a source type."""
    return _SUPPORTED_TOPOLOGIES_BY_SOURCE_TYPE.get(
        _normalize_text(source_type),
        tuple(),
    )


def topology_supported_for_source_type(
    topology: Topology,
    source_type: str,
) -> bool:
    """Return whether a topology is valid for the given source type."""
    supported = supported_topologies_for_source_type(source_type)
    if not supported:
        return True
    return topology in supported


def resolve_topology_kind(
    config: Mapping[str, Any],
    *,
    source_group: Mapping[str, Any] | None = None,
    runtime_mode: str | None = None,
    source_type: str | None = None,
) -> TopologyKind:
    """Resolve the high-level topology kind.

    Resolution order:
    1. explicit ``topology_kind`` or nested ``topology.kind``
    2. explicit user-facing ``topology`` name on config or source group
    3. migration runtime + source type when provided
    4. current brokered source/sink config defaults
    """
    explicit = _read_explicit_topology_kind(config)
    if explicit is not None:
        return explicit

    explicit_source = _read_explicit_topology_kind(source_group)
    if explicit_source is not None:
        return explicit_source

    explicit_topology = _read_explicit_topology(config)
    if explicit_topology is not None:
        return _TOPOLOGY_TO_KIND[explicit_topology]

    explicit_source_topology = _read_explicit_topology(source_group)
    if explicit_source_topology is not None:
        return _TOPOLOGY_TO_KIND[explicit_source_topology]

    normalized_runtime_mode = _normalize_text(runtime_mode)
    normalized_source_type = _resolve_source_type_name(
        config,
        source_group=source_group,
        source_type=source_type,
    )

    if normalized_runtime_mode == "native":
        if normalized_source_type == "mssql":
            return "mssql_fdw_pull"
        if normalized_source_type == "postgres":
            return "pg_logical"

    if normalized_runtime_mode == "brokered":
        return "brokered_redpanda"

    if _has_brokered_config(config) or _has_brokered_config(source_group):
        return "brokered_redpanda"

    return "unknown"


def resolve_runtime_engine(
    config: Mapping[str, Any],
    *,
    topology_kind: TopologyKind | None = None,
    runtime_mode: str | None = None,
) -> RuntimeEngine:
    """Resolve runtime engine from explicit config or topology defaults."""
    explicit = _read_explicit_runtime_engine(config)
    if explicit is not None:
        return explicit

    normalized_runtime_mode = _normalize_text(runtime_mode)
    effective_topology = topology_kind or resolve_topology_kind(
        config,
        runtime_mode=runtime_mode,
    )

    if normalized_runtime_mode == "native":
        return "postgres_native"

    if effective_topology in {"mssql_fdw_pull", "pg_logical"}:
        return "postgres_native"

    if effective_topology == "brokered_redpanda":
        return "bento"

    return "unknown"


def _read_explicit_topology(
    config: Mapping[str, Any] | None,
) -> Topology | None:
    if config is None:
        return None

    value = config.get("topology")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TOPOLOGY_VALUES:
            return cast(Topology, normalized)

    if isinstance(value, Mapping):
        for key in ("name", "value"):
            name = value.get(key)
            if not isinstance(name, str):
                continue
            normalized = name.strip().lower()
            if normalized in _TOPOLOGY_VALUES:
                return cast(Topology, normalized)

    return None


def _read_explicit_topology_kind(
    config: Mapping[str, Any] | None,
) -> TopologyKind | None:
    if config is None:
        return None

    value = config.get("topology_kind")
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TOPOLOGY_KIND_VALUES:
            return cast(TopologyKind, normalized)

    topology_raw = config.get("topology")
    if isinstance(topology_raw, Mapping):
        topology_kind = topology_raw.get("kind")
        if isinstance(topology_kind, str):
            normalized = topology_kind.strip().lower()
            if normalized in _TOPOLOGY_KIND_VALUES:
                return cast(TopologyKind, normalized)

    return None


def _resolve_source_type_name(
    config: Mapping[str, Any],
    *,
    source_group: Mapping[str, Any] | None = None,
    source_type: str | None = None,
) -> str:
    return _normalize_text(
        source_type
        or config.get("type")
        or config.get("server_type")
        or (source_group or {}).get("type")
        or (source_group or {}).get("server_type"),
    )


def _read_explicit_runtime_engine(
    config: Mapping[str, Any] | None,
) -> RuntimeEngine | None:
    if config is None:
        return None

    for key in ("runtime_engine", "runtime"):
        value = config.get(key)
        if not isinstance(value, str):
            continue
        normalized = value.strip().lower()
        if normalized in _RUNTIME_ENGINE_VALUES:
            return cast(RuntimeEngine, normalized)

    runtime_raw = config.get("runtime")
    if isinstance(runtime_raw, Mapping):
        runtime_engine = runtime_raw.get("engine")
        if isinstance(runtime_engine, str):
            normalized = runtime_engine.strip().lower()
            if normalized in _RUNTIME_ENGINE_VALUES:
                return cast(RuntimeEngine, normalized)

    return None


def _has_brokered_config(config: Mapping[str, Any] | None) -> bool:
    if config is None:
        return False
    if "broker_topology" in config:
        return True
    servers = config.get("servers")
    if isinstance(servers, Mapping):
        for server_config in servers.values():
            if isinstance(server_config, Mapping) and "kafka_bootstrap_servers" in server_config:
                return True
    return False


def _normalize_text(value: object | None) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()