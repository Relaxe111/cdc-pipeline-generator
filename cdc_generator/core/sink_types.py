"""Type definitions for sink server groups.

Sink groups inherit structure from source server groups but add sink-specific
fields like source_ref for inherited connections and source_group linkage.
"""

from typing import TypedDict


# ============================================================================
# Sink Server Types
# ============================================================================
class SinkServerSourceRef(TypedDict, total=False):
    """Server that inherits connection from source server group.

    Example:
        source_ref: foo/default  # references source-groups.yaml → foo → servers → default
    """

    source_ref: str  # format: '<source_group>/<server_name>'
    # Optional overrides (override inherited values)
    host: str
    port: str
    user: str
    password: str


class SinkServerStandalone(TypedDict, total=False):
    """Standalone sink server (own connection config)."""

    type: str  # postgres, http_client, http_server
    host: str
    port: str
    user: str
    password: str
    # HTTP-specific
    base_url: str
    method: str
    headers: dict[str, str]


SinkServerConfig = SinkServerSourceRef | SinkServerStandalone


# ============================================================================
# Sink Source Types (mirrors source structure from source-groups.yaml)
# ============================================================================
class SinkSourceEnvironment(TypedDict, total=False):
    """Per-environment sink source configuration.

    Mirrors SourceEnvironment from server_group but for sink destinations.
    """

    server: str  # references sink group's servers section
    database: str  # for postgres sinks
    schema: str  # sink schema within database
    path: str  # for http sinks
    table_count: int  # optional — for validation


class SinkSource(TypedDict, total=False):
    """Service sink source configuration.

    Mirrors Source from source-groups.yaml - same structure, same terminology.
    Environment keys are dynamic (dev, prod, stage, etc.)
    """

    schemas: list[str]  # sink schemas
    # Dynamic environment keys
    dev: SinkSourceEnvironment
    prod: SinkSourceEnvironment
    stage: SinkSourceEnvironment
    test: SinkSourceEnvironment
    # ... other environments


# ============================================================================
# Sink Group Types
# ============================================================================
class SinkGroupInherited(TypedDict, total=False):
    """Sink group that inherits from a source server group.

    Used for db-shared patterns where sink structure mirrors source.
    All servers use source_ref to inherit connection config.

    Note: source_group, pattern, type, kafka_topology are auto-deduced:
    - source_group: from sink group name (strip 'sink_' prefix)
    - pattern: 'inherited' if any server has source_ref
    - type: from first server's connection string
    - kafka_topology: inherited from source group's server_group_type
    """

    # Optional (auto-deduced if not specified):
    source_group: str  # references source-groups.yaml top-level key
    pattern: str  # db-shared, db-per-tenant
    type: str  # postgres, mssql, http_client, http_server
    kafka_topology: str  # shared, per-server, per-service
    description: str
    # Required:
    servers: dict[str, SinkServerConfig]
    sources: dict[str, SinkSource]  # service_name → sink source config
    # Internal (for tracking)
    _inherited_services: list[str]  # services inherited from source group


class SinkGroupStandalone(TypedDict, total=False):
    """Standalone sink group with its own servers.

    Used for external sinks (analytics warehouse, webhooks, etc.)
    that don't mirror source infrastructure.

    Note: source_group, pattern, type, kafka_topology are auto-deduced:
    - source_group: required (must specify which source feeds this)
    - pattern: 'standalone' if no servers have source_ref
    - type: from first server's type field or connection string
    - kafka_topology: inherited from source group if not specified
    """

    # Required for standalone:
    source_group: str  # which source group feeds this sink (for kafka topics)
    # Optional (auto-deduced if not specified):
    pattern: str  # db-shared, db-per-tenant
    type: str  # postgres, mssql, http_client, http_server
    kafka_topology: str  # optional — defaults to source group's topology
    description: str
    # Required:
    servers: dict[str, SinkServerConfig]
    sources: dict[str, SinkSource]


SinkGroupConfig = SinkGroupInherited | SinkGroupStandalone


class SinkServerGroups(TypedDict):
    """Root structure of sink-groups.yaml file.

    Top-level keys are sink group names (e.g., sink_foo, sink_analytics).
    """

    # Dynamic keys — sink group names
    # Example: sink_foo, sink_analytics, etc.


# ============================================================================
# Resolution Types (runtime)
# ============================================================================
class ResolvedSinkServer(TypedDict, total=False):
    """Sink server config after source_ref resolution.

    All source_ref references are resolved to concrete connection values.
    """

    type: str
    host: str
    port: str
    user: str
    password: str
    kafka_bootstrap_servers: str
    # HTTP-specific
    base_url: str
    method: str
    headers: dict[str, str]
    # Metadata
    _source_ref: str  # original reference (for debugging)
    _resolved_from: str  # source group/server path


class ResolvedSinkGroup(TypedDict, total=False):
    """Sink group with all source_ref and inheritance resolved."""

    source_group: str
    pattern: str
    type: str
    kafka_topology: str
    description: str
    servers: dict[str, ResolvedSinkServer]
    sources: dict[str, SinkSource]
    # Metadata
    _inherited_from: str | None  # source group name if inherited
