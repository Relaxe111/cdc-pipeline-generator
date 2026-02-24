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

    When used in an inherited sink group (inherits: true),
    source_ref is just the server name (e.g., 'default').
    The source group is deduced from the sink group name:
    sink_asma → source group 'asma' → servers → 'default'

    Example:
        source_ref: default  # references source-groups.yaml → {source_group} → servers → default
    """

    source_ref: str  # server name within the source group
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
    # Discovery patterns (same as source groups)
    extraction_patterns: list[dict[str, object]]  # regex patterns for db name extraction


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

    Rules:
    - inherits: true marks this as an inherited sink group
    - Name MUST be sink_{source_group_name} (e.g., sink_asma for source group asma)
    - source_group is auto-deduced from the name (strip 'sink_' prefix)
    - Servers use source_ref with just the server name (not group/server path)
    - One inherited sink per source group (1:1 relationship)
    """

    # Inheritance: true = inherits from source group deduced from sink name
    inherits: bool  # True = inherited from source group
    # Auto-deduced (do not set manually):
    source_group: str  # deduced from sink name: sink_X → source group X
    pattern: str  # db-shared, db-per-tenant
    type: str  # postgres, mssql, http_client, http_server
    kafka_topology: str  # shared, per-server, per-service
    environment_aware: bool  # inherited from source group
    description: str
    # Required:
    servers: dict[str, SinkServerConfig]
    # Sources available for sinking (from source group)
    inherited_sources: list[str]  # source names from source group
    sources: dict[str, SinkSource]  # service_name → sink source config
    source_custom_keys: dict[str, dict[str, str]]


class SinkGroupStandalone(TypedDict, total=False):
    """Standalone sink group with its own servers.

    Used for external sinks (analytics warehouse, webhooks, etc.)
    that don't mirror source infrastructure.
    Standalone sinks do NOT have 'inherits: true'.
    """

    # Required for standalone:
    source_group: str  # which source group feeds this sink (for kafka topics)
    # Optional (auto-deduced if not specified):
    pattern: str  # db-shared, db-per-tenant
    type: str  # postgres, mssql, http_client, http_server
    kafka_topology: str  # optional — defaults to source group's topology
    environment_aware: bool  # inherited from source group
    description: str
    # Discovery/filtering patterns (same as source groups)
    database_exclude_patterns: list[str]  # regex patterns for excluding databases
    schema_exclude_patterns: list[str]  # regex patterns for excluding schemas
    table_exclude_patterns: list[str]  # regex patterns for excluding tables
    # Required:
    servers: dict[str, SinkServerConfig]
    sources: dict[str, SinkSource]
    source_custom_keys: dict[str, dict[str, str]]


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
    _resolved_from: str  # fully-qualified source path (source_group/server)


class ResolvedSinkGroup(TypedDict, total=False):
    """Sink group with all source_ref and inheritance resolved."""

    source_group: str
    pattern: str
    type: str
    kafka_topology: str
    description: str
    database_exclude_patterns: list[str]
    schema_exclude_patterns: list[str]
    table_exclude_patterns: list[str]
    servers: dict[str, ResolvedSinkServer]
    sources: dict[str, SinkSource]
    # Metadata
    _inherited_from: str | None  # source group name if inherited
