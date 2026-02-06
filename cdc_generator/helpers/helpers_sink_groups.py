"""Helper functions for sink server group operations.

Handles:
- Loading sink group configurations
- Resolving source_ref references to source servers
- Inheriting services from source groups
- Validating sink group structure
"""

from pathlib import Path
from typing import Any, cast

from cdc_generator.core.sink_types import (
    ResolvedSinkGroup,
    ResolvedSinkServer,
    SinkGroupConfig,
    SinkServerConfig,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file, save_yaml_file

# Source reference format: "<group>/<server>"
SOURCE_REF_PARTS_COUNT = 2


def deduce_source_group(sink_group_name: str) -> str | None:
    """Deduce source group name from sink group name.

    Args:
        sink_group_name: Name of the sink group (e.g., 'sink_adopus')

    Returns:
        Source group name if deducible, None otherwise

    Example:
        'sink_adopus' → 'adopus'
        'sink_foo' → 'foo'
        'analytics' → None (no 'sink_' prefix)
    """
    if sink_group_name.startswith("sink_"):
        return sink_group_name[5:]  # strip 'sink_' prefix
    return None


def deduce_pattern(sink_group: SinkGroupConfig) -> str:
    """Deduce pattern from sink group configuration.

    Args:
        sink_group: Sink group configuration

    Returns:
        'inherited' if any server has source_ref, 'standalone' otherwise
    """
    servers = sink_group.get("servers", {})
    for server_config in servers.values():
        if "source_ref" in server_config:
            return "inherited"
    return "standalone"


def deduce_type(
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any],
) -> str | None:
    """Deduce database type from server configurations.

    Args:
        sink_group: Sink group configuration
        source_groups: All source server groups (for resolving source_ref)

    Returns:
        Database type if deducible, None otherwise

    Logic:
        1. If server has 'type' field → use it
        2. If server has source_ref → resolve and check connection string
        3. Parse connection string: 'postgresql://...' → 'postgresql'
    """
    servers = sink_group.get("servers", {})
    if not servers:
        return None

    # Check first server
    first_server = next(iter(servers.values()))

    # Explicit type field
    if "type" in first_server:
        return str(first_server["type"])

    # Resolve source_ref
    if "source_ref" in first_server:
        try:
            source_ref = first_server["source_ref"]
            resolved = resolve_source_ref(source_ref, source_groups)
            if "type" in resolved:
                return str(resolved["type"])
        except ValueError:
            return None

    return None


def deduce_kafka_topology(
    source_group_name: str | None,
    source_groups: dict[str, Any],
) -> str | None:
    """Deduce kafka_topology from source group's server_group_type.

    Args:
        source_group_name: Name of the source group
        source_groups: All source server groups

    Returns:
        Kafka topology if deducible, None otherwise

    Mapping:
        db-per-tenant → multi-tenant
        db-shared → per-server
    """
    if not source_group_name or source_group_name not in source_groups:
        return None

    source_group = source_groups[source_group_name]
    server_group_type = source_group.get("server_group_type")

    if server_group_type == "db-per-tenant":
        return "multi-tenant"
    if server_group_type == "db-shared":
        return "per-server"

    return None


def deduce_environment_aware(
    source_group_name: str | None,
    source_groups: dict[str, Any],
) -> bool:
    """Deduce environment_aware from source group.

    Args:
        source_group_name: Name of source group to inherit from
        source_groups: All source server groups

    Returns:
        environment_aware value from source group, defaults to False
    """
    if not source_group_name or source_group_name not in source_groups:
        return False

    source_group = source_groups[source_group_name]
    return bool(source_group.get("environment_aware", False))


def load_sink_groups(sink_file_path: Path) -> dict[str, SinkGroupConfig]:
    """Load sink server groups from sink-groups.yaml.

    Args:
        sink_file_path: Path to sink-groups.yaml

    Returns:
        Dictionary of sink group name → sink group config

    Raises:
        FileNotFoundError: If sink file doesn't exist
        ValueError: If YAML is invalid
    """
    data = load_yaml_file(sink_file_path)
    return cast(dict[str, SinkGroupConfig], data)


def save_sink_groups(
    sink_groups: dict[str, SinkGroupConfig],
    sink_file_path: Path,
) -> None:
    """Save sink server groups to sink-groups.yaml.

    Args:
        sink_groups: Dictionary of sink group configurations
        sink_file_path: Path to sink-groups.yaml
    """
    save_yaml_file(cast(dict[str, Any], sink_groups), sink_file_path)


def resolve_source_ref(
    source_ref: str,
    source_groups: dict[str, Any],
) -> dict[str, Any]:
    """Resolve source_ref to concrete server configuration.

    Args:
        source_ref: Reference string in format '<group>/<server>'
        source_groups: All source server groups from source-groups.yaml

    Returns:
        Resolved server config (excludes extraction_patterns)

    Raises:
        ValueError: If source_ref format is invalid or reference not found

    Example:
        source_ref: foo/default
        → resolves to source-groups.yaml → foo → servers → default
    """
    parts = source_ref.split("/")
    if len(parts) != SOURCE_REF_PARTS_COUNT:
        msg = (
            f"Invalid source_ref '{source_ref}'. "
            f"Expected format: '<group>/<server>'"
        )
        raise ValueError(msg)

    group_name, server_name = parts

    if group_name not in source_groups:
        msg = (
            f"source_ref references unknown source group '{group_name}'. "
            f"Available groups: {list(source_groups.keys())}"
        )
        raise ValueError(msg)

    group = source_groups[group_name]
    servers = group.get("servers", {})

    if server_name not in servers:
        msg = (
            f"source_ref references unknown server '{server_name}' "
            f"in source group '{group_name}'. "
            f"Available servers: {list(servers.keys())}"
        )
        raise ValueError(msg)

    # Copy server config (including extraction_patterns for standalone sinks)
    source_config = dict(servers[server_name])

    return source_config


def resolve_sink_server(
    server_config: SinkServerConfig,
    source_groups: dict[str, Any],
) -> ResolvedSinkServer:
    """Resolve sink server config (handles source_ref inheritance).

    Args:
        server_config: Sink server configuration (may have source_ref)
        source_groups: All source server groups

    Returns:
        Fully resolved server config with all connection details
    """
    if "source_ref" in server_config:
        # Inherited server — resolve reference
        source_ref = server_config["source_ref"]
        resolved = resolve_source_ref(source_ref, source_groups)

        # Apply overrides from sink config
        for key, value in server_config.items():
            if key != "source_ref":
                resolved[key] = value

        # Add metadata
        resolved["_source_ref"] = source_ref
        resolved["_resolved_from"] = source_ref

        return cast(ResolvedSinkServer, resolved)

    # Standalone server — use as-is
    return cast(ResolvedSinkServer, dict(server_config))


def resolve_sink_group(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any],
) -> ResolvedSinkGroup:
    """Resolve sink group (resolve all source_ref, inherit from source group).

    Auto-deduces missing fields:
    - source_group: from sink group name (strip 'sink_' prefix)
    - pattern: 'inherited' if any server has source_ref
    - type: from first server's type or connection string
    - kafka_topology: from source group's server_group_type

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration
        source_groups: All source server groups

    Returns:
        Fully resolved sink group with inherited values
    """
    resolved: dict[str, Any] = dict(sink_group)

    # Deduce source_group if not specified
    if "source_group" not in resolved:
        deduced_source = deduce_source_group(sink_group_name)
        if deduced_source:
            resolved["source_group"] = deduced_source

    source_group_name = resolved.get("source_group")

    # Deduce pattern if not specified
    if "pattern" not in resolved:
        resolved["pattern"] = deduce_pattern(sink_group)

    # Deduce type if not specified
    if "type" not in resolved:
        deduced_type = deduce_type(sink_group, source_groups)
        if deduced_type:
            resolved["type"] = deduced_type

    # Deduce kafka_topology if not specified
    if "kafka_topology" not in resolved:
        deduced_topology = deduce_kafka_topology(
            source_group_name,
            source_groups,
        )
        if deduced_topology:
            resolved["kafka_topology"] = deduced_topology

    # Deduce environment_aware if not specified
    if "environment_aware" not in resolved:
        resolved["environment_aware"] = deduce_environment_aware(
            source_group_name,
            source_groups,
        )

    # Resolve all servers
    resolved_servers: dict[str, ResolvedSinkServer] = {}
    for server_name, server_config in sink_group.get("servers", {}).items():
        resolved_servers[server_name] = resolve_sink_server(
            server_config,
            source_groups,
        )
    resolved["servers"] = resolved_servers

    # Validate source group exists if specified
    if source_group_name:
        if source_group_name not in source_groups:
            msg = (
                f"Sink group '{sink_group_name}' references unknown "
                f"source group '{source_group_name}'. "
                f"Available: {list(source_groups.keys())}"
            )
            raise ValueError(msg)

        # Add metadata
        if sink_group_name.startswith("sink_"):
            resolved["_inherited_from"] = source_group_name

    return cast(ResolvedSinkGroup, resolved)


def should_inherit_services(source_group: dict[str, Any]) -> bool:
    """Determine if sink should inherit services from source group.

    Args:
        source_group: Source server group configuration

    Returns:
        True if services should be inherited (db-shared)
        False if not (db-per-tenant)

    Rules:
        - db-shared: services map 1:1 → inherit
        - db-per-tenant: customer-based → skip (doesn't make sense)
    """
    pattern = source_group.get("pattern", "")
    return pattern == "db-shared"


def create_inherited_sink_group(
    source_group_name: str,
    source_group: dict[str, Any],
    source_services: dict[str, Any],
) -> SinkGroupConfig:
    """Create sink group scaffold that inherits from source group.

    Args:
        source_group_name: Name of source group (e.g., 'foo')
        source_group: Source group configuration
        source_services: Services from source group

    Returns:
        Sink group configuration with inherited servers/services

    Only called for db-shared patterns.
    """
    sink_group: dict[str, Any] = {
        "source_group": source_group_name,
        "pattern": source_group.get("pattern"),
        "type": source_group.get("type"),
        "kafka_topology": source_group.get("kafka_topology"),
        "description": (
            f"Sink group mirroring source group '{source_group_name}' "
            f"— same servers, sink sources"
        ),
    }

    # Add all servers with source_ref
    servers: dict[str, SinkServerConfig] = {}
    for server_name in source_group.get("servers", {}):
        servers[server_name] = cast(
            SinkServerConfig,
            {"source_ref": f"{source_group_name}/{server_name}"},
        )
    sink_group["servers"] = servers

    # Track inherited services (for documentation)
    sink_group["_inherited_services"] = list(source_services.keys())

    # Empty sources — user fills in which services to actually sink
    sink_group["sources"] = {}

    return cast(SinkGroupConfig, sink_group)


def create_standalone_sink_group(
    sink_group_name: str,
    source_group_name: str,
    sink_type: str = "postgres",
    pattern: str = "db-shared",
    environment_aware: bool = False,
    database_exclude_patterns: list[str] | None = None,
    schema_exclude_patterns: list[str] | None = None,
) -> SinkGroupConfig:
    """Create standalone sink group scaffold (own servers).

    Args:
        sink_group_name: Name for the new sink group
        source_group_name: Source group this sink consumes from
        sink_type: Type of sink (postgres, http_client, etc.)
        pattern: Pattern for sink group (db-shared or db-per-tenant)
        environment_aware: Enable environment-aware grouping
        database_exclude_patterns: Regex patterns for excluding databases
        schema_exclude_patterns: Regex patterns for excluding schemas

    Returns:
        Empty sink group scaffold for manual configuration
    """
    sink_group: dict[str, Any] = {
        "source_group": source_group_name,
        "pattern": pattern,
        "type": sink_type,
        "environment_aware": environment_aware,
        "description": f"Standalone sink group for {sink_group_name}",
        "servers": {},
        "sources": {},
    }
    
    # Add exclude patterns if provided
    if database_exclude_patterns:
        sink_group["database_exclude_patterns"] = database_exclude_patterns
    if schema_exclude_patterns:
        sink_group["schema_exclude_patterns"] = schema_exclude_patterns
    
    # Note: servers inherit 'type' from sink group level, no need to specify per-server

    return cast(SinkGroupConfig, sink_group)


def validate_sink_group_structure(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
) -> list[str]:
    """Validate sink group structure.

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration

    Returns:
        List of validation error messages (empty if valid)
    """
    errors: list[str] = []

    # Required fields
    if "source_group" not in sink_group:
        errors.append(
            f"Sink group '{sink_group_name}' missing required field 'source_group'"
        )

    if "servers" not in sink_group:
        errors.append(
            f"Sink group '{sink_group_name}' missing required field 'servers'"
        )

    # Validate servers
    for server_name, server_config in sink_group.get("servers", {}).items():
        has_source_ref = "source_ref" in server_config
        has_connection = any(
            k in server_config for k in ["host", "base_url", "type"]
        )

        if not has_source_ref and not has_connection:
            msg = (f"Server '{server_name}' in sink group '{sink_group_name}' "
                   f"must have either 'source_ref' or connection config "
                   f"(host/base_url/type)")
            errors.append(msg)

    # Validate sources reference existing servers
    for service_name, source in sink_group.get("sources", {}).items():
        for env_name, env_config in source.items():
            if env_name in ["schemas"]:
                continue

            if isinstance(env_config, dict):
                env_dict = cast(dict[str, Any], env_config)
                server_ref = env_dict.get("server")
                if server_ref and server_ref not in sink_group.get("servers", {}):
                    msg = (f"Source '{service_name}.{env_name}' references "
                           f"unknown server '{server_ref}'")
                    errors.append(msg)

    return errors
