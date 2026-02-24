"""Helper functions for sink server group operations.

Handles:
- Loading sink group configurations
- Resolving source_ref references to source servers
- Inheriting services from source groups
- Validating sink group structure
"""

from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any, TypedDict, cast

from cdc_generator.core.sink_types import (
    ResolvedSinkGroup,
    ResolvedSinkServer,
    SinkGroupConfig,
    SinkServerConfig,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file, yaml

_SERVICES_PREVIEW_LIMIT = 20


class StandaloneSinkGroupOptions(TypedDict, total=False):
    """Optional configuration for standalone sink groups."""

    sink_type: str
    pattern: str
    environment_aware: bool
    database_exclude_patterns: list[str] | None
    schema_exclude_patterns: list[str] | None
    table_exclude_patterns: list[str] | None


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
        Dictionary of sink group name → sink group config.
        Returns empty dict if file exists but is empty.

    Raises:
        FileNotFoundError: If sink file doesn't exist
        ValueError: If YAML is invalid
    """
    data = load_yaml_file(sink_file_path)
    if not data:
        return {}
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
    sink_file_path.parent.mkdir(parents=True, exist_ok=True)

    output_lines: list[str] = _build_sink_file_header_comments()
    output_lines.append(
        "# Updated at: "
        + datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    )

    for sink_group_name, sink_group in sink_groups.items():
        output_lines.append("")
        output_lines.extend(
            _build_sink_group_metadata_comments(sink_group_name, sink_group)
        )

        sink_group_buffer = StringIO()
        yaml.dump({sink_group_name: sink_group}, sink_group_buffer)
        sink_group_yaml = sink_group_buffer.getvalue().rstrip()
        output_lines.extend(sink_group_yaml.splitlines())

    with sink_file_path.open("w", encoding="utf-8") as sink_file:
        sink_file.write("\n".join(output_lines).rstrip() + "\n")


def _build_sink_file_header_comments() -> list[str]:
    """Build the standard header comments for sink-groups.yaml."""
    return [
        "# ============================================================================",
        "# AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY",
        "# Use 'cdc manage-sink-groups' commands to modify this file",
        "# ============================================================================",
        "# ",
        "# This file contains the sink group configuration for CDC pipelines.",
        "# Changes made directly to this file may be overwritten by CLI commands.",
        "# ",
        "# Common commands:",
        "#   - cdc manage-sink-groups --update                    # Refresh sink databases into sources",
        "#   - cdc manage-sink-groups --info <sink-group>         # Show sink-group details",
        "#   - cdc manage-sink-groups --add-to-ignore-list        # Add database exclude patterns",
        "#   - cdc manage-sink-groups --add-to-schema-excludes    # Add schema exclude patterns",
        "#   - cdc manage-sink-groups --add-to-table-excludes     # Add table exclude patterns",
        "# ",
        "# For detailed documentation, see:",
        "#   - CDC_CLI.md in the implementation repository",
        "#   - cdc-pipeline-generator/_docs/ for generator documentation",
        "# ============================================================================",
    ]


def _build_sink_group_metadata_comments(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
) -> list[str]:
    """Build metadata comment block for a single sink group."""
    services = _extract_sink_group_services(sink_group)
    warnings = get_sink_group_warnings(sink_group_name, sink_group)

    pattern = str(sink_group.get("pattern", "unknown"))
    sink_type = str(sink_group.get("type", "unknown"))
    servers = sink_group.get("servers", {})
    server_count = len(servers) if isinstance(servers, dict) else 0

    lines = [
        "# ----------------------------------------------------------------------------",
        f"# Sink Group: {sink_group_name}",
        (
            f"# Type: {sink_type} | Pattern: {pattern}"
            + f" | Servers: {server_count} | Services: {len(services)}"
        ),
    ]

    if services:
        services_preview = ", ".join(services[:_SERVICES_PREVIEW_LIMIT])
        if len(services) > _SERVICES_PREVIEW_LIMIT:
            services_preview += (
                f", ... (+{len(services) - _SERVICES_PREVIEW_LIMIT} more)"
            )
        lines.append(f"# Services ({len(services)}): {services_preview}")

    if warnings:
        for warning in warnings:
            lines.append(f"# ! Warning: {warning}")
    else:
        lines.append("# * Warnings: none")

    lines.append("# ----------------------------------------------------------------------------")
    return lines


def _extract_sink_group_services(sink_group: SinkGroupConfig) -> list[str]:
    """Extract service names from sink group configuration for metadata."""
    inherited_services = sink_group.get("inherited_sources", [])
    if isinstance(inherited_services, list):
        inherited = [
            str(service).strip()
            for service in cast(list[object], inherited_services)
            if str(service).strip()
        ]
        if inherited:
            return sorted(set(inherited))

    sources = sink_group.get("sources", {})
    if isinstance(sources, dict):
        service_names = [
            str(service_name).strip()
            for service_name in cast(dict[str, object], sources)
            if str(service_name).strip()
        ]
        return sorted(set(service_names))

    return []


def resolve_source_ref(
    source_ref: str,
    source_groups: dict[str, Any],
    source_group_name: str | None = None,
) -> dict[str, Any]:
    """Resolve source_ref to concrete server configuration.

    For inherited sink groups (inherits: true), source_ref is just the
    server name, and source_group_name is deduced from the sink name.

    Args:
        source_ref: Server name (e.g., 'default') for inherited sinks
        source_groups: All source server groups from source-groups.yaml
        source_group_name: Source group to look up (deduced from sink name)

    Returns:
        Resolved server config

    Raises:
        ValueError: If reference not found
    """
    if not source_group_name:
        msg = (
            f"Cannot resolve source_ref '{source_ref}': "
            f"no source_group_name provided"
        )
        raise ValueError(msg)

    if source_group_name not in source_groups:
        msg = (
            f"source_ref references unknown source group '{source_group_name}'. "
            f"Available groups: {list(source_groups.keys())}"
        )
        raise ValueError(msg)

    group = source_groups[source_group_name]
    servers = group.get("servers", {})
    server_name = source_ref

    if server_name not in servers:
        msg = (
            f"source_ref references unknown server '{server_name}' "
            f"in source group '{source_group_name}'. "
            f"Available servers: {list(servers.keys())}"
        )
        raise ValueError(msg)

    return dict(servers[server_name])


def resolve_sink_server(
    server_config: SinkServerConfig,
    source_groups: dict[str, Any],
    source_group_name: str | None = None,
) -> ResolvedSinkServer:
    """Resolve sink server config (handles source_ref inheritance).

    Args:
        server_config: Sink server configuration (may have source_ref)
        source_groups: All source server groups
        source_group_name: Source group name (for inherited sinks)

    Returns:
        Fully resolved server config with all connection details
    """
    if "source_ref" in server_config:
        # Inherited server — resolve reference
        source_ref = server_config["source_ref"]
        resolved = resolve_source_ref(
            source_ref, source_groups, source_group_name,
        )

        # Apply overrides from sink config
        for key, value in server_config.items():
            if key != "source_ref":
                resolved[key] = value

        # Add metadata
        full_ref = f"{source_group_name}/{source_ref}" if source_group_name else source_ref
        resolved["_source_ref"] = full_ref
        resolved["_resolved_from"] = full_ref

        return cast(ResolvedSinkServer, resolved)

    # Standalone server — use as-is
    return cast(ResolvedSinkServer, dict(server_config))


def _deduce_source_group_for_resolution(
    sink_group_name: str,
    resolved: dict[str, Any],
) -> str | None:
    """Deduce source_group for a sink group during resolution.

    Rules:
    - If inherits=True → deduce from sink name (sink_asma → asma)
    - If explicit source_group → use that
    - Otherwise → deduce from sink name
    """
    # Explicit source_group always wins
    explicit = resolved.get("source_group")
    if explicit:
        return str(explicit)
    # Deduce from sink name (sink_asma → asma)
    return deduce_source_group(sink_group_name)


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
        deduced = _deduce_source_group_for_resolution(
            sink_group_name, resolved,
        )
        if deduced:
            resolved["source_group"] = deduced

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
            source_group_name=str(source_group_name) if source_group_name else None,
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
        "inherits": True,
        "description": (
            f"Sink group inheriting from source group '{source_group_name}' "
            f"— servers resolved via source_ref"
        ),
    }

    # Add all servers with source_ref (server name only, source group
    # is deduced from sink name: sink_{source_group_name})
    servers: dict[str, SinkServerConfig] = {}
    for server_name in source_group.get("servers", {}):
        servers[server_name] = cast(
            SinkServerConfig,
            {"source_ref": server_name},
        )
    sink_group["servers"] = servers

    # Track inherited sources (required — must match source group sources)
    sink_group["inherited_sources"] = list(source_services.keys())

    return cast(SinkGroupConfig, sink_group)


def create_standalone_sink_group(
    sink_group_name: str,
    source_group_name: str,
    options: StandaloneSinkGroupOptions | None = None,
) -> SinkGroupConfig:
    """Create standalone sink group scaffold (own servers).

    Args:
        sink_group_name: Name for the new sink group
        source_group_name: Source group this sink consumes from
        options: Optional configuration (type, pattern, env-aware, etc.)

    Returns:
        Empty sink group scaffold for manual configuration
    """
    opts = options or {}
    sink_type = opts.get("sink_type", "postgres")
    pattern = opts.get("pattern", "db-shared")
    environment_aware = opts.get("environment_aware", False)
    database_exclude_patterns = opts.get("database_exclude_patterns")
    schema_exclude_patterns = opts.get("schema_exclude_patterns")
    table_exclude_patterns = opts.get("table_exclude_patterns")

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
    if table_exclude_patterns:
        sink_group["table_exclude_patterns"] = table_exclude_patterns

    # Note: servers inherit 'type' from sink group level, no need to specify per-server

    return cast(SinkGroupConfig, sink_group)


def validate_sink_group_structure(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    all_sink_groups: dict[str, SinkGroupConfig] | None = None,  # noqa: ARG001
    source_groups: dict[str, Any] | None = None,
) -> list[str]:
    """Validate sink group structure.

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration
        all_sink_groups: All sink groups (unused, kept for API compat)
        source_groups: All source groups (for source_group validation)

    Returns:
        List of validation error messages (empty if valid)
    """
    errors: list[str] = []

    inherits = sink_group.get("inherits", False)

    if inherits:
        # Inherited sink: validate name convention + source group exists
        errors.extend(
            _validate_inherits(sink_group_name, source_groups)
        )
        # inherited_sources is required for inherited sinks
        errors.extend(
            _validate_inherited_sources(
                sink_group_name, sink_group, source_groups,
            )
        )
    else:
        # Standalone sink: source_group is required
        if "source_group" not in sink_group:
            errors.append(
                f"Sink group '{sink_group_name}' missing required field"
                + " 'source_group' (or set 'inherits: true' with name"
                + " 'sink_<source_group>')"
            )

        # Validate source_group references a real source group
        source_group_ref = sink_group.get("source_group")
        if (
            source_group_ref
            and source_groups is not None
            and source_group_ref not in source_groups
        ):
            errors.append(
                f"Sink group '{sink_group_name}' references unknown"
                + f" source_group '{source_group_ref}'."
                + f" Available: {list(source_groups.keys())}"
            )

    # servers required
    if "servers" not in sink_group:
        errors.append(
            f"Sink group '{sink_group_name}' missing required field 'servers'"
        )

    # Deduce source_group_name for server validation
    source_group_name = _deduce_source_group_name(
        sink_group_name, sink_group,
    )

    # Validate servers and sources
    errors.extend(
        _validate_servers(
            sink_group_name, sink_group, source_groups, source_group_name,
        )
    )
    errors.extend(
        _validate_sources_refs(sink_group_name, sink_group)
    )

    return errors


def _deduce_source_group_name(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
) -> str | None:
    """Deduce source group name from config or sink name."""
    explicit = sink_group.get("source_group")
    if explicit:
        return str(explicit)
    return deduce_source_group(sink_group_name)


def _validate_servers(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any] | None,
    source_group_name: str | None = None,
) -> list[str]:
    """Validate server configs within a sink group."""
    errors: list[str] = []
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

        # Validate source_ref targets exist
        if has_source_ref and source_groups is not None:
            source_ref = str(server_config.get("source_ref", ""))
            errors.extend(
                _validate_source_ref(
                    sink_group_name, server_name, source_ref,
                    source_groups, source_group_name,
                )
            )

    return errors


def _validate_sources_refs(
    _sink_group_name: str,
    sink_group: SinkGroupConfig,
) -> list[str]:
    """Validate that sources reference existing servers."""
    errors: list[str] = []
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


def _validate_inherits(
    sink_group_name: str,
    source_groups: dict[str, Any] | None,
) -> list[str]:
    """Validate an inherited sink group.

    Rules:
    - Name must follow 'sink_<source_group>' convention
    - The deduced source group must exist in source_groups

    Args:
        sink_group_name: Name of the sink group being validated
        source_groups: All source groups for cross-referencing

    Returns:
        List of error messages
    """
    errors: list[str] = []

    # Name must start with 'sink_'
    deduced = deduce_source_group(sink_group_name)
    if not deduced:
        errors.append(
            f"Inherited sink group '{sink_group_name}' must follow naming"
            + " convention 'sink_<source_group>' (e.g., 'sink_asma')"
        )
        return errors

    # Deduced source group must exist
    if source_groups is not None and deduced not in source_groups:
        errors.append(
            f"Inherited sink group '{sink_group_name}' expects source group"
            + f" '{deduced}' but it does not exist."
            + f" Available source groups: {list(source_groups.keys())}"
        )

    return errors


def _validate_inherited_sources(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any] | None,
) -> list[str]:
    """Validate inherited_sources field for an inherited sink group.

    Rules:
    - inherited_sources must be present
    - Must be a non-empty list
    - All listed sources must exist in the source group's sources section

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration
        source_groups: All source groups

    Returns:
        List of error messages
    """
    errors: list[str] = []

    inherited_sources_raw = sink_group.get("inherited_sources")
    if not inherited_sources_raw:
        errors.append(
            f"Inherited sink group '{sink_group_name}' missing required"
            + " field 'inherited_sources' (list of sources from source group)"
        )
        return errors

    if not isinstance(inherited_sources_raw, list):
        errors.append(
            f"Inherited sink group '{sink_group_name}' must have at least"
            + " one entry in 'inherited_sources'"
        )
        return errors

    inherited_sources = cast(list[str], inherited_sources_raw)
    if len(inherited_sources) == 0:
        errors.append(
            f"Inherited sink group '{sink_group_name}' must have at least"
            + " one entry in 'inherited_sources'"
        )
        return errors

    # Validate all listed sources exist in source group
    if source_groups is None:
        return errors

    deduced = deduce_source_group(sink_group_name)
    if not deduced or deduced not in source_groups:
        return errors  # Name/group errors already reported by _validate_inherits

    source_group = source_groups[deduced]
    available_sources = list(source_group.get("sources", {}).keys())

    for source_name in inherited_sources:
        if source_name not in source_group.get("sources", {}):
            errors.append(
                f"Inherited sink group '{sink_group_name}' lists source"
                + f" '{source_name}' in inherited_sources but it does not"
                + f" exist in source group '{deduced}'."
                + f" Available: {available_sources}"
            )

    return errors


def _validate_source_ref(
    sink_group_name: str,
    server_name: str,
    source_ref: str,
    source_groups: dict[str, Any],
    source_group_name: str | None = None,
) -> list[str]:
    """Validate a server's source_ref points to existing server in source group.

    source_ref is just the server name (e.g., 'default', 'prod').
    The source group is deduced from the sink group name or explicit config.

    Args:
        sink_group_name: Name of the sink group
        server_name: Name of the server in the sink group
        source_ref: The source_ref value (server name within source group)
        source_groups: All source groups
        source_group_name: Source group to look up (deduced from sink name)

    Returns:
        List of error messages
    """
    errors: list[str] = []

    if not source_group_name:
        errors.append(
            f"Server '{server_name}' in '{sink_group_name}' has source_ref"
            + f" '{source_ref}' but no source group could be determined."
            + " Set 'source_group' or use naming convention"
            + " 'sink_<source_group>'."
        )
        return errors

    if source_group_name not in source_groups:
        errors.append(
            f"Server '{server_name}' in '{sink_group_name}' references"
            + f" source group '{source_group_name}' which does not exist."
            + f" Available source groups: {list(source_groups.keys())}"
        )
        return errors

    available_servers = list(
        source_groups[source_group_name].get("servers", {}).keys()
    )
    if source_ref not in source_groups[source_group_name].get("servers", {}):
        errors.append(
            f"Server '{server_name}' in '{sink_group_name}' references"
            + f" unknown server '{source_ref}' in source group"
            + f" '{source_group_name}' via source_ref."
            + f" Available servers: {available_servers}"
        )

    return errors


def get_sink_group_warnings(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
) -> list[str]:
    """Get warnings for a sink group (non-fatal issues).

    Checks:
    - Non-inherited sink groups without sources → not ready for use
    - Sink groups without servers configured

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration

    Returns:
        List of warning messages
    """
    warnings: list[str] = []

    inherits = sink_group.get("inherits", False)
    sources = sink_group.get("sources", {})

    if not inherits and not sources:
        warnings.append(
            f"Sink group '{sink_group_name}' has no 'sources' configured."
            + " It cannot be used as a sink target until services are added."
            + " Use 'cdc manage-services config --service <name> --add-sink"
            + f" {sink_group_name}.<service>' to add services."
        )

    if not sink_group.get("servers"):
        warnings.append(
            f"Sink group '{sink_group_name}' has no servers configured."
            + " Use 'cdc manage-sink-groups --sink-group"
            + f" {sink_group_name} --add-server <name>' to add servers."
        )

    return warnings


def is_sink_group_ready(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    all_sink_groups: dict[str, SinkGroupConfig] | None = None,
    source_groups: dict[str, Any] | None = None,
) -> bool:
    """Check if a sink group is ready for use as a sink target.

    A sink group is ready if:
    - No validation errors
    - Has servers (own or via source_ref)
    - Inherited groups: source group exists + servers have source_refs
    - Standalone groups: has source_group + servers with connection config

    Args:
        sink_group_name: Name of the sink group
        sink_group: Sink group configuration
        all_sink_groups: All sink groups (kept for API compat)
        source_groups: All source groups

    Returns:
        True if the sink group can be selected as a sink target
    """
    # Check for errors first
    errors = validate_sink_group_structure(
        sink_group_name, sink_group, all_sink_groups, source_groups,
    )
    if errors:
        return False

    # Must have servers
    if not sink_group.get("servers"):
        return False

    # Must have source_group (explicit or deducible)
    if not sink_group.get("source_group") and not sink_group.get("inherits"):
        # Try to deduce
        deduced = deduce_source_group(sink_group_name)
        if not deduced:
            return False

    return True
