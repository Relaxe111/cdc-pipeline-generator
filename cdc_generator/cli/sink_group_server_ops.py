"""Sink-group server management handlers (add/remove/update servers)."""

from __future__ import annotations

import argparse
from typing import Any, cast

from cdc_generator.cli.sink_group_common import (
    load_sink_group_for_server_op,
)
from cdc_generator.core.sink_types import SinkServerConfig
from cdc_generator.helpers.helpers_env import (
    append_env_vars_to_dotenv,
    print_env_removal_summary,
    print_env_update_summary,
    remove_env_vars_from_dotenv,
    sink_server_env_vars,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import save_sink_groups
from cdc_generator.validators.manage_server_group.patterns import (
    build_extraction_pattern_config,
    display_server_patterns,
)


def _build_server_config(args: argparse.Namespace, sink_group: dict[str, Any]) -> dict[str, object]:
    """Build server configuration from arguments.

    If host/port/user/password are not provided, generates env variable placeholders
    using pattern: ${<DB_TYPE>_SINK_<FIELD>_<SERVERGROUP>_<SERVER>}

    Example: sink_asma + nonprod + postgres → ${POSTGRES_SINK_HOST_ASMA_NONPROD}

    Note: Type is inherited from sink group level, not duplicated at server level.
    """

    server_config: dict[str, object] = {}

    # Determine server type for env var generation (but don't add to config)
    server_type = sink_group.get("type", "postgres")

    # Strip 'sink_' prefix for group name
    group_name = args.sink_group
    if group_name.startswith("sink_"):
        group_name = group_name[5:]

    # Generate env variable placeholders via shared helper
    placeholders = sink_server_env_vars(
        str(server_type), group_name, args.add_server,
    )

    server_config["host"] = args.host if args.host else placeholders["host"]
    server_config["port"] = args.port if args.port else placeholders["port"]
    server_config["user"] = args.user if args.user else placeholders["user"]
    server_config["password"] = (
        args.password if args.password else placeholders["password"]
    )

    # Add extraction patterns if provided (reuse source-group parsing logic)
    extraction_patterns = _build_extraction_pattern_entries(args)
    if extraction_patterns:
        server_config["extraction_patterns"] = extraction_patterns

    return server_config


def _validate_server_config(
    _server_config: dict[str, object],
) -> str | None:
    """Validate server config, return error message or None if valid.

    Note: Type is inherited from sink group level.
    Host/port are optional - will use env variable placeholders if not provided.
    """
    # Type is inherited from sink group, not required at server level
    # Host/port are optional (env vars used as defaults)
    return None


def _check_server_references(
    server_name: str,
    sink_group: dict[str, Any]
) -> list[str]:
    """Check if server is referenced in sources, return list of references."""
    sources = sink_group.get("sources", {})
    references: list[str] = []
    for service_name, source in sources.items():
        if isinstance(source, dict):
            source_dict = cast(dict[str, Any], source)
            for env_name, env_config in source_dict.items():
                if env_name != "schemas" and isinstance(env_config, dict):
                    env_dict = cast(dict[str, Any], env_config)
                    if env_dict.get("server") == server_name:
                        references.append(f"{service_name}.{env_name}")
    return references


def _build_extraction_pattern_entries(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Build structured extraction pattern entries from CLI args.

    Reuses source-group pattern parsing so sink-group and source-group keep
    consistent keys and semantics.
    """
    raw_patterns_value = getattr(args, "extraction_patterns", None)
    if not raw_patterns_value:
        return []

    raw_patterns: list[str]
    if isinstance(raw_patterns_value, list):
        raw_patterns = [
            str(pattern_value).strip()
            for pattern_value in cast(list[object], raw_patterns_value)
            if str(pattern_value).strip()
        ]
    else:
        pattern_value = str(raw_patterns_value).strip()
        raw_patterns = [pattern_value] if pattern_value else []

    entries: list[dict[str, Any]] = []
    for raw_pattern in raw_patterns:
        entries.append(build_extraction_pattern_config(args, raw_pattern))
    return entries


def _merge_extraction_patterns(
    existing_patterns_raw: object,
    incoming_patterns: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    """Merge extraction patterns by ``pattern`` key.

    Existing entry with same ``pattern`` is updated in place; otherwise incoming
    entry is appended.
    """
    existing_patterns: list[dict[str, Any]] = []
    if isinstance(existing_patterns_raw, list):
        for item in cast(list[object], existing_patterns_raw):
            if isinstance(item, dict):
                existing_patterns.append(dict(cast(dict[str, Any], item)))

    index_by_pattern: dict[str, int] = {}
    for index, item in enumerate(existing_patterns):
        pattern_value = item.get("pattern")
        if isinstance(pattern_value, str) and pattern_value and pattern_value not in index_by_pattern:
            index_by_pattern[pattern_value] = index

    updated_count = 0
    added_count = 0
    for incoming in incoming_patterns:
        pattern_value = incoming.get("pattern")
        if not isinstance(pattern_value, str) or not pattern_value:
            existing_patterns.append(incoming)
            added_count += 1
            continue

        existing_index = index_by_pattern.get(pattern_value)
        if existing_index is not None:
            existing_patterns[existing_index] = incoming
            updated_count += 1
            continue

        index_by_pattern[pattern_value] = len(existing_patterns)
        existing_patterns.append(incoming)
        added_count += 1

    return existing_patterns, added_count, updated_count


def handle_add_server_command(args: argparse.Namespace) -> int:
    """Add a server to a sink group."""
    if not args.sink_group or not args.add_server:
        print_error("--add-server requires --sink-group and server name")
        return 1

    result = load_sink_group_for_server_op(args, "--add-server")
    if isinstance(result, int):
        return result

    sink_groups, sink_group, sink_group_name, sink_file = result
    server_name = args.add_server

    # Inherited sink groups get servers from source group — cannot add manually
    if sink_group.get("inherits", False):
        source_name = sink_group_name.removeprefix("sink_")
        print_error(
            f"Cannot add server '{server_name}' to"
            + f" '{sink_group_name}' — it inherits from"
            + f" source group '{source_name}'"
        )
        print_info(
            "Servers are managed via source group. To add a server, use:"
        )
        print_info(
            "  cdc manage-source-groups --add-server"
            + f" {server_name} --server-group {source_name}"
        )
        return 1

    servers = sink_group.get("servers", {})

    if server_name in servers:
        print_warning(
            f"Server '{server_name}' already exists"
            + f" in sink group '{sink_group_name}'"
        )
        return 1

    # Build and validate server config
    server_config = _build_server_config(
        args, cast(dict[str, Any], sink_group)
    )
    error = _validate_server_config(server_config)
    if error:
        print_error(error)
        return 1

    servers[server_name] = cast(SinkServerConfig, server_config)
    cast(dict[str, Any], sink_group)["servers"] = servers

    save_sink_groups(sink_groups, sink_file)

    server_type = sink_group.get("type", "postgres")
    print_success(f"Added server '{server_name}' to sink group '{sink_group_name}'")
    print_info(f"Server type: {server_type} (inherited from sink group)")

    # Show what was configured
    host = server_config.get("host", "N/A")
    port = server_config.get("port", "N/A")
    if args.host:
        print_info(f"Host: {host}:{port}")
    else:
        print_info(f"Host: {host} (env variable)")
        print_info(f"Port: {port} (env variable)")
        print_info(f"User: {server_config.get('user', 'N/A')} (env variable)")
        print_warning("Remember to set environment variables at runtime")

    # Append env variables to .env
    group_name = sink_group_name
    if group_name.startswith("sink_"):
        group_name = group_name[5:]
    placeholders = sink_server_env_vars(
        str(sink_group.get("type", "postgres")), group_name, server_name,
    )
    env_count = append_env_vars_to_dotenv(
        placeholders,
        f"Sink Server: {sink_group_name} / {server_name}"
        + f" ({sink_group.get('type', 'postgres')})",
    )
    print_env_update_summary(env_count, placeholders)

    return 0


def handle_update_server_extraction_patterns_command(args: argparse.Namespace) -> int:
    """Update extraction patterns for an existing server in a sink group."""
    if not args.sink_group or not args.server:
        print_error("--server update requires --sink-group and --server")
        return 1
    if not args.extraction_patterns:
        print_error("--server update requires --extraction-patterns")
        return 1

    result = load_sink_group_for_server_op(args, "--server")
    if isinstance(result, int):
        return result

    sink_groups, sink_group, sink_group_name, sink_file = result
    server_name = args.server
    servers = sink_group.get("servers", {})

    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found in sink group '{sink_group_name}'"
        )
        print_info(
            "Use --add-server to create it first, or --remove-server to delete"
        )
        return 1

    extraction_patterns = _build_extraction_pattern_entries(args)
    if not extraction_patterns:
        print_error("No valid extraction patterns provided")
        return 1

    server_config = cast(dict[str, Any], servers[server_name])
    merged_patterns, added_count, updated_count = _merge_extraction_patterns(
        server_config.get("extraction_patterns", []),
        extraction_patterns,
    )
    server_config["extraction_patterns"] = merged_patterns
    servers[server_name] = cast(SinkServerConfig, server_config)
    cast(dict[str, Any], sink_group)["servers"] = servers

    save_sink_groups(sink_groups, sink_file)
    print_success(
        f"Updated extraction_patterns for server '{server_name}'"
        + f" in sink group '{sink_group_name}'"
    )
    print_info(
        f"Patterns: +{added_count} added, {updated_count} updated"
        + f" (total: {len(server_config['extraction_patterns'])})"
    )
    return 0


def handle_list_server_extraction_patterns_command(args: argparse.Namespace) -> int:
    """List extraction patterns for servers in a sink group.

    Use optional --list-server-extraction-patterns SERVER to filter to one server.
    """
    if not args.sink_group:
        print_error("--list-server-extraction-patterns requires --sink-group")
        return 1

    result = load_sink_group_for_server_op(args, "--list-server-extraction-patterns")
    if isinstance(result, int):
        return result

    _sink_groups, sink_group, sink_group_name, _sink_file = result
    servers = sink_group.get("servers", {})

    if not servers:
        print_warning(f"Sink group '{sink_group_name}' has no servers configured")
        return 1

    server_filter = args.list_server_extraction_patterns
    selected_servers: dict[str, Any]
    if server_filter:
        if server_filter not in servers:
            print_error(
                f"Server '{server_filter}' not found in sink group '{sink_group_name}'"
            )
            print_info(f"Available servers: {list(servers.keys())}")
            return 1
        selected_servers = {server_filter: servers[server_filter]}
    else:
        selected_servers = dict(servers)

    print_header(f"Sink Server Extraction Patterns: {sink_group_name}")

    has_any = False
    for server_name in sorted(selected_servers.keys()):
        server_config = cast(dict[str, Any], selected_servers[server_name])
        if display_server_patterns(server_name, server_config):
            has_any = True

    if not has_any:
        print()
        print_info("💡 Add extraction patterns with:")
        print_info(
            "   cdc manage-sink-groups --sink-group "
            + f"{sink_group_name} --server default --extraction-patterns "
            + "'^(?P<service>\\w+)_db_(?P<env>\\w+)$'"
        )

    return 0


def handle_remove_server_command(args: argparse.Namespace) -> int:
    """Remove a server from a sink group."""
    if not args.sink_group or not args.remove_server:
        print_error("--remove-server requires --sink-group and server name")
        return 1

    result = load_sink_group_for_server_op(args, "--remove-server")
    if isinstance(result, int):
        return result

    sink_groups, sink_group, sink_group_name, sink_file = result
    server_name = args.remove_server
    servers = sink_group.get("servers", {})

    # Check if server exists first — regardless of inherited status
    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found in"
            + f" sink group '{sink_group_name}'"
        )
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    # Inherited sink groups get servers from source group — cannot remove manually
    if sink_group.get("inherits", False):
        source_name = sink_group_name.removeprefix("sink_")
        print_error(
            f"Cannot remove server '{server_name}' from"
            + f" '{sink_group_name}' — it inherits from"
            + f" source group '{source_name}'"
        )
        print_info(
            "Servers are managed via source group. To remove a server, use:"
        )
        print_info(
            "  cdc manage-source-groups --remove-server"
            + f" {server_name} --server-group {source_name}"
        )
        return 1

    # Check if server is referenced in sources
    references = _check_server_references(
        server_name, cast(dict[str, Any], sink_group)
    )

    if references:
        print_error(f"Cannot remove server '{server_name}' — referenced in sources:")
        for ref in references:
            print_error(f"  - {ref}")
        return 1

    del servers[server_name]
    save_sink_groups(sink_groups, sink_file)

    print_success(f"Removed server '{server_name}' from sink group '{sink_group_name}'")

    # Remove env variables from .env
    group_name = sink_group_name
    if group_name.startswith("sink_"):
        group_name = group_name[5:]
    placeholders = sink_server_env_vars(
        str(sink_group.get("type", "postgres")), group_name, server_name,
    )
    env_count = remove_env_vars_from_dotenv(placeholders)
    print_env_removal_summary(env_count, placeholders)

    return 0
