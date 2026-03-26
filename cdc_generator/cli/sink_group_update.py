"""Sink-group update/introspect handlers and source-merge logic."""

from __future__ import annotations

import argparse
from typing import Any, cast

from cdc_generator.cli.sink_group_common import (
    get_sink_file_path,
    get_source_group_file_path,
    validate_inspect_args,
)
from cdc_generator.cli.sink_group_inspect import _fetch_databases
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_sink_groups import (
    resolve_sink_group,
    save_sink_groups,
)
from cdc_generator.helpers.source_custom_keys import (
    execute_source_custom_keys,
    normalize_source_custom_keys,
)
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file
from cdc_generator.validators.manage_server_group.autocomplete_definitions import (
    generate_service_autocomplete_definitions,
)


def _build_sink_sources_from_databases(
    databases: list[dict[str, Any]],
    server_name: str,
) -> dict[str, Any]:
    """Build sink-group ``sources`` mapping from inspected databases."""
    sources: dict[str, Any] = {}

    for db in databases:
        service = str(db.get("service", "")).strip()
        database_name = str(db.get("name", "")).strip()
        if not service or not database_name:
            continue

        env_raw = str(db.get("environment", "")).strip()
        env = env_raw if env_raw else "default"

        schemas_raw = db.get("schemas", [])
        schemas: list[str] = []
        if isinstance(schemas_raw, list):
            for schema_value in cast(list[object], schemas_raw):
                schema_name = str(schema_value).strip()
                if schema_name and schema_name not in schemas:
                    schemas.append(schema_name)

        table_count_raw = db.get("table_count", 0)
        try:
            table_count = int(table_count_raw)
        except (TypeError, ValueError):
            table_count = 0

        source_entry = cast(
            dict[str, Any],
            sources.setdefault(service, {"schemas": []}),
        )
        existing_schemas_raw = source_entry.get("schemas", [])
        existing_schemas: list[str] = []
        if isinstance(existing_schemas_raw, list):
            for existing_schema in cast(list[object], existing_schemas_raw):
                existing_schema_name = str(existing_schema).strip()
                if existing_schema_name and existing_schema_name not in existing_schemas:
                    existing_schemas.append(existing_schema_name)
        for schema_name in schemas:
            if schema_name not in existing_schemas:
                existing_schemas.append(schema_name)
        source_entry["schemas"] = existing_schemas
        env_entry: dict[str, Any] = {
            "server": server_name,
            "database": database_name,
            "table_count": table_count,
        }
        custom_values = db.get("source_custom_values")
        if isinstance(custom_values, dict):
            for key_raw, value_raw in cast(dict[str, object], custom_values).items():
                key = str(key_raw).strip()
                if not key:
                    continue
                if value_raw is None:
                    env_entry[key] = None
                    continue
                value = str(value_raw).strip()
                env_entry[key] = value if value else None
        source_entry[env] = env_entry

    return sources


def _merge_server_sources_update(
    existing_sources_raw: object,
    updated_sources: dict[str, Any],
    server_name: str,
) -> dict[str, Any]:
    """Merge updated sources for one server, preserving other server entries.

    Strategy:
    1. Remove existing env entries that belong to ``server_name``.
    2. Keep entries from other servers unchanged.
    3. Merge in newly discovered entries for ``server_name``.
    4. Union ``schemas`` lists per service.
    """
    merged = _copy_existing_sources_without_server(
        existing_sources_raw,
        server_name,
    )

    for service_name_raw, source_raw in updated_sources.items():
        service_name = str(service_name_raw).strip()
        if not service_name or not isinstance(source_raw, dict):
            continue

        incoming_source = cast(dict[str, Any], source_raw)
        target_source = cast(dict[str, Any], merged.setdefault(service_name, {}))

        merged_schemas = _merge_schema_lists(
            target_source.get("schemas", []),
            incoming_source.get("schemas", []),
        )
        if merged_schemas:
            target_source["schemas"] = merged_schemas

        for env_name_raw, env_cfg_raw in incoming_source.items():
            if env_name_raw == "schemas" or not isinstance(env_cfg_raw, dict):
                continue
            target_source[str(env_name_raw)] = dict(cast(dict[str, Any], env_cfg_raw))

    return merged


def _merge_schema_lists(primary_raw: object, secondary_raw: object) -> list[str]:
    """Merge two schema lists into a unique, ordered list."""
    merged: list[str] = []
    for raw in [primary_raw, secondary_raw]:
        if not isinstance(raw, list):
            continue
        for schema_value in cast(list[object], raw):
            schema_name = str(schema_value).strip()
            if schema_name and schema_name not in merged:
                merged.append(schema_name)
    return merged


def _copy_existing_sources_without_server(
    existing_sources_raw: object,
    server_name: str,
) -> dict[str, Any]:
    """Copy existing sources while removing env entries for target server."""
    copied: dict[str, Any] = {}
    if not isinstance(existing_sources_raw, dict):
        return copied

    for service_name_raw, source_raw in cast(dict[str, object], existing_sources_raw).items():
        service_name = str(service_name_raw).strip()
        if not service_name or not isinstance(source_raw, dict):
            continue

        source_map = cast(dict[str, Any], source_raw)
        service_copy: dict[str, Any] = {}

        schemas = _merge_schema_lists(source_map.get("schemas", []), [])
        if schemas:
            service_copy["schemas"] = schemas

        for env_name_raw, env_cfg_raw in source_map.items():
            if env_name_raw == "schemas" or not isinstance(env_cfg_raw, dict):
                continue
            env_cfg = cast(dict[str, Any], env_cfg_raw)
            if str(env_cfg.get("server", "")).strip() == server_name:
                continue
            service_copy[str(env_name_raw)] = dict(env_cfg)

        copied[service_name] = service_copy

    return copied


def handle_update_command(args: argparse.Namespace) -> int:
    """Inspect sink DBs and update sink-group sources section."""
    source_file = get_source_group_file_path()

    result = validate_inspect_args(args, action_flag="--update")
    if isinstance(result, int):
        return result

    sink_groups, sink_group, sink_group_name = result
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    servers = resolved.get("servers", {})
    if not servers:
        print_error(f"No servers in sink group '{sink_group_name}'")
        return 1

    server_name = args.server or next(iter(servers))
    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found"
            + f" in sink group '{sink_group_name}'"
        )
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    sink_type = resolved.get("type", "postgres")

    print_header(f"Updating Sink Group: {sink_group_name}")
    print_info(f"Inspecting server '{server_name}' ({sink_type})")

    try:
        databases = _fetch_databases(
            sink_type,
            server_config,
            resolved,
            args,
            server_name,
        )
    except ValueError as e:
        print_error(str(e))
        print_info("Only 'postgres' and 'mssql' sink types support update.")
        return 1
    except ImportError as e:
        print_error(f"Database driver not installed: {e}")
        return 1
    except Exception as e:
        print_error(f"Failed to inspect databases for update: {e}")
        return 1

    source_custom_keys = normalize_source_custom_keys(
        sink_group.get("source_custom_keys", {})
    )
    if source_custom_keys:
        execute_source_custom_keys(
            databases,
            db_type=str(sink_type),
            server_name=server_name,
            server_config=cast(Any, server_config),
            source_custom_keys=source_custom_keys,
            context_label=f"sink-group '{sink_group_name}'",
        )

    updated_sources = _build_sink_sources_from_databases(databases, server_name)
    sink_group_entry = cast(dict[str, Any], sink_groups[sink_group_name])
    merged_sources = _merge_server_sources_update(
        sink_group_entry.get("sources", {}),
        updated_sources,
        server_name,
    )
    sink_group_entry["sources"] = merged_sources

    save_sink_groups(sink_groups, get_sink_file_path())

    generate_service_autocomplete_definitions(
        cast(Any, resolved),
        cast(list[dict[str, Any]], databases),
        table_include_patterns=cast(list[str], resolved.get("table_include_patterns", [])),
        table_exclude_patterns=cast(list[str], resolved.get("table_exclude_patterns", [])),
        schema_exclude_patterns=cast(list[str], resolved.get("schema_exclude_patterns", [])),
    )

    print_success(
        f"Updated '{sink_group_name}' sources from {len(databases)} discovered database(s)"
    )
    print_info(f"Services updated: {len(updated_sources)}")
    return 0


def handle_introspect_types_command(args: argparse.Namespace) -> int:
    """Introspect column types from a sink group's database server."""
    from cdc_generator.validators.manage_server_group.type_introspector import (
        introspect_types,
    )

    source_file = get_source_group_file_path()

    # Validate args and load sink group
    result = validate_inspect_args(
        args,
        action_flag="--introspect-types",
    )
    if isinstance(result, int):
        return result

    _sink_groups, sink_group, sink_group_name = result

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group (handles inherited servers)
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    # Pick server: explicit --server or first available
    servers = resolved.get("servers", {})
    if not servers:
        print_error(f"No servers in sink group '{sink_group_name}'")
        return 1

    server_name = args.server or next(iter(servers))
    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found"
            + f" in sink group '{sink_group_name}'"
        )
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    engine = str(resolved.get("type", "postgres"))

    print_header(
        f"Introspecting {engine.upper()} types from"
        + f" server '{server_name}'"
    )

    # Build connection params from server config
    conn_params: dict[str, Any] = {
        "host": server_config.get("host", ""),
        "port": server_config.get("port", ""),
        "user": server_config.get(
            "username", server_config.get("user", "")
        ),
        "password": server_config.get("password", ""),
    }

    success = introspect_types(engine, conn_params)
    return 0 if success else 1


def handle_db_definitions_command(args: argparse.Namespace) -> int:
    """Generate services/_schemas/_definitions type file once from sink server."""
    from cdc_generator.validators.manage_service_schema.type_definitions import (
        generate_type_definitions,
    )

    source_file = get_source_group_file_path()

    # Validate args and load sink group
    result = validate_inspect_args(
        args,
        action_flag="--db-definitions",
    )
    if isinstance(result, int):
        return result

    _sink_groups, sink_group, sink_group_name = result

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group (handles inherited servers)
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    # Pick server: explicit --server or first available
    servers = resolved.get("servers", {})
    if not servers:
        print_error(f"No servers in sink group '{sink_group_name}'")
        return 1

    server_name = args.server or next(iter(servers))
    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found"
            + f" in sink group '{sink_group_name}'"
        )
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    db_type = str(resolved.get("type", "postgres"))
    if db_type not in {"postgres", "mssql"}:
        print_error(
            f"Unsupported sink type '{db_type}' for --db-definitions"
        )
        print_info("Supported sink types: postgres, mssql")
        return 1

    print_header(
        "Generating DB definitions from sink "
        + f"{db_type.upper()} server '{server_name}'"
    )

    conn_params: dict[str, Any] = {
        "host": server_config.get("host", ""),
        "port": server_config.get("port", ""),
        "user": server_config.get(
            "username", server_config.get("user", "")
        ),
        "password": server_config.get("password", ""),
    }

    success = generate_type_definitions(
        db_type,
        conn_params,
        source_label=(
            "manage-sink-groups --db-definitions"
            + f" ({sink_group_name}:{server_name})"
        ),
    )
    return 0 if success else 1
