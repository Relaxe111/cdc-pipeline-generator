"""Special action handlers extracted from source_group CLI facade."""

from __future__ import annotations

import argparse
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
)


def handle_introspect_types(args: argparse.Namespace) -> int:
    """Introspect column types from the source database server."""
    from cdc_generator.validators.manage_server_group.config import (
        get_single_server_group,
        load_server_groups,
    )
    from cdc_generator.validators.manage_server_group.type_introspector import (
        introspect_types,
    )

    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as exc:
        print_error(f"Failed to load server groups: {exc}")
        return 1

    if not server_group:
        print_error("No server group found in configuration")
        return 1

    servers = server_group.get("servers", {})
    if not servers:
        print_error("No servers configured in server group")
        return 1

    server_name: str = getattr(args, "server", None) or next(iter(servers))
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found")
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    engine = str(server_group.get("type", "postgres"))

    print_header(
        f"Introspecting {engine.upper()} types from"
        + f" server '{server_name}'"
    )

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


def handle_db_definitions(args: argparse.Namespace) -> int:
    """Generate services/_schemas/_definitions type file once from source DB server."""
    from cdc_generator.validators.manage_server_group.config import (
        get_single_server_group,
        load_server_groups,
    )
    from cdc_generator.validators.manage_service_schema.type_definitions import (
        generate_type_definitions,
    )

    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as exc:
        print_error(f"Failed to load server groups: {exc}")
        return 1

    if not server_group:
        print_error("No server group found in configuration")
        return 1

    servers = server_group.get("servers", {})
    if not servers:
        print_error("No servers configured in server group")
        return 1

    server_name: str = getattr(args, "server", None) or next(iter(servers))
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found")
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    db_type = str(server_group.get("type", "postgres"))
    if db_type not in {"postgres", "mssql"}:
        print_error(
            f"Unsupported source type '{db_type}' for --db-definitions"
        )
        print_info("Supported types: postgres, mssql")
        return 1

    print_header(
        "Generating DB definitions from "
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
            "manage-source-groups --db-definitions"
            + f" ({server_name})"
        ),
    )
    return 0 if success else 1


def handle_add_source_custom_key(args: argparse.Namespace) -> int:
    """Add or update SQL-based source custom key definition."""
    from cdc_generator.validators.manage_server_group.config import (
        get_single_server_group,
        load_server_groups,
    )
    from cdc_generator.validators.manage_server_group.yaml_io import (
        write_server_group_yaml,
    )

    key_name = str(args.add_source_custom_key or "").strip()
    key_value = str(args.custom_key_value or "").strip()
    exec_type = str(args.custom_key_exec_type or "sql").strip().lower()

    if not key_name:
        print_error("--add-source-custom-key requires a key name")
        return 1
    if not key_value:
        print_error("--custom-key-value is required with --add-source-custom-key")
        return 1
    if exec_type != "sql":
        print_error("Only --custom-key-exec-type sql is supported")
        return 1

    try:
        config = load_server_groups()
        server_group = get_single_server_group(config)
    except Exception as exc:
        print_error(f"Failed to load source groups: {exc}")
        return 1

    if not server_group:
        print_error("No source group found in configuration")
        return 1

    server_group_name = str(server_group.get('name', '')).strip()
    if not server_group_name:
        print_error("Failed to determine source group name")
        return 1

    source_custom_keys_raw = server_group.get("source_custom_keys")
    source_custom_keys: dict[str, dict[str, str]] = {}
    if isinstance(source_custom_keys_raw, dict):
        for existing_key_raw, existing_value_raw in cast(dict[str, object], source_custom_keys_raw).items():
            existing_key = str(existing_key_raw).strip()
            if not existing_key:
                continue
            if isinstance(existing_value_raw, dict):
                value_dict = cast(dict[str, object], existing_value_raw)
                existing_exec = str(value_dict.get("exec_type", "sql")).strip().lower() or "sql"
                existing_value = str(value_dict.get("value", "")).strip()
                if existing_value:
                    source_custom_keys[existing_key] = {
                        "exec_type": existing_exec,
                        "value": existing_value,
                    }
            elif isinstance(existing_value_raw, str):
                existing_value = existing_value_raw.strip()
                if existing_value:
                    source_custom_keys[existing_key] = {
                        "exec_type": "sql",
                        "value": existing_value,
                    }

    source_custom_keys[key_name] = {
        "exec_type": exec_type,
        "value": key_value,
    }
    server_group["source_custom_keys"] = source_custom_keys

    try:
        write_server_group_yaml(server_group_name, server_group)
    except Exception as exc:
        print_error(f"Failed to save source-groups.yaml: {exc}")
        return 1

    print_info(
        f"Saved source custom key '{key_name}' with exec_type '{exec_type}'"
    )
    return 0
