#!/usr/bin/env python3
"""
Manage the source group configuration file (source-groups.yaml).

This command helps you keep your source-groups.yaml file up-to-date by
inspecting the source database and populating it with the correct database
and schema information based on your configuration.

Usage:
    # Inspect the database and update source-groups.yaml
    cdc manage-source-groups --update

    # Inspect a specific server
    cdc manage-source-groups --update default
    cdc manage-source-groups --update prod

    # Inspect all servers
    cdc manage-source-groups --update --all

    # Show information about the configured source group
    cdc manage-source-groups --info

    # Manage database/schema exclude patterns
    cdc manage-source-groups --list-ignore-patterns
    cdc manage-source-groups --add-to-ignore-list "pattern_to_ignore"
    cdc manage-source-groups --list-schema-excludes
    cdc manage-source-groups --add-to-schema-excludes "schema_to_exclude"

Note:
To create a new source group, use 'cdc scaffold <name>' command.

Example:
    cdc scaffold myproject --pattern db-shared --source-type postgres \
        --extraction-pattern "" --environment-aware
"""

import argparse
import sys
from pathlib import Path
from typing import Any, cast

# When executed directly (python cdc_generator/cli/source_group.py), ensure the
# project root is on sys.path so package imports succeed.
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_warning,
)
from cdc_generator.helpers.yaml_loader import ConfigDict

# Import flag validator
from cdc_generator.validators.flag_validator import validate_manage_source_group_flags

# Import from modular package
from cdc_generator.validators.manage_server_group import (
    handle_add_extraction_pattern,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_add_server,
    handle_info,
    handle_list_envs,
    handle_list_extraction_patterns,
    handle_list_servers,
    handle_remove_extraction_pattern,
    handle_remove_server,
    handle_set_extraction_pattern,
    handle_set_kafka_topology,
    handle_set_validation_env,
    handle_update,
    load_database_exclude_patterns,
    load_schema_exclude_patterns,
)

# Backward-compat alias for tests/extensions that patch this symbol directly.
validate_manage_server_group_flags = validate_manage_source_group_flags


def _handle_introspect_types(args: argparse.Namespace) -> int:
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

    # Pick server: explicit --server or first available
    server_name: str = getattr(args, "server", None) or next(iter(servers))
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found")
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    engine = str(server_group.get("type", "postgres"))

    from cdc_generator.helpers.helpers_logging import print_header

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


def _handle_db_definitions(args: argparse.Namespace) -> int:
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

    # Pick server: explicit --server or first available
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


def _handle_add_source_custom_key(args: argparse.Namespace) -> int:
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


def main() -> int:
    # Note: .env loading handled by implementations, not generator library

    parser = argparse.ArgumentParser(
        description=(
            "Manage the source/service-groups.yaml file for your implementation."
        ),
        prog="cdc manage-source-groups",  # Use the alias in help messages
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Primary actions
    parser.add_argument(
        "--update",
        nargs="?",
        const="default",
        metavar="SERVER",
        help=(
            "Update the source group by inspecting the source database. "
            "Optionally provide a server name (default: 'default')."
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Update all servers (use with --update).",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show detailed information for the source group.",
    )
    parser.add_argument(
        "--view-services",
        action="store_true",
        help="View environment-grouped services (db-shared mode).",
    )

    # Exclude patterns management
    parser.add_argument(
        "--add-to-ignore-list",
        help=(
            "Add a pattern to the database exclude list "
            "(persisted in source-groups.yaml)."
        ),
    )
    parser.add_argument("--list-ignore-patterns", action="store_true",
                       help="List current database exclude patterns.")
    parser.add_argument(
        "--add-to-schema-excludes",
        help=(
            "Add a pattern to the schema exclude list "
            "(persisted in source-groups.yaml)."
        ),
    )
    parser.add_argument("--list-schema-excludes", action="store_true",
                       help="List current schema exclude patterns.")
    parser.add_argument(
        "--add-source-custom-key",
        metavar="KEY",
        help="Add/update source custom key name used during --update.",
    )
    parser.add_argument(
        "--custom-key-value",
        metavar="SQL",
        help="SQL expression/query used to resolve the custom key value.",
    )
    parser.add_argument(
        "--custom-key-exec-type",
        choices=["sql"],
        default="sql",
        help="Execution type for source custom key resolution (currently: sql).",
    )

    # Multi-server management
    parser.add_argument("--add-server", metavar="NAME",
                       help="Add a new server configuration (e.g., 'analytics', 'reporting'). " +
                            "Use with --source-type, --host, --port, --user, --password.")
    parser.add_argument("--list-servers", action="store_true",
                       help="List all configured servers in the source group.")
    parser.add_argument(
        "--remove-server",
        metavar="NAME",
        help=(
            "Remove a server configuration. "
            "Cannot remove 'default' or servers with services."
        ),
    )
    parser.add_argument("--set-kafka-topology", choices=["shared", "per-server"],
                       help="Change the Kafka topology. 'shared' = same Kafka for all servers, " +
                            "'per-server' = isolated Kafka per server.")

    # Validation environment management
    parser.add_argument(
        "--set-validation-env",
        metavar="ENV",
        help="Set the validation environment for the source group (e.g., 'dev', 'nonprod').",
    )
    parser.add_argument(
        "--list-envs",
        action="store_true",
        help="List available environments for the source group.",
    )

    parser.add_argument(
        "--set-extraction-pattern",
        nargs=2,
        metavar=("SERVER", "PATTERN"),
        help=(
            "Set extraction pattern for a specific server. "
            "Pattern is a regex with named groups: "
            "(?P<service>...), (?P<env>...), "
            "(?P<customer>...). "
            "Example: --set-extraction-pattern default "
            "'^(?P<service>\\w+)_(?P<env>\\w+)$'"
        ),
    )

    # Extraction pattern management (ordered multi-pattern approach)
    parser.add_argument(
        "--add-extraction-pattern",
        nargs=2,
        metavar=("SERVER", "PATTERN"),
        help=(
            "Add an extraction pattern to a server. "
            "Patterns are tried in order (first match wins). "
            "Pattern must include named group "
            "(?P<service>...) and optionally (?P<env>...). "
            "Use --env to fix environment "
            "(overrides captured group). "
            "Use --strip-patterns for regex-based removal "
            "(e.g., '_db' anywhere, '_db$' suffix only). "
            "Example: --add-extraction-pattern prod "
            "'^(?P<service>\\w+)_db_prod_adcuris$' "
            "--env prod_adcuris --strip-patterns '_db$'"
        ),
    )
    parser.add_argument(
        "--env",
        type=str,
        help=(
            "Fixed environment name for "
            "--add-extraction-pattern "
            "(overrides captured (?P<env>) group)."
        ),
    )
    parser.add_argument(
        "--strip-patterns",
        type=str,
        help=(
            "Comma-separated regex patterns to remove "
            "from service name (e.g., '_db' for anywhere, "
            "'_db$' for suffix only)."
        ),
    )
    parser.add_argument(
        "--env-mapping",
        type=str,
        help=(
            "Environment mapping in format 'from:to' "
            "(e.g., 'prod_adcuris:prod-adcuris'). "
            "Can be specified multiple times."
        ),
        action='append',
    )
    parser.add_argument("--description", type=str,
                       help="Human-readable description for --add-extraction-pattern.")
    parser.add_argument("--list-extraction-patterns", nargs='?', const='', metavar="SERVER",
                       help="List extraction patterns for all servers or a specific server.")
    parser.add_argument("--remove-extraction-pattern", nargs=2, metavar=("SERVER", "INDEX"),
                       help="Remove an extraction pattern by index. " +
                            "Use --list-extraction-patterns to see indices. " +
                            "Example: --remove-extraction-pattern prod 0")

    # Type introspection
    parser.add_argument("--introspect-types", action="store_true",
                       help="Introspect column types from the source database server " +
                            "and generate/update type definition files. " +
                            "Use --server to pick a specific server " +
                            "(default: first available).")
    parser.add_argument(
        "--db-definitions",
        action="store_true",
        help=(
            "Generate services/_schemas/_definitions/{pgsql|mssql}.yaml once "
            "from source database server metadata."
        ),
    )
    parser.add_argument("--server", metavar="NAME",
                       help="Server to use for --introspect-types/--db-definitions " +
                            "(default: first available).")

    args = parser.parse_args()

    # Validate flag combinations (Python-based validation)
    validation_result = validate_manage_server_group_flags(args)

    if validation_result.level == 'error':
        print_error(validation_result.message or "Invalid flag combination")
        if validation_result.suggestion:
            print(validation_result.suggestion)
        return 1

    if validation_result.level == 'warning':
        print(validation_result.message or "")
        print()  # Blank line before proceeding


    # Handle list schema exclude patterns
    if args.list_schema_excludes:
        patterns = load_schema_exclude_patterns()
        print_header("Schema Exclude Patterns")
        if patterns:
            print_info("Schemas matching these patterns will be excluded during '--update':")
            for pattern in patterns:
                print_info(f"  • {pattern}")
        else:
            print_warning("No schema exclude patterns defined.")
            print_info("You can add patterns to a comment in source-groups.yaml, for example:")
            print_info("  # schema_exclude_patterns: ['hdb_catalog', 'hdb_views', 'sessions']")
        return 0

    # Handle list ignore patterns
    if args.list_ignore_patterns:
        patterns = load_database_exclude_patterns()
        print_header("Database Exclude Patterns")
        if patterns:
            print_info(
                "Databases with names containing these patterns "
                + "will be excluded during '--update':"
            )
            for pattern in patterns:
                print_info(f"  • {pattern}")
        else:
            print_warning("No database exclude patterns defined.")
            print_info("You can add patterns to a comment in source-groups.yaml, for example:")
            print_info("  # database_exclude_patterns: ['test', 'dev', 'backup']")
        return 0

    # Handle add to ignore list
    if args.add_to_ignore_list:
        return handle_add_ignore_pattern(args)

    # Handle add to schema excludes
    if args.add_to_schema_excludes:
        return handle_add_schema_exclude(args)

    if args.add_source_custom_key:
        return _handle_add_source_custom_key(args)

    # Handle multi-server management
    if args.add_server:
        return handle_add_server(args)

    if args.list_servers:
        return handle_list_servers(args)

    if args.remove_server:
        return handle_remove_server(args)

    if args.set_kafka_topology:
        return handle_set_kafka_topology(args)

    if args.set_extraction_pattern:
        return handle_set_extraction_pattern(args)

    if args.add_extraction_pattern:
        return handle_add_extraction_pattern(args)

    if args.list_extraction_patterns is not None:
        return handle_list_extraction_patterns(args)

    if args.remove_extraction_pattern:
        return handle_remove_extraction_pattern(args)

    # Handle type introspection
    if args.introspect_types:
        return _handle_introspect_types(args)

    # Handle one-shot DB definitions generation
    if args.db_definitions:
        return _handle_db_definitions(args)

    # Handle info
    if args.info:
        return handle_info(args)

    # Handle view-services
    if args.view_services:
        from cdc_generator.validators.manage_server_group.config import (
            get_single_server_group,
            load_server_groups,
        )
        try:
            config = load_server_groups()
            server_group = get_single_server_group(config)

            if not server_group:
                print_error("No server group found in configuration")
                return 1

            # Check for 'sources' key (new structure) or fallback to 'services' (legacy)
            sources = server_group.get('sources', server_group.get('services', {}))

            if sources:
                print_header("Environment-Grouped Sources")
                for source_name, source_data in sorted(sources.items()):
                    # Type is already dict from YAML structure
                    src = cast(ConfigDict, source_data)
                    schemas_raw = src.get('schemas', [])
                    # Runtime validation: schemas must be a list of strings
                    schemas = (
                        [str(s) for s in schemas_raw]
                        if isinstance(schemas_raw, list)
                        else []
                    )
                    print_info(f"\n📦 Source: {source_name}")
                    print_info(f"   Schemas (shared): {', '.join(schemas)}")

                    # Display each environment with server reference
                    for key, value in sorted(src.items()):
                        if key == 'schemas':
                            continue  # Already displayed
                        if isinstance(value, dict) and 'database' in value:
                            env = key
                            env_data = value  # Type is already Dict[str, ConfigValue]
                            # Extract with defaults and explicit type conversion
                            server_raw = env_data.get('server', 'default')
                            database_raw = env_data.get('database', '')
                            table_count_raw = env_data.get('table_count', 0)
                            server = str(server_raw)
                            database = str(database_raw)
                            table_count = (
                                int(table_count_raw)
                                if isinstance(table_count_raw, (int, str))
                                else 0
                            )
                            print_info(f"   🌍 {env}:")
                            print_info(f"       Server: {server}")
                            print_info(f"       Database: {database}")
                            print_info(f"       Tables: {table_count}")
            else:
                print_warning("Server group has no sources configured.")
                print_info("Run 'cdc manage-source-groups --update' to discover databases.")
            return 0
        except Exception as e:
            print_error(f"Failed to view services: {e}")
            return 1


    # Handle set validation env
    if args.set_validation_env:
        return handle_set_validation_env(args)

    # Handle list envs
    if args.list_envs:
        return handle_list_envs(args)

    # Handle update (the primary action)
    if args.all and args.update is None:
        print_error("'--all' requires '--update'.")
        print_info("Example: cdc manage-source-groups --update --all")
        return 1

    if args.update is not None:
        return handle_update(args)

    # No action specified
    print_error("No action specified. Use --update or --info.")
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

