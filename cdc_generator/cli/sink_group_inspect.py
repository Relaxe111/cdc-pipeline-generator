"""Sink-group inspection handlers (inspect databases on sink servers)."""

from __future__ import annotations

import argparse
from typing import Any, cast

from cdc_generator.cli.sink_group_common import (
    get_source_group_file_path,
    validate_inspect_args,
)
from cdc_generator.core.sink_types import ResolvedSinkGroup
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_sink_groups import resolve_sink_group
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file


def _run_inspection(
    resolved: ResolvedSinkGroup,
    sink_group_name: str,
    args: argparse.Namespace,
) -> int:
    """Run database inspection on a resolved sink group server.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    server_name = args.server or "default"
    servers = resolved.get("servers", {})
    if server_name not in servers:
        print_error(
            f"Server '{server_name}' not found" +
            f" in sink group '{sink_group_name}'"
        )
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    sink_type = resolved.get("type", "postgres")

    print_header(f"Inspecting Sink Server: {server_name} ({sink_type})")

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
        print_info(
            "Only 'postgres' and 'mssql' sink types support inspection."
        )
        return 1
    except ImportError as e:
        print_error(f"Database driver not installed: {e}")
        return 1
    except Exception as e:
        print_error(f"Failed to inspect databases: {e}")
        return 1

    # Display results
    print_success(f"\nFound {len(databases)} database(s):\n")
    for db in databases:
        schemas_str = ", ".join(db["schemas"])
        print(f"  {db['name']}")
        print(f"    Service:     {db['service']}")
        print(f"    Environment: {db['environment'] or 'N/A'}")
        print(f"    Schemas:     {schemas_str}")
        print(f"    Tables:      {db['table_count']}")
        print()

    print_info(
        "Note: Use this information to manually configure"
        + " sink destinations."
    )
    print_info(
        "Future enhancement: Add databases to sink group"
        + " configuration automatically."
    )
    return 0


def _fetch_databases(
    sink_type: object,
    server_config: object,
    resolved: ResolvedSinkGroup,
    args: argparse.Namespace,
    server_name: str,
) -> list[dict[str, Any]]:
    """Fetch databases from a sink server based on sink type.

    Raises:
        ValueError: If sink type is not supported for inspection.
    """
    from cdc_generator.validators.manage_server_group.db_inspector import (
        list_mssql_databases,
        list_postgres_databases,
    )

    if sink_type == "mssql":
        return list_mssql_databases(
            server_config=server_config,  # type: ignore[arg-type]
            server_group_config=resolved,  # type: ignore[arg-type]
            include_pattern=args.include_pattern,
            database_exclude_patterns=resolved.get(
                "database_exclude_patterns"
            ),
            schema_exclude_patterns=resolved.get(
                "schema_exclude_patterns"
            ),
            table_include_patterns=resolved.get(
                "table_include_patterns"
            ),
            table_exclude_patterns=resolved.get(
                "table_exclude_patterns"
            ),
            server_name=server_name,
        )
    if sink_type == "postgres":
        return list_postgres_databases(
            server_config=server_config,  # type: ignore[arg-type]
            server_group_config=resolved,  # type: ignore[arg-type]
            include_pattern=args.include_pattern,
            database_exclude_patterns=resolved.get(
                "database_exclude_patterns"
            ),
            schema_exclude_patterns=resolved.get(
                "schema_exclude_patterns"
            ),
            table_include_patterns=resolved.get(
                "table_include_patterns"
            ),
            table_exclude_patterns=resolved.get(
                "table_exclude_patterns"
            ),
            server_name=server_name,
        )
    raise ValueError(
        f"Inspection not supported for sink type: {sink_type}"
    )


def handle_inspect_command(args: argparse.Namespace) -> int:
    """Inspect databases on a standalone sink server."""
    source_file = get_source_group_file_path()

    # Validate args and load sink group
    result = validate_inspect_args(args, action_flag="--inspect")
    if isinstance(result, int):
        return result

    _sink_groups, sink_group, sink_group_name = result

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    return _run_inspection(resolved, sink_group_name, args)
