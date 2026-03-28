"""Sink-group info and list display handlers."""

from __future__ import annotations

import argparse
from typing import Any, cast

from cdc_generator.cli.sink_group_common import (
    get_sink_file_path,
    get_source_group_file_path,
)
from cdc_generator.core.sink_types import ResolvedSinkGroup
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import (
    load_sink_groups,
    resolve_sink_group,
)
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file


def handle_list(_args: argparse.Namespace) -> int:
    """List all sink groups."""
    sink_file = get_sink_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_info(f"No sink groups file found: {sink_file}")
        print_info("Use --create to create your first sink group.")
        return 0

    if not sink_groups:
        print_info("No sink groups configured.")
        return 0

    print_header("Sink Groups")
    for sink_group_name, sink_group in sink_groups.items():
        source_group = sink_group.get("source_group", "N/A")
        sink_type = sink_group.get("type", "N/A")
        pattern = sink_group.get("pattern", "N/A")
        server_count = len(sink_group.get("servers", {}))
        source_count = len(sink_group.get("sources", {}))

        print(f"\n{sink_group_name}:")
        print(f"  Source Group:  {source_group}")
        print(f"  Pattern:       {pattern}")
        print(f"  Type:          {sink_type}")
        print(f"  Servers:       {server_count}")
        print(f"  Sources:       {source_count}")

        if sink_group.get("inherited_sources"):
            inherited = sink_group.get("inherited_sources", [])
            if isinstance(inherited, list):
                inherited_str = cast(list[str], inherited)
                print(f"  Inherited:     {', '.join(inherited_str)}")

    return 0


def handle_info_command(args: argparse.Namespace) -> int:
    """Show detailed information about a sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    if not sink_groups:
        print_warning("sink-groups.yaml is empty — no sink groups defined")
        print_info("Create one with: cdc manage-sink-groups --create")
        return 1

    sink_group_name = args.info
    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found.")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    print_header(f"Sink Group: {sink_group_name}")
    print(f"Source Group:      {resolved.get('source_group', 'N/A')}")
    print(f"Pattern:           {resolved.get('pattern', 'N/A')}")
    print(f"Type:              {resolved.get('type', 'N/A')}")
    broker_topology = resolved.get('broker_topology')
    if broker_topology is not None:
        print(f"Broker Topology:   {broker_topology}")
    print(f"Topology Kind:     {resolved.get('topology_kind', 'N/A')}")
    print(f"Runtime Engine:    {resolved.get('runtime_engine', 'N/A')}")
    print(f"Environment Aware: {resolved.get('environment_aware', False)}")
    print(f"Description:       {resolved.get('description', 'N/A')}")

    # Show exclude patterns if present
    if resolved.get('database_exclude_patterns'):
        db_patterns = resolved.get('database_exclude_patterns', [])
        print(f"Database Exclude:  {', '.join(db_patterns)}")
    if resolved.get('schema_exclude_patterns'):
        schema_patterns = resolved.get('schema_exclude_patterns', [])
        print(f"Schema Exclude:    {', '.join(schema_patterns)}")
    if resolved.get('table_exclude_patterns'):
        table_patterns = resolved.get('table_exclude_patterns', [])
        print(f"Table Exclude:     {', '.join(table_patterns)}")

    if resolved.get("_inherited_from"):
        print(f"Inherited From:   {resolved.get('_inherited_from', 'N/A')}")

    print("\nServers:")
    for server_name, server_config in resolved.get("servers", {}).items():
        print(f"\n  {server_name}:")
        if "_source_ref" in server_config:
            print(f"    Source Ref:  {server_config['_source_ref']}")
        print(f"    Type:        {server_config.get('type', 'N/A')}")
        print(f"    Host:        {server_config.get('host', 'N/A')}")
        print(f"    Port:        {server_config.get('port', 'N/A')}")

    _print_sources_info(resolved)

    return 0


def _print_sources_info(resolved: ResolvedSinkGroup) -> None:
    """Print inherited_sources or sources section of a resolved sink group."""
    # Show inherited_sources for inherited sinks
    inherited_sources = resolved.get("inherited_sources")
    if inherited_sources and isinstance(inherited_sources, list):
        inherited_list = cast(list[str], inherited_sources)
        print(f"\nInherited Sources ({len(inherited_list)}):")
        for src in inherited_list:
            print(f"  - {src}")

    # Show sources for standalone sinks
    sources = resolved.get("sources", {})
    if sources:
        print("\nSources:")
        for service_name, source in sources.items():
            _print_source_detail(service_name, source)
    elif not inherited_sources:
        print("\nSources:")
        print("  (none configured)")


def _print_source_detail(
    service_name: str,
    source: object,
) -> None:
    """Print a single source entry in --info output."""
    print(f"\n  {service_name}:")
    source_dict = cast(dict[str, Any], source)
    for env_name, env_config in source_dict.items():
        if env_name == "schemas":
            print(f"    Schemas: {env_config}")
        elif isinstance(env_config, dict):
            env_dict = cast(dict[str, Any], env_config)
            server = env_dict.get("server", "N/A")
            database = env_dict.get("database", "N/A")
            schema = env_dict.get("schema", "N/A")
            print(f"    {env_name}:")
            print(f"      Server:   {server}")
            print(f"      Database: {database}")
            print(f"      Schema:   {schema}")
