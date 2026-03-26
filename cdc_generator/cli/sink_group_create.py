"""Sink-group creation handlers (scaffold, inherited, standalone)."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, cast

from cdc_generator.cli.sink_group_common import (
    get_sink_file_path,
    get_source_group_file_path,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import (
    StandaloneSinkGroupOptions,
    create_inherited_sink_group,
    create_standalone_sink_group,
    load_sink_groups,
    save_sink_groups,
    should_inherit_services,
)
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file


def _auto_scaffold_sink_groups(
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
) -> int:
    """Auto-scaffold sink groups for all db-shared source groups."""
    print_header("Auto-scaffolding sink groups from source groups")

    created_count = 0
    for source_group_name, source_group in source_groups.items():
        if not should_inherit_services(source_group):
            pattern = source_group.get("pattern", "unknown")
            msg = (f"Skipping '{source_group_name}' (pattern: {pattern})"
                   " — inheritance only for db-shared")
            print_info(msg)
            continue

        sink_group_name = f"sink_{source_group_name}"

        if sink_group_name in sink_groups:
            print_warning(f"Sink group '{sink_group_name}' already exists, skipping")
            continue

        source_services = source_group.get("sources", {})
        if not isinstance(source_services, dict):
            print_warning(f"Invalid sources for '{source_group_name}', skipping")
            continue

        new_sink_group = create_inherited_sink_group(
            source_group_name,
            source_group,
            source_services,
        )

        sink_groups[sink_group_name] = new_sink_group
        created_count += 1
        print_success(f"Created '{sink_group_name}' from source group '{source_group_name}'")

    if created_count == 0:
        print_warning("No new sink groups created")
        print_info("Use --source-group or --name to create specific sink groups")
        return 0

    save_sink_groups(sink_groups, sink_file)
    print_success(f"\nCreated {created_count} sink group(s) in {sink_file}")
    print_info("\nNext steps:")
    print_info("1. Edit services/*.yaml to add sink configurations")
    print_info("2. Run 'cdc generate' to create sink pipelines")

    return 0


def _create_inherited_sink_group_from_source(
    args: argparse.Namespace,
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
    source_file: Path,
) -> int:
    """Create inherited sink group from specific source group."""
    source_group_name = args.source_group

    if source_group_name not in source_groups:
        print_error(f"Source group '{source_group_name}' not found in {source_file}")
        print_info(f"Available source groups: {list(source_groups.keys())}")
        return 1

    source_group = source_groups[source_group_name]

    if not should_inherit_services(source_group):
        pattern = source_group.get("pattern", "unknown")
        msg = (f"Source group '{source_group_name}' has pattern '{pattern}'. "
               "Inheriting services only makes sense for 'db-shared' pattern.")
        print_warning(msg)
        print_info("Recommendation: Use --name to create a standalone sink group instead.")
        return 1

    sink_group_name = f"sink_{source_group_name}"

    if sink_group_name in sink_groups:
        print_warning(f"Sink group '{sink_group_name}' already exists.")
        return 1

    source_services = source_group.get("sources", {})
    if not isinstance(source_services, dict):
        print_error(f"Invalid sources configuration for '{source_group_name}'")
        return 1

    new_sink_group = create_inherited_sink_group(
        source_group_name,
        source_group,
        source_services,
    )

    sink_groups[sink_group_name] = new_sink_group

    servers = new_sink_group.get("servers", {})
    print_success(f"Created inherited sink group '{sink_group_name}'")
    print_info(f"Source group: {source_group_name}")
    print_info(f"Pattern: {source_group.get('pattern')}")
    print_info(f"Inherited servers: {list(servers.keys())}")
    print_info(f"Inherited sources: {new_sink_group.get('inherited_sources', [])}")
    print_info("\nNext steps:")
    print_info("1. Edit services/*.yaml to add sink configurations referencing this sink group")
    print_info("2. Run 'cdc generate' to create sink pipelines")

    save_sink_groups(sink_groups, sink_file)
    print_success(f"Saved to {sink_file}")
    return 0


def _create_standalone_sink(
    args: argparse.Namespace,
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
    source_file: Path,
) -> int:
    """Create standalone sink group."""
    base_name = args.add_new_sink_group
    # Always add sink_ prefix
    sink_group_name = f"sink_{base_name}" if not base_name.startswith("sink_") else base_name
    sink_type = args.type or "postgres"
    pattern = args.pattern or "db-shared"
    environment_aware = args.environment_aware
    source_group_name = args.for_source_group

    if not source_group_name:
        if not source_groups:
            print_error("No source groups found in source-groups.yaml")
            return 1
        source_group_name = next(iter(source_groups.keys()))
        print_info(f"No --for-source-group specified, using '{source_group_name}'")

    if source_group_name not in source_groups:
        print_error(f"Source group '{source_group_name}' not found in {source_file}")
        print_info(f"Available source groups: {list(source_groups.keys())}")
        return 1

    if sink_group_name in sink_groups:
        print_warning(f"Sink group '{sink_group_name}' already exists.")
        return 1

    new_sink_group = create_standalone_sink_group(
        sink_group_name,
        source_group_name,
        StandaloneSinkGroupOptions(
            sink_type=sink_type,
            pattern=pattern,
            environment_aware=environment_aware,
            database_exclude_patterns=getattr(
                args, 'database_exclude_patterns', None
            ),
            schema_exclude_patterns=getattr(
                args, 'schema_exclude_patterns', None
            ),
            table_exclude_patterns=getattr(
                args, 'table_exclude_patterns', None
            ),
        ),
    )

    sink_groups[sink_group_name] = new_sink_group

    print_success(f"Created standalone sink group '{sink_group_name}'")
    print_info(f"Type: {sink_type}")
    print_info(f"Pattern: {pattern}")
    print_info(f"Environment Aware: {environment_aware}")
    print_info(f"Source group: {source_group_name}")
    if getattr(args, 'database_exclude_patterns', None):
        print_info(f"Database exclude patterns: {args.database_exclude_patterns}")
    if getattr(args, 'schema_exclude_patterns', None):
        print_info(f"Schema exclude patterns: {args.schema_exclude_patterns}")
    if getattr(args, 'table_exclude_patterns', None):
        print_info(f"Table exclude patterns: {args.table_exclude_patterns}")
    print_info("\nNext steps:")
    print_info(f"1. Add servers to sink group '{sink_group_name}' using --add-server")
    print_info("2. Edit services/*.yaml to add sink configurations")
    print_info("3. Run 'cdc generate' to create sink pipelines")

    save_sink_groups(sink_groups, sink_file)
    print_success(f"Saved to {sink_file}")
    return 0


def handle_create(args: argparse.Namespace) -> int:
    """Create a new sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    # Load existing sink groups (or create empty)
    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        sink_groups = {}

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Route to appropriate create handler
    if not args.source_group:
        return _auto_scaffold_sink_groups(sink_groups, source_groups, sink_file)

    if args.source_group:
        return _create_inherited_sink_group_from_source(
            args, sink_groups, source_groups, sink_file, source_file
        )

    print_error("Unexpected state in handle_create")
    return 1



def handle_add_new_sink_group(args: argparse.Namespace) -> int:
    """Add a new standalone sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    # Load existing sink groups (or create empty)
    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        sink_groups = {}

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    return _create_standalone_sink(
        args, sink_groups, source_groups, sink_file, source_file
    )
