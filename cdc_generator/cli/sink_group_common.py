"""Shared helpers for sink-group CLI subcommand modules.

Provides path resolution, argument validation, and group resolution
utilities used by multiple ``sink_group_*`` extraction modules.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cdc_generator.core.sink_types import SinkGroupConfig
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import load_sink_groups
from cdc_generator.helpers.service_config import get_project_root


def get_sink_file_path() -> Path:
    """Get path to sink-groups.yaml in current implementation."""
    return get_project_root() / "sink-groups.yaml"


def get_source_group_file_path() -> Path:
    """Get path to source-groups.yaml in current implementation."""
    root = get_project_root()
    source_file = root / "source-groups.yaml"
    if not source_file.exists():
        print_error(f"Source server group file not found: {source_file}")
        print_info("This command must be run from an implementation directory.")
        sys.exit(1)
    return source_file


def validate_inspect_args(
    args: argparse.Namespace,
    *,
    action_flag: str = "--inspect",
) -> tuple[dict[str, SinkGroupConfig], SinkGroupConfig, str] | int:
    """Validate inspect command arguments and load required data.

    Returns:
        Tuple of (sink_groups, sink_group, sink_group_name) on success,
        or an int exit code on validation failure.
    """
    sink_file = get_sink_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    if not sink_groups:
        print_warning("sink-groups.yaml is empty — no sink groups defined")
        print_info("Create one with: cdc manage-sink-groups --create")
        return 1

    sink_group_name = args.sink_group
    if not sink_group_name:
        if action_flag in {"--inspect", "--update"} and len(sink_groups) == 1:
            sink_group_name = next(iter(sink_groups.keys()))
            print_info(
                "No --sink-group specified; using only available sink group: "
                + f"{sink_group_name}"
            )
            args.sink_group = sink_group_name
        else:
            if action_flag in {"--inspect", "--update"}:
                print_error(
                    "More than one sink group found. Please pick one sink group with --sink-group."
                )
                print_info(f"Available sink groups: {list(sink_groups.keys())}")
            else:
                print_error(
                    f"Error: {action_flag} requires --sink-group <name>"
                )
            print_info(
                "Usage: cdc manage-sink-groups "
                + f"{action_flag} --sink-group <name>"
            )
            return 1

    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found.")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]

    # Inherited sink groups have source_ref — cannot be inspected
    servers = sink_group.get("servers", {})
    if servers:
        first_server = next(iter(servers.values()))
        if "source_ref" in first_server:
            print_error(
                "Error: Cannot inspect inherited sink group" +
                f" '{sink_group_name}'"
            )
            print_info(
                "Inspection is only available for standalone" +
                " sink groups (created with --add-new-sink-group)"
            )
            print_info(
                "Inherited sink groups (created with --create)"
                + " use source_ref and inherit from source groups."
            )
            return 1

    return sink_groups, sink_group, sink_group_name


def load_sink_group_for_server_op(
    args: argparse.Namespace,
    _operation: str,
) -> tuple[dict[str, SinkGroupConfig], SinkGroupConfig, str, Path] | int:
    """Load and validate sink group for add/remove server operations.

    Args:
        args: Parsed CLI arguments (must have sink_group attribute).
        _operation: Operation name for error messages (e.g. '--add-server').

    Returns:
        Tuple of (sink_groups, sink_group, sink_group_name, sink_file)
        on success, or an int exit code on validation failure.
    """
    sink_file = get_sink_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    if not sink_groups:
        print_warning("sink-groups.yaml is empty — no sink groups defined")
        print_info("Create one with: cdc manage-sink-groups --create")
        return 1

    sink_group_name = args.sink_group
    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found")
        available = list(sink_groups.keys())
        print_info(f"Available sink groups: {available}")
        return 1

    return sink_groups, sink_groups[sink_group_name], sink_group_name, sink_file


def resolve_sink_group_for_pattern_update(
    args: argparse.Namespace,
    *,
    action_flag: str,
) -> tuple[dict[str, SinkGroupConfig], str, SinkGroupConfig, Path] | int:
    """Resolve target sink group for pattern update operations.

    Behavior:
    - If ``--sink-group`` is provided: validate and use it.
    - If omitted and exactly one sink group exists: auto-select it.
    - If omitted and multiple sink groups exist: fail with a friendly message.
    """
    sink_file = get_sink_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    if not sink_groups:
        print_error("No sink groups found in sink-groups.yaml")
        return 1

    sink_group_name = args.sink_group
    if not sink_group_name:
        if len(sink_groups) == 1:
            sink_group_name = next(iter(sink_groups.keys()))
            print_info(
                "No --sink-group specified; using only available sink group: "
                + f"{sink_group_name}"
            )
            args.sink_group = sink_group_name
        else:
            print_error(
                "More than one sink group found. Please pick one sink group with --sink-group."
            )
            print_info(f"Available sink groups: {list(sink_groups.keys())}")
            print_info(
                "Usage: cdc manage-sink-groups "
                + f"{action_flag} <pattern> --sink-group <name>"
            )
            return 1

    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]
    if sink_group.get("inherits", False):
        source_name = sink_group_name.removeprefix("sink_")
        print_error(
            f"Cannot apply {action_flag} for '{sink_group_name}'"
            + f" — it inherits from source group '{source_name}'"
        )
        print_info("Apply this configuration on the source group instead.")
        return 1

    return sink_groups, sink_group_name, sink_group, sink_file
