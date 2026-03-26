"""Sink-group validation handlers."""

from __future__ import annotations

import argparse
from typing import cast

from cdc_generator.cli.sink_group_common import (
    get_sink_file_path,
    get_source_group_file_path,
)
from cdc_generator.core.sink_types import SinkGroupConfig
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import (
    deduce_source_group,
    get_sink_group_warnings,
    is_sink_group_ready,
    load_sink_groups,
    resolve_sink_group,
    validate_sink_group_structure,
)
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file


def _validate_single_sink_group(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    sink_groups: dict[str, SinkGroupConfig],
    source_groups: dict[str, ConfigDict],
) -> tuple[bool, bool]:
    """Validate a single sink group and print results.

    Returns:
        Tuple of (is_valid, has_warnings)
    """
    is_valid = _check_structure_and_resolution(
        sink_group_name, sink_group, sink_groups, source_groups,
    )

    # Skip readiness/warnings when structure is invalid — they'd be misleading
    if not is_valid:
        return is_valid, False

    has_warnings = _check_readiness_and_warnings(
        sink_group_name, sink_group, sink_groups, source_groups,
    )
    return is_valid, has_warnings


def _check_structure_and_resolution(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    sink_groups: dict[str, SinkGroupConfig],
    source_groups: dict[str, ConfigDict],
) -> bool:
    """Check structure and resolution validity, printing results."""
    is_valid = True

    errors = validate_sink_group_structure(
        sink_group_name,
        sink_group,
        all_sink_groups=sink_groups,
        source_groups=source_groups,
    )
    if errors:
        is_valid = False
        for error in errors:
            print_error(f"  ✗ {error}")
    else:
        print_success("  ✓ Structure valid")

    # Resolution validation (only if no structural errors)
    if not errors:
        try:
            resolve_sink_group(sink_group_name, sink_group, source_groups)
            print_success("  ✓ All references resolve successfully")
        except ValueError as e:
            is_valid = False
            print_error(f"  ✗ Resolution failed: {e}")

    return is_valid


def _check_readiness_and_warnings(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    sink_groups: dict[str, SinkGroupConfig],
    source_groups: dict[str, ConfigDict],
) -> bool:
    """Check readiness and warnings, printing results. Returns True if warnings exist."""
    ready = is_sink_group_ready(
        sink_group_name, sink_group, sink_groups, source_groups,
    )
    if ready:
        print_success("  ✓ Ready for use as sink target")
    else:
        print_warning("  ⚠ Not ready for use as sink target")

    warnings = get_sink_group_warnings(sink_group_name, sink_group)
    for warning in warnings:
        print_warning(f"  ⚠ {warning}")

    inherits = sink_group.get("inherits", False)
    if inherits:
        source_group = deduce_source_group(sink_group_name)
        print_info(f"  → Inherits from source group: {source_group}")

    return bool(warnings)


def handle_validate_command(_args: argparse.Namespace) -> int:
    """Validate sink group configuration."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_info(f"No sink groups file found: {sink_file}")
        print_info("Create one with: cdc manage-sink-groups --create")
        return 0

    if not sink_groups:
        print_warning("sink-groups.yaml is empty — no sink groups to validate")
        print_info("Create one with: cdc manage-sink-groups --create")
        return 0

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    print_header("Validating Sink Groups")

    all_valid = True
    has_warnings = False

    for sink_group_name in sink_groups:
        print(f"\nValidating '{sink_group_name}'...")

        sink_group_val: SinkGroupConfig | None = cast(
            SinkGroupConfig | None, sink_groups[sink_group_name],
        )
        if sink_group_val is None:
            print_error(
                f"  ✗ '{sink_group_name}' has no configuration"
                + " (empty entry in sink-groups.yaml)"
            )
            print_info(
                f"  Either define it or remove the '{sink_group_name}:'"
                + " line from sink-groups.yaml"
            )
            all_valid = False
            continue

        valid, warned = _validate_single_sink_group(
            sink_group_name, sink_group_val, sink_groups, source_groups,
        )
        if not valid:
            all_valid = False
        if warned:
            has_warnings = True

    if all_valid and not has_warnings:
        print_success("\n✓ All sink groups are valid")
        return 0

    if all_valid and has_warnings:
        print_warning("\n⚠ All sink groups are structurally valid but have warnings")
        return 0

    print_error("\n✗ Validation failed")
    return 1
