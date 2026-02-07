"""Sink server group validators.

Validates sink group configuration structure, references, and compatibility
with source server groups.
"""

from pathlib import Path
from typing import Any

from cdc_generator.core.sink_types import SinkGroupConfig
from cdc_generator.helpers.helpers_sink_groups import (
    deduce_source_group,
    resolve_sink_group,
    validate_sink_group_structure,
)


class SinkGroupValidationError(Exception):
    """Raised when sink group validation fails."""

    pass


def validate_source_ref_format(source_ref: str) -> str:
    """Validate source_ref format (server name only).

    Args:
        source_ref: Server name within the source group

    Returns:
        The validated server name

    Raises:
        SinkGroupValidationError: If format is invalid
    """
    if not source_ref or "/" in source_ref:
        msg = (
            f"Invalid source_ref '{source_ref}'. "
            f"Expected format: '<server_name>' (e.g., 'default', 'prod'). "
            f"Source group is deduced from sink name."
        )
        raise SinkGroupValidationError(msg)

    return source_ref


def validate_sink_group_references(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any],
) -> list[str]:
    """Validate all references in sink group resolve correctly.

    Args:
        sink_group_name: Name of sink group
        sink_group: Sink group configuration
        source_groups: All source server groups

    Returns:
        List of validation error messages (empty if valid)
    """
    errors: list[str] = []

    # Determine source group: explicit or deduced from name
    source_group_name = sink_group.get("source_group")
    if not source_group_name:
        source_group_name = deduce_source_group(sink_group_name)

    if source_group_name and source_group_name not in source_groups:
        msg = (f"Sink group '{sink_group_name}' references unknown "
               f"source group '{source_group_name}'. "
               f"Available: {list(source_groups.keys())}")
        errors.append(msg)

    # Validate server source_ref references
    for server_name, server_config in sink_group.get("servers", {}).items():
        if "source_ref" in server_config:
            source_ref = server_config["source_ref"]
            try:
                srv_name = validate_source_ref_format(source_ref)

                if source_group_name and source_group_name in source_groups:
                    available = source_groups[source_group_name].get(
                        "servers", {},
                    )
                    if srv_name not in available:
                        msg = (
                            f"Server '{server_name}' source_ref "
                            f"'{source_ref}' references unknown server "
                            f"'{srv_name}' in source group "
                            f"'{source_group_name}'"
                        )
                        errors.append(msg)
                elif not source_group_name:
                    msg = (
                        f"Server '{server_name}' has source_ref but no "
                        f"source group could be determined for "
                        f"'{sink_group_name}'"
                    )
                    errors.append(msg)
            except SinkGroupValidationError as e:
                errors.append(f"Server '{server_name}': {e}")

    return errors


def validate_sink_group_compatibility(
    sink_group_name: str,
    sink_group: SinkGroupConfig,
    source_groups: dict[str, Any],
) -> list[str]:
    """Validate sink group compatibility with source group.

    Args:
        sink_group_name: Name of sink group
        sink_group: Sink group configuration
        source_groups: All source server groups

    Returns:
        List of validation warnings (not errors)
    """
    warnings: list[str] = []

    source_group_name = sink_group.get("source_group")
    if not source_group_name or source_group_name not in source_groups:
        return warnings

    source_group = source_groups[source_group_name]

    # Check pattern compatibility
    source_pattern = source_group.get("pattern")
    sink_pattern = sink_group.get("pattern")

    if sink_pattern and source_pattern and sink_pattern != source_pattern:
        msg = (f"Sink group '{sink_group_name}' pattern '{sink_pattern}' "
               f"differs from source group pattern '{source_pattern}'. "
               f"This may cause pipeline generation issues.")
        warnings.append(msg)

    # Check if inherited sources makes sense
    if sink_group.get("inherited_sources") and source_pattern != "db-shared":
        msg = (f"Sink group '{sink_group_name}' inherits sources from "
               f"source group with pattern '{source_pattern}'. "
               f"Source inheritance only makes sense for 'db-shared' pattern.")
        warnings.append(msg)

    return warnings


def validate_all_sink_groups(
    sink_groups: dict[str, SinkGroupConfig],
    source_groups: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Validate all sink groups.

    Args:
        sink_groups: All sink group configurations
        source_groups: All source server groups

    Returns:
        Tuple of (errors, warnings)
    """
    all_errors: list[str] = []
    all_warnings: list[str] = []

    for sink_group_name, sink_group in sink_groups.items():
        # Structure validation
        structure_errors = validate_sink_group_structure(
            sink_group_name,
            sink_group,
        )
        all_errors.extend(structure_errors)

        # Reference validation
        ref_errors = validate_sink_group_references(
            sink_group_name,
            sink_group,
            source_groups,
        )
        all_errors.extend(ref_errors)

        # Compatibility validation
        compat_warnings = validate_sink_group_compatibility(
            sink_group_name,
            sink_group,
            source_groups,
        )
        all_warnings.extend(compat_warnings)

        # Try resolution
        if not structure_errors and not ref_errors:
            try:
                resolve_sink_group(sink_group_name, sink_group, source_groups)
            except Exception as e:
                all_errors.append(
                    f"Sink group '{sink_group_name}' resolution failed: {e}"
                )

    return all_errors, all_warnings


def validate_sink_file(
    sink_file_path: Path,
    source_file_path: Path,
) -> tuple[bool, list[str], list[str]]:
    """Validate entire sink groups file.

    Args:
        sink_file_path: Path to sink-groups.yaml
        source_file_path: Path to source-groups.yaml

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    from cdc_generator.helpers.helpers_sink_groups import load_sink_groups
    from cdc_generator.helpers.yaml_loader import load_yaml_file

    # Load files
    try:
        sink_groups = load_sink_groups(sink_file_path)
    except FileNotFoundError:
        return True, [], []  # No sink file is valid (optional)
    except Exception as e:
        return False, [f"Failed to load sink groups file: {e}"], []

    try:
        from typing import cast
        source_groups_raw = load_yaml_file(source_file_path)
        source_groups = cast(dict[str, Any], source_groups_raw)
    except Exception as e:
        return False, [f"Failed to load source groups file: {e}"], []

    # Validate all sink groups
    errors, warnings = validate_all_sink_groups(sink_groups, source_groups)

    is_valid = len(errors) == 0
    return is_valid, errors, warnings
