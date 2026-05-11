"""Compatibility validation helpers for add-sink-table flow."""

import re
from collections.abc import Callable
from typing import Any, cast

from cdc_generator.core.migration_generator.columns import (
    NATIVE_CDC_METADATA_COLUMNS,
)
from cdc_generator.core.migration_generator.service_parsing import (
    resolve_source_group_config,
)
from cdc_generator.helpers.helpers_logging import print_info, print_warning
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.topology_runtime import resolve_runtime_mode

from .sink_mapping import load_table_columns, validate_column_mappings
from .sink_operations_helpers import _get_source_tables_dict, _get_target_service_from_sink_key
from .sink_operations_table_config import TableConfigOptions
from .sink_operations_type_compatibility import _normalize_type_name
from .sink_type_compatibility import check_type_compatibility_impl

_NATIVE_GENERATED_SINK_COLUMNS = {
    "customer_id",
    *(str(column["name"]) for column in NATIVE_CDC_METADATA_COLUMNS),
}


def _is_required_sink_column(column: dict[str, Any]) -> bool:
    nullable = bool(column.get("nullable", True))
    primary_key = bool(column.get("primary_key", False))
    has_default = column.get("default") is not None or column.get("default_value") is not None
    if has_default:
        return False
    return (not nullable) or primary_key


def _collect_named_column_types(
    columns: list[dict[str, Any]],
) -> dict[str, str]:
    types: dict[str, str] = {}
    for column in columns:
        name = column.get("name")
        type_name = column.get("type")
        if isinstance(name, str) and isinstance(type_name, str):
            types[name] = type_name
    return types


def _check_identity_type_compatibility(
    source_type: str,
    sink_type: str,
    source_table: str,
    column_name: str,
    check_type_compatibility: Callable[..., bool],
) -> bool:
    return check_type_compatibility(
        source_type,
        sink_type,
        source_table=source_table,
        source_column=column_name,
    )


def _analyze_identity_coverage(
    source_types: dict[str, str],
    sink_types: dict[str, str],
    source_table: str,
    mapped_sink_columns: set[str],
    generated_columns: set[str],
    check_type_compatibility: Callable[..., bool],
) -> tuple[set[str], list[tuple[str, str, str]]]:
    identity_covered: set[str] = set()
    incompatible_identity: list[tuple[str, str, str]] = []

    for sink_column, sink_type in sink_types.items():
        if sink_column in mapped_sink_columns or sink_column in generated_columns:
            continue

        source_type = source_types.get(sink_column)
        if source_type is None:
            continue

        try:
            compatible = _check_identity_type_compatibility(
                source_type,
                sink_type,
                source_table,
                sink_column,
                check_type_compatibility,
            )
        except ValueError as exc:
            raise ValueError(f"{sink_column} ({source_type}->{sink_type}): {exc}") from exc

        if compatible:
            identity_covered.add(sink_column)
        else:
            incompatible_identity.append((sink_column, source_type, sink_type))

    return identity_covered, incompatible_identity


def _find_required_unmapped_sink_columns(
    sink_columns: list[dict[str, Any]],
    covered_sink_columns: set[str],
) -> list[str]:
    return sorted(
        str(column.get("name"))
        for column in sink_columns
        if isinstance(column.get("name"), str) and _is_required_sink_column(column) and str(column.get("name")) not in covered_sink_columns
    )


def _validate_accepted_columns(
    accepted_columns: list[str],
    sink_columns: list[dict[str, Any]],
) -> str | None:
    sink_column_names = {str(col.get("name")) for col in sink_columns if isinstance(col.get("name"), str)}
    invalid = sorted(column_name for column_name in accepted_columns if column_name not in sink_column_names)
    if not invalid:
        return None

    available = ", ".join(sorted(sink_column_names))
    return "Invalid --accept-column value(s): " + ", ".join(invalid) + f"\nAvailable sink columns: {available}"


def _collect_column_template_coverage(opts: TableConfigOptions) -> set[str]:
    if not opts.column_template:
        return set()

    if opts.column_template_name:
        return {opts.column_template_name}

    try:
        from cdc_generator.core.column_templates import get_template

        template = get_template(opts.column_template)
    except (FileNotFoundError, ValueError):
        template = None

    if template is None:
        return {opts.column_template}

    return {template.name, opts.column_template}


def _collect_add_transform_coverage(opts: TableConfigOptions) -> set[str]:
    if not opts.add_transform:
        return set()

    return _collect_transform_coverage_from_entries(
        [{"bloblang_ref": opts.add_transform}],
    )


def _resolve_runtime_generated_columns(config: dict[str, object]) -> set[str]:
    """Return sink columns populated implicitly by the active runtime."""
    project_root = get_project_root()
    source_group_cfg = resolve_source_group_config(project_root)
    runtime_mode = resolve_runtime_mode(config, source_group=source_group_cfg)
    if runtime_mode != "native":
        return set()
    return set(_NATIVE_GENERATED_SINK_COLUMNS)


def _extract_bloblang_output_columns(bloblang: str) -> set[str]:
    from cdc_generator.validators.bloblang_parser import (
        strip_bloblang_comments,
    )

    normalized_bloblang = strip_bloblang_comments(bloblang)
    output_columns: set[str] = set()

    for match in re.finditer(
        r"root\.([a-zA-Z_][a-zA-Z0-9_$]*)\s*=",
        normalized_bloblang,
    ):
        output_columns.add(match.group(1))

    for merge_match in re.finditer(
        r"merge\(\s*\{(.*?)\}\s*\)",
        normalized_bloblang,
        re.DOTALL,
    ):
        body = merge_match.group(1)
        for key_match in re.finditer(r"[\"']([a-zA-Z_][a-zA-Z0-9_$]*)[\"']\s*:", body):
            output_columns.add(key_match.group(1))

    return output_columns


def _collect_transform_coverage_from_entries(
    transforms: list[object],
) -> set[str]:
    covered: set[str] = set()

    for item in transforms:
        if not isinstance(item, dict):
            continue

        entry = cast(dict[str, object], item)

        expected_output = entry.get("expected_output_column")
        if isinstance(expected_output, str) and expected_output:
            covered.add(expected_output)

        rule_name = entry.get("rule")
        if isinstance(rule_name, str) and rule_name:
            try:
                from cdc_generator.core.transform_rules import get_rule

                rule = get_rule(rule_name)
            except (FileNotFoundError, ValueError):
                rule = None

            if rule is not None and rule.output_column is not None:
                covered.add(rule.output_column.name)

        bloblang_ref = entry.get("bloblang_ref")
        if isinstance(bloblang_ref, str) and bloblang_ref:
            try:
                from cdc_generator.core.bloblang_refs import read_bloblang_ref

                bloblang = read_bloblang_ref(bloblang_ref)
            except (FileNotFoundError, ValueError):
                bloblang = None

            if isinstance(bloblang, str) and bloblang:
                covered.update(_extract_bloblang_output_columns(bloblang))

    return covered


def _collect_source_transform_coverage(
    config: dict[str, object],
    source_table: str,
) -> set[str]:
    source_tables = _get_source_tables_dict(config)
    source_cfg_raw = source_tables.get(source_table)
    if not isinstance(source_cfg_raw, dict):
        return set()

    source_cfg = cast(dict[str, object], source_cfg_raw)
    transforms_raw = source_cfg.get("transforms")
    if not isinstance(transforms_raw, list):
        return set()

    return _collect_transform_coverage_from_entries(cast(list[object], transforms_raw))


def _build_add_table_compatibility_guidance(
    source_table: str,
    sink_table: str,
    incompatible_identity: list[tuple[str, str, str]],
    required_unmapped: list[str],
    source_names: list[str],
) -> str:
    guidance_lines: list[str] = [
        "Source/sink column compatibility check failed.",
        f"Source table: {source_table}",
        f"Sink table: {sink_table}",
    ]

    if incompatible_identity:
        guidance_lines.append("Incompatible same-name columns:")
        for col_name, src_type, sink_type in incompatible_identity:
            guidance_lines.append(f"  - {col_name}: source={src_type}, sink={sink_type}")
            guidance_lines.append(f"    Suggestion: --map-column {col_name} <sink_column>")

    if required_unmapped:
        guidance_lines.append("Required sink columns without compatible source mapping:")
        for col_name in sorted(required_unmapped):
            guidance_lines.append(f"  - {col_name}")
            if source_names:
                candidates = ", ".join(source_names[:5])
                guidance_lines.append(
                    "    Suggestion: add --map-column " + f"<source_column> {col_name} " + f"(available source columns: {candidates})"
                )

    guidance_lines.append("When columns match by name and type, mapping is applied implicitly.")
    return "\n".join(guidance_lines)


def validate_add_table_schema_compatibility(
    config: dict[str, object],
    service: str,
    sink_key: str,
    source_fallback_table: str,
    sink_table_key: str,
    opts: TableConfigOptions,
    *,
    load_table_columns_fn: Callable[[str, str], list[dict[str, Any]] | None] = load_table_columns,
) -> str | None:
    """Validate source/sink compatibility for add_sink_table flow."""
    if opts.replicate_structure or not opts.target_exists:
        return None

    source_table = opts.from_table if opts.from_table else source_fallback_table
    sink_table = opts.target if opts.target else sink_table_key

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        return f"Invalid sink key format: '{sink_key}'"

    source_columns = load_table_columns_fn(service, source_table)
    sink_columns = load_table_columns_fn(target_service, sink_table)
    if source_columns is None or sink_columns is None:
        print_warning("Skipping add-time compatibility check because schema files are missing.")
        print_info("Run inspect/save to enable strict checks: " + f"--inspect --all --save and --inspect-sink {sink_key} --all --save")
        return None

    explicit_mappings = opts.columns or {}
    mapping_errors = validate_column_mappings(
        list(explicit_mappings.items()),
        source_columns,
        sink_columns,
        source_table,
        sink_table,
        check_type_compatibility=check_type_compatibility_impl,
        normalize_type_name=_normalize_type_name,
    )
    if mapping_errors:
        details = "\n  - ".join(mapping_errors)
        return "Invalid --map-column configuration:\n" + f"  - {details}"

    source_types = _collect_named_column_types(source_columns)
    sink_types = _collect_named_column_types(sink_columns)
    accepted_columns = opts.accepted_columns or []
    accepted_columns_set = {column_name.strip() for column_name in accepted_columns if column_name.strip()}
    accepted_validation_error = _validate_accepted_columns(
        sorted(accepted_columns_set),
        sink_columns,
    )
    if accepted_validation_error:
        return accepted_validation_error

    mapped_sink_columns = set(explicit_mappings.values())
    template_covered = _collect_column_template_coverage(opts)
    transform_covered = _collect_source_transform_coverage(config, source_table)
    add_transform_covered = _collect_add_transform_coverage(opts)
    generated_covered = (
        template_covered | transform_covered | add_transform_covered | _resolve_runtime_generated_columns(config) | accepted_columns_set
    )
    try:
        identity_covered, incompatible_identity = _analyze_identity_coverage(
            source_types,
            sink_types,
            source_table,
            mapped_sink_columns,
            generated_covered,
            check_type_compatibility_impl,
        )
    except ValueError as exc:
        return "Type compatibility map error: " + str(exc)
    covered = mapped_sink_columns | generated_covered | identity_covered
    required_unmapped = _find_required_unmapped_sink_columns(
        sink_columns,
        covered,
    )

    if not incompatible_identity and not required_unmapped:
        return None

    source_names = sorted(source_types.keys())
    return _build_add_table_compatibility_guidance(
        source_table,
        sink_table,
        incompatible_identity,
        required_unmapped,
        source_names,
    )
