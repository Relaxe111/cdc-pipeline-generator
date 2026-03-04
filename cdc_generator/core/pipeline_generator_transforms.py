"""Transform and sink-table helper functions for pipeline generation."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any, cast

from cdc_generator.core.column_template_operations import ResolvedTransform, resolve_column_templates, resolve_transforms
from cdc_generator.core.pipeline_generator_common import find_source_name_case_insensitive, load_source_groups_config
from cdc_generator.core.source_ref_resolver import parse_source_ref, resolve_source_ref
from cdc_generator.validators.bloblang_parser import extract_root_assignments


def escape_bloblang_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"')


def resolve_template_expr(
    template_value: str,
    value_source: str,
    customer_name: str,
    env_name: str,
    server_group_name: str,
) -> str:
    if value_source == 'source_ref':
        parsed_ref = parse_source_ref(template_value)
        if parsed_ref is None:
            raise ValueError(f"Invalid source reference in column template: {template_value}")

        source_name = find_source_name_case_insensitive(server_group_name, customer_name)
        if not source_name:
            raise ValueError(
                f"Could not resolve source name for customer '{customer_name}' in group '{server_group_name}'"
            )

        resolved = resolve_source_ref(
            parsed_ref,
            source_name=source_name,
            env=env_name,
            config=load_source_groups_config(),
        )
        resolved_text = str(resolved).strip()
        if resolved_text.casefold() in {'none', 'null', ''}:
            raise ValueError(
                "Source reference resolved to null/empty value: "
                + f"{template_value} for customer '{customer_name}' env '{env_name}'. "
                + "Populate source-groups.yaml with concrete per-source value before generation."
            )
        return f'"{escape_bloblang_string(resolved_text)}"'

    if value_source == 'env':
        env_match = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", template_value.strip())
        if env_match:
            return f'env("{env_match.group(1)}")'
        return f'"{escape_bloblang_string(template_value)}"'

    if value_source == 'sql':
        lowered = template_value.strip().casefold()
        if lowered == 'now()':
            return 'now()'
        if lowered == 'gen_random_uuid()':
            return 'uuid_v4()'
        return f'"{escape_bloblang_string(template_value)}"'

    return template_value


def build_sink_table_enrichment(
    table_cfg: dict[str, Any],
    customer_name: str,
    env_name: str,
    server_group_name: str,
) -> tuple[list[str], list[str], list[str]]:
    extra_columns: list[str] = []
    extra_args: list[str] = []
    processor_steps: list[str] = []

    for resolved_template in resolve_column_templates(cast(dict[str, object], table_cfg)):
        extra_columns.append(resolved_template.name)
        extra_args.append(f'this.{resolved_template.name}')
        expr = resolve_template_expr(
            template_value=resolved_template.value,
            value_source=resolved_template.template.value_source,
            customer_name=customer_name,
            env_name=env_name,
            server_group_name=server_group_name,
        )
        processor_steps.append(f'root.{resolved_template.name} = {expr}')

    for resolved_transform in resolve_transforms(cast(dict[str, object], table_cfg)):
        for output_name in sorted(extract_root_assignments(resolved_transform.bloblang)):
            if output_name not in extra_columns:
                extra_columns.append(output_name)
                extra_args.append(f'this.{output_name}')
        if resolved_transform.execution_stage == 'sink':
            processor_steps.append(resolved_transform.bloblang)

    return extra_columns, extra_args, processor_steps


def select_sink_table_cfg_for_source(
    service_cfg: dict[str, Any],
    source_table: str,
) -> dict[str, Any] | None:
    sinks_raw = service_cfg.get('sinks', {})
    if not isinstance(sinks_raw, dict):
        return None

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return None

    sink_root_raw = sinks.get(next(iter(sinks)))
    if not isinstance(sink_root_raw, dict):
        return None

    sink_root = cast(dict[str, Any], sink_root_raw)
    tables_raw = sink_root.get('tables', {})
    if not isinstance(tables_raw, dict):
        return None

    tables = cast(dict[str, Any], tables_raw)
    exact_match: dict[str, Any] | None = None
    fallback: dict[str, Any] | None = None

    for sink_table_key, sink_table_cfg_raw in tables.items():
        if not isinstance(sink_table_cfg_raw, dict):
            continue
        sink_table_cfg = cast(dict[str, Any], sink_table_cfg_raw)
        from_ref = str(sink_table_cfg.get('from', '')).strip()
        if not from_ref:
            continue

        source_ref_table = from_ref.split('.', 1)[1] if '.' in from_ref else from_ref
        if source_ref_table.casefold() != source_table.casefold():
            continue

        fallback = sink_table_cfg
        target_table = str(sink_table_key).split('.', 1)[1] if '.' in str(sink_table_key) else str(sink_table_key)
        if target_table.casefold() == source_table.casefold():
            exact_match = sink_table_cfg
            break

    return exact_match if exact_match is not None else fallback


def collect_sink_table_cfgs_for_source(
    service_cfg: dict[str, Any],
    source_table: str,
) -> list[dict[str, Any]]:
    sinks_raw = service_cfg.get('sinks', {})
    if not isinstance(sinks_raw, dict):
        return []

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return []

    sink_root_raw = sinks.get(next(iter(sinks)))
    if not isinstance(sink_root_raw, dict):
        return []

    sink_root = cast(dict[str, Any], sink_root_raw)
    tables_raw = sink_root.get('tables', {})
    if not isinstance(tables_raw, dict):
        return []

    tables = cast(dict[str, Any], tables_raw)
    matching: list[dict[str, Any]] = []
    for sink_table_cfg_raw in tables.values():
        if not isinstance(sink_table_cfg_raw, dict):
            continue
        sink_table_cfg = cast(dict[str, Any], sink_table_cfg_raw)
        from_ref = str(sink_table_cfg.get('from', '')).strip()
        if not from_ref:
            continue
        source_ref_table = from_ref.split('.', 1)[1] if '.' in from_ref else from_ref
        if source_ref_table.casefold() == source_table.casefold():
            matching.append(sink_table_cfg)

    return matching


def build_runtime_processor_case(
    schema_name: str,
    table_name: str,
    processor_steps: Iterable[str],
) -> str:
    steps = list(processor_steps)
    if not steps:
        return ""

    bloblang_parts = "\n".join(f"        {step}" for step in steps)
    return (
        f'- check: \'this.__routing_schema == "{schema_name}" && '
        + f'this.__routing_table == "{table_name}"\'\n'
        + '  processors:\n'
        + '    - bloblang: |\n'
        + bloblang_parts
    )


def build_runtime_processors_block(processor_cases: list[str]) -> str:
    if not processor_cases:
        return ""

    indented_cases = "\n".join(
        "          " + processor_case.replace("\n", "\n          ")
        for processor_case in processor_cases
    )
    return "- switch:\n        cases:\n" + indented_cases


def build_source_transform_processors(
    table_cfgs: list[dict[str, Any]],
) -> str:
    if not table_cfgs:
        return ""

    source_stage: list[ResolvedTransform] = []
    seen_refs: set[str] = set()
    for table_cfg in table_cfgs:
        for transform in resolve_transforms(cast(dict[str, object], table_cfg)):
            if transform.execution_stage != 'source':
                continue
            if transform.bloblang_ref in seen_refs:
                continue
            seen_refs.add(transform.bloblang_ref)
            source_stage.append(transform)

    if not source_stage:
        return ""

    blocks: list[str] = [
        "    # Apply source-stage transforms configured for this table",
    ]

    for transform in source_stage:
        bloblang = transform.bloblang.replace("\n", "\n        ")
        blocks.append("    - bloblang: |\n        " + bloblang)

    blocks.extend([
        "    # Normalize transformed output to an array and split into messages",
        "    - bloblang: 'root = if this.type() == \"array\" { this } else { [ this ] }'",
        "    - unarchive:",
        "        format: json_array",
    ])

    return "\n" + "\n".join(blocks)