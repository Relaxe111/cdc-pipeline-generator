"""Schema/template/transform completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
_SINK_KEY_PARTS = 2


def schemas_from_sink_tables(sink_cfg: dict[str, Any]) -> set[str]:
    """Extract schema names from sink table keys (schema.table)."""
    schemas: set[str] = set()
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        return schemas

    tables = cast(dict[str, Any], tables_raw)
    for table_key in tables:
        if "." not in table_key:
            continue
        schema, _table = table_key.split(".", 1)
        if schema:
            schemas.add(schema)

    return schemas


def schemas_from_schema_table_specs(items: list[str]) -> set[str]:
    """Extract schema names from ``schema.table`` strings."""
    schemas: set[str] = set()
    for item in items:
        if "." not in item:
            continue
        schema, _table = item.split(".", 1)
        if schema:
            schemas.add(schema)
    return schemas


def common_sink_schema_data_for_all_sinks(
    service: str,
    safe_call: SafeCall,
) -> tuple[list[str], set[str]]:
    """Return common schemas plus subset that originates from custom-tables."""
    from cdc_generator.helpers.autocompletions.schemas import list_schemas_for_service
    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_definitions_for_sink_target,
    )
    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return ([], set())

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return ([], set())

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return ([], set())

    common: set[str] | None = None
    common_custom: set[str] | None = None
    for sink_key, sink_cfg_raw in sinks.items():
        if "." not in sink_key:
            continue

        _sink_group, target_service = sink_key.split(".", 1)
        sink_cfg = cast(dict[str, Any], sink_cfg_raw) if isinstance(sink_cfg_raw, dict) else {}

        candidates: set[str] = set(safe_call(list_schemas_for_service, target_service))
        candidates.update(schemas_from_sink_tables(sink_cfg))
        custom_candidates = schemas_from_schema_table_specs(
            safe_call(list_custom_table_definitions_for_sink_target, sink_key)
        )
        candidates.update(custom_candidates)

        if common is None:
            common = candidates
        else:
            common &= candidates

        if common_custom is None:
            common_custom = custom_candidates
        else:
            common_custom &= custom_candidates

    if not common:
        return ([], set())

    custom_subset = (common_custom or set()) & common
    return (sorted(common), custom_subset)


def complete_target_schema_impl(
    ctx: click.Context,
    incomplete: str,
    service: str,
    sink_key: str,
    safe_call: SafeCall,
) -> list[CompletionItem]:
    """Complete schemas for a sink's target service."""
    if not sink_key:
        all_mode = bool(ctx.params.get("all_flag"))
        if all_mode and service:
            common_schemas, common_custom_schemas = common_sink_schema_data_for_all_sinks(
                service,
                safe_call,
            )
            normalized_incomplete = incomplete.casefold()
            items: list[CompletionItem] = []
            for schema in common_schemas:
                display_value = (
                    f"custom:{schema}" if schema in common_custom_schemas else schema
                )
                if not display_value.casefold().startswith(normalized_incomplete):
                    continue
                if schema in common_custom_schemas:
                    items.append(CompletionItem(display_value, help="custom-table schema"))
                    continue

                items.append(CompletionItem(display_value))

            return items
        return []

    parts = sink_key.split(".")
    if len(parts) < _SINK_KEY_PARTS:
        return []
    target_service = parts[1]

    from cdc_generator.helpers.autocompletions.schemas import (
        list_schemas_for_service,
    )
    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_definitions_for_sink_target,
    )
    from cdc_generator.helpers.service_config import load_service_config

    candidates: set[str] = set(
        safe_call(list_schemas_for_service, target_service)
    )
    custom_schemas = schemas_from_schema_table_specs(
        safe_call(list_custom_table_definitions_for_sink_target, sink_key)
    )
    candidates.update(custom_schemas)

    try:
        config = load_service_config(service)
        sinks_raw = config.get("sinks")
        if isinstance(sinks_raw, dict):
            sink_cfg_raw = cast(dict[str, Any], sinks_raw).get(sink_key)
            if isinstance(sink_cfg_raw, dict):
                candidates.update(
                    schemas_from_sink_tables(cast(dict[str, Any], sink_cfg_raw))
                )
    except Exception:
        pass

    normalized_incomplete = incomplete.casefold()
    items: list[CompletionItem] = []
    for schema in sorted(candidates):
        display_value = (
            f"custom:{schema}" if schema in custom_schemas else schema
        )
        if not display_value.casefold().startswith(normalized_incomplete):
            continue
        if schema in custom_schemas:
            items.append(CompletionItem(display_value, help="custom-table schema"))
            continue

        items.append(CompletionItem(display_value))
    return items


def complete_templates_on_table_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    sink_table: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete column templates applied to a sink table."""
    if not service or not sink_key or not sink_table:
        return []

    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_column_templates_for_table,
    )

    return filter_items(
        safe_call(
            list_column_templates_for_table, service, sink_key, sink_table
        ),
        incomplete,
    )


def complete_transforms_on_table_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    sink_table: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete transforms applied to a sink table."""
    if not service or not sink_key or not sink_table:
        return []

    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_transforms_for_table,
    )

    return filter_items(
        safe_call(
            list_transforms_for_table, service, sink_key, sink_table
        ),
        incomplete,
    )
