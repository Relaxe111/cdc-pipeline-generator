"""Map/include/accept column completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetMultiParamValuesFn = Callable[[click.Context, str], list[str]]
GetSelectedColumnsFn = Callable[[click.Context], set[str]]
GetSelectedColumnsWithIncompleteFn = Callable[[click.Context, str], set[str]]
_SCHEMA_TABLE_PARTS = 2


def resolve_map_column_tables(
    service: str,
    sink_key: str,
    sink_table: str,
    target_table: str,
    from_table: str,
    add_sink_table: str,
    sink_schema: str,
) -> tuple[str | None, str | None]:
    """Resolve source/target tables for map-column completions."""
    from cdc_generator.helpers.autocompletions.sinks import (
        load_sink_tables_for_autocomplete,
    )

    source_table: str | None = None
    target_table_resolved: str | None = None

    if sink_table:
        source_table = sink_table
        target_table_resolved = sink_table

        sink_tables_dict = load_sink_tables_for_autocomplete(service, sink_key) or {}
        table_cfg = sink_tables_dict.get(sink_table)
        if isinstance(table_cfg, dict):
            table_cfg_dict = cast(dict[str, object], table_cfg)
            configured_from = table_cfg_dict.get("from")
            configured_target = table_cfg_dict.get("target")
            if isinstance(configured_from, str) and configured_from:
                source_table = configured_from
            if isinstance(configured_target, str) and configured_target:
                target_table_resolved = configured_target

    if add_sink_table:
        source_table = from_table or add_sink_table
        target_table_resolved = target_table or add_sink_table
        if sink_schema and target_table_resolved and "." in target_table_resolved:
            target_table_resolved = (
                f"{sink_schema}.{target_table_resolved.split('.', 1)[1]}"
            )

    if from_table and target_table:
        source_table = from_table
        target_table_resolved = target_table

    return source_table, target_table_resolved


def mapped_map_column_state(
    values: list[str],
) -> tuple[set[str], set[str], str | None]:
    """Return mapped targets/sources and pending legacy source token."""
    mapped_targets: set[str] = set()
    mapped_sources: set[str] = set()
    pending_legacy_source: str | None = None

    for value in values:
        if ":" in value:
            target_name_raw, source_name_raw = value.split(":", 1)
            target_name = target_name_raw.strip()
            source_name = source_name_raw.strip()
            if target_name:
                mapped_targets.add(target_name.casefold())
            if source_name:
                mapped_sources.add(source_name.casefold())
        else:
            pending_legacy_source = value

    return mapped_targets, mapped_sources, pending_legacy_source


def filter_unmapped_pairs(
    pairs: list[str],
    mapped_targets: set[str],
    mapped_sources: set[str],
) -> list[str]:
    """Remove pairs whose target or source is already mapped."""
    filtered_pairs: list[str] = []
    for pair in pairs:
        if ":" not in pair:
            continue
        target_name_raw, source_name_raw = pair.split(":", 1)
        target_name = target_name_raw.strip()
        source_name = source_name_raw.strip()
        if target_name.casefold() in mapped_targets:
            continue
        if source_name.casefold() in mapped_sources:
            continue
        filtered_pairs.append(pair)
    return filtered_pairs


def complete_map_column_impl(  # noqa: PLR0913
    ctx: click.Context,
    incomplete: str,
    service: str,
    sink_key: str,
    sink_table: str,
    target_table: str,
    from_table: str,
    add_sink_table: str,
    sink_schema: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_multi_param_values: GetMultiParamValuesFn,
    map_column_max_suggestions: int,
) -> list[CompletionItem]:
    """Complete --map-column args with source/target compatibility filtering."""
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_compatible_map_column_pairs_for_target_prefix,
        list_compatible_target_columns_for_source_column,
        list_compatible_target_prefixes_for_map_column,
    )

    source_table, target_table_resolved = resolve_map_column_tables(
        service,
        sink_key,
        sink_table,
        target_table,
        from_table,
        add_sink_table,
        sink_schema,
    )

    if not source_table or not target_table_resolved:
        return []

    provided_parts = [
        value for value in get_multi_param_values(ctx, "map_column") if value
    ]

    mapped_targets, mapped_sources, pending_legacy_source = mapped_map_column_state(
        provided_parts,
    )

    if pending_legacy_source:
        source_column = pending_legacy_source

        target_candidates = safe_call(
            list_compatible_target_columns_for_source_column,
            service,
            sink_key,
            source_table,
            target_table_resolved,
            source_column,
        )
        filtered_targets = [
            target_name
            for target_name in target_candidates
            if target_name.casefold() not in mapped_targets
        ]
        return filter_items(filtered_targets, incomplete)

    if ":" in incomplete:
        target_prefix, source_prefix = incomplete.split(":", 1)
        try:
            pair_suggestions = list_compatible_map_column_pairs_for_target_prefix(
                service,
                sink_key,
                source_table,
                target_table_resolved,
                target_prefix,
                source_prefix,
                map_column_max_suggestions,
            )
        except Exception:
            return []

        filtered_pairs = filter_unmapped_pairs(
            pair_suggestions,
            mapped_targets,
            mapped_sources,
        )

        return filter_items(filtered_pairs, incomplete)

    try:
        target_prefixes = list_compatible_target_prefixes_for_map_column(
            service,
            sink_key,
            source_table,
            target_table_resolved,
            map_column_max_suggestions,
        )
    except Exception:
        return []

    filtered_prefixes = [
        prefix
        for prefix in target_prefixes
        if prefix[:-1].casefold() not in mapped_targets
    ]

    return filter_items(filtered_prefixes, incomplete)


def complete_include_sink_columns_impl(
    incomplete: str,
    service: str,
    table_spec: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete columns for --include-sink-columns (from --add-sink-table)."""
    if not service or not table_spec:
        return []

    parts = table_spec.split(".")
    if len(parts) != _SCHEMA_TABLE_PARTS:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_columns_for_table,
    )

    return filter_items(
        safe_call(list_columns_for_table, service, parts[0], parts[1]),
        incomplete,
    )


def complete_accept_column_impl(  # noqa: PLR0913
    ctx: click.Context,
    incomplete: str,
    service: str,
    sink_key: str,
    sink_table: str,
    target_table: str,
    from_table: str,
    add_sink_table: str,
    sink_schema: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_selected_map_column_targets: GetSelectedColumnsFn,
    get_selected_add_column_template_targets: GetSelectedColumnsWithIncompleteFn,
    get_selected_accept_columns: GetSelectedColumnsWithIncompleteFn,
) -> list[CompletionItem]:
    """Complete --accept-column with uncovered sink target columns."""
    if not service or not sink_key or not add_sink_table:
        return []

    _source_table, target_table_resolved = resolve_map_column_tables(
        service,
        sink_key,
        sink_table,
        target_table,
        from_table,
        add_sink_table,
        sink_schema,
    )

    if not target_table_resolved:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_target_columns_for_sink_table,
    )

    target_columns = safe_call(
        list_target_columns_for_sink_table,
        sink_key,
        target_table_resolved,
    )

    blocked_columns = (
        get_selected_map_column_targets(ctx)
        | get_selected_add_column_template_targets(ctx, incomplete)
        | get_selected_accept_columns(ctx, incomplete)
    )

    remaining_columns = [
        column_name
        for column_name in target_columns
        if column_name.casefold() not in blocked_columns
    ]

    return filter_items(remaining_columns, incomplete)
