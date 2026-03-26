"""Sink-key and template completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetMultiParamValuesFn = Callable[[click.Context, str], list[str]]
ResolveMapColumnTablesFn = Callable[
    [str, str, str, str, str, str, str],
    tuple[str | None, str | None],
]
MappedMapColumnStateFn = Callable[[list[str]], tuple[set[str], set[str], str | None]]
GetSelectedAddColumnTemplateTargetsFn = Callable[[click.Context, str], set[str]]

_SINK_KEY_PARTS = 2


def _sink_target_service(sink_key: str) -> str:
    """Extract target service from sink key (sink_group.target_service)."""
    parts = sink_key.split(".", 1)
    if len(parts) != _SINK_KEY_PARTS:
        return ""
    return parts[1]


def complete_available_sink_keys_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with available sink keys from sink-groups.yaml."""
    from cdc_generator.helpers.autocompletions.sinks import (
        list_available_sink_keys,
        list_sink_keys_for_service,
    )

    available_keys = safe_call(list_available_sink_keys)
    if not service:
        return filter_items(available_keys, incomplete)

    existing_keys = set(safe_call(list_sink_keys_for_service, service))
    filtered_keys = sorted(
        key for key in available_keys
        if key not in existing_keys
        and _sink_target_service(key) != service
    )
    return filter_items(filtered_keys, incomplete)


def complete_column_templates_impl(  # noqa: PLR0913
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
    resolve_map_column_tables: ResolveMapColumnTablesFn,
    mapped_map_column_state: MappedMapColumnStateFn,
    get_multi_param_values: GetMultiParamValuesFn,
    get_selected_add_column_template_targets: GetSelectedAddColumnTemplateTargetsFn,
    map_column_max_suggestions: int,
) -> list[CompletionItem]:
    """Complete column templates (plain or target:template in add mode)."""
    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_column_template_keys,
        list_compatible_column_template_pairs_for_target_prefix,
        list_compatible_target_prefixes_for_column_template,
    )

    is_add_sink_table_mode = bool(add_sink_table)
    if not is_add_sink_table_mode or not service or not sink_key:
        return filter_items(safe_call(list_column_template_keys), incomplete)

    _source_table, target_table_resolved = resolve_map_column_tables(
        service,
        sink_key,
        sink_table,
        target_table,
        from_table,
        add_sink_table,
        sink_schema,
    )

    mapped_targets, _mapped_sources, _pending_legacy = mapped_map_column_state(
        [value for value in get_multi_param_values(ctx, "map_column") if value],
    )
    selected_template_targets = get_selected_add_column_template_targets(
        ctx,
        incomplete,
    )
    blocked_targets = mapped_targets | selected_template_targets

    if not target_table_resolved:
        return filter_items(safe_call(list_column_template_keys), incomplete)

    if ":" in incomplete:
        target_prefix, template_prefix = incomplete.split(":", 1)
        try:
            pair_suggestions = list_compatible_column_template_pairs_for_target_prefix(
                sink_key,
                target_table_resolved,
                target_prefix,
                template_prefix,
                map_column_max_suggestions,
            )
        except Exception:
            pair_suggestions = []
        if pair_suggestions:
            filtered_pairs: list[str] = []
            for pair in pair_suggestions:
                if ":" not in pair:
                    continue
                target_name, _template_name = pair.split(":", 1)
                if target_name.casefold() in blocked_targets:
                    continue
                filtered_pairs.append(pair)
            pair_suggestions = filtered_pairs
        if pair_suggestions:
            return filter_items(pair_suggestions, incomplete)
        return filter_items(safe_call(list_column_template_keys), incomplete)

    try:
        prefix_suggestions = list_compatible_target_prefixes_for_column_template(
            sink_key,
            target_table_resolved,
            map_column_max_suggestions,
        )
    except Exception:
        prefix_suggestions = []
    if prefix_suggestions:
        prefix_suggestions = [
            prefix
            for prefix in prefix_suggestions
            if prefix[:-1].casefold() not in blocked_targets
        ]
    if prefix_suggestions:
        return filter_items(prefix_suggestions, incomplete)

    return filter_items(safe_call(list_column_template_keys), incomplete)


def complete_transform_rules_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with transform Bloblang file references."""
    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_transform_rule_keys,
    )

    return filter_items(safe_call(list_transform_rule_keys), incomplete)
