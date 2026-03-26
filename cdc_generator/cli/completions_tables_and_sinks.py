"""Table and sink completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetMultiParamValuesFn = Callable[[click.Context, str], list[str]]
GetExistingSourceColumnRefsFn = Callable[[str, str], set[str]]

_SINK_KEY_PARTS = 2
_SCHEMA_TABLE_PARTS = 2


def complete_available_tables_impl(
    ctx: click.Context,
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_multi_param_values: GetMultiParamValuesFn,
) -> list[CompletionItem]:
    """Complete with available tables from service-schemas."""
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_source_tables_for_service,
        list_tables_for_service,
    )

    candidates = safe_call(list_tables_for_service, service)

    selected_tables = {
        table.casefold()
        for table in get_multi_param_values(ctx, "add_source_table")
        if table.strip()
    }

    existing_source_tables = {
        table.casefold()
        for table in safe_call(list_source_tables_for_service, service)
        if table.strip()
    }

    excluded_tables = selected_tables.union(existing_source_tables)

    filtered_candidates = [
        table_name
        for table_name in candidates
        if table_name.casefold() not in excluded_tables
    ]

    return filter_items(filtered_candidates, incomplete)


def complete_source_tables_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with existing source tables in service YAML."""
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_source_tables_for_service,
    )

    return filter_items(
        safe_call(list_source_tables_for_service, service),
        incomplete,
    )


def complete_track_tables_impl(
    ctx: click.Context,
    incomplete: str,
    service: str,
    inspect_sink_value: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_multi_param_values: GetMultiParamValuesFn,
) -> list[CompletionItem]:
    """Complete tracked table refs (schema.table) from existing schema resources."""
    if not service:
        return []

    if inspect_sink_value and inspect_sink_value not in {"", "__all_sinks__"}:
        parts = inspect_sink_value.split(".", 1)
        if len(parts) == _SINK_KEY_PARTS and parts[1].strip():
            service = parts[1].strip()

    from cdc_generator.helpers.autocompletions.tables import (
        list_tables_for_service_autocomplete,
    )

    candidates = safe_call(list_tables_for_service_autocomplete, service)
    already_selected = {
        value.casefold()
        for value in get_multi_param_values(ctx, "track_table")
        if value.strip()
    }

    filtered = [
        table_name
        for table_name in candidates
        if table_name.casefold() not in already_selected
    ]
    return filter_items(filtered, incomplete)


def complete_from_table_impl(
    base: list[CompletionItem],
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --from from service source.tables keys plus all."""
    all_item = CompletionItem("all")
    if "all".startswith(incomplete):
        return [all_item, *base]
    return base


def complete_sink_keys_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with sink keys for current service."""
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_keys_for_service,
    )

    return filter_items(
        safe_call(list_sink_keys_for_service, service),
        incomplete,
    )


def complete_schemas_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with schemas for current service."""
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.schemas import (
        list_schemas_for_service,
    )

    return filter_items(
        safe_call(list_schemas_for_service, service),
        incomplete,
    )


def complete_columns_impl(  # noqa: PLR0913
    ctx: click.Context,
    incomplete: str,
    service: str,
    table_spec: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_multi_param_values: GetMultiParamValuesFn,
    get_existing_source_column_refs: GetExistingSourceColumnRefsFn,
) -> list[CompletionItem]:
    """Complete columns for a service table."""
    if not service or not table_spec:
        return []

    parts = table_spec.split(".")
    if len(parts) != _SCHEMA_TABLE_PARTS:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_columns_for_table,
    )

    candidates = safe_call(list_columns_for_table, service, parts[0], parts[1])

    excluded = set(get_multi_param_values(ctx, "track_columns"))
    excluded.update(get_multi_param_values(ctx, "ignore_columns"))
    excluded.update(get_existing_source_column_refs(service, table_spec))

    filtered = [col for col in candidates if col not in excluded]
    return filter_items(filtered, incomplete)


def complete_sink_tables_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete sink tables for current service and sink."""
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_tables_for_service,
    )

    return filter_items(
        safe_call(list_sink_tables_for_service, service, sink_key),
        incomplete,
    )


def complete_add_sink_table_impl(
    incomplete: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete tables available to add to a sink."""
    if not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_tables_for_sink_target,
    )

    return filter_items(
        safe_call(list_tables_for_sink_target, sink_key),
        incomplete,
    )


def complete_add_custom_sink_table_impl(
    incomplete: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete table refs for --add-custom-sink-table from schema resources."""
    if not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_definitions_for_sink_target,
    )

    return filter_items(
        safe_call(list_custom_table_definitions_for_sink_target, sink_key),
        incomplete,
    )


def complete_remove_sink_table_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete tables to remove from a sink."""
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_tables_for_service,
    )

    return filter_items(
        safe_call(list_sink_tables_for_service, service, sink_key),
        incomplete,
    )


def complete_target_tables_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete target tables for a sink."""
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_target_tables_for_sink,
    )

    return filter_items(
        safe_call(list_target_tables_for_sink, service, sink_key),
        incomplete,
    )
