"""Custom-table and sink-group completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetParamFn = Callable[[click.Context, str], str]
CompletionFn = Callable[[click.Context, click.Parameter, str], list[CompletionItem]]


def complete_custom_tables_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete custom tables for a sink."""
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_tables_for_service_sink,
    )

    return filter_items(
        safe_call(list_custom_tables_for_service_sink, service, sink_key),
        incomplete,
    )


def complete_custom_table_columns_impl(
    incomplete: str,
    service: str,
    sink_key: str,
    table_key: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete columns for a custom table."""
    if not service or not sink_key or not table_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_columns_for_autocomplete,
    )

    return filter_items(
        safe_call(
            list_custom_table_columns_for_autocomplete,
            service,
            sink_key,
            table_key,
        ),
        incomplete,
    )


def complete_sink_group_servers_impl(
    ctx: click.Context,
    incomplete: str,
    get_param: GetParamFn,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete servers for a sink group."""
    sink_group = get_param(ctx, "sink_group")
    if not sink_group:
        sink_group = get_param(ctx, "sink_group_positional")
    if not sink_group and ctx.args:
        args_list = list(ctx.args)
        for index, token in enumerate(args_list):
            if token != "--update":
                continue
            next_index = index + 1
            if next_index < len(args_list):
                candidate = args_list[next_index]
                if candidate and not candidate.startswith("-"):
                    sink_group = candidate
                    break
    if not sink_group:
        return []

    from cdc_generator.helpers.autocompletions.server_groups import (
        list_servers_for_sink_group,
    )

    return filter_items(
        safe_call(list_servers_for_sink_group, sink_group),
        incomplete,
    )


def complete_sink_group_context_aware_impl(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
    get_param: GetParamFn,
    complete_non_inherited_sink_group_names: CompletionFn,
    complete_sink_group_names: CompletionFn,
) -> list[CompletionItem]:
    """Complete sink group — non-inherited when adding/removing servers."""
    add_server = get_param(ctx, "add_server")
    remove_server = get_param(ctx, "remove_server")

    if add_server or remove_server:
        return complete_non_inherited_sink_group_names(
            ctx, param, incomplete,
        )
    return complete_sink_group_names(ctx, param, incomplete)
