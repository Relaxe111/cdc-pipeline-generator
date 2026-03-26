"""Shared context/helper utilities for CLI shell completions."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, cast

import click
from click.shell_completion import CompletionItem


def filter_items(items: list[str], incomplete: str) -> list[CompletionItem]:
    """Filter a list of strings by prefix and wrap as CompletionItem."""
    normalized_incomplete = incomplete.casefold()
    return [
        CompletionItem(s)
        for s in items
        if s.casefold().startswith(normalized_incomplete)
    ]


def safe_call(
    func: Callable[..., Iterable[str] | str | None],
    *args: str,
) -> list[str]:
    """Call an autocompletion function, returning [] on any error."""
    try:
        result = func(*args)
        if result is None:
            return []
        if isinstance(result, str):
            return [result]
        return list(result)
    except Exception:
        return []


def get_param(ctx: click.Context, name: str) -> str:
    """Read a parameter from ctx.params, returning '' if missing."""
    val = ctx.params.get(name)
    if val is None:
        return ""
    if isinstance(val, tuple):
        sequence = list(cast(tuple[object, ...], val))
        values = [str(item) for item in sequence if str(item)]
        if not values:
            return ""
        return values[0]
    if isinstance(val, list):
        sequence = cast(list[object], val)
        values = [str(item) for item in sequence if str(item)]
        if not values:
            return ""
        return values[0]
    return str(val)


def autodetect_single_service_name() -> str:
    """Return the only existing service name when exactly one is present."""
    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )

    existing_services = safe_call(list_existing_services)
    if len(existing_services) == 1:
        return existing_services[0]
    return ""


def get_service(ctx: click.Context) -> str:
    """Get service name from --service flag or positional argument.

    manage-services config supports both ``--service directory`` and the shorthand
    ``cdc manage-services config directory``.  With ``allow_extra_args=True``,
    Click puts the positional word into ``ctx.args`` (not ``ctx.params``).
    We pick the first non-flag token from ``ctx.args`` as the service name.
    """
    svc = get_param(ctx, "service")
    if not svc:
        svc = get_param(ctx, "source")
    if not svc:
        svc = get_param(ctx, "service_positional")
    if not svc and ctx.args:
        for arg in ctx.args:
            if not arg.startswith("-"):
                return arg

    if svc:
        return svc

    return autodetect_single_service_name()


def get_sink_key_with_default(ctx: click.Context) -> str:
    """Get --sink value, falling back to auto-default for single-sink services."""
    sink_key = get_param(ctx, "sink")
    if not sink_key:
        service = get_service(ctx)
        if service:
            from cdc_generator.helpers.autocompletions.sinks import (
                get_default_sink_for_service,
            )

            result = safe_call(get_default_sink_for_service, service)
            sink_key = result[0] if result else ""
    return sink_key


def get_table_spec(ctx: click.Context) -> str:
    """Get table spec from --add-source-table or --source-table."""
    spec = get_param(ctx, "add_source_table")
    if not spec:
        spec = get_param(ctx, "source_table")
    return spec


def get_multi_param_values(ctx: click.Context, name: str) -> list[str]:
    """Get values for multi-value params, flattening tuples/lists."""
    raw: object = ctx.params.get(name)
    if raw is None:
        return []

    if isinstance(raw, str):
        return [raw]

    if not isinstance(raw, tuple | list):
        return [str(raw)]

    values: list[str] = []
    raw_items = cast(Iterable[object], raw)
    for item in raw_items:
        if isinstance(item, str):
            values.append(item)
        elif isinstance(item, tuple | list):
            nested_values = cast(Iterable[object], item)
            values.extend(str(nested) for nested in nested_values)
        else:
            values.append(str(item))
    return values


def get_option_values_from_args(ctx: click.Context, option_name: str) -> list[str]:
    """Collect values passed after a repeated option from raw ``ctx.args``."""
    values: list[str] = []
    args_list = list(ctx.args)
    for index, token in enumerate(args_list):
        if token != option_name:
            continue
        next_index = index + 1
        if next_index >= len(args_list):
            continue
        value = args_list[next_index]
        if not value or value.startswith("-"):
            continue
        values.append(value)
    return values


def get_selected_accept_columns(
    ctx: click.Context,
    incomplete: str,
) -> set[str]:
    """Collect already selected values from ``--accept-column``."""
    selected: set[str] = set()

    values = get_multi_param_values(ctx, "accept_column") + get_option_values_from_args(
        ctx,
        "--accept-column",
    )
    for value in values:
        column_name = value.strip()
        if not column_name or column_name == incomplete:
            continue
        selected.add(column_name.casefold())

    return selected


def get_selected_add_column_template_targets(
    ctx: click.Context,
    incomplete: str,
) -> set[str]:
    """Collect already selected target columns from add-column-template pairs.

    Supports both parsed params and raw ``ctx.args`` tokens, so repeated
    ``--add-column-template target:template`` entries in the same command line
    can be filtered even during completion.
    """
    selected_targets: set[str] = set()

    parsed_values = get_multi_param_values(ctx, "add_column_template")
    for value in parsed_values:
        if ":" not in value:
            continue
        target_name_raw, template_name_raw = value.split(":", 1)
        target_name = target_name_raw.strip()
        template_name = template_name_raw.strip()
        if not target_name or not template_name:
            continue
        if value == incomplete:
            continue
        selected_targets.add(target_name.casefold())

    args_list = list(ctx.args)
    for index, token in enumerate(args_list):
        if token != "--add-column-template":
            continue
        next_index = index + 1
        if next_index >= len(args_list):
            continue
        value = args_list[next_index]
        if not value or value.startswith("-") or ":" not in value:
            continue
        target_name_raw, template_name_raw = value.split(":", 1)
        target_name = target_name_raw.strip()
        template_name = template_name_raw.strip()
        if not target_name or not template_name:
            continue
        if value == incomplete:
            continue
        selected_targets.add(target_name.casefold())

    return selected_targets


def get_existing_source_column_refs(
    service: str,
    table_spec: str,
) -> set[str]:
    """Get fully-qualified column refs already configured on source table."""
    from cdc_generator.helpers.service_config import load_service_config

    existing: set[str] = set()
    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return existing

    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return existing

    source_cfg = cast(dict[str, Any], source_raw)
    tables_raw = source_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        return existing
    tables_cfg = cast(dict[str, Any], tables_raw)

    table_raw = tables_cfg.get(table_spec)
    if not isinstance(table_raw, dict):
        return existing

    table_cfg = cast(dict[str, Any], table_raw)
    for key in ["include_columns", "ignore_columns"]:
        cols_raw = table_cfg.get(key)
        if not isinstance(cols_raw, list):
            continue
        cols = cast(list[object], cols_raw)
        for col in cols:
            if not isinstance(col, str):
                continue
            existing.add(col if "." in col else f"{table_spec}.{col}")

    return existing


def get_resource_service(ctx: click.Context) -> str:
    """Resolve service for manage-services resources actions."""
    service = get_param(ctx, "service")
    if service:
        return service

    source = get_param(ctx, "source")
    if source:
        return source

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        autodetect_single_service,
    )

    detected = safe_call(autodetect_single_service)
    return detected[0] if detected else ""
