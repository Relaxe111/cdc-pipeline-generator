"""Source-override completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetMultiParamValuesFn = Callable[[click.Context, str], list[str]]
GetParamFn = Callable[[click.Context, str], str]


def complete_set_source_override_impl(
    ctx: click.Context,
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_multi_param_values: GetMultiParamValuesFn,
) -> list[CompletionItem]:
    """Complete ``schema.table.column:type`` for --set-source-override."""
    if not service:
        return []

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        list_overridden_column_refs,
        list_source_column_refs,
        list_valid_override_types_for_column,
        normalize_source_column_ref,
    )

    source_refs = safe_call(list_source_column_refs, service)
    ref_display_by_normalized: dict[str, str] = {}
    for source_ref in source_refs:
        try:
            normalized_source_ref = normalize_source_column_ref(source_ref)
        except Exception:
            continue
        ref_display_by_normalized.setdefault(normalized_source_ref, source_ref)

    if ":" not in incomplete:
        overridden_refs = {
            normalize_source_column_ref(ref)
            for ref in safe_call(list_overridden_column_refs, service)
            if ref.strip()
        }
        candidates = [
            ref_display
            for normalized_ref, ref_display in ref_display_by_normalized.items()
            if normalized_ref not in overridden_refs
        ]
        candidates.extend(
            f"{ref_display}:"
            for normalized_ref, ref_display in ref_display_by_normalized.items()
            if normalized_ref not in overridden_refs
        )
        return filter_items(candidates, incomplete)

    ref_part, type_part = incomplete.rsplit(":", 1)
    if not ref_part.strip():
        return []

    try:
        normalized_ref = normalize_source_column_ref(ref_part)
    except Exception:
        return []

    display_ref = ref_display_by_normalized.get(normalized_ref, ref_part.strip())

    selected_types = {
        value.rsplit(":", 1)[1].strip().casefold()
        for value in get_multi_param_values(ctx, "set_source_override")
        if ":" in value and value != incomplete
    }

    candidates: list[str] = []
    for type_name in safe_call(list_valid_override_types_for_column, service, normalized_ref):
        normalized_type = type_name.strip().casefold()
        if not normalized_type or normalized_type in selected_types:
            continue

        if not normalized_type.startswith(type_part.casefold()):
            continue

        candidates.append(f"{display_ref}:{normalized_type}")

    return filter_items(candidates, incomplete)


def complete_remove_source_override_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete existing ``schema.table.column`` override refs."""
    if not service:
        return []

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        list_overridden_column_refs,
    )

    return filter_items(safe_call(list_overridden_column_refs, service), incomplete)


def complete_source_override_ref_for_set_impl(
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete source refs for canonical source-overrides set subcommand."""
    if not service:
        return []

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        list_overridden_column_refs,
        list_source_column_refs,
    )

    all_refs = safe_call(list_source_column_refs, service)
    overridden_refs = {
        ref.casefold()
        for ref in safe_call(list_overridden_column_refs, service)
        if ref.strip()
    }
    candidates = [
        ref
        for ref in all_refs
        if ref.casefold() not in overridden_refs
    ]
    return filter_items(candidates, incomplete)


def complete_source_override_type_for_ref_impl(
    ctx: click.Context,
    incomplete: str,
    service: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_param: GetParamFn,
) -> list[CompletionItem]:
    """Complete type values for canonical source-overrides set subcommand."""
    source_ref = get_param(ctx, "source_ref") or get_param(ctx, "source_spec")
    if not service or not source_ref:
        return []

    if ":" in source_ref:
        source_ref = source_ref.split(":", 1)[0]

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        list_valid_override_types_for_column,
        normalize_source_column_ref,
    )

    try:
        normalized_ref = normalize_source_column_ref(source_ref)
    except Exception:
        return []

    candidates: list[str] = []
    for type_name in safe_call(
        list_valid_override_types_for_column,
        service,
        normalized_ref,
    ):
        normalized_type = type_name.strip().casefold()
        if not normalized_type:
            continue
        if not normalized_type.startswith(incomplete.casefold()):
            continue
        candidates.append(normalized_type)

    return filter_items(candidates, incomplete)
