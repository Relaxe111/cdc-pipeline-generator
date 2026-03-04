"""Runtime helpers for ``manage-services resources source-overrides`` Click commands."""

from __future__ import annotations

from typing import cast

import click

_SOURCE_OVERRIDES_MISSING_SUBCOMMAND_LINES = (
    "❌ Missing subcommand for source-overrides",
    "   Try: cdc mss source-overrides list [--service <svc>]",
    "        cdc mss source-overrides set <schema.table.column:type> --reason <text> [--service <svc>]",
    "        cdc mss source-overrides set <schema.table.column> <type> --reason <text> [--service <svc>]",
    "        cdc mss source-overrides remove <schema.table.column> [--service <svc>]",
)


def _parent_source_overrides_service(ctx: click.Context) -> str | None:
    if ctx.parent is None:
        return None

    parent_obj_raw: object = ctx.parent.obj
    if not isinstance(parent_obj_raw, dict):
        return None

    parent_obj = cast(dict[str, object], parent_obj_raw)
    parent_service = parent_obj.get("source_overrides_service")
    if not isinstance(parent_service, str):
        return None

    return parent_service or None


def resolve_source_override_service(
    service: str | None,
    source: str | None,
    parent_service: str | None,
) -> str:
    """Resolve service for source-overrides subcommands."""
    resolved = service or source or parent_service
    if resolved:
        return resolved

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        autodetect_single_service,
    )

    return autodetect_single_service()


def execute_source_overrides_group(
    ctx: click.Context,
    service: str | None,
    source: str | None,
) -> int:
    """Handle source-overrides group invocation and context storage."""
    ctx.ensure_object(dict)
    ctx.obj["source_overrides_service"] = service or source

    if ctx.invoked_subcommand is not None:
        return 0

    for line in _SOURCE_OVERRIDES_MISSING_SUBCOMMAND_LINES:
        click.echo(line)
    return 1


def execute_source_overrides_set(
    ctx: click.Context,
    source_spec: str,
    override_type: str | None,
    service: str | None,
    source: str | None,
    reason: str,
) -> int:
    """Set one source override from source-overrides subcommand."""
    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        set_source_override as set_source_override_entry,
    )

    resolved_service = resolve_source_override_service(
        service,
        source,
        _parent_source_overrides_service(ctx),
    )
    if not resolved_service:
        click.echo("❌ source-overrides set requires --service when multiple services exist")
        return 1

    spec = source_spec if override_type is None else f"{source_spec}:{override_type}"
    try:
        return 0 if set_source_override_entry(resolved_service, spec, reason) else 1
    except ValueError as exc:
        click.echo(f"❌ {exc}")
        return 1


def execute_source_overrides_remove(
    ctx: click.Context,
    source_ref: str,
    service: str | None,
    source: str | None,
) -> int:
    """Remove one source override from source-overrides subcommand."""
    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        remove_source_override as remove_source_override_entry,
    )

    resolved_service = resolve_source_override_service(
        service,
        source,
        _parent_source_overrides_service(ctx),
    )
    if not resolved_service:
        click.echo("❌ source-overrides remove requires --service when multiple services exist")
        return 1

    try:
        return 0 if remove_source_override_entry(resolved_service, source_ref) else 1
    except ValueError as exc:
        click.echo(f"❌ {exc}")
        return 1


def execute_source_overrides_list(
    ctx: click.Context,
    service: str | None,
    source: str | None,
) -> int:
    """List source overrides from source-overrides subcommand."""
    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        print_source_overrides,
    )

    resolved_service = resolve_source_override_service(
        service,
        source,
        _parent_source_overrides_service(ctx),
    )
    if not resolved_service:
        click.echo("❌ source-overrides list requires --service when multiple services exist")
        return 1

    try:
        print_source_overrides(resolved_service)
        return 0
    except ValueError as exc:
        click.echo(f"❌ {exc}")
        return 1
