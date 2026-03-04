"""Runtime helpers for manage-services resources click command."""

from __future__ import annotations

from dataclasses import dataclass

import click


@dataclass(frozen=True)
class ManageServicesResourcesOptions:
    """Parsed options for manage-services resources command."""

    service: str | None
    source: str | None
    sink: str | None
    track_table: tuple[str, ...]
    custom_tables: bool
    column_templates: bool
    transforms: bool
    list_source_overrides: bool
    set_source_override: str | None
    remove_source_override: str | None


def options_from_kwargs(kwargs: dict[str, object]) -> ManageServicesResourcesOptions:
    """Build typed options from click kwargs payload."""

    def _optional_str(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    track_raw = kwargs.get("track_table")
    track_table = (
        tuple(str(value) for value in track_raw)
        if isinstance(track_raw, tuple | list)
        else ()
    )

    return ManageServicesResourcesOptions(
        service=_optional_str(kwargs.get("service")),
        source=_optional_str(kwargs.get("source")),
        sink=_optional_str(kwargs.get("sink")),
        track_table=track_table,
        custom_tables=bool(kwargs.get("custom_tables", False)),
        column_templates=bool(kwargs.get("column_templates", False)),
        transforms=bool(kwargs.get("transforms", False)),
        list_source_overrides=bool(kwargs.get("list_source_overrides", False)),
        set_source_override=_optional_str(kwargs.get("set_source_override")),
        remove_source_override=_optional_str(kwargs.get("remove_source_override")),
    )


def _handle_source_overrides(options: ManageServicesResourcesOptions) -> int | None:
    if not (
        options.list_source_overrides
        or options.set_source_override
        or options.remove_source_override
    ):
        return None

    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        autodetect_single_service,
        print_source_overrides,
    )
    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        remove_source_override as remove_source_override_entry,
    )
    from cdc_generator.validators.manage_service_schema.source_type_overrides import (
        set_source_override as set_source_override_entry,
    )

    resolved_source = options.source or options.service or autodetect_single_service()
    if not resolved_source:
        click.echo(
            "❌ Source override actions require --service <name> "
            + "(or --source <name>) when multiple services exist"
        )
        return 1

    try:
        if options.list_source_overrides:
            print_source_overrides(resolved_source)
            return 0
        if options.set_source_override:
            return 0 if set_source_override_entry(resolved_source, options.set_source_override) else 1
        if options.remove_source_override:
            return 0 if remove_source_override_entry(resolved_source, options.remove_source_override) else 1
    except ValueError as exc:
        click.echo(f"❌ {exc}")
        return 1

    return None


def _handle_track_tables(options: ManageServicesResourcesOptions) -> int | None:
    if not options.track_table:
        return None

    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )
    from cdc_generator.helpers.autocompletions.sinks import (
        get_default_sink_for_service,
    )
    from cdc_generator.validators.manage_service.schema_saver import (
        add_tracked_tables,
    )

    resolved_source = options.source or options.service
    if not resolved_source:
        existing_services = list_existing_services()
        if len(existing_services) == 1:
            resolved_source = existing_services[0]

    if not resolved_source:
        click.echo(
            "❌ --track-table requires --source <service> "
            + "(or exactly one existing service)"
        )
        return 1

    resolved_sink = options.sink
    if not resolved_sink:
        sink_defaults = get_default_sink_for_service(resolved_source)
        if len(sink_defaults) == 1:
            resolved_sink = sink_defaults[0]

    target_service = resolved_source
    if resolved_sink and "." in resolved_sink:
        target_service = resolved_sink.split(".", 1)[1]

    ok = add_tracked_tables(target_service, list(options.track_table))
    return 0 if ok else 1


def _print_missing_subcommand_help() -> None:
    click.echo("❌ Missing subcommand for manage-services resources")
    click.echo(
        "   Try: cdc manage-services resources inspect "
        + "--service directory --inspect --all"
    )
    click.echo(
        "        cdc manage-services resources inspect "
        + "--service directory --inspect-sink sink_asma.chat --all"
    )
    click.echo("   Try: cdc manage-services resources custom-tables --list-services")
    click.echo("        cdc manage-services resources --source <svc> --track-table <schema.table>")
    click.echo("        cdc mss source-overrides list --service <svc>")
    click.echo("        cdc mss --service <svc> --set-source-override <schema.table.column:type>")
    click.echo("        cdc manage-services resources column-templates --list")
    click.echo("        cdc manage-services resources transforms --list-rules")


def execute_manage_services_resources(
    ctx: click.Context,
    options: ManageServicesResourcesOptions,
) -> int:
    """Execute manage-services resources top-level command logic."""
    if ctx.invoked_subcommand is None:
        source_override_result = _handle_source_overrides(options)
        if source_override_result is not None:
            return source_override_result

        track_tables_result = _handle_track_tables(options)
        if track_tables_result is not None:
            return track_tables_result

        if options.custom_tables or options.column_templates or options.transforms:
            return 0

        _print_missing_subcommand_help()
        return 1

    return 0
