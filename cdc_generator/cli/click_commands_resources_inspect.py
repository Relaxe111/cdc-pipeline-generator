"""Runtime helper for `manage-services resources inspect` Click command."""

from __future__ import annotations

import argparse

import click


def execute_manage_services_resources_inspect(  # noqa: PLR0913
    service: str | None,
    inspect: bool,
    inspect_sink: str | None,
    schema: str | None,
    all_flag: bool,
    save: bool,
    track_table: tuple[str, ...],
    env: str,
) -> int:
    """Handle resources inspect action dispatch."""
    from cdc_generator.cli.service_handlers_inspect import handle_inspect
    from cdc_generator.cli.service_handlers_inspect_sink import handle_inspect_sink
    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )

    resolved_service = service
    if not resolved_service:
        existing_services = list_existing_services()
        if len(existing_services) == 1:
            resolved_service = existing_services[0]

    args = argparse.Namespace(
        service=resolved_service,
        inspect=inspect,
        inspect_sink=inspect_sink,
        schema=schema,
        all=all_flag,
        save=save,
        track_table=list(track_table),
        env=env,
    )

    if inspect:
        return handle_inspect(args)

    if inspect_sink:
        if not resolved_service:
            click.echo("❌ --inspect-sink requires --service <name>")
            return 1
        return handle_inspect_sink(args)

    click.echo("❌ Missing action for manage-services resources inspect")
    click.echo("   Try: cdc manage-services resources inspect --service <svc> --inspect --all")
    click.echo("        cdc manage-services resources inspect --service <svc> --inspect-sink sink_group.target --all")
    return 1
