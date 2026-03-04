"""Service/server/env/sink-group name completion logic extracted from main completions module."""

from __future__ import annotations

from collections.abc import Callable

import click
from click.shell_completion import CompletionItem

SafeCall = Callable[..., list[str]]
FilterFn = Callable[[list[str], str], list[CompletionItem]]
GetParamFn = Callable[[click.Context, str], str]
GetServiceFn = Callable[[click.Context], str]
CompleteFn = Callable[[click.Context, click.Parameter, str], list[CompletionItem]]


def complete_existing_services_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with existing service names from services/*.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )

    return filter_items(safe_call(list_existing_services), incomplete)


def complete_service_positional_impl(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
    complete_available_tables: CompleteFn,
    complete_existing_services: CompleteFn,
) -> list[CompletionItem]:
    """Complete positional service context-aware for manage-services config."""
    add_source_table_value = ctx.params.get("add_source_table")
    if add_source_table_value:
        return complete_available_tables(ctx, param, incomplete)

    return complete_existing_services(ctx, param, incomplete)


def complete_schema_services_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete services from schema directories (services/_schemas or legacy)."""
    from cdc_generator.helpers.autocompletions.service_schemas import (
        list_schema_services,
    )

    return filter_items(safe_call(list_schema_services), incomplete)


def complete_available_services_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with available services from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_available_services_from_server_group,
    )

    return filter_items(
        safe_call(list_available_services_from_server_group),
        incomplete,
    )


def complete_available_validation_databases_impl(
    ctx: click.Context,
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
    get_param: GetParamFn,
    get_service: GetServiceFn,
) -> list[CompletionItem]:
    """Complete validation database names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_available_validation_databases,
    )

    service_name = get_param(ctx, "create_service")
    if not service_name:
        service_name = get_service(ctx)

    return filter_items(
        safe_call(list_available_validation_databases, service_name),
        incomplete,
    )


def complete_server_names_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with server names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_servers_from_server_group,
    )

    return filter_items(safe_call(list_servers_from_server_group), incomplete)


def complete_available_envs_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with available environment names from source-groups.yaml."""
    from cdc_generator.validators.manage_server_group import (
        get_single_server_group,
        load_server_groups,
    )
    from cdc_generator.validators.manage_server_group.handlers_validation_env import (
        get_available_envs,
    )

    def _list_envs() -> list[str]:
        config = load_server_groups()
        server_group = get_single_server_group(config)
        if not server_group:
            return []
        return get_available_envs(server_group)

    return filter_items(safe_call(_list_envs), incomplete)


def complete_migration_envs_impl(
    ctx: click.Context,
    incomplete: str,
    get_param: GetParamFn,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete env names from migration manifest databases."""
    from cdc_generator.cli.migration_cli_validation import list_manifest_envs
    from cdc_generator.helpers.service_config import get_project_root

    sink_filter = get_param(ctx, "sink") or None
    migrations_dir = get_project_root() / "migrations"

    try:
        envs = list_manifest_envs(
            migrations_dir=migrations_dir,
            sink_filter=sink_filter,
        )
    except Exception:
        return []

    return filter_items(envs, incomplete)


def complete_server_group_names_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with server group names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_server_group_names,
    )

    return filter_items(safe_call(list_server_group_names), incomplete)


def complete_sink_group_names_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with sink group names from sink-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_sink_group_names,
    )

    return filter_items(safe_call(list_sink_group_names), incomplete)


def complete_non_inherited_sink_group_names_impl(
    incomplete: str,
    safe_call: SafeCall,
    filter_items: FilterFn,
) -> list[CompletionItem]:
    """Complete with non-inherited sink group names."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_non_inherited_sink_group_names,
    )

    return filter_items(
        safe_call(list_non_inherited_sink_group_names),
        incomplete,
    )
