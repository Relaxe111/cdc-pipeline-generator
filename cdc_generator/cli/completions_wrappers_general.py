"""General completion callback wrappers extracted from completions facade."""

from __future__ import annotations

import click
from click.shell_completion import CompletionItem

from cdc_generator.cli.completions_context import (
    filter_items as _filter,
)
from cdc_generator.cli.completions_context import (
    get_multi_param_values as _get_multi_param_values,
)
from cdc_generator.cli.completions_context import (
    get_param as _get_param,
)
from cdc_generator.cli.completions_context import (
    get_selected_add_column_template_targets as _get_selected_add_column_template_targets,
)
from cdc_generator.cli.completions_context import (
    get_service as _get_service,
)
from cdc_generator.cli.completions_context import (
    get_sink_key_with_default as _get_sink_key_with_default,
)
from cdc_generator.cli.completions_context import (
    safe_call as _safe_call,
)
from cdc_generator.cli.completions_map_columns import (
    mapped_map_column_state as _mapped_map_column_state,
)
from cdc_generator.cli.completions_map_columns import (
    resolve_map_column_tables as _resolve_map_column_tables,
)
from cdc_generator.cli.completions_names_envs import (
    complete_available_envs_impl,
    complete_available_services_impl,
    complete_available_validation_databases_impl,
    complete_existing_services_impl,
    complete_migration_envs_impl,
    complete_non_inherited_sink_group_names_impl,
    complete_schema_services_impl,
    complete_server_group_names_impl,
    complete_server_names_impl,
    complete_sink_group_names_impl,
)
from cdc_generator.cli.completions_pg_types_columns import (
    complete_custom_table_column_spec_impl,
    complete_pg_types_impl,
)
from cdc_generator.cli.completions_sink_keys_templates import (
    complete_available_sink_keys_impl,
    complete_column_templates_impl,
    complete_transform_rules_impl,
)

_MAP_COLUMN_MAX_SUGGESTIONS = 40


def complete_existing_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_existing_services_impl(incomplete, _safe_call, _filter)


def complete_schema_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_schema_services_impl(incomplete, _safe_call, _filter)


def complete_available_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_available_services_impl(incomplete, _safe_call, _filter)


def complete_available_validation_databases(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_available_validation_databases_impl(
        ctx,
        incomplete,
        _safe_call,
        _filter,
        _get_param,
        _get_service,
    )


def complete_server_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_server_names_impl(incomplete, _safe_call, _filter)


def complete_available_envs(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_available_envs_impl(incomplete, _safe_call, _filter)


def complete_migration_envs(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_migration_envs_impl(incomplete, _safe_call, _filter)


def complete_server_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_server_group_names_impl(incomplete, _safe_call, _filter)


def complete_sink_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_sink_group_names_impl(incomplete, _safe_call, _filter)


def complete_non_inherited_sink_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_non_inherited_sink_group_names_impl(
        incomplete,
        _safe_call,
        _filter,
    )


def complete_available_sink_keys(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_available_sink_keys_impl(
        incomplete,
        _get_service(ctx),
        _safe_call,
        _filter,
    )


def complete_column_templates(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_column_templates_impl(
        ctx,
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _get_param(ctx, "sink_table"),
        _get_param(ctx, "target"),
        _get_param(ctx, "from_table"),
        _get_param(ctx, "add_sink_table"),
        _get_param(ctx, "sink_schema"),
        _safe_call,
        _filter,
        _resolve_map_column_tables,
        _mapped_map_column_state,
        _get_multi_param_values,
        _get_selected_add_column_template_targets,
        _MAP_COLUMN_MAX_SUGGESTIONS,
    )


def complete_transform_rules(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_transform_rules_impl(incomplete, _safe_call, _filter)


def complete_pg_types(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_pg_types_impl(incomplete, _safe_call, _filter)


def complete_custom_table_column_spec(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    return complete_custom_table_column_spec_impl(incomplete, _safe_call, _filter)
