"""Shell completion callbacks for Click-based CDC CLI.

Each function follows the Click shell_complete callback signature:
    (ctx: click.Context, param: click.Parameter, incomplete: str) -> list[CompletionItem]

These callbacks delegate to the existing autocompletions module,
which remains the single source of truth for completion data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.shell_completion import CompletionItem

from cdc_generator.cli.completions_context import (
    filter_items as _filter,
)
from cdc_generator.cli.completions_context import (
    get_existing_source_column_refs as _get_existing_source_column_refs,
)
from cdc_generator.cli.completions_context import (
    get_multi_param_values as _get_multi_param_values,
)
from cdc_generator.cli.completions_context import (
    get_option_values_from_args as _get_option_values_from_args,
)
from cdc_generator.cli.completions_context import (
    get_param as _get_param,
)
from cdc_generator.cli.completions_context import (
    get_resource_service as _get_resource_service,
)
from cdc_generator.cli.completions_context import (
    get_selected_accept_columns as _get_selected_accept_columns,
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
    get_table_spec as _get_table_spec,
)
from cdc_generator.cli.completions_context import (
    safe_call as _safe_call,
)
from cdc_generator.cli.completions_custom_and_sink_groups import (
    complete_custom_table_columns_impl,
    complete_custom_tables_impl,
    complete_sink_group_context_aware_impl,
    complete_sink_group_servers_impl,
)
from cdc_generator.cli.completions_map_columns import (
    complete_accept_column_impl,
    complete_include_sink_columns_impl,
    complete_map_column_impl,
)
from cdc_generator.cli.completions_map_columns import (
    mapped_map_column_state as _mapped_map_column_state,
)
from cdc_generator.cli.completions_names_envs import (
    complete_service_positional_impl,
)
from cdc_generator.cli.completions_schema_templates import (
    complete_target_schema_impl,
    complete_templates_on_table_impl,
    complete_transforms_on_table_impl,
)
from cdc_generator.cli.completions_source_overrides import (
    complete_remove_source_override_impl,
    complete_set_source_override_impl,
    complete_source_override_ref_for_set_impl,
    complete_source_override_type_for_ref_impl,
)
from cdc_generator.cli.completions_tables_and_sinks import (
    complete_add_custom_sink_table_impl,
    complete_add_sink_table_impl,
    complete_available_tables_impl,
    complete_columns_impl,
    complete_from_table_impl,
    complete_remove_sink_table_impl,
    complete_schemas_impl,
    complete_sink_keys_impl,
    complete_sink_tables_impl,
    complete_source_tables_impl,
    complete_target_tables_impl,
    complete_track_tables_impl,
)
from cdc_generator.cli.completions_wrappers_general import (
    complete_available_envs,
    complete_available_services,
    complete_available_sink_keys,
    complete_available_validation_databases,
    complete_column_templates,
    complete_custom_table_column_spec,
    complete_existing_services,
    complete_migration_envs,
    complete_non_inherited_sink_group_names,
    complete_pg_types,
    complete_schema_services,
    complete_server_group_names,
    complete_server_names,
    complete_sink_group_names,
    complete_transform_rules,
)

if TYPE_CHECKING:
    import click

__all__ = [
    "complete_available_envs",
    "complete_available_services",
    "complete_available_sink_keys",
    "complete_available_validation_databases",
    "complete_column_templates",
    "complete_custom_table_column_spec",
    "complete_existing_services",
    "complete_migration_envs",
    "complete_non_inherited_sink_group_names",
    "complete_pg_types",
    "complete_schema_services",
    "complete_server_group_names",
    "complete_server_names",
    "complete_set_source_name_map",
    "complete_sink_group_names",
    "complete_transform_rules",
]

_MAP_COLUMN_MAX_SUGGESTIONS = 40


def _get_last_option_values_from_args(
    ctx: click.Context,
    option_name: str,
    max_values: int,
) -> list[str]:
    """Return the parsed values following the last occurrence of an option token."""
    args_list: list[str] = []
    search_ctx: click.Context | None = ctx
    last_index = -1
    while search_ctx is not None:
        candidate_args = [str(token) for token in search_ctx.args]
        for index, token in enumerate(candidate_args):
            if token == option_name:
                args_list = candidate_args
                last_index = index

        if last_index != -1:
            break

        search_ctx = search_ctx.parent

    if last_index == -1:
        return []

    values: list[str] = []
    next_index = last_index + 1
    while next_index < len(args_list) and len(values) < max_values:
        value = args_list[next_index]
        if not value or value.startswith("-"):
            break
        values.append(value)
        next_index += 1

    return values


def _get_selected_map_column_targets(ctx: click.Context) -> set[str]:
    """Collect mapped target columns from ``--map-column`` entries."""
    selected_targets: set[str] = set()

    map_values = _get_multi_param_values(ctx, "map_column") + _get_option_values_from_args(
        ctx,
        "--map-column",
    )

    mapped_targets, _mapped_sources, _pending_legacy_source = _mapped_map_column_state(
        [value for value in map_values if value],
    )
    selected_targets.update(mapped_targets)
    return selected_targets


def complete_service_positional(
    ctx: click.Context,
    param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete positional service context-aware for manage-services config.

    Click treats extra values after options as positional completions, but
    argparse may still consume them as option values (e.g. --add-source-tables).
    In that case, keep completing source tables instead of service names.
    """
    return complete_service_positional_impl(
        ctx,
        param,
        incomplete,
        complete_available_tables,
        complete_existing_services,
    )


def complete_available_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with available tables from service-schemas."""
    return complete_available_tables_impl(
        ctx,
        incomplete,
        _get_service(ctx),
        _safe_call,
        _filter,
        _get_multi_param_values,
    )


def complete_source_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with existing source tables in service YAML."""
    return complete_source_tables_impl(
        incomplete,
        _get_service(ctx),
        _safe_call,
        _filter,
    )


def complete_track_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete tracked table refs (schema.table) from existing schema resources."""
    return complete_track_tables_impl(
        ctx,
        incomplete,
        _get_service(ctx),
        _get_param(ctx, "inspect_sink"),
        _safe_call,
        _filter,
        _get_multi_param_values,
    )


def complete_set_source_override(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete ``schema.table.column:type`` for --set-source-override."""
    return complete_set_source_override_impl(
        ctx,
        incomplete,
        _get_resource_service(ctx),
        _safe_call,
        _filter,
        _get_multi_param_values,
    )


def complete_remove_source_override(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete existing ``schema.table.column`` override refs."""
    return complete_remove_source_override_impl(
        incomplete,
        _get_resource_service(ctx),
        _safe_call,
        _filter,
    )


def complete_source_override_ref_for_set(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete source refs for canonical source-overrides set subcommand."""
    return complete_source_override_ref_for_set_impl(
        incomplete,
        _get_resource_service(ctx),
        _safe_call,
        _filter,
    )


def complete_source_override_type_for_ref(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete type values for canonical source-overrides set subcommand."""
    return complete_source_override_type_for_ref_impl(
        ctx,
        incomplete,
        _get_resource_service(ctx),
        _safe_call,
        _filter,
        _get_param,
    )


def complete_from_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --from from service ``source.tables`` keys plus ``all``."""
    base = complete_source_tables(ctx, _param, incomplete)
    return complete_from_table_impl(base, incomplete)


def complete_sink_keys(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with sink keys for current service."""
    return complete_sink_keys_impl(
        incomplete,
        _get_service(ctx),
        _safe_call,
        _filter,
    )


def complete_target_sink_env(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete target sink env keys from the selected ``--add-sink`` value."""
    sink_key = _get_param(ctx, "add_sink")
    if not sink_key:
        selected_sink_keys = _get_option_values_from_args(ctx, "--add-sink")
        sink_key = selected_sink_keys[0] if selected_sink_keys else ""

    if not sink_key:
        return []

    from cdc_generator.core.sink_env_routing import get_sink_target_env_keys
    from cdc_generator.helpers.service_config import get_project_root

    env_keys, _warning = get_sink_target_env_keys(get_project_root(), sink_key)
    if not env_keys:
        return []

    return _filter(sorted(env_keys), incomplete)


def complete_set_source_name_map(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete the first ``--set-source-name-map`` value from database names."""
    parsed_values = _get_multi_param_values(ctx, "set_source_name_map")
    if not parsed_values:
        parsed_values = _get_last_option_values_from_args(
            ctx,
            "--set-source-name-map",
            2,
        )

    if len(parsed_values) > 0:
        return []

    from cdc_generator.helpers.autocompletions.server_groups import (
        list_databases_from_server_group,
    )

    return _filter(
        _safe_call(list_databases_from_server_group),
        incomplete,
    )


def complete_set_target_sink_env(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete SOURCE, SOURCE_ENV, then TARGET_SINK_ENV for source-group routing."""
    parsed_values = _get_multi_param_values(ctx, "set_target_sink_env")
    if not parsed_values:
        parsed_values = _get_last_option_values_from_args(
            ctx,
            "--set-target-sink-env",
            3,
        )

    if len(parsed_values) == 0:
        from cdc_generator.helpers.autocompletions.server_groups import (
            list_source_names_from_server_group,
        )

        return _filter(
            _safe_call(list_source_names_from_server_group),
            incomplete,
        )

    if len(parsed_values) == 1:
        from cdc_generator.helpers.autocompletions.server_groups import (
            list_source_envs_from_server_group,
        )

        return _filter(
            _safe_call(list_source_envs_from_server_group, parsed_values[0]),
            incomplete,
        )

    from cdc_generator.helpers.autocompletions.server_groups import (
        list_sink_target_envs_from_sink_groups,
    )

    return _filter(
        _safe_call(list_sink_target_envs_from_sink_groups),
        incomplete,
    )


def complete_schemas(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with schemas for current service."""
    return complete_schemas_impl(
        incomplete,
        _get_service(ctx),
        _safe_call,
        _filter,
    )


def complete_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for a service table (needs --source-table or --add-source-table)."""
    return complete_columns_impl(
        ctx,
        incomplete,
        _get_service(ctx),
        _get_table_spec(ctx),
        _safe_call,
        _filter,
        _get_multi_param_values,
        _get_existing_source_column_refs,
    )


def complete_sink_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete sink tables for current service and sink."""
    return complete_sink_tables_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _safe_call,
        _filter,
    )


def complete_add_sink_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete tables available to add to a sink."""
    service = _get_service(ctx)
    sink_key = _get_param(ctx, "sink")
    if not sink_key and service:
        from cdc_generator.helpers.autocompletions.sinks import (
            get_default_sink_for_service,
        )

        result = _safe_call(get_default_sink_for_service, service)
        sink_key = result[0] if result else ""

    return complete_add_sink_table_impl(
        incomplete,
        sink_key,
        _safe_call,
        _filter,
    )


def complete_add_custom_sink_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete table refs for --add-custom-sink-table from schema resources."""
    return complete_add_custom_sink_table_impl(
        incomplete,
        _get_sink_key_with_default(ctx),
        _safe_call,
        _filter,
    )


def complete_remove_sink_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete tables to remove from a sink."""
    return complete_remove_sink_table_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _safe_call,
        _filter,
    )


def complete_target_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete target tables for a sink."""
    return complete_target_tables_impl(
        incomplete,
        _get_service(ctx),
        _get_param(ctx, "sink"),
        _safe_call,
        _filter,
    )


def complete_target_schema(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete schemas for a sink's target service."""
    return complete_target_schema_impl(
        ctx,
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _safe_call,
    )


def complete_templates_on_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete column templates applied to a sink table."""
    return complete_templates_on_table_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _get_param(ctx, "sink_table"),
        _safe_call,
        _filter,
    )


def complete_transforms_on_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete transforms applied to a sink table."""
    return complete_transforms_on_table_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _get_param(ctx, "sink_table"),
        _safe_call,
        _filter,
    )


def complete_map_column(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --map-column args with source/target compatibility filtering."""
    return complete_map_column_impl(
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
        _get_multi_param_values,
        _MAP_COLUMN_MAX_SUGGESTIONS,
    )


def complete_include_sink_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for --include-sink-columns (from --add-sink-table)."""
    return complete_include_sink_columns_impl(
        incomplete,
        _get_service(ctx),
        _get_param(ctx, "add_sink_table"),
        _safe_call,
        _filter,
    )


def complete_accept_column(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --accept-column with uncovered sink target columns."""
    return complete_accept_column_impl(
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
        _get_selected_map_column_targets,
        _get_selected_add_column_template_targets,
        _get_selected_accept_columns,
    )


def complete_custom_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete custom tables for a sink."""
    return complete_custom_tables_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _safe_call,
        _filter,
    )


def complete_custom_table_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for a custom table."""
    return complete_custom_table_columns_impl(
        incomplete,
        _get_service(ctx),
        _get_sink_key_with_default(ctx),
        _get_param(ctx, "modify_custom_table"),
        _safe_call,
        _filter,
    )


def complete_sink_group_servers(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete servers for a sink group."""
    return complete_sink_group_servers_impl(
        ctx,
        incomplete,
        _get_param,
        _safe_call,
        _filter,
    )


def complete_sink_group_context_aware(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete sink group — non-inherited when adding/removing servers."""
    return complete_sink_group_context_aware_impl(
        ctx,
        _param,
        incomplete,
        _get_param,
        complete_non_inherited_sink_group_names,
        complete_sink_group_names,
    )
