"""Shell completion callbacks for Click-based CDC CLI.

Each function follows the Click shell_complete callback signature:
    (ctx: click.Context, param: click.Parameter, incomplete: str) -> list[CompletionItem]

These callbacks delegate to the existing autocompletions module,
which remains the single source of truth for completion data.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from click.shell_completion import CompletionItem

if TYPE_CHECKING:
    import click

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SINK_KEY_PARTS = 2


def _filter(items: list[str], incomplete: str) -> list[CompletionItem]:
    """Filter a list of strings by prefix and wrap as CompletionItem."""
    return [CompletionItem(s) for s in items if s.startswith(incomplete)]


def _safe_call(
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


def _get_param(ctx: click.Context, name: str) -> str:
    """Read a parameter from ctx.params, returning '' if missing."""
    val = ctx.params.get(name)
    if val is None:
        return ""
    return str(val)


def _get_service(ctx: click.Context) -> str:
    """Get service name from --service flag or positional argument.

    manage-service supports both ``--service directory`` and the shorthand
    ``cdc manage-service directory``.  With ``allow_extra_args=True``,
    Click puts the positional word into ``ctx.args`` (not ``ctx.params``).
    We pick the first non-flag token from ``ctx.args`` as the service name.
    """
    svc = _get_param(ctx, "service")
    if not svc:
        svc = _get_param(ctx, "service_positional")
    if not svc and ctx.args:
        # First non-flag token in extra args is the positional service name
        for arg in ctx.args:
            if not arg.startswith("-"):
                return arg
    return svc


def _get_sink_key_with_default(ctx: click.Context) -> str:
    """Get --sink value, falling back to auto-default for single-sink services."""
    sink_key = _get_param(ctx, "sink")
    if not sink_key:
        service = _get_service(ctx)
        if service:
            from cdc_generator.helpers.autocompletions.sinks import (
                get_default_sink_for_service,
            )

            result = _safe_call(get_default_sink_for_service, service)
            sink_key = result[0] if result else ""
    return sink_key


def _get_table_spec(ctx: click.Context) -> str:
    """Get table spec from --add-source-table or --source-table."""
    spec = _get_param(ctx, "add_source_table")
    if not spec:
        spec = _get_param(ctx, "source_table")
    return spec


# ---------------------------------------------------------------------------
# No-arg completions (global lists)
# ---------------------------------------------------------------------------


def complete_existing_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with existing service names from services/*.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )

    return _filter(_safe_call(list_existing_services), incomplete)


def complete_available_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with available services from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_available_services_from_server_group,
    )

    return _filter(
        _safe_call(list_available_services_from_server_group),
        incomplete,
    )


def complete_server_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with server names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_servers_from_server_group,
    )

    return _filter(_safe_call(list_servers_from_server_group), incomplete)


def complete_available_envs(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
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

    return _filter(_safe_call(_list_envs), incomplete)


def complete_server_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with server group names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_server_group_names,
    )

    return _filter(_safe_call(list_server_group_names), incomplete)


def complete_sink_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with sink group names from sink-groups.yaml."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_sink_group_names,
    )

    return _filter(_safe_call(list_sink_group_names), incomplete)


def complete_non_inherited_sink_group_names(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with non-inherited sink group names."""
    from cdc_generator.helpers.autocompletions.server_groups import (
        list_non_inherited_sink_group_names,
    )

    return _filter(
        _safe_call(list_non_inherited_sink_group_names),
        incomplete,
    )


def complete_available_sink_keys(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with available sink keys from sink-groups.yaml.

        When a service is selected (via ``--service`` or positional shorthand),
        the result is filtered to hide:
        - sink keys already configured on that service
        - sink keys whose target service is the same as the source service
            (self-sink, e.g. ``sink_asma.directory`` on ``directory``)

        This keeps ``--add-sink`` focused on valid cross-service candidates.
    """
    from cdc_generator.helpers.autocompletions.sinks import (
        list_available_sink_keys,
        list_sink_keys_for_service,
    )

    available_keys = _safe_call(list_available_sink_keys)
    service = _get_service(ctx)

    if not service:
        return _filter(available_keys, incomplete)

    existing_keys = set(_safe_call(list_sink_keys_for_service, service))
    filtered_keys = sorted(
        key for key in available_keys
        if key not in existing_keys
        and _sink_target_service(key) != service
    )
    return _filter(filtered_keys, incomplete)


def _sink_target_service(sink_key: str) -> str:
    """Extract target service from sink key (sink_group.target_service)."""
    parts = sink_key.split(".", 1)
    if len(parts) != _SINK_KEY_PARTS:
        return ""
    return parts[1]


def complete_column_templates(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with column template keys from column-templates.yaml."""
    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_column_template_keys,
    )

    return _filter(_safe_call(list_column_template_keys), incomplete)


def complete_transform_rules(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with transform rule keys from transform-rules.yaml."""
    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_transform_rule_keys,
    )

    return _filter(_safe_call(list_transform_rule_keys), incomplete)


def complete_pg_types(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with PostgreSQL column types."""
    from cdc_generator.helpers.autocompletions.types import (
        list_pg_column_types,
    )

    return _filter(_safe_call(list_pg_column_types), incomplete)


# ---------------------------------------------------------------------------
# Service-context completions (need --service)
# ---------------------------------------------------------------------------


def complete_available_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with available tables from service-schemas."""
    service = _get_service(ctx)
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_tables_for_service,
    )

    return _filter(_safe_call(list_tables_for_service, service), incomplete)


def complete_source_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with existing source tables in service YAML."""
    service = _get_service(ctx)
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_source_tables_for_service,
    )

    return _filter(
        _safe_call(list_source_tables_for_service, service),
        incomplete,
    )


def complete_sink_keys(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with sink keys for current service."""
    service = _get_service(ctx)
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_keys_for_service,
    )

    return _filter(
        _safe_call(list_sink_keys_for_service, service),
        incomplete,
    )


def complete_schemas(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete with schemas for current service."""
    service = _get_service(ctx)
    if not service:
        return []

    from cdc_generator.helpers.autocompletions.schemas import (
        list_schemas_for_service,
    )

    return _filter(
        _safe_call(list_schemas_for_service, service),
        incomplete,
    )


def complete_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for a service table (needs --source-table or --add-source-table)."""
    service = _get_service(ctx)
    table_spec = _get_table_spec(ctx)
    if not service or not table_spec:
        return []

    parts = table_spec.split(".")
    _schema_table_parts = 2
    if len(parts) != _schema_table_parts:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_columns_for_table,
    )

    return _filter(
        _safe_call(list_columns_for_table, service, parts[0], parts[1]),
        incomplete,
    )


# ---------------------------------------------------------------------------
# Sink-context completions (need --service + --sink)
# ---------------------------------------------------------------------------


def complete_sink_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete sink tables for current service and sink."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_tables_for_service,
    )

    return _filter(
        _safe_call(list_sink_tables_for_service, service, sink_key),
        incomplete,
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

    if not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_tables_for_sink_target,
    )

    return _filter(
        _safe_call(list_tables_for_sink_target, sink_key),
        incomplete,
    )


def complete_remove_sink_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete tables to remove from a sink."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_sink_tables_for_service,
    )

    return _filter(
        _safe_call(list_sink_tables_for_service, service, sink_key),
        incomplete,
    )


def complete_target_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete target tables for a sink."""
    service = _get_service(ctx)
    sink_key = _get_param(ctx, "sink")
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_target_tables_for_sink,
    )

    return _filter(
        _safe_call(list_target_tables_for_sink, service, sink_key),
        incomplete,
    )


def complete_target_schema(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete schemas for a sink's target service."""
    sink_key = _get_param(ctx, "sink")
    if not sink_key:
        return []

    parts = sink_key.split(".")
    _sink_key_parts = 2
    if len(parts) < _sink_key_parts:
        return []
    target_service = parts[1]

    from cdc_generator.helpers.autocompletions.schemas import (
        list_schemas_for_service,
    )

    return _filter(
        _safe_call(list_schemas_for_service, target_service),
        incomplete,
    )


def complete_templates_on_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete column templates applied to a sink table."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    sink_table = _get_param(ctx, "sink_table")
    if not service or not sink_key or not sink_table:
        return []

    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_column_templates_for_table,
    )

    return _filter(
        _safe_call(
            list_column_templates_for_table, service, sink_key, sink_table
        ),
        incomplete,
    )


def complete_transforms_on_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete transforms applied to a sink table."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    sink_table = _get_param(ctx, "sink_table")
    if not service or not sink_key or not sink_table:
        return []

    from cdc_generator.helpers.autocompletions.column_template_completions import (
        list_transforms_for_table,
    )

    return _filter(
        _safe_call(
            list_transforms_for_table, service, sink_key, sink_table
        ),
        incomplete,
    )


def complete_map_column(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --map-column args (source columns context-aware)."""
    service = _get_service(ctx)
    sink_key = _get_param(ctx, "sink")
    sink_table = _get_param(ctx, "sink_table")
    target_table = _get_param(ctx, "target")
    add_sink_table = _get_param(ctx, "add_sink_table")

    if sink_table and sink_key and service:
        from cdc_generator.helpers.autocompletions.sinks import (
            list_source_columns_for_sink_table,
            list_target_columns_for_sink_table,
        )

        # First arg = source columns, second = target columns
        # Click nargs=2 will call this for each position
        return _filter(
            _safe_call(
                list_source_columns_for_sink_table,
                service,
                sink_key,
                sink_table,
            ),
            incomplete,
        )
    if sink_key and target_table:
        from cdc_generator.helpers.autocompletions.sinks import (
            list_target_columns_for_sink_table,
        )

        return _filter(
            _safe_call(
                list_target_columns_for_sink_table, sink_key, target_table
            ),
            incomplete,
        )
    if sink_key and add_sink_table:
        from cdc_generator.helpers.autocompletions.sinks import (
            list_target_columns_for_sink_table,
        )

        return _filter(
            _safe_call(
                list_target_columns_for_sink_table, sink_key, add_sink_table
            ),
            incomplete,
        )
    return []


def complete_include_sink_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for --include-sink-columns (from --add-sink-table)."""
    service = _get_service(ctx)
    table_spec = _get_param(ctx, "add_sink_table")
    if not service or not table_spec:
        return []

    parts = table_spec.split(".")
    _schema_table_parts = 2
    if len(parts) != _schema_table_parts:
        return []

    from cdc_generator.helpers.autocompletions.tables import (
        list_columns_for_table,
    )

    return _filter(
        _safe_call(list_columns_for_table, service, parts[0], parts[1]),
        incomplete,
    )


# ---------------------------------------------------------------------------
# Custom table completions
# ---------------------------------------------------------------------------


def complete_custom_tables(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete custom tables for a sink."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    if not service or not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_tables_for_service_sink,
    )

    return _filter(
        _safe_call(list_custom_tables_for_service_sink, service, sink_key),
        incomplete,
    )


def complete_custom_table_columns(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete columns for a custom table."""
    service = _get_service(ctx)
    sink_key = _get_sink_key_with_default(ctx)
    table_key = _get_param(ctx, "modify_custom_table")
    if not service or not sink_key or not table_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_columns_for_autocomplete,
    )

    return _filter(
        _safe_call(
            list_custom_table_columns_for_autocomplete,
            service,
            sink_key,
            table_key,
        ),
        incomplete,
    )


# ---------------------------------------------------------------------------
# Sink group server completions
# ---------------------------------------------------------------------------


def complete_sink_group_servers(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete servers for a sink group."""
    sink_group = _get_param(ctx, "sink_group")
    if not sink_group:
        return []

    from cdc_generator.helpers.autocompletions.server_groups import (
        list_servers_for_sink_group,
    )

    return _filter(
        _safe_call(list_servers_for_sink_group, sink_group),
        incomplete,
    )


# ---------------------------------------------------------------------------
# Context-aware sink group completion (different lists for add/remove server)
# ---------------------------------------------------------------------------


def complete_sink_group_context_aware(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete sink group — non-inherited when adding/removing servers."""
    # Check if --add-server or --remove-server is on the command line
    add_server = _get_param(ctx, "add_server")
    remove_server = _get_param(ctx, "remove_server")

    if add_server or remove_server:
        return complete_non_inherited_sink_group_names(
            ctx, _param, incomplete
        )
    return complete_sink_group_names(ctx, _param, incomplete)
