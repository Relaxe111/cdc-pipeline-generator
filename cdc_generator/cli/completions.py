"""Shell completion callbacks for Click-based CDC CLI.

Each function follows the Click shell_complete callback signature:
    (ctx: click.Context, param: click.Parameter, incomplete: str) -> list[CompletionItem]

These callbacks delegate to the existing autocompletions module,
which remains the single source of truth for completion data.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, cast

from click.shell_completion import CompletionItem

if TYPE_CHECKING:
    import click

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SINK_KEY_PARTS = 2
_COLUMN_SPEC_NAME_ONLY_PARTS = 1
_COLUMN_SPEC_NAME_TYPE_PARTS = 2


def _extract_default_aliases(raw_defaults: object) -> list[str]:
    """Extract default alias names from a YAML defaults node."""
    aliases: list[str] = []

    if isinstance(raw_defaults, str):
        return [raw_defaults]

    if isinstance(raw_defaults, dict):
        defaults_map = cast(dict[str, Any], raw_defaults)
        return [str(alias) for alias in defaults_map]

    if isinstance(raw_defaults, list):
        defaults_list = cast(list[Any], raw_defaults)
        for item in defaults_list:
            if isinstance(item, str):
                aliases.append(item)
            elif isinstance(item, dict):
                item_map = cast(dict[str, Any], item)
                aliases.extend(str(alias) for alias in item_map)

    return aliases


def _default_aliases_from_definitions(col_type: str) -> list[str]:
    """Resolve default aliases from new schema declarations model only."""
    from cdc_generator.helpers.service_config import get_project_root
    from cdc_generator.helpers.yaml_loader import load_yaml_file

    project_root = get_project_root()
    definitions_file = (
        project_root
        / "services"
        / "_schemas"
        / "_definitions"
        / "pgsql.yaml"
    )
    if not definitions_file.is_file():
        return []

    try:
        raw_data = load_yaml_file(definitions_file)
        data = cast(dict[str, Any], raw_data)
        normalized_type = col_type.strip().lower()
        resolved: list[str] = []

        type_defaults_raw = data.get("type_defaults")
        if isinstance(type_defaults_raw, dict):
            type_defaults = cast(dict[str, Any], type_defaults_raw)
            explicit = type_defaults.get(normalized_type)
            resolved.extend(_extract_default_aliases(explicit))

        categories_raw = data.get("categories")
        if isinstance(categories_raw, dict):
            categories = cast(dict[str, Any], categories_raw)
            for category_data in categories.values():
                if not isinstance(category_data, dict):
                    continue
                category = cast(dict[str, Any], category_data)
                type_names_raw = category.get("types")
                if not isinstance(type_names_raw, list):
                    continue

                type_names_list = cast(list[Any], type_names_raw)
                type_names = {
                    str(type_name).strip().lower()
                    for type_name in type_names_list
                    if isinstance(type_name, str)
                }
                if normalized_type not in type_names:
                    continue

                defaults_from_category = _extract_default_aliases(
                    category.get("defaults"),
                )
                if defaults_from_category:
                    resolved.extend(defaults_from_category)

        # Preserve order while removing duplicates.
        unique: list[str] = []
        seen: set[str] = set()
        for alias in resolved:
            if alias in seen:
                continue
            seen.add(alias)
            unique.append(alias)
        return unique
    except Exception:
        return []


def _default_modifiers_for_pg_type(col_type: str) -> list[str]:
    """Return sensible ``default_*`` completion modifiers for a PG type."""
    normalized = col_type.strip().lower()
    return _default_aliases_from_definitions(normalized)


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
    if isinstance(val, tuple | list):
        values = [str(item) for item in val if str(item)]
        if not values:
            return ""
        return values[0]
    return str(val)


def _get_service(ctx: click.Context) -> str:
    """Get service name from --service flag or positional argument.

    manage-services config supports both ``--service directory`` and the shorthand
    ``cdc manage-services config directory``.  With ``allow_extra_args=True``,
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

    if svc:
        return svc

    return _autodetect_single_service_name()


def _autodetect_single_service_name() -> str:
    """Return the only existing service name when exactly one is present."""
    from cdc_generator.helpers.autocompletions.services import (
        list_existing_services,
    )

    existing_services = _safe_call(list_existing_services)
    if len(existing_services) == 1:
        return existing_services[0]
    return ""


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


def _get_multi_param_values(ctx: click.Context, name: str) -> list[str]:
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


def _get_existing_source_column_refs(
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


def _schemas_from_sink_tables(sink_cfg: dict[str, Any]) -> set[str]:
    """Extract schema names from sink table keys (schema.table)."""
    schemas: set[str] = set()
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        return schemas

    tables = cast(dict[str, Any], tables_raw)
    for table_key in tables:
        if "." not in table_key:
            continue
        schema, _table = table_key.split(".", 1)
        if schema:
            schemas.add(schema)

    return schemas


def _common_sink_schemas_for_all_sinks(service: str) -> list[str]:
    """List schema names available across all configured sinks for a service.

    Used by --sink-schema completion when --all fanout mode is active.
    Includes schemas from:
    1) target service schema declarations
    2) already-configured sink tables (including custom/not-yet-created tables)
    """
    from cdc_generator.helpers.autocompletions.schemas import list_schemas_for_service
    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return []

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return []

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return []

    common: set[str] | None = None
    for sink_key, sink_cfg_raw in sinks.items():
        if "." not in sink_key:
            continue

        _sink_group, target_service = sink_key.split(".", 1)
        sink_cfg = cast(dict[str, Any], sink_cfg_raw) if isinstance(sink_cfg_raw, dict) else {}

        candidates: set[str] = set(_safe_call(list_schemas_for_service, target_service))
        candidates.update(_schemas_from_sink_tables(sink_cfg))

        if common is None:
            common = candidates
        else:
            common &= candidates

    return sorted(common) if common else []


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
    add_source_table_value = ctx.params.get("add_source_table")
    if add_source_table_value:
        return complete_available_tables(ctx, param, incomplete)

    return complete_existing_services(ctx, param, incomplete)


def complete_schema_services(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete services from schema directories (services/_schemas or legacy)."""
    from cdc_generator.helpers.autocompletions.service_schemas import (
        list_schema_services,
    )

    return _filter(_safe_call(list_schema_services), incomplete)


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


def complete_available_validation_databases(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete validation database names from source-groups.yaml."""
    from cdc_generator.helpers.autocompletions.services import (
        list_available_validation_databases,
    )

    service_name = _get_param(ctx, "create_service")
    if not service_name:
        service_name = _get_service(ctx)

    return _filter(
        _safe_call(list_available_validation_databases, service_name),
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


def complete_custom_table_column_spec(
    _ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete ``--column`` specs for schema custom table CRUD.

    Supported format: ``name:type[:modifier[:modifier...]]``.
    """
    parts = incomplete.split(":")

    # No ':' yet -> keep quiet to avoid noisy/ambiguous suggestions.
    if len(parts) == _COLUMN_SPEC_NAME_ONLY_PARTS:
        return []

    col_name = parts[0].strip()
    if not col_name:
        return []

    # name:<type>
    if len(parts) == _COLUMN_SPEC_NAME_TYPE_PARTS:
        type_prefix = parts[1]
        from cdc_generator.helpers.autocompletions.types import (
            list_pg_column_types,
        )

        pg_types = _safe_call(list_pg_column_types)
        candidates = [
            f"{col_name}:{pg_type}"
            for pg_type in pg_types
            if pg_type.startswith(type_prefix)
        ]
        return _filter(candidates, incomplete)

    # name:type:<modifier>
    col_type = parts[1].strip().lower()
    modifiers = parts[2:]
    active_modifiers = {
        mod.strip().lower() for mod in modifiers[:-1] if mod.strip()
    }
    modifier_prefix = modifiers[-1] if modifiers else ""

    default_candidates = _default_modifiers_for_pg_type(col_type)

    structural_candidates: list[str] = []
    has_pk = "pk" in active_modifiers
    has_not_null = "not_null" in active_modifiers
    has_nullable = "nullable" in active_modifiers

    if not has_nullable:
        if not has_pk:
            structural_candidates.append("pk")
        if not has_not_null and not has_pk:
            structural_candidates.append("not_null")

    if not has_pk and not has_not_null and not has_nullable:
        structural_candidates.append("nullable")

    modifier_candidates = [
        *structural_candidates,
        *default_candidates,
    ]

    base = ":".join(parts[:-1])
    candidates = [
        f"{base}:{candidate}"
        for candidate in modifier_candidates
        if candidate.startswith(modifier_prefix)
        and candidate.lower() not in active_modifiers
    ]
    return _filter(candidates, incomplete)


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
        list_source_tables_for_service,
        list_tables_for_service,
    )

    candidates = _safe_call(list_tables_for_service, service)

    selected_tables = {
        table.casefold()
        for table in _get_multi_param_values(ctx, "add_source_table")
        if table.strip()
    }

    existing_source_tables = {
        table.casefold()
        for table in _safe_call(list_source_tables_for_service, service)
        if table.strip()
    }

    excluded_tables = selected_tables.union(existing_source_tables)

    filtered_candidates = [
        table_name
        for table_name in candidates
        if table_name.casefold() not in excluded_tables
    ]

    return _filter(filtered_candidates, incomplete)


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


def complete_from_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete --from from service ``source.tables`` keys only."""
    return complete_source_tables(ctx, _param, incomplete)


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

    candidates = _safe_call(list_columns_for_table, service, parts[0], parts[1])

    excluded = set(_get_multi_param_values(ctx, "track_columns"))
    excluded.update(_get_multi_param_values(ctx, "ignore_columns"))
    excluded.update(_get_existing_source_column_refs(service, table_spec))

    filtered = [col for col in candidates if col not in excluded]
    return _filter(filtered, incomplete)


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


def complete_add_custom_sink_table(
    ctx: click.Context,
    _param: click.Parameter,
    incomplete: str,
) -> list[CompletionItem]:
    """Complete table refs for --add-custom-sink-table from schema resources."""
    sink_key = _get_sink_key_with_default(ctx)
    if not sink_key:
        return []

    from cdc_generator.helpers.autocompletions.sinks import (
        list_custom_table_definitions_for_sink_target,
    )

    return _filter(
        _safe_call(list_custom_table_definitions_for_sink_target, sink_key),
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
    service = _get_service(ctx)
    sink_key = _get_param(ctx, "sink")

    if not sink_key:
        all_mode = bool(ctx.params.get("all_flag"))
        if all_mode and service:
            return _filter(
                _common_sink_schemas_for_all_sinks(service),
                incomplete,
            )
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
        sink_group = _get_param(ctx, "sink_group_positional")
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
