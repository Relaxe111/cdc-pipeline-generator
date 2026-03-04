"""Unique column template preflight checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from cdc_generator.core.column_template_operations import ResolvedColumnTemplate, resolve_column_templates
from cdc_generator.core.sink_env_routing import get_sink_target_env_keys
from cdc_generator.core.source_ref_resolver import parse_source_ref, resolve_source_ref
from cdc_generator.helpers.yaml_loader import load_yaml_file

from .types import (
    ValidationConfig,
    iter_source_env_entries,
    load_sink_group_config,
    load_source_group_context,
    project_root,
)

_SINK_KEY_PARTS_COUNT = 2
_MIN_COLLISION_ROUTE_COUNT = 2


@dataclass
class UniqueIssueState:
    """Mutable collector state for unique template validation."""

    collisions: dict[tuple[str, str, str, str], list[str]]
    errors: list[str]
    missing_unique_values: list[str]


@dataclass(frozen=True)
class SinkScopeContext:
    """Context required to evaluate unique scope for one sink/table."""

    sink_key: str
    table_key: str
    sink_env_aware: bool
    sink_target_envs: set[str] | None
    server_group_name: str


def _resolve_scope_key(
    sink_env_aware: bool,
    sink_target_envs: set[str] | None,
    env_cfg: dict[str, Any],
    server_group_name: str,
) -> str | None:
    """Resolve collision scope key for the current source route."""
    if not sink_env_aware:
        return f"source_group:{server_group_name}"

    target_sink_env_raw = env_cfg.get("target_sink_env")
    target_sink_env = str(target_sink_env_raw).strip() if isinstance(target_sink_env_raw, str) else ""
    if sink_target_envs is None or not target_sink_env:
        return None
    if target_sink_env not in sink_target_envs:
        return None
    return f"sink_env:{target_sink_env}"


def _resolve_unique_value(
    sink_key: str,
    table_key: str,
    route_identity: str,
    source_name: str,
    env_name: str,
    source_groups: dict[str, Any],
    resolved_template: ResolvedColumnTemplate,
) -> tuple[str | None, str | None]:
    """Resolve template value and return (value,error)."""
    template_key = str(resolved_template.template_key)
    value_source = str(resolved_template.template.value_source)
    raw_value = str(resolved_template.value)

    if value_source != "source_ref":
        return raw_value.strip(), None

    parsed_ref = parse_source_ref(raw_value)
    if parsed_ref is None:
        return None, (
            f"Invalid source_ref template value '{raw_value}' for unique template "
            + f"'{template_key}' on sink table '{table_key}'"
        )

    try:
        resolved_ref = resolve_source_ref(
            parsed_ref,
            source_name=source_name,
            env=env_name,
            config=source_groups,
        )
    except Exception as ref_error:
        return None, (
            f"{sink_key}.{table_key}: failed to resolve unique source_ref "
            + f"'{raw_value}' for {route_identity}: {ref_error}"
        )

    return str(resolved_ref).strip(), None


def _collect_sink_table_unique_issues(
    context: SinkScopeContext,
    source_entries: dict[str, Any],
    source_groups: dict[str, Any],
    unique_templates: list[ResolvedColumnTemplate],
    state: UniqueIssueState,
) -> None:
    """Collect unique-value issues for one sink table across source routes."""
    for source_name, source_entry_raw in source_entries.items():
        if not isinstance(source_entry_raw, dict):
            continue
        source_entry = cast(dict[str, Any], source_entry_raw)
        env_entries = iter_source_env_entries(source_entry)
        if not env_entries:
            continue

        for env_name, env_cfg in env_entries:
            scope_key = _resolve_scope_key(
                sink_env_aware=context.sink_env_aware,
                sink_target_envs=context.sink_target_envs,
                env_cfg=env_cfg,
                server_group_name=context.server_group_name,
            )
            if scope_key is None:
                continue

            route_identity = f"{source_name}.{env_name}"
            for unique_template_raw in unique_templates:
                resolved_template = cast(Any, unique_template_raw)
                normalized_value, error = _resolve_unique_value(
                    sink_key=context.sink_key,
                    table_key=context.table_key,
                    route_identity=route_identity,
                    source_name=str(source_name),
                    env_name=env_name,
                    source_groups=source_groups,
                    resolved_template=resolved_template,
                )
                if error is not None:
                    state.errors.append(error)
                    continue
                if normalized_value is None:
                    continue

                if normalized_value.casefold() in {"", "null", "none"}:
                    state.missing_unique_values.append(
                        f"{context.sink_key}.{context.table_key}: unique template '{resolved_template.template_key}' "
                        + f"resolved to empty/null for route {route_identity}"
                    )
                    continue

                collision_key = (
                    context.sink_key,
                    scope_key,
                    resolved_template.template_key,
                    normalized_value,
                )
                state.collisions.setdefault(collision_key, []).append(route_identity)


def collect_unique_template_issues(
    service: str,
    config: ValidationConfig,
) -> list[str]:
    """Collect errors for unique template collisions and invalid resolved values."""
    errors: list[str] = []

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        return errors

    source_context = load_source_group_context(service, config)
    server_group_name = source_context["server_group_name"]
    source_group_cfg = source_context["source_group_cfg"]
    if source_group_cfg is None or server_group_name is None:
        return errors

    sources_raw = source_group_cfg.get("sources")
    if not isinstance(sources_raw, dict):
        return errors

    source_entries = cast(dict[str, Any], sources_raw)
    source_groups = cast(dict[str, Any], load_yaml_file(project_root() / "source-groups.yaml"))

    collisions: dict[tuple[str, str, str, str], list[str]] = {}
    missing_unique_values: list[str] = []
    state = UniqueIssueState(
        collisions=collisions,
        errors=errors,
        missing_unique_values=missing_unique_values,
    )

    for sink_key_raw, sink_cfg_raw in sinks_raw.items():
        sink_key = str(sink_key_raw)
        if not isinstance(sink_cfg_raw, dict):
            continue
        sink_cfg = cast(dict[str, Any], sink_cfg_raw)

        sink_parts = sink_key.split(".", 1)
        sink_group_name = sink_parts[0] if len(sink_parts) == _SINK_KEY_PARTS_COUNT else ""
        sink_group_cfg = load_sink_group_config(sink_group_name) if sink_group_name else None
        sink_env_aware = bool(sink_group_cfg.get("environment_aware", False)) if sink_group_cfg else False

        sink_target_envs, _warning = get_sink_target_env_keys(project_root(), sink_key)

        tables_raw = sink_cfg.get("tables")
        if not isinstance(tables_raw, dict):
            continue

        tables = cast(dict[str, Any], tables_raw)
        for table_key, table_cfg_raw in tables.items():
            if not isinstance(table_cfg_raw, dict):
                continue
            table_cfg = cast(dict[str, object], table_cfg_raw)
            resolved_templates = resolve_column_templates(table_cfg)
            unique_templates = [tpl for tpl in resolved_templates if tpl.template.unique]
            if not unique_templates:
                continue
            _collect_sink_table_unique_issues(
                context=SinkScopeContext(
                    sink_key=sink_key,
                    table_key=str(table_key),
                    sink_env_aware=sink_env_aware,
                    sink_target_envs=sink_target_envs,
                    server_group_name=server_group_name,
                ),
                source_entries=source_entries,
                source_groups=source_groups,
                unique_templates=cast(list[ResolvedColumnTemplate], unique_templates),
                state=state,
            )

    errors.extend(missing_unique_values)

    for (sink_key, scope_key, template_key, value), routes in collisions.items():
        if len(routes) < _MIN_COLLISION_ROUTE_COUNT:
            continue
        route_list = ", ".join(sorted(routes))
        errors.append(
            f"{sink_key}: unique template collision in {scope_key} for template '{template_key}' "
            + f"value '{value}' across routes: {route_list}"
        )

    return errors
