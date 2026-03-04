"""Service and sink parsing helpers for migration generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.yaml_loader import load_yaml_file

from .data_structures import GenerationResult, SinkTarget


def _load_source_groups(project_root: Path) -> dict[str, Any]:
    """Load source-groups.yaml from the project root."""
    sg_path = project_root / "source-groups.yaml"
    if not sg_path.exists():
        return {}
    raw = load_yaml_file(sg_path)
    return cast(dict[str, Any], raw)


def derive_target_schemas(
    sink_tables: dict[str, dict[str, Any]],
) -> list[str]:
    """Derive unique target schemas from sink table keys."""
    schemas: set[str] = set()
    for sink_key in sink_tables:
        parts = sink_key.split(".", 1)
        if len(parts) > 1:
            schema = parts[0]
            if schema != "public":
                schemas.add(schema)
    return sorted(schemas)


def get_source_table_config(
    service_config: dict[str, object],
    source_key: str,
) -> dict[str, Any]:
    """Get source table config (e.g., ignore_columns) for a source key."""
    source_raw = service_config.get("source")
    if not isinstance(source_raw, dict):
        return {}
    source = cast(dict[str, Any], source_raw)
    tables_raw = source.get("tables", {})
    if not isinstance(tables_raw, dict):
        return {}
    tables = cast(dict[str, Any], tables_raw)
    entry = tables.get(source_key)
    if isinstance(entry, dict):
        return cast(dict[str, Any], entry)
    return {}


def get_sinks(
    service_config: dict[str, object],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Extract sink table configs organized per sink target."""
    sinks_raw = service_config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return {}
    sinks = cast(dict[str, Any], sinks_raw)

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for sink_name, sink_cfg_raw in sinks.items():
        if not isinstance(sink_cfg_raw, dict):
            continue
        sink_cfg = cast(dict[str, Any], sink_cfg_raw)
        tables_raw = sink_cfg.get("tables", {})
        if not isinstance(tables_raw, dict):
            continue
        tables: dict[str, dict[str, Any]] = {}
        for table_key, table_cfg_raw in cast(dict[str, Any], tables_raw).items():
            if isinstance(table_cfg_raw, dict):
                tables[str(table_key)] = cast(dict[str, Any], table_cfg_raw)
        if tables:
            result[str(sink_name)] = tables
    return result


def resolve_sink_target(sink_name: str, project_root: Path) -> SinkTarget:
    """Resolve a sink name to a SinkTarget with per-env database names."""
    parts = sink_name.split(".", 1)
    sink_group = parts[0]
    sink_service = parts[1] if len(parts) > 1 else ""

    databases: dict[str, str] = {}
    sg_path = project_root / "sink-groups.yaml"
    if sg_path.exists():
        raw = load_yaml_file(sg_path)
        group_cfg = cast(dict[str, Any], raw).get(sink_group, {})
        if isinstance(group_cfg, dict):
            sources = cast(dict[str, Any], group_cfg).get("sources", {})
            if isinstance(sources, dict):
                service_cfg = cast(dict[str, Any], sources).get(sink_service, {})
                if isinstance(service_cfg, dict):
                    for env_key, env_val in cast(dict[str, Any], service_cfg).items():
                        if isinstance(env_val, dict) and "database" in env_val:
                            databases[str(env_key)] = str(cast(dict[str, Any], env_val)["database"])

    return SinkTarget(
        sink_name=sink_name,
        sink_group=sink_group,
        sink_service=sink_service,
        databases=databases,
    )


def resolve_pattern(project_root: Path) -> str:
    """Determine architecture pattern from source-groups.yaml."""
    source_groups = _load_source_groups(project_root)
    for _group_name, group_cfg_raw in source_groups.items():
        if not isinstance(group_cfg_raw, dict):
            continue
        group_cfg = cast(dict[str, Any], group_cfg_raw)
        pattern = str(group_cfg.get("pattern", "")).strip().lower()
        if pattern:
            return pattern
    return "db-per-tenant"


def validate_db_shared_customer_id(
    sinks: dict[str, dict[str, dict[str, Any]]],
    result: GenerationResult,
) -> None:
    """Warn when db-shared sink tables miss customer_id column template."""
    for _sink_name, tables in sinks.items():
        for table_key, table_cfg in tables.items():
            if bool(table_cfg.get("target_exists", False)):
                continue
            templates = table_cfg.get("column_templates", [])
            has_customer_id = False
            if isinstance(templates, list):
                for template in cast(list[object], templates):
                    if isinstance(template, dict):
                        template_dict = cast(dict[str, Any], template)
                        if str(template_dict.get("name", "")).casefold() == "customer_id":
                            has_customer_id = True
                            break
                    elif isinstance(template, str) and template.casefold() == "customer_id":
                        has_customer_id = True
                        break
            if not has_customer_id:
                result.warnings.append(
                    f"db-shared: {table_key} has no customer_id column_template "
                    + "— multi-tenant isolation may be incomplete",
                )