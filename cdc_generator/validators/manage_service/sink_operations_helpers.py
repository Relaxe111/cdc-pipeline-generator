"""Shared helper functions for sink operations."""

from typing import cast

from cdc_generator.helpers.helpers_logging import print_error, print_info
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs

_SINK_KEY_SEPARATOR = "."
_SINK_KEY_PARTS = 2


def _parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse 'sink_group.target_service' → (sink_group, target_service)."""
    parts = sink_key.split(_SINK_KEY_SEPARATOR, 1)
    if len(parts) != _SINK_KEY_PARTS:
        return None
    return parts[0], parts[1]


def _validate_sink_group_exists(sink_group: str) -> bool:
    """Return True if *sink_group* exists in sink-groups.yaml."""
    sink_file = get_project_root() / "sink-groups.yaml"
    if not sink_file.exists():
        return False
    try:
        from cdc_generator.helpers.yaml_loader import load_yaml_file

        sink_groups = load_yaml_file(sink_file)
        return sink_group in sink_groups
    except (FileNotFoundError, ValueError):
        return False


def _validate_and_parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse *sink_key*, printing an error if the format is invalid."""
    parsed = _parse_sink_key(sink_key)
    if parsed is None:
        print_error(
            f"Invalid sink key '{sink_key}'. Expected format: "
            + "sink_group.target_service (e.g., sink_asma.chat)"
        )
    return parsed


def _get_target_service_from_sink_key(sink_key: str) -> str | None:
    """Extract target_service from sink key 'sink_group.target_service'."""
    parsed = _parse_sink_key(sink_key)
    return parsed[1] if parsed else None


def _list_tables_in_service_schemas(target_service: str) -> list[str]:
    """List all tables for sink target across preferred/legacy schema roots."""
    tables: set[str] = set()
    for service_dir in get_service_schema_read_dirs(target_service, get_project_root()):
        if not service_dir.is_dir():
            continue
        for schema_dir in service_dir.iterdir():
            if not schema_dir.is_dir():
                continue
            for table_file in schema_dir.glob("*.yaml"):
                tables.add(f"{schema_dir.name}.{table_file.stem}")
    return sorted(tables)


def _validate_table_in_schemas(
    sink_key: str,
    table_key: str,
) -> bool:
    """Validate that table_key exists in schema files for the sink target."""
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return False

    has_service_dir = any(
        service_dir.is_dir()
        for service_dir in get_service_schema_read_dirs(target_service, get_project_root())
    )
    if not has_service_dir:
        print_error(f"No schemas found for sink target '{target_service}'")
        print_info(
            "To fetch schemas, run:\n"
            + "  cdc manage-services config --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        print_info(
            "Or create manually: "
            + f"services/_schemas/{target_service}/<schema>/<Table>.yaml"
        )
        return False

    available = _list_tables_in_service_schemas(target_service)
    if not available:
        print_error(
            f"Schema directory for '{target_service}' exists but is empty"
        )
        print_info(
            "To populate schemas, run:\n"
            + "  cdc manage-services config --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return False

    if table_key not in available:
        print_error(
            f"Table '{table_key}' not found in schemas for "
            + f"target service '{target_service}'"
        )
        print_info(
            "Available tables:\n  "
            + "\n  ".join(available)
        )
        return False

    return True


def _get_source_tables_dict(config: dict[str, object]) -> dict[str, object]:
    """Return source.tables dict (typed), or empty dict if absent."""
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return {}
    source = cast(dict[str, object], source_raw)
    tables_raw = source.get("tables")
    if not isinstance(tables_raw, dict):
        return {}
    return cast(dict[str, object], tables_raw)


def _get_source_table_keys(config: dict[str, object]) -> list[str]:
    """Return list of source table keys (e.g. ['public.users', …])."""
    return [str(key) for key in _get_source_tables_dict(config)]


def _get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return sinks section, creating it if absent."""
    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        config["sinks"] = {}
        return cast(dict[str, object], config["sinks"])
    return cast(dict[str, object], sinks_raw)


def _get_sink_tables(sink_cfg: dict[str, object]) -> dict[str, object]:
    """Return the tables dict inside a sink config, creating if absent."""
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        sink_cfg["tables"] = {}
        return cast(dict[str, object], sink_cfg["tables"])
    return cast(dict[str, object], tables_raw)


def _resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None (with error) if invalid."""
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Invalid sink configuration for '{sink_key}'")
        return None
    return cast(dict[str, object], sink_raw)
