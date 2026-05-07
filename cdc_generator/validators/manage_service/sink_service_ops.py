"""Service-level sink add/remove operations."""

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import (
    get_project_root,
    load_service_config,
)
from cdc_generator.helpers.yaml_loader import yaml
from cdc_generator.validators.manage_service.preflight.types import (
    ValidationConfig,
    load_sink_group_config,
    load_source_group_context,
)
from cdc_generator.validators.manage_service.validation import validate_service_sink_preflight
from cdc_generator.core.sink_env_routing import get_sink_target_env_keys

from .config import save_service_config
from .sink_operations_helpers import (
    _get_sinks_dict,
    _validate_and_parse_sink_key,
    _validate_sink_group_exists,
)

_SINK_KEY_PARTS_COUNT = 2


def _rollback_source_groups_file(
    source_groups_path: Path,
    original_text: str | None,
) -> None:
    """Restore the previous source-groups.yaml contents after a failed sink add."""
    if original_text is None:
        return

    source_groups_path.write_text(original_text, encoding="utf-8")


def _save_target_sink_env_to_source_group(
    source_group_name: str,
    target_sink_env: str,
) -> tuple[bool, str | None, Path]:
    """Persist ``target_sink_env`` onto every source route in the source group."""
    source_groups_path = get_project_root() / "source-groups.yaml"
    if not source_groups_path.exists():
        print_error("source-groups.yaml not found")
        return False, None, source_groups_path

    original_text = source_groups_path.read_text(encoding="utf-8")
    with source_groups_path.open(encoding="utf-8") as handle:
        source_groups_data = yaml.load(handle)

    if not isinstance(source_groups_data, dict):
        print_error("source-groups.yaml must contain a top-level mapping")
        return False, None, source_groups_path

    source_group_raw = source_groups_data.get(source_group_name)
    if not isinstance(source_group_raw, dict):
        print_error(
            f"Source group '{source_group_name}' not found in source-groups.yaml"
        )
        return False, None, source_groups_path

    source_group_cfg = cast(dict[str, Any], source_group_raw)
    sources_raw = source_group_cfg.get("sources")
    if not isinstance(sources_raw, dict):
        print_error(
            f"Source group '{source_group_name}' has no sources defined in source-groups.yaml"
        )
        return False, None, source_groups_path

    updated_routes = 0
    sources = cast(dict[str, Any], sources_raw)
    for source_entry_raw in sources.values():
        if not isinstance(source_entry_raw, dict):
            continue

        source_entry = cast(dict[str, Any], source_entry_raw)
        for env_name, env_cfg_raw in source_entry.items():
            if env_name == "schemas" or not isinstance(env_cfg_raw, dict):
                continue

            env_cfg = cast(dict[str, Any], env_cfg_raw)
            env_cfg["target_sink_env"] = target_sink_env
            updated_routes += 1

    if updated_routes == 0:
        print_error(
            f"Source group '{source_group_name}' has no source routes to update"
        )
        return False, None, source_groups_path

    with source_groups_path.open("w", encoding="utf-8") as handle:
        yaml.dump(source_groups_data, handle)

    return True, original_text, source_groups_path


def _prepare_target_sink_env_update(
    service: str,
    config: dict[str, object],
    sink_key: str,
    target_sink_env: str | None,
) -> tuple[bool, str | None, Path | None]:
    """Validate and persist target sink env routing when it is required."""
    source_context = load_source_group_context(
        service,
        cast(ValidationConfig, config),
    )
    source_group_name = source_context["server_group_name"]
    source_group_cfg = source_context["source_group_cfg"]
    if not source_group_name or source_group_cfg is None:
        return True, None, None

    sink_parts = sink_key.split(".", 1)
    if len(sink_parts) != _SINK_KEY_PARTS_COUNT:
        return True, None, None

    sink_group_cfg = load_sink_group_config(sink_parts[0])
    if sink_group_cfg is None:
        return True, None, None

    source_env_aware = bool(source_group_cfg.get("environment_aware", False))
    sink_env_aware = bool(sink_group_cfg.get("environment_aware", False))
    if not sink_env_aware or source_env_aware:
        if target_sink_env:
            print_warning(
                f"Ignoring --target-sink-env for '{sink_key}' because sink routing does not require it"
            )
        return True, None, None

    sink_target_envs, topology_warning = get_sink_target_env_keys(
        get_project_root(),
        sink_key,
    )
    if sink_target_envs is None:
        if target_sink_env:
            warning_message = topology_warning or "sink topology unavailable"
            print_warning(
                f"Skipping target sink env validation for '{sink_key}': {warning_message}"
            )
        return True, None, None

    if not target_sink_env:
        print_error(
            f"Adding sink '{sink_key}' requires --target-sink-env because source group "
            + f"'{source_group_name}' is not environment-aware and the sink group is environment-aware"
        )
        print_info(
            "Available target sink env values: "
            + ", ".join(sorted(sink_target_envs))
        )
        return False, None, None

    if target_sink_env not in sink_target_envs:
        print_error(
            f"Invalid --target-sink-env '{target_sink_env}' for sink '{sink_key}'"
        )
        print_info(
            "Available target sink env values: "
            + ", ".join(sorted(sink_target_envs))
        )
        return False, None, None

    updated, original_text, source_groups_path = _save_target_sink_env_to_source_group(
        source_group_name,
        target_sink_env,
    )
    if not updated:
        return False, None, None

    return True, original_text, source_groups_path


def add_sink_to_service(
    service: str,
    sink_key: str,
    target_sink_env: str | None = None,
) -> bool:
    """Add a sink destination to service config."""
    parsed = _validate_and_parse_sink_key(sink_key)
    if parsed is None:
        return False

    sink_group, _target = parsed
    if not _validate_sink_group_exists(sink_group):
        print_error(f"Sink group '{sink_group}' not found in sink-groups.yaml")
        print_info("Run 'cdc manage-sink-groups --list' to see available groups")
        return False

    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key in sinks:
        print_warning(f"Sink '{sink_key}' already exists in service '{service}'")
        return False

    routing_ready, source_groups_original, source_groups_path = _prepare_target_sink_env_update(
        service,
        config,
        sink_key,
        target_sink_env,
    )
    if not routing_ready:
        return False

    sinks[sink_key] = {"tables": {}}

    preflight_errors, preflight_warnings = validate_service_sink_preflight(service, config)
    if preflight_errors:
        for error in preflight_errors:
            print_error(f"  ✗ {error}")
        del sinks[sink_key]
        if source_groups_path is not None:
            _rollback_source_groups_file(source_groups_path, source_groups_original)
        return False
    for warning in preflight_warnings:
        print_warning(f"  ⚠ {warning}")

    if not save_service_config(service, config):
        if source_groups_path is not None:
            _rollback_source_groups_file(source_groups_path, source_groups_original)
        return False

    print_success(f"Added sink '{sink_key}' to service '{service}'")
    return True


def remove_sink_from_service(service: str, sink_key: str) -> bool:
    """Remove a sink destination from service config."""
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_warning(f"Sink '{sink_key}' not found in service '{service}'")
        available = [str(k) for k in sinks]
        if available:
            print_info(f"Available sinks: {', '.join(available)}")
        return False

    del sinks[sink_key]
    if not save_service_config(service, config):
        return False

    print_success(f"Removed sink '{sink_key}' from service '{service}'")
    return True
