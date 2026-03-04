"""Service-level sink add/remove operations."""

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import load_service_config
from cdc_generator.validators.manage_service.validation import validate_service_sink_preflight

from .config import save_service_config
from .sink_operations_helpers import (
    _get_sinks_dict,
    _validate_and_parse_sink_key,
    _validate_sink_group_exists,
)


def add_sink_to_service(service: str, sink_key: str) -> bool:
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

    sinks[sink_key] = {"tables": {}}

    preflight_errors, preflight_warnings = validate_service_sink_preflight(service, config)
    if preflight_errors:
        for error in preflight_errors:
            print_error(f"  ✗ {error}")
        del sinks[sink_key]
        return False
    for warning in preflight_warnings:
        print_warning(f"  ⚠ {warning}")

    if not save_service_config(service, config):
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
