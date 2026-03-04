"""List and validate helpers for sink operations."""

from typing import cast

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.service_config import load_service_config

from .sink_listing import format_sink_entry
from .sink_operations_helpers import _get_source_table_keys
from .sink_validation import validate_single_sink


def list_sinks_impl(service: str) -> bool:
    """List all sinks configured for *service*."""
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return False

    sinks = cast(dict[str, object], sinks_raw)
    print_header(f"Sinks for service '{service}'")

    for sk_raw, sc_raw in sinks.items():
        sk = str(sk_raw)
        sc = cast(dict[str, object], sc_raw) if isinstance(sc_raw, dict) else {}
        format_sink_entry(sk, sc)

    src_count = len(_get_source_table_keys(config))
    print(f"\n{Colors.DIM}Source tables: {src_count}{Colors.RESET}")
    return True


def validate_sinks_impl(service: str) -> bool:
    """Validate sink configuration for *service*."""
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return True

    sinks = cast(dict[str, object], sinks_raw)
    source_tables = _get_source_table_keys(config)
    all_valid = True

    for sk_raw, sc_raw in sinks.items():
        for error in validate_single_sink(str(sk_raw), sc_raw, source_tables):
            print_error(f"  ✗ {error}")
            all_valid = False

    if all_valid:
        print_success(f"Sink configuration for service '{service}' is valid")
    else:
        print_error(f"Sink validation failed for service '{service}'")

    return all_valid
