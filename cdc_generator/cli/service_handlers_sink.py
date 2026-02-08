"""Sink CLI operations for manage-service."""

import argparse
from typing import cast

from cdc_generator.cli.service_handlers_sink_custom import (
    add_column_to_custom_table,
    add_custom_sink_table,
    remove_column_from_custom_table,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)
from cdc_generator.validators.manage_service.sink_operations import (
    add_sink_table,
    add_sink_to_service,
    list_sinks,
    remove_sink_from_service,
    remove_sink_table,
    validate_sinks,
)


def _resolve_sink_key(args: argparse.Namespace) -> str | None:
    """Resolve sink key from --sink or auto-default when only one sink.

    Returns:
        Sink key string, or None if not resolvable.
    """
    if args.sink:
        return str(args.sink)

    # Auto-default: load service config to check sink count
    from cdc_generator.helpers.service_config import (
        load_service_config,
    )

    try:
        config = load_service_config(args.service)
    except FileNotFoundError:
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return None

    sinks = cast(dict[str, object], sinks_raw)
    sink_keys = list(sinks.keys())
    if len(sink_keys) == 1:
        sink_key = sink_keys[0]
        print_info(f"Auto-selected sink: {sink_key}")
        return sink_key

    if len(sink_keys) == 0:
        print_error("No sinks configured for this service")
    else:
        print_error(
            "--sink is required when service has multiple sinks"
        )
        print_info(
            "Available sinks: "
            + ", ".join(sink_keys)
        )
    return None


def handle_sink_list(args: argparse.Namespace) -> int:
    """List all sink configurations for service."""
    return 0 if list_sinks(args.service) else 1


def handle_sink_validate(args: argparse.Namespace) -> int:
    """Validate sink configuration."""
    return 0 if validate_sinks(args.service) else 1


def handle_sink_add(args: argparse.Namespace) -> int:
    """Add a sink destination to a service."""
    if add_sink_to_service(args.service, args.add_sink):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_remove(args: argparse.Namespace) -> int:
    """Remove a sink destination from a service."""
    if remove_sink_from_service(args.service, args.remove_sink):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_add_table(args: argparse.Namespace) -> int:
    """Add table to sink (requires --sink or auto-defaults if only one sink)."""
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    if not hasattr(args, "target_exists") or args.target_exists is None:
        print_error(
            "--add-sink-table requires --target-exists "
            + "(true or false)"
        )
        print_info(
            "Example (autocreate): --target-exists false\n"
            + "Example (map existing): --target-exists true "
            + "--target public.users"
        )
        return 1

    table_opts: dict[str, object] = {
        "target_exists": args.target_exists == "true",
    }

    if args.target is not None:
        table_opts["target"] = args.target
    if args.map_column:
        table_opts["columns"] = dict(args.map_column)
    if args.target_schema:
        table_opts["target_schema"] = args.target_schema
    if args.include_sink_columns:
        table_opts["include_columns"] = args.include_sink_columns

    if add_sink_table(
        args.service,
        sink_key,
        args.add_sink_table,
        table_opts=table_opts if table_opts else None,
    ):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_remove_table(args: argparse.Namespace) -> int:
    """Remove table from sink (requires --sink)."""
    if remove_sink_table(
        args.service, args.sink, args.remove_sink_table,
    ):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_map_column_error() -> int:
    """Error: --map-column used without --add-sink-table."""
    print_error(
        "--map-column requires --add-sink-table "
        + "to specify which table to map"
    )
    print_info(
        "Example: cdc manage-service --service directory "
        + "--sink sink_asma.chat "
        + "--add-sink-table public.users "
        + "--map-column id user_id"
    )
    return 1


def handle_sink_add_custom_table(args: argparse.Namespace) -> int:
    """Add a custom table to a sink with column definitions."""
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    if not args.column:
        print_error(
            "--add-custom-sink-table requires at least one --column"
        )
        print_info(
            "Example: cdc manage-service --service directory "
            + "--sink sink_asma.proxy "
            + "--add-custom-sink-table public.audit_log "
            + "--column id:uuid:pk "
            + "--column event_type:text:not_null"
        )
        return 1

    if add_custom_sink_table(
        args.service,
        sink_key,
        args.add_custom_sink_table,
        args.column,
    ):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_modify_custom_table(args: argparse.Namespace) -> int:
    """Modify a custom table (add/remove columns)."""
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    if args.add_column:
        if add_column_to_custom_table(
            args.service,
            sink_key,
            args.modify_custom_table,
            args.add_column,
        ):
            print_info("Run 'cdc generate' to update pipelines")
            return 0
        return 1

    if args.remove_column:
        if remove_column_from_custom_table(
            args.service,
            sink_key,
            args.modify_custom_table,
            args.remove_column,
        ):
            print_info("Run 'cdc generate' to update pipelines")
            return 0
        return 1

    print_error(
        "--modify-custom-table requires "
        + "--add-column or --remove-column"
    )
    print_info(
        "Example: cdc manage-service --service directory "
        + "--sink sink_asma.proxy "
        + "--modify-custom-table public.audit_log "
        + "--add-column updated_at:timestamptz:default_now"
    )
    return 1
