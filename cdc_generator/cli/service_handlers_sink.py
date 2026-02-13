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
    map_sink_columns,
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
    """Add sink destination(s) to a service."""
    raw = getattr(args, "add_sink", None)
    sink_keys: list[str] = (
        [str(x) for x in cast(list[object], raw)]
        if isinstance(raw, list)
        else [str(raw)]
    )
    success_count = 0

    for sink_key in sink_keys:
        if add_sink_to_service(args.service, sink_key):
            success_count += 1

    if success_count > 0:
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_remove(args: argparse.Namespace) -> int:
    """Remove sink destination(s) from a service."""
    raw = getattr(args, "remove_sink", None)
    sink_keys: list[str] = (
        [str(x) for x in cast(list[object], raw)]
        if isinstance(raw, list)
        else [str(raw)]
    )
    success_count = 0

    for sink_key in sink_keys:
        if remove_sink_from_service(args.service, sink_key):
            success_count += 1

    if success_count > 0:
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_add_table(args: argparse.Namespace) -> int:
    """Add table to sink (requires --sink or auto-defaults if only one sink)."""
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    # Check if replicate_structure is set
    replicate_structure = hasattr(args, "replicate_structure") and args.replicate_structure

    # Validate sink_schema is provided when replicate_structure is set
    if replicate_structure and (not hasattr(args, "sink_schema") or args.sink_schema is None):
        print_error(
            "--replicate-structure requires --sink-schema "
            + "(specify target schema for custom table)"
        )
        print_info(
            "Example: --replicate-structure --sink-schema notification"
        )
        return 1

    # Auto-set target_exists to false if replicate_structure is set
    if replicate_structure:
        if not hasattr(args, "target_exists") or args.target_exists is None:
            target_exists_value = "false"
            print_info(
                "Auto-setting --target-exists false "
                + "(table will be created with replicate_structure)"
            )
        else:
            target_exists_value = args.target_exists
    else:
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
        target_exists_value = args.target_exists

    # Auto-name from --from if table name not provided
    table_name = args.add_sink_table
    from_table: str | None = getattr(args, "from_table", None)

    if from_table is None:
        print_error(
            "--add-sink-table requires --from <source_schema.source_table>"
        )
        print_info(
            "Example: --add-sink-table public.customer_user "
            + "--from public.customer_user --target-exists false"
        )
        return 1

    if table_name is None:
        if from_table is not None:
            table_name = from_table
            print_info(
                f"Using source table name '{table_name}' for sink table"
            )
        else:
            print_error(
                "--add-sink-table requires a table name or --from flag"
            )
            print_info(
                "Example: --add-sink-table public.users\n"
                + "Or: --from public.customer_user (uses same name)"
            )
            return 1

    table_opts: dict[str, object] = {
        "target_exists": target_exists_value == "true",
    }

    # Handle 'from' field for source table reference
    if from_table is not None:
        table_opts["from"] = from_table

    # Handle replicate_structure flag
    if replicate_structure:
        table_opts["replicate_structure"] = True

    # Handle sink_schema override
    if hasattr(args, "sink_schema") and args.sink_schema is not None:
        table_opts["sink_schema"] = args.sink_schema

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
        table_name,
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


def handle_sink_update_schema(args: argparse.Namespace) -> int:
    """Update schema of a sink table (requires --sink, --sink-table, --update-schema)."""
    from cdc_generator.validators.manage_service.sink_operations import (
        update_sink_table_schema,
    )

    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    if not hasattr(args, "sink_table") or args.sink_table is None:
        print_error("--update-schema requires --sink-table")
        print_info(
            "Example: cdc manage-service --service directory "
            + "--sink sink_asma.calendar "
            + "--sink-table public.customer_user "
            + "--update-schema calendar"
        )
        return 1

    if update_sink_table_schema(
        args.service,
        sink_key,
        args.sink_table,
        args.update_schema,
    ):
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def handle_sink_map_column_on_table(args: argparse.Namespace) -> int:
    """Map columns on an existing sink table with validation.

    Requires --sink (or auto-default), --sink-table, and --map-column pairs.
    Validates column existence and type compatibility.
    """
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    if not hasattr(args, "sink_table") or args.sink_table is None:
        print_error("--map-column on existing table requires --sink-table")
        print_info(
            "Example: cdc manage-service --service directory "
            + "--sink sink_asma.proxy "
            + "--sink-table public.directory_user_name "
            + "--map-column brukerBrukerNavn user_name"
        )
        return 1

    column_mappings: list[tuple[str, str]] = [
        (str(pair[0]), str(pair[1])) for pair in args.map_column
    ]

    if map_sink_columns(
        args.service,
        sink_key,
        args.sink_table,
        column_mappings,
    ):
        return 0
    return 1


def handle_sink_map_column_error() -> int:
    """Error: --map-column used without --add-sink-table or --sink-table."""
    print_error(
        "--map-column requires --add-sink-table or "
        + "--sink-table to specify which table to map"
    )
    print_info(
        "Add new table: cdc manage-service --service directory "
        + "--sink sink_asma.chat "
        + "--add-sink-table public.users "
        + "--map-column id user_id"
    )
    print_info(
        "Map existing: cdc manage-service --service directory "
        + "--sink sink_asma.proxy "
        + "--sink-table public.users "
        + "--map-column id user_id"
    )
    return 1


def handle_sink_add_custom_table(args: argparse.Namespace) -> int:
    """Add a custom table to a sink with column definitions.

    Supports two modes:
    1. Inline columns: ``--column id:uuid:pk --column name:text``
    2. From pre-defined custom table: ``--from public.audit_log``
       (loads from service-schemas/{target}/custom-tables/)
    """
    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return 1

    from_table: str | None = getattr(args, "from_table", None)

    if from_table is None:
        print_error(
            "--add-custom-sink-table requires --from <source_schema.source_table>"
        )
        print_info(
            "Example (inline + source ref): cdc manage-service --service directory "
            + "--sink sink_asma.proxy "
            + "--add-custom-sink-table public.audit_log "
            + "--from public.audit_log "
            + "--column id:uuid:pk "
            + "--column event_type:text:not_null"
        )
        print_info(
            "Example (from source schema): cdc manage-service --service directory "
            + "--sink sink_asma.proxy "
            + "--add-custom-sink-table public.audit_log "
            + "--from public.audit_log"
        )
        return 1

    if add_custom_sink_table(
        args.service,
        sink_key,
        args.add_custom_sink_table,
        args.column or [],
        from_custom_table=from_table,
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
