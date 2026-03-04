"""Sink CLI operations for manage-services config."""

import argparse
from typing import cast

from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    build_table_opts as _build_table_opts_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    list_sink_keys_for_service as _list_sink_keys_for_service_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    list_source_tables_for_service as _list_source_tables_for_service_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    parse_map_column_pairs as _parse_map_column_pairs_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    resolve_add_table_names as _resolve_add_table_names_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    resolve_effective_sink_table_key as _resolve_effective_sink_table_key_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    resolve_sink_key as _resolve_sink_key_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    resolve_target_exists_value as _resolve_target_exists_value_impl,
)
from cdc_generator.cli.service_handlers_sink_add_table_helpers import (
    validate_sink_add_table_mode as _validate_sink_add_table_mode_impl,
)
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
from cdc_generator.validators.manage_service.sink_template_ops import (
    add_transform_to_table,
)

_MAP_COLUMN_PAIR_PARTS = 2


def _list_sink_keys_for_service(service: str) -> list[str]:
    """Return configured sink keys for a service."""
    return _list_sink_keys_for_service_impl(service)


def _list_source_tables_for_service(service: str) -> list[str]:
    """Return configured source table keys for a service."""
    return _list_source_tables_for_service_impl(service)


def _resolve_sink_keys_for_add_table(
    args: argparse.Namespace,
) -> tuple[list[str], bool] | None:
    """Resolve sink targets for add-sink-table flow."""
    all_sinks_mode = bool(getattr(args, "all", False)) or getattr(
        args, "sink", None,
    ) == "all"
    if all_sinks_mode:
        sink_keys = _list_sink_keys_for_service(args.service)
        if not sink_keys:
            print_error(f"No sinks configured for service '{args.service}'")
            return None
        return sink_keys, True

    sink_key = _resolve_sink_key(args)
    if not sink_key:
        return None
    return [sink_key], False


def _resolve_target_exists_value(
    args: argparse.Namespace,
    replicate_structure: bool,
) -> str | None:
    """Resolve target_exists value (including replicate defaults)."""
    return _resolve_target_exists_value_impl(args, replicate_structure)


def _build_table_opts(
    args: argparse.Namespace,
    target_exists_value: str,
    replicate_structure: bool,
    map_column_pairs: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    """Build add_sink_table options from CLI args."""
    return _build_table_opts_impl(
        args,
        target_exists_value,
        replicate_structure,
        map_column_pairs,
    )


def _resolve_effective_sink_table_key(
    table_name: str,
    sink_schema: str | None,
) -> str:
    """Resolve final sink table key after optional --sink-schema override."""
    return _resolve_effective_sink_table_key_impl(table_name, sink_schema)


def _apply_transform_after_add(
    args: argparse.Namespace,
    sink_key: str,
    table_name: str,
) -> bool:
    """Apply add-time transform to a newly created sink table entry."""
    add_transform = getattr(args, "add_transform", None)
    if not add_transform:
        return True

    final_table_key = _resolve_effective_sink_table_key(
        table_name,
        getattr(args, "sink_schema", None),
    )
    skip_validation = bool(getattr(args, "skip_validation", False))
    if add_transform_to_table(
        args.service,
        sink_key,
        final_table_key,
        str(add_transform),
        skip_validation,
    ):
        return True

    print_error(
        f"Failed to add transform '{add_transform}' to '{final_table_key}'. "
        + "Rolling back sink table add."
    )
    remove_sink_table(args.service, sink_key, final_table_key)
    return False


def _parse_map_column_pairs(
    map_column_raw: object,
) -> list[tuple[str, str]] | None:
    """Parse map-column values into ``(source, target)`` tuples.

    Supports both ``TARGET:SOURCE`` and legacy ``SOURCE TARGET`` tuple-like
    values used by older tests or direct handler invocations.
    """
    return _parse_map_column_pairs_impl(map_column_raw)


def _handle_from_all_add_table(
    args: argparse.Namespace,
    sink_keys: list[str],
    table_opts: dict[str, object],
) -> int:
    """Handle add-sink-table fanout for --from all."""
    source_tables = _list_source_tables_for_service(args.service)
    if not source_tables:
        print_error(
            f"No source tables found for service '{args.service}'"
        )
        return 1

    print_info(f"Adding {len(source_tables)} source tables to sink(s)...")

    success_count = 0
    for src_table in source_tables:
        table_opts["from"] = src_table
        for sink_key in sink_keys:
            if add_sink_table(
                args.service,
                sink_key,
                src_table,
                table_opts=table_opts,
            ) and _apply_transform_after_add(args, sink_key, src_table):
                success_count += 1

    if success_count > 0:
        print_info(
            f"Successfully added {success_count} table configurations."
        )
        print_info("Run 'cdc generate' to update pipelines")
        return 0
    return 1


def _validate_sink_add_table_mode(
    all_sinks_mode: bool,
    replicate_structure: bool,
    args: argparse.Namespace,
) -> bool:
    """Validate add-sink-table mode-specific constraints."""
    return _validate_sink_add_table_mode_impl(
        all_sinks_mode,
        replicate_structure,
        args,
    )


def _resolve_add_table_names(
    args: argparse.Namespace,
    all_sinks_mode: bool,
) -> tuple[str | None, str] | None:
    """Resolve and validate table name and --from value."""
    return _resolve_add_table_names_impl(args, all_sinks_mode)


def _resolve_sink_key(args: argparse.Namespace) -> str | None:
    """Resolve sink key from --sink or auto-default when only one sink.

    Returns:
        Sink key string, or None if not resolvable.
    """
    return _resolve_sink_key_impl(args)


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
    sink_resolution = _resolve_sink_keys_for_add_table(args)
    if sink_resolution is None:
        return 1
    sink_keys, all_sinks_mode = sink_resolution

    replicate_structure = bool(
        hasattr(args, "replicate_structure") and args.replicate_structure
    )
    if not _validate_sink_add_table_mode(
        all_sinks_mode,
        replicate_structure,
        args,
    ):
        return 1

    target_exists_value = _resolve_target_exists_value(
        args, replicate_structure,
    )
    if target_exists_value is None:
        return 1

    table_resolution = _resolve_add_table_names(args, all_sinks_mode)
    if table_resolution is None:
        return 1
    table_name, from_table = table_resolution

    map_column_pairs = _parse_map_column_pairs(getattr(args, "map_column", None))
    if map_column_pairs is None:
        return 1

    table_opts = _build_table_opts(
        args,
        target_exists_value,
        replicate_structure,
        map_column_pairs,
    )

    if from_table == "all":
        if table_name is not None:
            print_error(
                "When using --from all, omit the value for --add-sink-table"
            )
        else:
            return _handle_from_all_add_table(args, sink_keys, table_opts)

    if from_table != "all" and table_name is None:
        table_name = from_table
        print_info(
            f"Using source table name '{table_name}' for sink table"
        )

    if table_name is None:
        print_error(
            "Unable to resolve sink table name."
        )
        return 1

    table_opts["from"] = from_table

    success_count = 0
    for sink_key in sink_keys:
        if add_sink_table(
            args.service,
            sink_key,
            table_name,
            table_opts=table_opts,
        ) and _apply_transform_after_add(args, sink_key, table_name):
            success_count += 1

    if success_count > 0:
        if all_sinks_mode:
            print_info(
                f"Added sink table to {success_count}/{len(sink_keys)} sinks"
            )
        print_info("Run 'cdc generate' to update pipelines")

    return 0 if success_count > 0 else 1


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
            "Example: cdc manage-services config --service directory "
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
            "Example: cdc manage-services config --service directory "
            + "--sink sink_asma.proxy "
            + "--sink-table public.directory_user_name "
            + "--map-column user_name:brukerBrukerNavn"
        )
        return 1

    column_mappings = _parse_map_column_pairs(args.map_column)
    if column_mappings is None:
        return 1

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
        "Add new table: cdc manage-services config --service directory "
        + "--sink sink_asma.chat "
        + "--add-sink-table public.users "
        + "--map-column user_id:id"
    )
    print_info(
        "Map existing: cdc manage-services config --service directory "
        + "--sink sink_asma.proxy "
        + "--sink-table public.users "
        + "--map-column user_id:id"
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
            "Example (inline + source ref): cdc manage-services config --service directory "
            + "--sink sink_asma.proxy "
            + "--add-custom-sink-table public.audit_log "
            + "--from public.audit_log "
            + "--column id:uuid:pk "
            + "--column event_type:text:not_null"
        )
        print_info(
            "Example (from source schema): cdc manage-services config --service directory "
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
        "Example: cdc manage-services config --service directory "
        + "--sink sink_asma.proxy "
        + "--modify-custom-table public.audit_log "
        + "--add-column updated_at:timestamptz:default_now"
    )
    return 1
