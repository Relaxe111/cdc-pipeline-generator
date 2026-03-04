"""Add-table and sink-key helper logic for sink service handlers."""

from __future__ import annotations

import argparse
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)
from cdc_generator.validators.manage_service.sink_operations import (
    add_sink_table,
    remove_sink_table,
)
from cdc_generator.validators.manage_service.sink_template_ops import (
    add_transform_to_table,
)

_MAP_COLUMN_PAIR_PARTS = 2


def _list_sink_keys_for_service(service: str) -> list[str]:
    """Return configured sink keys for a service."""
    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return []

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return []

    sinks = cast(dict[str, object], sinks_raw)
    return sorted(sinks)


def _list_source_tables_for_service(service: str) -> list[str]:
    """Return configured source table keys for a service."""
    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return []

    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return []
    source = cast(dict[str, object], source_raw)

    tables_raw = source.get("tables")
    if not isinstance(tables_raw, dict):
        return []
    tables = cast(dict[str, object], tables_raw)

    return sorted(tables)


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
    if replicate_structure:
        if not hasattr(args, "target_exists") or args.target_exists is None:
            print_info(
                "Auto-setting --target-exists false "
                + "(table will be created with replicate_structure)"
            )
            return "false"
        return str(args.target_exists)

    if not hasattr(args, "target_exists") or args.target_exists is None:
        has_map_column = bool(getattr(args, "map_column", None))
        has_target = bool(getattr(args, "target", None))
        if has_map_column or has_target:
            print_info(
                "Auto-setting --target-exists true "
                + "(mapping/target options imply existing table)"
            )
            return "true"

        print_error(
            "--add-sink-table requires --target-exists "
            + "(true or false)"
        )
        print_info(
            "Example (autocreate): --target-exists false\n"
            + "Example (map existing): --target-exists true "
            + "--target public.users"
        )
        return None

    return str(args.target_exists)


def _build_table_opts(
    args: argparse.Namespace,
    target_exists_value: str,
    replicate_structure: bool,
    map_column_pairs: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    """Build add_sink_table options from CLI args."""

    def _parse_template_selector(raw_value: str) -> tuple[str, str | None]:
        if ":" not in raw_value:
            return raw_value, None

        target_column, template_key = raw_value.split(":", 1)
        target_column_name = target_column.strip()
        template_name = template_key.strip()
        if target_column_name and template_name:
            return template_name, target_column_name
        return raw_value, None

    def _normalize_sink_schema_token(raw_value: str) -> str:
        if raw_value.startswith("custom:"):
            return raw_value.split(":", 1)[1]
        return raw_value

    table_opts: dict[str, object] = {
        "target_exists": target_exists_value == "true",
    }
    if replicate_structure:
        table_opts["replicate_structure"] = True
    if hasattr(args, "sink_schema") and args.sink_schema is not None:
        table_opts["sink_schema"] = _normalize_sink_schema_token(args.sink_schema)
    if args.target is not None:
        table_opts["target"] = args.target
    if map_column_pairs:
        table_opts["columns"] = dict(map_column_pairs)
    if args.target_schema:
        table_opts["target_schema"] = args.target_schema
    if args.include_sink_columns:
        table_opts["include_columns"] = args.include_sink_columns
    accepted_columns = getattr(args, "accept_column", None)
    if accepted_columns:
        if isinstance(accepted_columns, list | tuple):
            table_opts["accepted_columns"] = [
                str(value) for value in accepted_columns if str(value).strip()
            ]
        else:
            single_value = str(accepted_columns).strip()
            if single_value:
                table_opts["accepted_columns"] = [single_value]
    if hasattr(args, "add_column_template") and args.add_column_template:
        template_key, target_column_override = _parse_template_selector(
            str(args.add_column_template),
        )
        table_opts["column_template"] = template_key
        if hasattr(args, "column_name") and args.column_name:
            table_opts["column_template_name"] = args.column_name
        elif target_column_override:
            table_opts["column_template_name"] = target_column_override
        if hasattr(args, "value") and args.value:
            table_opts["column_template_value"] = args.value
    if hasattr(args, "add_transform") and args.add_transform:
        table_opts["add_transform"] = args.add_transform
    return table_opts


def _resolve_effective_sink_table_key(
    table_name: str,
    sink_schema: str | None,
) -> str:
    """Resolve final sink table key after optional --sink-schema override."""
    if sink_schema is None:
        return table_name

    normalized_sink_schema = (
        sink_schema.split(":", 1)[1]
        if sink_schema.startswith("custom:")
        else sink_schema
    )

    if "." not in table_name:
        return table_name

    _schema, table_part = table_name.split(".", 1)
    return f"{normalized_sink_schema}.{table_part}"


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
    """Parse map-column values into ``(source, target)`` tuples."""
    if map_column_raw is None:
        return []

    if not isinstance(map_column_raw, list | tuple):
        print_error(
            "Invalid --map-column value. Expected TARGET:SOURCE format"
        )
        return None

    def _parse_target_source_token(token: str) -> tuple[str, str] | None:
        if ":" not in token:
            return None
        target, source = token.split(":", 1)
        target_name = target.strip()
        source_name = source.strip()
        if not target_name or not source_name:
            return None
        return source_name, target_name

    pairs: list[tuple[str, str]] = []
    entries: list[object] = (
        cast(list[object], map_column_raw)
        if isinstance(map_column_raw, list)
        else list(cast(tuple[object, ...], map_column_raw))
    )

    for entry in entries:
        parsed_pair: tuple[str, str] | None = None

        if isinstance(entry, str):
            parsed_pair = _parse_target_source_token(entry)
        elif isinstance(entry, list | tuple):
            raw_values: list[object] = (
                cast(list[object], entry)
                if isinstance(entry, list)
                else list(cast(tuple[object, ...], entry))
            )

            entry_values = [str(value).strip() for value in raw_values]
            if len(entry_values) == 1:
                parsed_pair = _parse_target_source_token(entry_values[0])
            elif len(entry_values) == _MAP_COLUMN_PAIR_PARTS:
                source_name = entry_values[0]
                target_name = entry_values[1]
                if source_name and target_name:
                    parsed_pair = (source_name, target_name)

        if parsed_pair is None:
            print_error(
                "Invalid --map-column format. Use TARGET:SOURCE"
            )
            return None

        pairs.append(parsed_pair)

    return pairs


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
    if all_sinks_mode and not replicate_structure:
        print_error(
            "--all with --add-sink-table is only supported with --replicate-structure"
        )
        print_info(
            "Example: --all --add-sink-table --from public.customer_user "
            + "--replicate-structure --sink-schema activities"
        )
        return False

    if replicate_structure and (
        not hasattr(args, "sink_schema") or args.sink_schema is None
    ):
        print_error(
            "--replicate-structure requires --sink-schema "
            + "(specify target schema for custom table)"
        )
        print_info(
            "Example: --replicate-structure --sink-schema notification"
        )
        return False

    return True


def _resolve_add_table_names(
    args: argparse.Namespace,
    all_sinks_mode: bool,
) -> tuple[str | None, str] | None:
    """Resolve and validate table name and --from value."""
    table_name = args.add_sink_table
    if table_name is True:
        table_name = None
    from_table: str | None = getattr(args, "from_table", None)

    if all_sinks_mode and table_name is not None:
        print_error(
            "When using --all with --replicate-structure, omit the value for --add-sink-table"
        )
        print_info(
            "Use: --add-sink-table --from <schema.table> --replicate-structure --sink-schema <schema>"
        )
        return None

    if from_table is None:
        print_error(
            "--add-sink-table requires --from <source_schema.source_table> or --from all"
        )
        print_info(
            "Example: --add-sink-table public.customer_user "
            + "--from public.customer_user --target-exists false"
        )
        return None

    return table_name, from_table


def _resolve_sink_key(args: argparse.Namespace) -> str | None:
    """Resolve sink key from --sink or auto-default when only one sink."""
    if args.sink:
        return str(args.sink)

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


list_sink_keys_for_service = _list_sink_keys_for_service
list_source_tables_for_service = _list_source_tables_for_service
resolve_sink_keys_for_add_table = _resolve_sink_keys_for_add_table
resolve_target_exists_value = _resolve_target_exists_value
build_table_opts = _build_table_opts
resolve_effective_sink_table_key = _resolve_effective_sink_table_key
parse_map_column_pairs = _parse_map_column_pairs
validate_sink_add_table_mode = _validate_sink_add_table_mode
resolve_add_table_names = _resolve_add_table_names
resolve_sink_key = _resolve_sink_key
