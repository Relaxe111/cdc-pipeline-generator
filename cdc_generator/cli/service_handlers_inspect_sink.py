"""Sink database inspection handlers for manage-service."""

import argparse

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.validators.manage_service.schema_saver import (
    save_detailed_schema,
)
from cdc_generator.validators.manage_service.sink_inspector import (
    inspect_sink_schema,
)


def _validate_inspect_sink_args(
    args: argparse.Namespace,
) -> tuple[str, list[str]] | int:
    """Validate --inspect-sink arguments.

    Returns:
        (sink_key, available_sinks) on success, or int exit code.
    """
    from cdc_generator.validators.manage_service.db_inspector_common import (
        get_available_sinks,
    )

    sink_key: str = args.inspect_sink
    available = get_available_sinks(args.service)

    if sink_key not in available:
        print_error(
            f"Sink '{sink_key}' not found in service '{args.service}'"
        )
        if available:
            print_info(
                "Available sinks: "
                + ", ".join(available)
            )
        return 1

    if not args.all and not args.schema:
        print_error(
            "Error: --inspect-sink requires either "
            + "--all (for all schemas) or --schema <name>"
        )
        return 1

    return sink_key, available


def handle_inspect_sink(args: argparse.Namespace) -> int:
    """Inspect sink database schema and list available tables.

    Works like --inspect but targets a sink database instead of
    the source. The sink key identifies which sink group and
    target service to connect to.
    """
    validation = _validate_inspect_sink_args(args)
    if isinstance(validation, int):
        return validation

    sink_key, _available = validation

    result = inspect_sink_schema(
        args.service, sink_key, args.env,
    )
    if result is None:
        return 1

    tables, db_type, allowed_schemas = result
    schema: str | None = args.schema

    if schema and allowed_schemas and schema not in allowed_schemas:
        print_error(
            f"Schema '{schema}' not allowed for sink '{sink_key}'"
        )
        print_error(
            "Allowed schemas: "
            + ", ".join(allowed_schemas)
        )
        return 1

    schema_msg = (
        f"schemas: {', '.join(allowed_schemas)}"
        if args.all
        else f"schema: {schema}"
    )
    print_header(
        f"Inspecting sink {db_type.upper()} for "
        + f"{args.service} → {sink_key} ({schema_msg})"
    )

    return _run_sink_inspection(
        args, tables, db_type, allowed_schemas, schema,
    )


def _run_sink_inspection(
    args: argparse.Namespace,
    tables: list[dict[str, object]],
    db_type: str,
    allowed_schemas: list[str],
    schema: str | None,
) -> int:
    """Execute sink inspection filtering and output."""
    # Filter tables by schema
    if args.all and allowed_schemas:
        tables = [
            t for t in tables
            if t["TABLE_SCHEMA"] in allowed_schemas
        ]
    elif schema:
        tables = [
            t for t in tables
            if t["TABLE_SCHEMA"] == schema
        ]

    if not tables:
        if args.all:
            schemas_str = ", ".join(allowed_schemas)
            print_warning(
                "No tables found in allowed schemas: "
                + schemas_str
            )
        else:
            print_warning(
                f"No tables found in schema '{schema}'"
            )
        return 1

    if args.save:
        ok = save_detailed_schema(
            args.service,
            args.env,
            schema or "",
            tables,
            db_type,
        )
        return 0 if ok else 1

    print_success(f"Found {len(tables)} tables:\n")
    current_schema: str | None = None
    for table in tables:
        tbl_schema = str(table["TABLE_SCHEMA"])
        if tbl_schema != current_schema:
            print(
                f"\n{Colors.CYAN}[{tbl_schema}]{Colors.RESET}"
            )
            current_schema = tbl_schema
        col_count = table["COLUMN_COUNT"]
        print(
            f"  {table['TABLE_NAME']} ({col_count} columns)"
        )

    return 0
