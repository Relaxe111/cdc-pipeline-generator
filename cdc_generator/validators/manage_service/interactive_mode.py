"""Interactive workflow for legacy table mapping functionality."""

import argparse
from dataclasses import dataclass

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs

from .config import (
    detect_service_mode,
    get_available_services,
    get_table_schema_definition,
    load_service_schema_tables,
)
from .interactive import (
    prompt_mappings,
    prompt_multiselect,
    prompt_select,
    validate_table_compatibility,
)


def _list_service_schemas(service: str) -> list[str]:
    """List schema directory names for a service across supported roots."""
    schemas: set[str] = set()
    for service_dir in get_service_schema_read_dirs(service):
        if not service_dir.exists() or not service_dir.is_dir():
            continue
        schemas.update(entry.name for entry in service_dir.iterdir() if entry.is_dir())
    return sorted(schemas)


@dataclass
class _InteractiveSelection:
    source_service: str
    source_schema: str
    source_table: str
    source_table_def: dict[str, object]
    source_columns: list[str]
    sink_service: str | None


def _prepare_interactive_selection(
    args: argparse.Namespace,
) -> _InteractiveSelection | None:
    available_services = get_available_services()
    if not available_services:
        print_error("No services found in services/")
        return None

    source_service = args.source or prompt_select(
        "Select source service:",
        available_services,
    )
    if not source_service:
        return None

    source_mode = detect_service_mode(source_service)
    print_info(f"Source service '{source_service}' mode: {source_mode}")

    sink_service = args.sink or prompt_select(
        "Select sink service (optional):",
        available_services,
        allow_empty=True,
    )
    if sink_service:
        sink_mode = detect_service_mode(sink_service)
        print_info(f"Sink service '{sink_service}' mode: {sink_mode}")

    source_schemas = _list_service_schemas(source_service)
    if not source_schemas:
        print_error(f"No schemas found for service '{source_service}'")
        return None

    source_schema = args.source_schema or prompt_select(
        "Select source schema:",
        source_schemas,
    )
    if not source_schema:
        return None

    source_tables = load_service_schema_tables(source_service, source_schema)
    if not source_tables:
        print_error(
            f"No tables found for service '{source_service}' in schema '{source_schema}'"
        )
        return None

    source_table = args.source_table or prompt_select(
        "Select source table:",
        source_tables,
    )
    if not source_table:
        return None

    source_table_def = get_table_schema_definition(
        source_service,
        source_schema,
        source_table,
    )
    if not source_table_def:
        print_error(f"Table definition not found: {source_table}")
        return None

    source_columns = [
        col["name"]
        for col in source_table_def.get("columns", [])
        if isinstance(col, dict) and "name" in col
    ]
    print_success(f"Source table '{source_table}' has {len(source_columns)} columns")

    return _InteractiveSelection(
        source_service=source_service,
        source_schema=source_schema,
        source_table=source_table,
        source_table_def=source_table_def,
        source_columns=source_columns,
        sink_service=sink_service,
    )


def _handle_sink_configuration(
    args: argparse.Namespace,
    selection: _InteractiveSelection,
    ignore_columns: list[str],
) -> bool:
    sink_service = selection.sink_service
    if not sink_service:
        return True

    sink_schemas = _list_service_schemas(sink_service)
    if not sink_schemas:
        print_error(f"No schemas found for service '{sink_service}'")
        return False

    sink_schema = args.sink_schema or prompt_select("Select sink schema:", sink_schemas)
    if not sink_schema:
        return False

    sink_tables = load_service_schema_tables(sink_service, sink_schema)
    if not sink_tables:
        print_error(
            f"No tables found for service '{sink_service}' in schema '{sink_schema}'"
        )
        return False

    sink_table = args.sink_table or prompt_select("Select sink table:", sink_tables)
    if not sink_table:
        return False

    sink_table_def = get_table_schema_definition(sink_service, sink_schema, sink_table)
    if not sink_table_def:
        print_error(f"Table definition not found: {sink_table}")
        return False

    sink_columns = [
        col["name"]
        for col in sink_table_def.get("columns", [])
        if isinstance(col, dict) and "name" in col
    ]
    print_success(f"Sink table '{sink_table}' has {len(sink_columns)} columns")

    tracked_columns = [
        col
        for col in selection.source_columns
        if col not in ignore_columns
    ]
    mappings = prompt_mappings(tracked_columns, sink_columns)

    if not validate_table_compatibility(
        selection.source_table_def,
        sink_table_def,
        mappings,
    ):
        print_error("Table compatibility validation failed")
        return False

    print_header("Configuration Summary")
    print(
        f"{Colors.CYAN}Source:{Colors.RESET} "
        + f"{selection.source_service}.{selection.source_schema}.{selection.source_table}"
    )
    print(
        f"{Colors.CYAN}Sink:{Colors.RESET} "
        + f"{sink_service}.{sink_schema}.{sink_table}"
    )
    print(
        f"{Colors.CYAN}Ignored columns:{Colors.RESET} "
        + f"{', '.join(ignore_columns) if ignore_columns else 'None'}"
    )
    print(f"{Colors.CYAN}Mappings:{Colors.RESET}")
    for src, snk in mappings.items():
        print(f"  {src} → {snk}")

    print_warning("\n⚠️  Configuration saving not yet implemented")
    return True


def run_interactive_mode(args: argparse.Namespace) -> int:
    """Run interactive table mapping workflow.

    This is legacy functionality for creating table mappings between source and sink.
    Most users should use the command-line flags instead.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    print_header("CDC Table Mapping Manager")

    selection = _prepare_interactive_selection(args)
    if selection is None:
        return 1

    # Step 5: Ignore/track columns
    print_info("\nConfigure column tracking:")
    ignore_columns = prompt_multiselect(
        "Select columns to IGNORE:",
        selection.source_columns,
    )

    if not _handle_sink_configuration(args, selection, ignore_columns):
        return 1

    if not selection.sink_service:
        print_header("Configuration Summary")
        print(
            f"{Colors.CYAN}Source:{Colors.RESET} "
            + f"{selection.source_service}.{selection.source_schema}.{selection.source_table}"
        )
        print(
            f"{Colors.CYAN}Ignored columns:{Colors.RESET} "
            + f"{', '.join(ignore_columns) if ignore_columns else 'None'}"
        )

        # TODO: Save configuration to service YAML
        print_warning("\n⚠️  Configuration saving not yet implemented")

    print_success("\n✓ Table configuration completed!")
    return 0
