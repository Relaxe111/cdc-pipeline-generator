"""Custom sink table management for manage-services config CLI.

Handles creation and modification of custom tables — tables that don't exist
in any source and will be auto-created in the sink database.

Custom tables have:
    custom: true   — marks as manually created (not inferred from schemas)
    managed: true  — can be modified via CLI (add/remove columns)
"""

from cdc_generator.cli.service_handlers_sink_custom_column_mutations import (
    ColumnMutationDeps,
)
from cdc_generator.cli.service_handlers_sink_custom_column_mutations import (
    add_column_to_custom_table as add_column_to_custom_table_impl,
)
from cdc_generator.cli.service_handlers_sink_custom_column_mutations import (
    remove_column_from_custom_table as remove_column_from_custom_table_impl,
)
from cdc_generator.cli.service_handlers_sink_custom_config_nav import (
    extract_target_service,
    get_sink_tables,
    get_sinks_dict,
    resolve_sink_config,
)
from cdc_generator.cli.service_handlers_sink_custom_output import (
    build_column_not_found_messages,
    build_created_custom_table_messages,
    build_custom_table_disabled_messages,
    build_custom_table_unmanaged_messages,
    build_no_column_definitions_messages,
    build_source_schema_missing_messages,
    build_source_table_not_found_messages,
    build_table_exists_messages,
)
from cdc_generator.cli.service_handlers_sink_custom_parsing import (
    build_custom_table_config,
    build_schema_yaml,
    parse_column_spec,
    parse_multiple_columns,
    split_table_key,
)
from cdc_generator.cli.service_handlers_sink_custom_schema_files import (
    load_columns_map_from_schema,
    update_schema_file_add_column,
    update_schema_file_remove_column,
)
from cdc_generator.cli.service_handlers_sink_custom_source_loader import (
    SourceLoaderDeps,
    load_columns_from_source_table,
)
from cdc_generator.cli.service_handlers_sink_custom_table_access import (
    CustomTableAccessDeps,
    load_custom_table,
)
from cdc_generator.cli.service_handlers_sink_custom_table_access import (
    list_custom_table_columns as list_custom_table_columns_impl,
)
from cdc_generator.cli.service_handlers_sink_custom_table_access import (
    list_custom_tables_for_sink as list_custom_tables_for_sink_impl,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import (
    load_service_config,
)
from cdc_generator.helpers.service_schema_paths import (
    get_service_schema_read_dirs,
)
from cdc_generator.helpers.yaml_loader import (
    load_yaml_file,
)
from cdc_generator.validators.manage_service.config import (
    SERVICE_SCHEMAS_DIR,
    save_service_config,
)
from cdc_generator.validators.manage_service.sink_operations import (
    validate_pg_schema_name,
)

_build_custom_table_config = build_custom_table_config
_build_schema_yaml = build_schema_yaml
_extract_target_service_impl = extract_target_service
_get_sink_tables_impl = get_sink_tables
_get_sinks_dict_impl = get_sinks_dict
_load_columns_map_from_schema_impl = load_columns_map_from_schema
_load_columns_from_source_table_impl = load_columns_from_source_table
_load_custom_table_impl = load_custom_table
_parse_column_spec = parse_column_spec
_parse_multiple_columns = parse_multiple_columns
_resolve_sink_config_impl = resolve_sink_config
_split_table_key = split_table_key
_update_schema_file_add_column_impl = update_schema_file_add_column
_update_schema_file_remove_column_impl = update_schema_file_remove_column

_SOURCE_LOADER_DEPS = SourceLoaderDeps(
    load_service_config_fn=load_service_config,
    get_service_schema_read_dirs_fn=get_service_schema_read_dirs,
    load_yaml_file_fn=load_yaml_file,
    build_source_table_not_found_messages_fn=build_source_table_not_found_messages,
    build_source_schema_missing_messages_fn=build_source_schema_missing_messages,
    print_error_fn=print_error,
    print_info_fn=print_info,
)

try:
    from cdc_generator.helpers.yaml_loader import yaml
except ImportError:
    yaml = None


# ---------------------------------------------------------------------------
# Column parsing
# ---------------------------------------------------------------------------

# Expected parts when splitting schema.table
_TABLE_KEY_PARTS = 2


# ---------------------------------------------------------------------------
# Schema file generation
# ---------------------------------------------------------------------------




def _save_schema_file(
    target_service: str,
    table_key: str,
    columns: list[dict[str, object]],
) -> bool:
    """Save column definitions to service-schemas/{target}/{schema}/{Table}.yaml.

    Creates directories if they don't exist.

    Returns:
        True on success.
    """
    if yaml is None:
        print_error("PyYAML is required for schema file generation")
        return False

    schema_name, table_name = _split_table_key(table_key)

    schema_dir = SERVICE_SCHEMAS_DIR / target_service / schema_name
    schema_dir.mkdir(parents=True, exist_ok=True)

    schema_data = _build_schema_yaml(table_key, columns, target_service)

    schema_file = schema_dir / f"{table_name}.yaml"
    with schema_file.open("w", encoding="utf-8") as f:
        yaml.dump(schema_data, f)

    return True


# ---------------------------------------------------------------------------
# Service YAML operations
# ---------------------------------------------------------------------------


def _get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return the sinks section, creating it if absent."""
    return _get_sinks_dict_impl(config)


def _get_sink_tables(
    sink_cfg: dict[str, object],
) -> dict[str, object]:
    """Return tables dict inside a sink config, creating if absent."""
    return _get_sink_tables_impl(sink_cfg)


def _resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None if not found."""
    sink_cfg = _resolve_sink_config_impl(sinks, sink_key)
    if sink_cfg is None:
        print_error(f"Sink '{sink_key}' not found or invalid")
        return None
    return sink_cfg


def _extract_target_service(sink_key: str) -> str | None:
    """Extract target_service from 'sink_group.target_service'."""
    target_service = _extract_target_service_impl(sink_key, _TABLE_KEY_PARTS)
    if target_service is None:
        print_error(
            f"Invalid sink key '{sink_key}'. "
            + "Expected: sink_group.target_service"
        )
        return None
    return target_service


# ---------------------------------------------------------------------------
# Public: add custom sink table
# ---------------------------------------------------------------------------


def _validate_custom_table_inputs(
    sink_key: str,
    table_key: str,
    column_specs: list[str],
) -> tuple[str, str, str, list[dict[str, object]]] | None:
    """Validate inputs for adding a custom sink table.

    Returns:
        (target_service, schema_name, table_name, columns) or None on error.
    """
    target_service = _extract_target_service(sink_key)
    if not target_service:
        return None

    parts = table_key.split(".", 1)
    if len(parts) != _TABLE_KEY_PARTS:
        print_error(
            f"Invalid table format '{table_key}'. "
            + "Expected: schema.table (e.g., public.audit_log)"
        )
        return None

    schema_name, table_name = parts

    # Validate schema name is a valid PostgreSQL identifier
    schema_error = validate_pg_schema_name(schema_name)
    if schema_error:
        print_error(schema_error)
        return None

    schema_file = (
        SERVICE_SCHEMAS_DIR / target_service / schema_name
        / f"{table_name}.yaml"
    )
    if schema_file.exists():
        error_message, info_message = build_table_exists_messages(
            table_key,
            target_service,
            schema_name,
        )
        print_error(error_message)
        print_info(info_message)
        return None

    columns = _parse_multiple_columns(column_specs)
    if columns is None:
        return None

    return target_service, schema_name, table_name, columns


def add_custom_sink_table(
    service: str,
    sink_key: str,
    table_key: str,
    column_specs: list[str],
    from_custom_table: str | None = None,
) -> bool:
    """Add a custom table to a sink with column definitions.

    Columns can come from:
    1. Inline ``--column`` specs (column_specs)
    2. A source table in this service via ``--from`` (from_custom_table)

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Table in format 'schema.table'.
        column_specs: Column specs (e.g., ['id:uuid:pk', 'name:text:not_null']).
        from_custom_table: Optional source table ref (schema.table) to
            load columns from source service schemas.

    Returns:
        True on success.
    """
    if from_custom_table is None:
        print_error(
            "--add-custom-sink-table requires --from <schema.table>"
        )
        return False

    # If --from references a source table and no inline columns are supplied,
    # load columns from source schemas.
    if not column_specs:
        columns = _load_columns_from_source_table(
            service,
            from_custom_table,
        )
        if columns is None:
            return False
        return _add_custom_sink_table_with_columns(
            service,
            sink_key,
            table_key,
            columns,
            from_table=from_custom_table,
        )

    validated = _validate_custom_table_inputs(sink_key, table_key, column_specs)
    if validated is None:
        return False

    _target_service, _schema_name, _table_name, columns = validated
    return _add_custom_sink_table_with_columns(
        service,
        sink_key,
        table_key,
        columns,
        from_table=from_custom_table,
    )


def _load_columns_from_source_table(
    service: str,
    table_ref: str,
) -> list[dict[str, object]] | None:
    """Load column definitions from a source table schema.

    Validates that ``table_ref`` exists in ``source.tables`` of the service,
    then reads from preferred/legacy schema paths for that same service.

    Returns:
        List of column defs, or None on error.
    """
    return _load_columns_from_source_table_impl(service, table_ref, _SOURCE_LOADER_DEPS)


def _add_custom_sink_table_with_columns(
    service: str,
    sink_key: str,
    table_key: str,
    columns: list[dict[str, object]],
    from_table: str | None = None,
) -> bool:
    """Core logic for adding a custom sink table with parsed columns.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table in format 'schema.table'.
        columns: Parsed column definitions.

    Returns:
        True on success.
    """
    target_service = _extract_target_service(sink_key)
    if not target_service:
        return False

    schema_name, table_name = _split_table_key(table_key)

    # Load and update service config
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        available = [str(k) for k in sinks]
        if available:
            print_info("Available sinks: " + ", ".join(available))
        return False

    tables = _get_sink_tables(sink_cfg)
    if table_key in tables:
        print_warning(
            f"Table '{table_key}' already in sink '{sink_key}'"
        )
        return False

    # Build and save
    tables[table_key] = _build_custom_table_config(from_table=from_table)

    if not save_service_config(service, config):
        return False

    # Generate schema file
    if not _save_schema_file(target_service, table_key, columns):
        print_warning(
            "Service config saved but schema file generation failed"
        )

    col_names = [str(c["name"]) for c in columns]
    success_message, columns_message, schema_saved_message = (
        build_created_custom_table_messages(
            table_key,
            sink_key,
            col_names,
            target_service,
            schema_name,
            table_name,
        )
    )
    print_success(success_message)
    print_info(columns_message)
    print_info(schema_saved_message)
    return True


# ---------------------------------------------------------------------------
# Public: modify custom sink table (add/remove columns)
# ---------------------------------------------------------------------------


def _load_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], str] | None:
    """Load and validate a custom+managed table for modification.

    Returns:
        (config, table_config, columns_dict, target_service) or None on error.
    """
    return _load_custom_table_impl(
        service,
        sink_key,
        table_key,
        _build_table_access_deps(),
    )


def _build_table_access_deps() -> CustomTableAccessDeps:
    """Create table-access dependencies lazily to avoid init-order issues."""
    return CustomTableAccessDeps(
        load_service_config_fn=load_service_config,
        extract_target_service_fn=_extract_target_service,
        load_columns_map_from_schema_fn=lambda target_service, table_key: (
            _load_columns_map_from_schema_impl(
                SERVICE_SCHEMAS_DIR,
                yaml,
                target_service,
                table_key,
            )
        ),
        build_custom_table_disabled_messages_fn=build_custom_table_disabled_messages,
        build_custom_table_unmanaged_messages_fn=build_custom_table_unmanaged_messages,
        build_no_column_definitions_messages_fn=build_no_column_definitions_messages,
        print_error_fn=print_error,
        print_info_fn=print_info,
    )


def _build_column_mutation_deps() -> ColumnMutationDeps:
    """Create column-mutation dependencies lazily."""
    return ColumnMutationDeps(
        load_custom_table_fn=_load_custom_table,
        parse_column_spec_fn=_parse_column_spec,
        save_service_config_fn=save_service_config,
        update_schema_file_add_column_fn=_update_schema_file_add_column,
        update_schema_file_remove_column_fn=_update_schema_file_remove_column,
        build_column_not_found_messages_fn=build_column_not_found_messages,
        print_error_fn=print_error,
        print_info_fn=print_info,
        print_success_fn=print_success,
        print_warning_fn=print_warning,
    )


def add_column_to_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
    column_spec: str,
) -> bool:
    """Add a column to a custom table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table in format 'schema.table'.
        column_spec: Column spec (e.g., 'updated_at:timestamptz:default_now').

    Returns:
        True on success.
    """
    return add_column_to_custom_table_impl(
        service,
        sink_key,
        table_key,
        column_spec,
        _build_column_mutation_deps(),
    )


def remove_column_from_custom_table(
    service: str,
    sink_key: str,
    table_key: str,
    column_name: str,
) -> bool:
    """Remove a column from a custom table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table in format 'schema.table'.
        column_name: Column name to remove.

    Returns:
        True on success.
    """
    return remove_column_from_custom_table_impl(
        service,
        sink_key,
        table_key,
        column_name,
        _build_column_mutation_deps(),
    )


# ---------------------------------------------------------------------------
# Schema file update helpers
# ---------------------------------------------------------------------------


def _update_schema_file_add_column(
    target_service: str,
    table_key: str,
    col: dict[str, object],
) -> None:
    """Add a column to the service-schemas YAML file."""
    _update_schema_file_add_column_impl(
        SERVICE_SCHEMAS_DIR,
        yaml,
        target_service,
        table_key,
        col,
    )


def _update_schema_file_remove_column(
    target_service: str,
    table_key: str,
    column_name: str,
) -> None:
    """Remove a column from the service-schemas YAML file."""
    _update_schema_file_remove_column_impl(
        SERVICE_SCHEMAS_DIR,
        yaml,
        target_service,
        table_key,
        column_name,
    )


# ---------------------------------------------------------------------------
# Public: list custom table columns
# ---------------------------------------------------------------------------


def list_custom_table_columns(
    service: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """Return column names for a custom table in a sink.

    Used for autocompletion of --remove-column.

    Returns:
        List of column names, or empty list.
    """
    return list_custom_table_columns_impl(
        service,
        sink_key,
        table_key,
        _build_table_access_deps(),
    )


def list_custom_tables_for_sink(
    service: str,
    sink_key: str,
) -> list[str]:
    """Return table keys where custom=true for a given sink.

    Used for autocompletion of --modify-custom-table.

    Returns:
        List of table keys (e.g., ['public.audit_log']).
    """
    return list_custom_tables_for_sink_impl(
        service,
        sink_key,
        load_service_config_fn=load_service_config,
    )
