"""Sink operations for CDC service configuration.

Handles adding, removing, and listing sink configurations in service YAML files.
Sinks define WHERE source tables are sent and HOW they are mapped.

Sink key format: {sink_group}.{target_service}
    - sink_group: references sink-groups.yaml (e.g., sink_asma)
    - target_service: target service/database in that sink group
"""

from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.validators.manage_service.validation import validate_service_sink_preflight

from .config import SERVICE_SCHEMAS_DIR, save_service_config
from .sink_add_table_compatibility import (
    validate_add_table_schema_compatibility as _validate_add_table_schema_compatibility,
)
from .sink_list_validation import list_sinks_impl, validate_sinks_impl
from .sink_mapping import (
    apply_column_mappings as _apply_column_mappings,
)
from .sink_mapping import (
    load_table_columns as _load_table_columns,
)
from .sink_mapping import (
    resolve_mapping_context as _resolve_mapping_context,
)
from .sink_mapping import (
    validate_column_mappings as _validate_column_mappings,
)
from .sink_mapping import (
    warn_unmapped_required as _warn_unmapped_required,
)
from .sink_operations_helpers import (
    _get_sink_tables,
    _get_sinks_dict,
    _resolve_sink_config,
    _validate_table_in_schemas,
)
from .sink_operations_table_config import TableConfigOptions, _build_table_config
from .sink_operations_type_compatibility import (
    _AUTO_ENGINE,
    _normalize_type_name,
    validate_pg_schema_name,
)
from .sink_service_ops import (
    add_sink_to_service as _add_sink_to_service,
)
from .sink_service_ops import (
    remove_sink_from_service as _remove_sink_from_service,
)
from .sink_table_mutations import (
    remove_sink_table as _remove_sink_table_impl,
)
from .sink_table_mutations import (
    save_custom_table_structure as _save_custom_table_structure_impl,
)
from .sink_table_mutations import (
    update_sink_table_schema as _update_sink_table_schema_impl,
)
from .sink_table_mutations import (
    validate_table_add as _validate_table_add_impl,
)
from .sink_type_compatibility import (
    check_type_compatibility_impl as _check_type_compatibility_impl,
)

__all__ = [
    "SERVICE_SCHEMAS_DIR",
    "TableConfigOptions",
    "_build_table_config",
    "_check_type_compatibility",
    "_load_table_columns",
    "_validate_table_add",
    "_validate_table_in_schemas",
    "add_sink_table",
    "add_sink_to_service",
    "check_type_compatibility",
    "get_project_root",
    "list_sinks",
    "map_sink_column",
    "map_sink_columns",
    "remove_sink_from_service",
    "remove_sink_table",
    "update_sink_table_schema",
    "validate_sinks",
]

# ---------------------------------------------------------------------------
# Public API — add / remove sink
# ---------------------------------------------------------------------------


def add_sink_to_service(service: str, sink_key: str) -> bool:
    """Add a sink destination to *service*.

    Args:
        service: Service name (e.g., 'directory').
        sink_key: Sink key 'sink_group.target_service' (e.g. 'sink_asma.chat').

    Returns:
        True on success, False otherwise.
    """
    return _add_sink_to_service(service, sink_key)


def remove_sink_from_service(service: str, sink_key: str) -> bool:
    """Remove a sink destination from *service*.

    Returns:
        True on success, False otherwise.
    """
    return _remove_sink_from_service(service, sink_key)


# ---------------------------------------------------------------------------
# Public API — add / remove sink table
# ---------------------------------------------------------------------------


def _validate_table_add(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object],
    skip_schema_validation: bool = False,
) -> tuple[dict[str, object] | None, str | None]:
    """Validate parameters for adding table to sink."""
    return _validate_table_add_impl(
        config,
        sink_key,
        table_key,
        table_opts,
        skip_schema_validation,
        _validate_table_in_schemas,
    )


def _save_custom_table_structure(
    sink_key: str,
    table_key: str,
    from_table: str,
    source_service: str,
) -> None:
    """Save minimal custom-table reference file."""
    _save_custom_table_structure_impl(
        sink_key,
        table_key,
        from_table,
        source_service,
        get_project_root,
        SERVICE_SCHEMAS_DIR,
        get_service_schema_read_dirs,
    )


def add_sink_table(
    service: str,
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object] | None = None,
) -> bool:
    """Add *table_key* to the sink identified by *sink_key*.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.chat').
        table_key: Source table in format 'schema.table'.
        table_opts: Optional table config dict. REQUIRED key:
            target_exists (bool). Other keys: target, target_schema,
            include_columns, columns, from, replicate_structure, sink_schema,
            column_template, column_template_name, column_template_value.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    opts = table_opts if table_opts is not None else {}

    sink_schema = opts.get("sink_schema")
    final_table_key = table_key

    if sink_schema is not None:
        schema_error = validate_pg_schema_name(str(sink_schema))
        if schema_error:
            print_error(schema_error)
            return False

        if "." in table_key:
            _schema, table_name = table_key.split(".", 1)
            final_table_key = f"{sink_schema}.{table_name}"
            print_info(
                f"Using sink schema '{sink_schema}' "
                + f"(table: {final_table_key})"
            )
        else:
            print_error(
                f"Invalid table key '{table_key}': expected 'schema.table' format"
            )
            return False

    tables, error = _validate_table_add(
        config,
        sink_key,
        final_table_key,
        opts,
        skip_schema_validation=sink_schema is not None,
    )

    if error or tables is None:
        if error:
            print_error(error)
        return False

    target_exists = bool(opts.get("target_exists", False))
    target = opts.get("target")
    from_table = opts.get("from")
    replicate_structure = bool(opts.get("replicate_structure", False))

    raw_target_schema = opts.get("target_schema")
    if raw_target_schema is not None:
        ts_error = validate_pg_schema_name(str(raw_target_schema))
        if ts_error:
            print_error(ts_error)
            return False

    config_opts = TableConfigOptions(
        target_exists=target_exists,
        target=str(target) if target else None,
        target_schema=(
            str(opts["target_schema"]) if "target_schema" in opts else None
        ),
        include_columns=(
            cast(list[str], opts["include_columns"])
            if "include_columns" in opts
            else None
        ),
        columns=(
            cast(dict[str, str], opts["columns"])
            if "columns" in opts
            else None
        ),
        from_table=str(from_table) if from_table is not None else None,
        replicate_structure=replicate_structure,
        sink_schema=str(sink_schema) if sink_schema is not None else None,
        column_template=(
            str(opts["column_template"])
            if "column_template" in opts
            else None
        ),
        column_template_name=(
            str(opts["column_template_name"])
            if "column_template_name" in opts
            else None
        ),
        column_template_value=(
            str(opts["column_template_value"])
            if "column_template_value" in opts
            else None
        ),
        add_transform=(
            str(opts["add_transform"])
            if "add_transform" in opts
            else None
        ),
        accepted_columns=(
            cast(list[str], opts["accepted_columns"])
            if "accepted_columns" in opts
            else None
        ),
    )

    compatibility_error = _validate_add_table_schema_compatibility(
        config,
        service,
        sink_key,
        table_key,
        final_table_key,
        config_opts,
        load_table_columns_fn=_load_table_columns,
    )
    if compatibility_error:
        print_error(compatibility_error)
        return False

    tables[final_table_key] = _build_table_config(config_opts)

    preflight_errors, preflight_warnings = validate_service_sink_preflight(service, config)
    if preflight_errors:
        for preflight_error in preflight_errors:
            print_error(f"  ✗ {preflight_error}")
        del tables[final_table_key]
        return False
    for warning in preflight_warnings:
        print_warning(f"  ⚠ {warning}")

    save_success = save_service_config(service, config)

    if save_success and sink_schema is not None and replicate_structure:
        source_table = str(from_table) if from_table else table_key
        _save_custom_table_structure(
            sink_key,
            final_table_key,
            source_table,
            service,
        )

    if save_success:
        label = f"→ '{target}'" if target_exists and target else "(clone)"
        print_success(f"Added table '{final_table_key}' {label} to sink '{sink_key}'")

    return save_success


def remove_sink_table(service: str, sink_key: str, table_key: str) -> bool:
    """Remove *table_key* from a service sink.

    Also removes the related custom-table YAML file from
    ``service-schemas/{target_service}/custom-tables/`` if it exists.

    Returns:
        True on success, False otherwise.
    """
    return _remove_sink_table_impl(service, sink_key, table_key)


def update_sink_table_schema(
    service: str,
    sink_key: str,
    table_key: str,
    new_schema: str,
) -> bool:
    """Update the schema portion of a sink table's name.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.chat').
        table_key: Current table key (e.g., 'public.customer_user').
        new_schema: New schema name (e.g., 'calendar').

    Returns:
        True on success, False otherwise.

    Example:
        update_sink_table_schema(
            'directory', 'sink_asma.calendar',
            'public.customer_user', 'calendar'
        )
        # Changes 'public.customer_user' to 'calendar.customer_user'
    """
    return _update_sink_table_schema_impl(
        service,
        sink_key,
        table_key,
        new_schema,
        validate_pg_schema_name=validate_pg_schema_name,
    )


# ---------------------------------------------------------------------------
# Public API — column mapping
# ---------------------------------------------------------------------------


def map_sink_column(
    service: str,
    sink_key: str,
    table_key: str,
    source_column: str,
    target_column: str,
) -> bool:
    """Add/update a column mapping.  Sets target_exists=true automatically.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return False

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return False

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        return False

    tbl_raw = tables[table_key]
    if not isinstance(tbl_raw, dict):
        tbl_raw = {}
        tables[table_key] = tbl_raw
    tbl_cfg = cast(dict[str, object], tbl_raw)
    tbl_cfg["target_exists"] = True

    cols_raw = tbl_cfg.get("columns")
    if not isinstance(cols_raw, dict):
        tbl_cfg["columns"] = {}
        cols_raw = tbl_cfg["columns"]
    cols = cast(dict[str, str], cols_raw)
    cols[source_column] = target_column

    if not save_service_config(service, config):
        return False

    print_success(
        f"Mapped column '{source_column}' → '{target_column}'"
        + f" in '{table_key}' of sink '{sink_key}'"
    )
    return True


# ---------------------------------------------------------------------------
# Public API — column mapping on existing sink table (with validation)
# ---------------------------------------------------------------------------


def _check_type_compatibility(
    source_type: str,
    sink_type: str,
    source_engine: str = _AUTO_ENGINE,
    sink_engine: str = _AUTO_ENGINE,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Check if source_type is compatible with sink_type."""
    return _check_type_compatibility_impl(
        source_type,
        sink_type,
        source_engine,
        sink_engine,
        source_table,
        source_column,
    )


def check_type_compatibility(
    source_type: str,
    sink_type: str,
    source_engine: str = _AUTO_ENGINE,
    sink_engine: str = _AUTO_ENGINE,
    source_table: str | None = None,
    source_column: str | None = None,
) -> bool:
    """Public compatibility helper for source/sink SQL column types."""
    return _check_type_compatibility(
        source_type,
        sink_type,
        source_engine,
        sink_engine,
        source_table,
        source_column,
    )


def map_sink_columns(
    service: str,
    sink_key: str,
    table_key: str,
    column_mappings: list[tuple[str, str]],
) -> bool:
    """Map multiple columns on an existing sink table with validation.

    Validates that:
    - Source columns exist in the source table schema
    - Sink columns exist in the sink table schema
    - Column types are compatible between source and sink
    - Warns about unmapped required (non-nullable) sink columns

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Sink table key (e.g., 'public.directory_user_name').
        column_mappings: List of (source_column, sink_column) tuples.

    Returns:
        True on success, False on validation error.
    """
    ctx = _resolve_mapping_context(service, sink_key, table_key)
    if ctx is None:
        return False

    # Validate each mapping
    errors = _validate_column_mappings(
        column_mappings,
        ctx.source_columns,
        ctx.sink_columns,
        ctx.source_table,
        table_key,
        check_type_compatibility=_check_type_compatibility,
        normalize_type_name=_normalize_type_name,
    )
    if errors:
        for err in errors:
            print_error(f"  ✗ {err}")
        return False

    # Apply mappings
    cols = _apply_column_mappings(ctx.tables, table_key, column_mappings)

    if not save_service_config(service, ctx.config):
        return False

    for src_col, tgt_col in column_mappings:
        print_success(f"Mapped column '{src_col}' → '{tgt_col}'")

    # Warn about unmapped required sink columns
    source_col_names = {
        col["name"] for col in ctx.source_columns if "name" in col
    }
    sink_col_names = {
        col["name"] for col in ctx.sink_columns if "name" in col
    }
    _warn_unmapped_required(
        ctx.sink_columns, cols, source_col_names, sink_col_names,
    )

    print_info("Run 'cdc generate' to update pipelines")
    return True


# ---------------------------------------------------------------------------
# Public API — list & validate
# ---------------------------------------------------------------------------


def list_sinks(service: str) -> bool:
    """List all sinks configured for *service*.

    Returns:
        True if sinks were found and displayed, False otherwise.
    """
    return list_sinks_impl(service)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_sinks(service: str) -> bool:
    """Validate sink configuration for *service*.

    Checks sink key format, sink group existence, source table presence,
    and required fields for target_exists=true tables.

    Returns:
        True if all validations pass, False otherwise.
    """
    return validate_sinks_impl(service)
