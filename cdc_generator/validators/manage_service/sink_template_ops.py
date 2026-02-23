"""Column template and transform operations for sink tables.

Manages column_templates and transforms in sink table configurations.
Transforms are referenced as Bloblang files under services/_bloblang/.

Template/transform definitions:
  - service-schemas/column-templates.yaml
    - services/_bloblang/**/*.blobl
"""

from typing import cast

from cdc_generator.core.column_template_operations import (
    add_column_template,
    add_transform,
    list_column_templates,
    list_transforms,
    remove_column_template,
    remove_transform,
)
from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
)
from cdc_generator.helpers.service_config import load_service_config

from .config import save_service_config

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_table_config(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, object], dict[str, object]] | None:
    """Load service config and resolve to the specific table config dict.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.notification').
        table_key: Table key (e.g., 'notification.customer_user').

    Returns:
        Tuple of (service_config, table_config) on success, None on error.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        print_error(f"No sinks configured for service '{service}'")
        return None

    sinks = cast(dict[str, object], sinks_raw)
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = cast(dict[str, object], sink_raw)
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        print_error(f"No tables in sink '{sink_key}'")
        return None

    tables = cast(dict[str, object], tables_raw)
    table_raw = tables.get(table_key)
    if not isinstance(table_raw, dict):
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        _show_available_tables(tables)
        return None

    return config, cast(dict[str, object], table_raw)


def _show_available_tables(tables: dict[str, object]) -> None:
    """Print available table keys as help.

    Args:
        tables: Tables dict from sink config.
    """
    available = sorted(str(k) for k in tables)
    if available:
        print_info(f"Available tables: {', '.join(available)}")


def _resolve_source_table_key(
    table_cfg: dict[str, object],
    table_key: str,
) -> str | None:
    """Derive the source table key for schema validation.

    Sink tables often live under a *different* schema name than their
    source (e.g. sink key ``directory_replica.customers`` references
    source ``public.customers`` via the ``from`` field).

    When the ``from`` field points to a different schema than
    *table_key*, we return the ``from`` value so schema validation
    loads the correct source schema file.

    Args:
        table_cfg: Resolved table configuration dict.
        table_key: The sink table key (e.g. "directory_replica.customers").

    Returns:
        The source table key to use for schema lookup, or ``None`` when
        *table_key* already matches the source schema.
    """
    from_value = table_cfg.get("from")
    if not isinstance(from_value, str) or not from_value:
        return None

    # Return from_value whenever it differs from table_key
    # (different schema OR different table name)
    if from_value != table_key:
        return from_value

    return None


def _get_sink_service(sink_key: str) -> str | None:
    """Extract target service from sink key ``sink_group.target_service``.

    Args:
        sink_key: E.g. ``"sink_asma.proxy"``.

    Returns:
        Target service name (e.g. ``"proxy"``), or None if invalid.
    """
    parts = sink_key.split(".", 1)
    if len(parts) < 2:  # noqa: PLR2004
        return None
    return parts[1]


def _validate_sink_column(
    sink_key: str,
    table_key: str,
    column_name: str,
) -> bool:
    """Validate that *column_name* exists on the sink table.

    Loads the sink-side schema and checks the column is present.
    Prints an error on failure, a warning when the schema is unavailable.

    Args:
        sink_key: Sink key (e.g. ``"sink_asma.proxy"``).
        table_key: Table key.
        column_name: Column name to validate.

    Returns:
        True if valid or schema unavailable (warn only), False on error.
    """
    from cdc_generator.validators.template_validator import (
        validate_sink_column_exists,
    )

    sink_service = _get_sink_service(sink_key)
    if sink_service is None:
        return True  # can't validate — don't block

    is_valid, errors, warnings = validate_sink_column_exists(
        sink_service, table_key, column_name,
    )

    for warning in warnings:
        print_info(f"  ⚠ {warning}")
    for error in errors:
        print_error(f"  {error}")

    if not is_valid:
        print_error(
            "Sink column validation failed. "
            + "Use --skip-validation to bypass."
        )

    return is_valid


def _validate_column_mapping(
    service: str,
    sink_key: str,
    table_key: str,
    table_cfg: dict[str, object],
) -> None:
    """Validate source↔sink column mappings for target_exists=true tables.

    Loads both source and sink schemas, then checks each mapping entry
    for missing columns and type mismatches.  Prints warnings — does
    **not** block the operation.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        table_cfg: Table config dict.
    """
    from cdc_generator.validators.template_validator import (
        get_sink_table_schema,
        validate_column_mapping_types,
    )

    columns_raw = table_cfg.get("columns")
    if not isinstance(columns_raw, dict):
        return  # no explicit mapping → nothing to check

    columns_mapping = cast(dict[str, str], columns_raw)

    sink_service = _get_sink_service(sink_key)
    if sink_service is None:
        return

    # Load source schema
    from cdc_generator.validators.template_validator import (
        get_source_table_schema,
    )

    source_table_key = _resolve_source_table_key(table_cfg, table_key)
    source_schema = get_source_table_schema(
        service, table_key, source_table_key=source_table_key,
    )

    # Load sink schema
    sink_schema = get_sink_table_schema(sink_service, table_key)

    if source_schema is None or sink_schema is None:
        return  # can't validate — silently skip

    type_warnings = validate_column_mapping_types(
        source_schema, sink_schema, columns_mapping, table_key,
    )

    for warning in type_warnings:
        print_info(f"  ⚠ {warning}")


# ---------------------------------------------------------------------------
# Public API — extra columns
# ---------------------------------------------------------------------------


def add_column_template_to_table(
    service: str,
    sink_key: str,
    table_key: str,
    template_key: str,
    name_override: str | None = None,
    value_override: str | None = None,
    skip_validation: bool = False,
) -> bool:
    """Add a column template reference to a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        template_key: Column template key from column-templates.yaml.
        name_override: Optional column name override.
        value_override: Optional value override (Bloblang expression or
            source-group reference like ``{asma.sources.*.key}``).
        skip_validation: Skip database schema validation (for migrations).

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved

    # When target_exists=true, the table already exists in the sink database.
    # Adding a column template without --column-name would try to create a
    # new column (e.g. _tenant_id) that doesn't exist on the target table.
    # Require an explicit name override so the template maps to an existing column.
    target_exists = table_cfg.get("target_exists", False)
    if target_exists and name_override is None:
        from cdc_generator.core.column_templates import get_template

        tpl = get_template(template_key)
        default_name = tpl.name if tpl else f"_{template_key}"
        print_error(
            f"Table '{table_key}' has target_exists=true —"
            + f" cannot add column '{default_name}' (it may not exist on the target).\n"
            + "  Use --column-name to map to an existing column, e.g.:\n"
            + f"  --add-column-template {template_key} --column-name <existing_column>"
        )
        return False

    # Validate --column-name exists on the sink table (target_exists=true)
    if target_exists and name_override and not skip_validation and not _validate_sink_column(
        sink_key, table_key, name_override,
    ):
        return False

    # Validate source↔sink column mapping for target_exists=true tables
    if target_exists and not skip_validation:
        _validate_column_mapping(
            service, sink_key, table_key, table_cfg,
        )

    # Validate template against database schema before adding
    if not skip_validation:
        from cdc_generator.validators.template_validator import (
            validate_templates_for_table,
        )

        # Resolve source table key from 'from' field when the sink table
        # key differs from the actual source schema location.
        # e.g. table_key="directory_replica.customers" with from="public.customers"
        source_table_key = _resolve_source_table_key(table_cfg, table_key)

        print_info(f"\nValidating template '{template_key}' for table '{table_key}'...")
        if not validate_templates_for_table(
            service, table_key, [template_key],
            source_table_key=source_table_key,
            value_override=value_override,
        ):
            print_error("Template validation failed. Use --skip-validation to bypass.")
            return False

    if not add_column_template(
        table_cfg, template_key, name_override,
        value_override=value_override, table_key=table_key,
    ):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def remove_column_template_from_table(
    service: str,
    sink_key: str,
    table_key: str,
    template_key: str,
) -> bool:
    """Remove a column template reference from a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        template_key: Column template key to remove.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not remove_column_template(table_cfg, template_key):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def list_column_templates_on_table(
    service: str,
    sink_key: str,
    table_key: str,
) -> bool:
    """List all column templates on a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.

    Returns:
        True if any columns found, False otherwise.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    _config, table_cfg = resolved
    columns = list_column_templates(table_cfg)

    print_header(f"Column templates on '{table_key}' in sink '{sink_key}'")
    if not columns:
        print_info("No column templates configured")
        return False

    for tpl_key in columns:
        from cdc_generator.core.column_templates import get_template

        template = get_template(tpl_key)
        if template:
            print(
                f"  {Colors.CYAN}{tpl_key}{Colors.RESET}"
                + f" → {Colors.OKGREEN}{template.name}{Colors.RESET}"

                + f" ({template.column_type})"
                + f"  {Colors.DIM}{template.description}{Colors.RESET}"
            )
        else:
            print(f"  {Colors.YELLOW}{tpl_key}{Colors.RESET} (unknown template)")

    return True


# ---------------------------------------------------------------------------
# Public API — transforms
# ---------------------------------------------------------------------------


def add_transform_to_table(
    service: str,
    sink_key: str,
    table_key: str,
    bloblang_ref: str,
    skip_validation: bool = False,
) -> bool:
    """Add a transform Bloblang reference to a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        bloblang_ref: Transform Bloblang file ref under services/_bloblang/.
        skip_validation: Skip database schema validation (for migrations).

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved

    # Validate transform against database schema before adding
    if not skip_validation:
        from cdc_generator.validators.template_validator import (
            validate_transforms_for_table,
        )

        # Resolve source table key from 'from' field when the sink table
        # key differs from the actual source schema location.
        source_table_key = _resolve_source_table_key(table_cfg, table_key)

        print_info(
            f"\nValidating transform '{bloblang_ref}' for table '{table_key}'..."
        )

        existing_refs = list_transforms(table_cfg)
        refs_to_validate = list(existing_refs)
        if bloblang_ref not in refs_to_validate:
            refs_to_validate.append(bloblang_ref)

        if not validate_transforms_for_table(
            service, table_key, refs_to_validate,
            source_table_key=source_table_key,
        ):
            print_error("Transform validation failed. Use --skip-validation to bypass.")
            return False

    if not add_transform(table_cfg, bloblang_ref):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def remove_transform_from_table(
    service: str,
    sink_key: str,
    table_key: str,
    bloblang_ref: str,
) -> bool:
    """Remove a transform Bloblang reference from a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        bloblang_ref: Transform Bloblang ref to remove.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not remove_transform(table_cfg, bloblang_ref):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def list_transforms_on_table(
    service: str,
    sink_key: str,
    table_key: str,
) -> bool:
    """List all transforms on a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.

    Returns:
        True if any transforms found, False otherwise.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    _config, table_cfg = resolved
    refs = list_transforms(table_cfg)

    print_header(f"Transforms on '{table_key}' in sink '{sink_key}'")
    if not refs:
        print_info("No transforms configured")
        return False

    for bloblang_ref in refs:
        print(f"  {Colors.CYAN}{bloblang_ref}{Colors.RESET}")

    return True
