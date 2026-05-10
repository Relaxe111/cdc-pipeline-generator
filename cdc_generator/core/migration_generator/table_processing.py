"""Table processing helpers for migration generation."""

from __future__ import annotations

from typing import Any, cast

from cdc_generator.core.migration_generator.data_structures import (
    GenerationResult,
    RuntimeMode,
    TableMigration,
)
from cdc_generator.helpers.fdw_identifiers import (
    build_base_foreign_table_name,
    build_foreign_table_name,
    build_min_lsn_table_name,
)
from cdc_generator.helpers.type_mapper import TypeMapper

from .columns import (
    add_cdc_metadata_columns,
    add_native_cdc_metadata_columns,
    add_column_template_columns,
    build_columns_from_table_def,
)
from .service_parsing import get_source_table_config

_MAX_WARNING_TABLES = 10


def process_table(
    sink_key: str,
    sink_cfg: dict[str, Any],
    service_config: dict[str, object],
    table_defs: dict[str, dict[str, Any]],
    result: GenerationResult,
    type_mapper: TypeMapper | None = None,
    *,
    runtime_mode: RuntimeMode = "brokered",
    duplicate_source_table_name_count: int = 1,
) -> TableMigration | None:
    """Process a single sink table into a TableMigration."""
    from_ref = sink_cfg.get("from", "")
    if not isinstance(from_ref, str) or not from_ref:
        result.warnings.append(f"Table {sink_key}: missing 'from' reference, skipped")
        return None

    from_parts = from_ref.split(".", 1)
    source_schema = from_parts[0] if len(from_parts) > 1 else "dbo"
    source_table = from_parts[-1]

    key_parts = sink_key.split(".", 1)
    target_schema = key_parts[0] if len(key_parts) > 1 else "public"
    table_name = key_parts[-1]

    target_exists = bool(sink_cfg.get("target_exists", False))
    replicate_structure = bool(sink_cfg.get("replicate_structure", False))

    if runtime_mode != "native" and target_exists and not replicate_structure:
        return None

    table_def = table_defs.get(from_ref)
    if table_def is None:
        table_def = table_defs.get(source_table)
    if table_def is None:
        available = sorted(table_defs.keys())[:_MAX_WARNING_TABLES]
        result.warnings.append(
            f"Table {sink_key}: no schema definition for '{from_ref}' "
            + f"(have {len(table_defs)}: {', '.join(available)}{'...' if len(table_defs) > _MAX_WARNING_TABLES else ''})",
        )
        return None

    source_cfg = get_source_table_config(service_config, from_ref)
    ignore_raw = source_cfg.get("ignore_columns")
    ignore_cols = [str(c) for c in cast(list[object], ignore_raw)] if isinstance(ignore_raw, list) else None

    columns, primary_keys = build_columns_from_table_def(
        table_def,
        ignore_cols,
        type_mapper,
    )

    columns = add_column_template_columns(columns, cast(dict[str, object], sink_cfg))
    if runtime_mode == "native":
        columns = add_native_cdc_metadata_columns(columns)
    else:
        columns = add_cdc_metadata_columns(columns)

    if not columns:
        result.warnings.append(f"Table {sink_key}: no columns resolved, skipped")
        return None

    if runtime_mode == "native":
        customer_id_present = any(column.name.casefold() == "customer_id" for column in columns)
        if not customer_id_present:
            result.errors.append(
                f"Table {sink_key}: native runtime requires a customer_id column template",
            )
            return None
        primary_keys = _prepend_customer_id(primary_keys)

    foreign_table_name = build_foreign_table_name(
        source_schema,
        source_table,
        duplicate_table_name_count=duplicate_source_table_name_count,
    )

    return TableMigration(
        table_name=table_name,
        target_schema=target_schema,
        source_schema=source_schema,
        columns=columns,
        primary_keys=primary_keys,
        replicate_structure=replicate_structure,
        target_exists=target_exists,
        source_table=source_table,
        source_key=from_ref,
        foreign_table_name=foreign_table_name,
        base_foreign_table_name=build_base_foreign_table_name(
            source_schema,
            source_table,
            duplicate_table_name_count=duplicate_source_table_name_count,
        ),
        min_lsn_table_name=build_min_lsn_table_name(foreign_table_name),
        capture_instance_name=f"{source_schema}_{source_table}",
    )


def _prepend_customer_id(primary_keys: list[str]) -> list[str]:
    """Ensure customer_id leads the composite PK in native runtime mode."""
    deduped_primary_keys = [primary_key for primary_key in primary_keys if primary_key.casefold() != "customer_id"]
    return ["customer_id", *deduped_primary_keys]
