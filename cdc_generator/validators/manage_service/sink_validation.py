"""Sink validation helpers for manage-service operations."""

from typing import cast

from cdc_generator.helpers.helpers_logging import print_warning

from .sink_operations_helpers import _parse_sink_key, _validate_sink_group_exists
from .sink_operations_type_compatibility import validate_pg_schema_name


def validate_single_sink(
    sink_key_str: str,
    sink_raw: object,
    source_tables: list[str],
) -> list[str]:
    """Validate one sink entry and return error messages."""
    errors: list[str] = []

    parsed = _parse_sink_key(sink_key_str)
    if parsed is None:
        errors.append(
            f"Invalid sink key '{sink_key_str}'. "
            + "Expected: sink_group.target_service"
        )
        return errors

    sink_group, _ts = parsed
    if not _validate_sink_group_exists(sink_group):
        errors.append(
            f"Sink group '{sink_group}' (in '{sink_key_str}') "
            + "not found in sink-groups.yaml"
        )

    if not isinstance(sink_raw, dict):
        return errors

    sink_cfg = cast(dict[str, object], sink_raw)
    tables_raw = sink_cfg.get("tables", {})
    if not isinstance(tables_raw, dict):
        return errors

    tables = cast(dict[str, object], tables_raw)
    for tbl_key_raw, tbl_raw in tables.items():
        tbl_key = str(tbl_key_raw)

        if "." in tbl_key:
            tbl_schema = tbl_key.split(".", 1)[0]
            schema_err = validate_pg_schema_name(tbl_schema)
            if schema_err:
                errors.append(
                    f"Table '{tbl_key}' in sink '{sink_key_str}': {schema_err}"
                )

        if tbl_key not in source_tables:
            print_warning(
                f"Table '{tbl_key}' in sink '{sink_key_str}'"
                + " not found in source.tables"
            )
        if not isinstance(tbl_raw, dict):
            continue
        tbl_cfg = cast(dict[str, object], tbl_raw)

        if "target_exists" not in tbl_cfg:
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}' "
                + "missing required field 'target_exists'. "
                + "Use 'target_exists: true' (map to existing table) or "
                + "'target_exists: false' (autocreate clone)"
            )
            continue

        target_exists = tbl_cfg.get("target_exists", False)
        if not isinstance(target_exists, bool):
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}' "
                + "has invalid 'target_exists' value. Must be true or false"
            )
            continue

        if target_exists and "target" not in tbl_cfg:
            errors.append(
                f"Table '{tbl_key}' in sink '{sink_key_str}'"
                + " has target_exists=true but no 'target' field"
            )

    return errors
