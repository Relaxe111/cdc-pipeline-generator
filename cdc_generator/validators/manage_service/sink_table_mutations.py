"""Table mutation helpers for sink operations."""

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs

from .config import SERVICE_SCHEMAS_DIR, save_service_config
from .sink_operations_helpers import (
    _get_sink_tables,
    _get_sinks_dict,
    _get_source_table_keys,
    _get_target_service_from_sink_key,
    _resolve_sink_config,
    _validate_table_in_schemas,
)


def validate_table_add(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object],
    skip_schema_validation: bool = False,
    validate_table_in_schemas: Callable[[str, str], bool] | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    """Validate parameters for adding table to sink."""
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key) if sink_key in sinks else None

    if sink_cfg is None:
        return None, f"Sink '{sink_key}' not found"

    tables = _get_sink_tables(sink_cfg)

    if table_key in tables:
        print_warning(f"Table '{table_key}' already in sink '{sink_key}'")
        return None, None

    if "target_exists" not in table_opts:
        return (
            None,
            "Missing required parameter 'target_exists'. "
            + "Specify --target-exists true (map to existing table) or "
            + "--target-exists false (autocreate clone)",
        )

    from_table = table_opts.get("from")
    if from_table is None:
        return (
            None,
            "Missing required parameter 'from'. "
            + "Specify --from <schema.table> to map sink table data source.",
        )

    source_tables = _get_source_table_keys(config)
    if str(from_table) not in source_tables:
        available = "\n  ".join(source_tables) if source_tables else "(none)"
        return (
            None,
            f"Source table '{from_table}' not found in service.\n"
            + f"Available source tables:\n  {available}",
        )

    validate_fn = validate_table_in_schemas or _validate_table_in_schemas
    if not skip_schema_validation and not validate_fn(sink_key, table_key):
        return None, None

    return tables, None


def save_custom_table_structure(
    sink_key: str,
    table_key: str,
    from_table: str,
    source_service: str,
    get_project_root_fn: Callable[[], Path] | None = None,
    service_schemas_dir: Path | None = None,
    get_service_schema_read_dirs_fn: Callable[[str, Path], list[Path]] | None = None,
) -> None:
    """Save minimal reference file under service schemas custom-tables."""
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        print_warning("Could not determine target service from sink key")
        return

    if "." not in from_table or "." not in table_key:
        print_warning(f"Invalid table format: {from_table} or {table_key}")
        return

    source_schema, source_table = from_table.split(".", 1)
    target_schema, target_table = table_key.split(".", 1)

    source_file: Path | None = None
    project_root = get_project_root_fn() if get_project_root_fn else get_project_root()

    schema_read_dirs = get_service_schema_read_dirs_fn or get_service_schema_read_dirs

    for source_service_dir in schema_read_dirs(
        source_service,
        project_root,
    ):
        candidate = source_service_dir / source_schema / f"{source_table}.yaml"
        if candidate.exists():
            source_file = candidate
            break

    if source_file is None:
        print_warning(
            "Source table schema not found for "
            + f"{source_service}.{source_schema}.{source_table}\n"
            + "Custom table reference will not be saved. "
            + "Run inspect on source service first."
        )
        return

    reference_data: dict[str, object] = {
        "source_reference": {
            "service": source_service,
            "schema": source_schema,
            "table": source_table,
        },
        "sink_target": {
            "schema": target_schema,
            "table": target_table,
        },
    }

    schemas_dir = service_schemas_dir if service_schemas_dir is not None else SERVICE_SCHEMAS_DIR
    target_dir = schemas_dir / target_service / "custom-tables"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / f"{table_key.replace('/', '_')}.yaml"

    try:
        target_file.write_text(
            "# Minimal reference file - base structure deduced from source at generation time\n"
            + "\n"
            + "# source_reference: Points to the source table schema\n"
            + "# sink_target: Defines the target schema/table in sink database\n"
            + "#\n"
            + "# Base structure (columns, types, PKs) is deduced from source at generation time.\n"
            + "# Non-deducible sink behavior (column_templates, transforms)\n"
            + "# is stored in services/<service>.yaml under sinks.<sink>.tables.<table>.\n"
            + "\n",
            encoding="utf-8",
        )

        with target_file.open("a", encoding="utf-8") as handle:
            import yaml

            yaml.dump(reference_data, handle, default_flow_style=False, sort_keys=False)

        print_success(
            f"Saved custom table reference: {target_file.relative_to(schemas_dir.parent)}"
        )
    except Exception as exc:
        print_warning(f"Failed to save custom table reference: {exc}")


def remove_custom_table_file(sink_key: str, table_key: str) -> None:
    """Remove custom-table YAML reference file(s) if they exist."""
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return

    filename = f"{table_key.replace('/', '_')}.yaml"
    removed_paths: list[str] = []
    project_root = get_project_root()
    for service_dir in get_service_schema_read_dirs(target_service, project_root):
        custom_file = service_dir / "custom-tables" / filename
        if custom_file.is_file():
            custom_file.unlink()
            removed_paths.append(str(custom_file.relative_to(project_root)))

    if removed_paths:
        print_info("Removed custom table file(s): " + ", ".join(removed_paths))


def remove_sink_table(service: str, sink_key: str, table_key: str) -> bool:
    """Remove table from sink and clean up custom-table reference files."""
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
        print_warning(f"Table '{table_key}' not found in sink '{sink_key}'")
        return False

    del tables[table_key]
    if not save_service_config(service, config):
        return False

    remove_custom_table_file(sink_key, table_key)

    print_success(f"Removed table '{table_key}' from sink '{sink_key}'")
    return True


def validate_schema_update_inputs(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Validate inputs for updating sink table schema."""
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks = _get_sinks_dict(config)
    if sink_key not in sinks:
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    if table_key not in tables:
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        print_info(
            f"Available tables in '{sink_key}':\n  "
            + "\n  ".join(str(k) for k in tables)
        )
        return None

    return cast(dict[str, Any], config), cast(dict[str, Any], tables)


def update_sink_table_schema(
    service: str,
    sink_key: str,
    table_key: str,
    new_schema: str,
    *,
    validate_pg_schema_name: Callable[[str], str | None],
) -> bool:
    """Update schema part of sink table key."""
    schema_error = validate_pg_schema_name(new_schema)
    if schema_error:
        print_error(schema_error)
        return False

    result = validate_schema_update_inputs(service, sink_key, table_key)
    if result is None:
        return False

    config, tables = result

    if "." not in table_key:
        print_error(
            f"Invalid table key '{table_key}': expected 'schema.table' format"
        )
        return False

    old_schema, table_name = table_key.split(".", 1)
    new_table_key = f"{new_schema}.{table_name}"

    if new_table_key in tables:
        print_error(
            f"Table '{new_table_key}' already exists in sink '{sink_key}'"
        )
        return False

    table_config = tables[table_key]
    tables[new_table_key] = table_config
    del tables[table_key]

    if not save_service_config(service, config):
        return False

    print_success(
        f"Updated table schema: '{old_schema}.{table_name}' → "
        + f"'{new_schema}.{table_name}' in sink '{sink_key}'"
    )
    return True
