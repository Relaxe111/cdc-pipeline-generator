"""Custom sink table management for manage-services config CLI.

Handles creation and modification of custom tables — tables that don't exist
in any source and will be auto-created in the sink database.

Custom tables have:
    custom: true   — marks as manually created (not inferred from schemas)
    managed: true  — can be modified via CLI (add/remove columns)
"""

from typing import Any, cast

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

try:
    from cdc_generator.helpers.yaml_loader import yaml
except ImportError:
    yaml = None


# ---------------------------------------------------------------------------
# Column parsing
# ---------------------------------------------------------------------------

# Valid PostgreSQL types for quick validation
_PG_TYPES = frozenset({
    "smallint", "integer", "bigint", "serial", "bigserial",
    "numeric", "real", "double precision",
    "text", "varchar", "char", "citext",
    "boolean",
    "date", "time", "timestamp", "timestamptz", "interval",
    "uuid",
    "json", "jsonb",
    "bytea",
    "inet", "cidr", "macaddr",
    "text[]", "integer[]", "uuid[]", "jsonb[]",
})

# Default expressions mapping (shorthand → SQL)
_DEFAULT_EXPRESSIONS: dict[str, str] = {
    "now": "now()",
    "default_now": "now()",
    "current_timestamp": "CURRENT_TIMESTAMP",
    "current_date": "CURRENT_DATE",
    "gen_random_uuid": "gen_random_uuid()",
    "uuid": "gen_random_uuid()",
    "default_gen_random_uuid": "gen_random_uuid()",
    "default_0": "0",
    "default_false": "false",
    "default_true": "true",
    "default_empty": "''",
}

# Minimum parts in a column spec (name:type)
_MIN_SPEC_PARTS = 2

# Expected parts when splitting schema.table
_TABLE_KEY_PARTS = 2


def _split_table_key(table_key: str) -> tuple[str, str]:
    """Split 'schema.table' into (schema_name, table_name).

    Defaults to schema='public' if no dot separator.
    """
    parts = table_key.split(".", 1)
    if len(parts) == _TABLE_KEY_PARTS:
        return parts[0], parts[1]
    return "public", parts[0]


def _parse_column_spec(spec: str) -> dict[str, object] | None:
    """Parse a column specification string into a column definition dict.

    Format: "name:type[:modifier[:modifier...]]"

    Modifiers:
        pk          - primary key
        not_null    - NOT NULL constraint
        nullable    - allow NULL (explicit)
        default_X   - default expression (e.g., default_now, default_uuid)

    Examples:
        "id:uuid:pk"                        → {type: uuid, primary_key: true}
        "name:text:not_null"                → {type: text, nullable: false}
        "created_at:timestamptz:not_null:default_now"
            → {type: timestamptz, nullable: false, default: "now()"}

    Returns:
        Column definition dict, or None on error.
    """
    parts = spec.split(":")
    if len(parts) < _MIN_SPEC_PARTS:
        print_error(
            f"Invalid column spec '{spec}'. "
            + "Format: name:type[:pk][:not_null][:default_X]"
        )
        return None

    col_name = parts[0].strip()
    col_type = parts[1].strip().lower()

    if not col_name:
        print_error("Column name cannot be empty")
        return None

    if col_type not in _PG_TYPES:
        print_warning(
            f"Type '{col_type}' is not a standard PostgreSQL type. "
            + "Proceeding anyway."
        )

    col_def: dict[str, object] = {"type": col_type}

    # Parse modifiers
    for modifier in parts[2:]:
        mod = modifier.strip().lower()
        if mod == "pk":
            col_def["primary_key"] = True
            col_def["nullable"] = False  # PKs are always NOT NULL
        elif mod == "not_null":
            col_def["nullable"] = False
        elif mod == "nullable":
            col_def["nullable"] = True
        elif mod in _DEFAULT_EXPRESSIONS:
            col_def["default"] = _DEFAULT_EXPRESSIONS[mod]
        elif mod.startswith("default_"):
            # Try to resolve default_X
            default_key = mod
            if default_key in _DEFAULT_EXPRESSIONS:
                col_def["default"] = _DEFAULT_EXPRESSIONS[default_key]
            else:
                # Use as raw default expression
                col_def["default"] = mod.removeprefix("default_")
        elif mod:
            print_warning(f"Unknown modifier '{mod}' — ignoring")

    return {"name": col_name, **col_def}


def _parse_multiple_columns(
    column_specs: list[str],
) -> list[dict[str, object]] | None:
    """Parse multiple column specs into column definitions.

    Returns:
        List of column defs, or None if any spec is invalid.
    """
    columns: list[dict[str, object]] = []
    names_seen: set[str] = set()

    for spec in column_specs:
        col = _parse_column_spec(spec)
        if col is None:
            return None
        name = str(col["name"])
        if name in names_seen:
            print_error(f"Duplicate column name: '{name}'")
            return None
        names_seen.add(name)
        columns.append(col)

    if not columns:
        print_error("At least one --column is required")
        return None

    # Validate at least one PK exists
    has_pk = any(col.get("primary_key") for col in columns)
    if not has_pk:
        print_warning(
            "No primary key defined. Consider adding :pk to a column."
        )

    return columns


# ---------------------------------------------------------------------------
# Schema file generation
# ---------------------------------------------------------------------------


def _build_schema_yaml(
    table_key: str,
    columns: list[dict[str, object]],
    target_service: str,
) -> dict[str, object]:
    """Build a service-schemas YAML structure from column definitions.

    Matches the existing schema format used by --inspect-sink --save.
    """
    schema_name, table_name = _split_table_key(table_key)

    # Find primary key
    pk_cols = [
        str(c["name"]) for c in columns if c.get("primary_key")
    ]
    primary_key = pk_cols[0] if len(pk_cols) == 1 else None

    # Build column list in schema format
    schema_columns: list[dict[str, object]] = []
    for col in columns:
        schema_col: dict[str, object] = {
            "name": col["name"],
            "type": col["type"],
            "nullable": col.get("nullable", True),
            "primary_key": bool(col.get("primary_key", False)),
        }
        if "default" in col:
            schema_col["default"] = col["default"]
        schema_columns.append(schema_col)

    result: dict[str, object] = {
        "schema": schema_name,
        "service": target_service,
        "table": table_name,
        "columns": schema_columns,
    }
    if primary_key:
        result["primary_key"] = primary_key

    return result


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


def _build_custom_table_config(
    from_table: str | None = None,
) -> dict[str, object]:
    """Build the per-table config dict for a custom table.

    Always sets target_exists=false, custom=true, managed=true.
    Keeps only fields that are not deducible from schema resources.
    """
    cfg: dict[str, object] = {
        "target_exists": False,
        "custom": True,
        "managed": True,
    }
    if from_table is not None:
        cfg["from"] = from_table
    return cfg


def _get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return the sinks section, creating it if absent."""
    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        config["sinks"] = {}
        return cast(dict[str, object], config["sinks"])
    return cast(dict[str, object], sinks_raw)


def _get_sink_tables(
    sink_cfg: dict[str, object],
) -> dict[str, object]:
    """Return tables dict inside a sink config, creating if absent."""
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        sink_cfg["tables"] = {}
        return cast(dict[str, object], sink_cfg["tables"])
    return cast(dict[str, object], tables_raw)


def _resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None if not found."""
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Sink '{sink_key}' not found or invalid")
        return None
    return cast(dict[str, object], sink_raw)


def _extract_target_service(sink_key: str) -> str | None:
    """Extract target_service from 'sink_group.target_service'."""
    parts = sink_key.split(".", 1)
    if len(parts) != _TABLE_KEY_PARTS:
        print_error(
            f"Invalid sink key '{sink_key}'. "
            + "Expected: sink_group.target_service"
        )
        return None
    return parts[1]


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
        print_error(
            f"Table '{table_key}' already exists in "
            + f"service-schemas/{target_service}/{schema_name}/"
        )
        print_info(
            "Use --add-sink-table instead for existing schema tables"
        )
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
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    source = config.get("source")
    tables = source.get("tables") if isinstance(source, dict) else None
    if not isinstance(tables, dict) or table_ref not in tables:
        print_error(
            f"Source table '{table_ref}' not found in service '{service}'"
        )
        table_keys = sorted(str(k) for k in tables) if isinstance(tables, dict) else []
        if table_keys:
            print_info("Available source tables: " + ", ".join(table_keys))
        return None

    if "." not in table_ref:
        print_error(
            f"Invalid source table reference '{table_ref}'. Expected schema.table"
        )
        return None

    schema_name, table_name = table_ref.split(".", 1)

    table_schema: dict[str, Any] | None = None
    for service_dir in get_service_schema_read_dirs(service):
        schema_file = service_dir / schema_name / f"{table_name}.yaml"
        if not schema_file.exists():
            continue
        loaded = load_yaml_file(schema_file)
        if isinstance(loaded, dict):
            table_schema = loaded
            break

    if table_schema is None:
        print_error(
            f"Schema for source table '{table_ref}' not found in service-schemas"
        )
        print_info(
            "Run: cdc manage-services config --service "
            + f"{service} --inspect --all --save"
        )
        return None

    columns_raw: list[dict[str, Any]] = table_schema.get("columns", [])
    if not columns_raw:
        print_error(
            f"Source table '{table_ref}' has no columns"
        )
        return None

    # Convert to internal parsed-column format used by schema file generation
    columns: list[dict[str, object]] = []
    for col in columns_raw:
        col_def: dict[str, object] = {
            "name": col.get("name", ""),
            "type": col.get("type", "text"),
        }
        if col.get("primary_key"):
            col_def["primary_key"] = True
            col_def["nullable"] = False
        elif col.get("nullable") is not None:
            col_def["nullable"] = col["nullable"]
        if col.get("default"):
            col_def["default"] = col["default"]
        columns.append(col_def)

    return columns


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
    print_success(
        f"Created custom table '{table_key}' in sink '{sink_key}'"
    )
    print_info(f"Columns: {', '.join(col_names)}")
    print_info(
        f"Schema saved to: service-schemas/{target_service}/"
        + f"{schema_name}/{table_name}.yaml"
    )
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
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        return None

    tbl_cfg = cast(dict[str, object], tbl_raw)

    if not tbl_cfg.get("custom"):
        print_error(
            f"Table '{table_key}' is not a custom table "
            + "- it was inferred from source schemas"
        )
        print_info(
            "Only tables with 'custom: true' can be modified via CLI"
        )
        return None

    if not tbl_cfg.get("managed"):
        print_error(
            f"Table '{table_key}' has custom=true but managed=false "
            + "- CLI modifications are disabled"
        )
        print_info("Set 'managed: true' in the YAML to enable CLI edits")
        return None

    target_service = _extract_target_service(sink_key)
    if target_service is None:
        return None

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        return config, tbl_cfg, cast(dict[str, object], cols_raw), target_service

    schema_columns = _load_columns_map_from_schema(target_service, table_key)
    if schema_columns is None:
        print_error(f"No column definitions available for '{table_key}'")
        print_info(
            "Ensure schema exists under services/_schemas/<target>/<schema>/<table>.yaml"
        )
        return None

    return config, tbl_cfg, schema_columns, target_service


def _load_columns_map_from_schema(
    target_service: str,
    table_key: str,
) -> dict[str, object] | None:
    """Load column definitions from target schema file as mapping."""
    if yaml is None:
        return None

    schema_name, table_name = _split_table_key(table_key)
    schema_file = (
        SERVICE_SCHEMAS_DIR / target_service / schema_name / f"{table_name}.yaml"
    )
    if not schema_file.exists():
        return None

    with schema_file.open(encoding="utf-8") as f:
        data = yaml.load(f)

    if not isinstance(data, dict):
        return None

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns")
    if not isinstance(columns_raw, list):
        return None

    cols: dict[str, object] = {}
    for col in columns_raw:
        if not isinstance(col, dict):
            continue
        name = col.get("name")
        col_type = col.get("type")
        if not isinstance(name, str) or not isinstance(col_type, str):
            continue
        col_cfg: dict[str, object] = {"type": col_type}
        if col.get("primary_key"):
            col_cfg["primary_key"] = True
        if col.get("nullable") is not None and not col.get("nullable"):
            col_cfg["nullable"] = False
        if col.get("default") is not None:
            col_cfg["default"] = col.get("default")
        cols[name] = col_cfg

    return cols


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
    result = _load_custom_table(service, sink_key, table_key)
    if result is None:
        return False

    config, tbl_cfg, columns, target_service = result

    col = _parse_column_spec(column_spec)
    if col is None:
        return False

    col_name = str(col["name"])
    if col_name in columns:
        print_warning(f"Column '{col_name}' already exists in '{table_key}'")
        return False

    # Build column entry (without the name key)
    col_entry: dict[str, object] = {"type": col["type"]}
    if col.get("primary_key"):
        col_entry["primary_key"] = True
    if col.get("nullable") is not None and not col.get("nullable"):
        col_entry["nullable"] = False
    if "default" in col:
        col_entry["default"] = col["default"]

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        columns[col_name] = col_entry
        if not save_service_config(service, config):
            return False

    # Canonical definition lives in schema file
    _update_schema_file_add_column(target_service, table_key, col)

    print_success(
        f"Added column '{col_name}' ({col['type']}) "
        + f"to custom table '{table_key}'"
    )
    return True


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
    result = _load_custom_table(service, sink_key, table_key)
    if result is None:
        return False

    config, tbl_cfg, columns, target_service = result

    if column_name not in columns:
        print_error(
            f"Column '{column_name}' not found in '{table_key}'"
        )
        available = [str(k) for k in columns]
        if available:
            print_info("Available columns: " + ", ".join(available))
        return False

    # Check if it's the last column
    if len(columns) <= 1:
        print_error(
            "Cannot remove last column — "
            + "use --remove-sink-table to remove the entire table"
        )
        return False

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        del columns[column_name]
        if not save_service_config(service, config):
            return False

    # Canonical definition lives in schema file
    _update_schema_file_remove_column(
        target_service, table_key, column_name,
    )

    print_success(
        f"Removed column '{column_name}' from custom table '{table_key}'"
    )
    return True


# ---------------------------------------------------------------------------
# Schema file update helpers
# ---------------------------------------------------------------------------


def _update_schema_file_add_column(
    target_service: str,
    table_key: str,
    col: dict[str, object],
) -> None:
    """Add a column to the service-schemas YAML file."""
    if yaml is None:
        return

    schema_name, table_name = _split_table_key(table_key)

    schema_file = (
        SERVICE_SCHEMAS_DIR / target_service / schema_name
        / f"{table_name}.yaml"
    )
    if not schema_file.exists():
        return

    with schema_file.open(encoding="utf-8") as f:
        data = yaml.load(f)

    if not isinstance(data, dict):
        return

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns", [])
    if not isinstance(columns_raw, list):
        return

    columns_list = cast(list[dict[str, object]], columns_raw)

    new_col: dict[str, object] = {
        "name": col["name"],
        "type": col["type"],
        "nullable": col.get("nullable", True),
        "primary_key": bool(col.get("primary_key", False)),
    }
    if "default" in col:
        new_col["default"] = col["default"]

    columns_list.append(new_col)

    with schema_file.open("w", encoding="utf-8") as f:
        yaml.dump(schema_data, f)


def _update_schema_file_remove_column(
    target_service: str,
    table_key: str,
    column_name: str,
) -> None:
    """Remove a column from the service-schemas YAML file."""
    if yaml is None:
        return

    schema_name, table_name = _split_table_key(table_key)

    schema_file = (
        SERVICE_SCHEMAS_DIR / target_service / schema_name
        / f"{table_name}.yaml"
    )
    if not schema_file.exists():
        return

    with schema_file.open(encoding="utf-8") as f:
        data = yaml.load(f)

    if not isinstance(data, dict):
        return

    schema_data = cast(dict[str, Any], data)
    columns_raw = schema_data.get("columns", [])
    if not isinstance(columns_raw, list):
        return

    columns_list = cast(list[dict[str, object]], columns_raw)
    schema_data["columns"] = [
        c for c in columns_list
        if c.get("name") != column_name
    ]

    with schema_file.open("w", encoding="utf-8") as f:
        yaml.dump(schema_data, f)


# ---------------------------------------------------------------------------
# Public: list custom table columns
# ---------------------------------------------------------------------------


def _get_tables_dict_from_config(
    service: str,
    sink_key: str,
) -> dict[str, object] | None:
    """Load config and navigate to the tables dict for a sink.

    Returns:
        Tables dict or None if not found.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return None

    sink_raw = cast(dict[str, object], sinks_raw).get(sink_key)
    if not isinstance(sink_raw, dict):
        return None

    tables_raw = cast(dict[str, object], sink_raw).get("tables")
    if not isinstance(tables_raw, dict):
        return None

    return cast(dict[str, object], tables_raw)


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
    tables = _get_tables_dict_from_config(service, sink_key)
    if tables is None:
        return []

    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        return []

    tbl_cfg = cast(dict[str, object], tbl_raw)
    if not tbl_cfg.get("custom"):
        return []

    cols_raw = tbl_cfg.get("columns")
    if isinstance(cols_raw, dict):
        cols = cast(dict[str, object], cols_raw)
        return sorted(str(k) for k in cols)

    target_service = _extract_target_service(sink_key)
    if target_service is None:
        return []

    schema_cols = _load_columns_map_from_schema(target_service, table_key)
    if schema_cols is None:
        return []

    return sorted(str(k) for k in schema_cols)


def list_custom_tables_for_sink(
    service: str,
    sink_key: str,
) -> list[str]:
    """Return table keys where custom=true for a given sink.

    Used for autocompletion of --modify-custom-table.

    Returns:
        List of table keys (e.g., ['public.audit_log']).
    """
    tables = _get_tables_dict_from_config(service, sink_key)
    if tables is None:
        return []

    result: list[str] = []
    for tbl_key_raw, tbl_raw in tables.items():
        if isinstance(tbl_raw, dict):
            tbl_cfg = cast(dict[str, object], tbl_raw)
            if tbl_cfg.get("custom"):
                result.append(str(tbl_key_raw))

    return sorted(result)
