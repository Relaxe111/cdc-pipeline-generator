"""Sink operations for CDC service configuration.

Handles adding, removing, and listing sink configurations in service YAML files.
Sinks define WHERE source tables are sent and HOW they are mapped.

Sink key format: {sink_group}.{target_service}
    - sink_group: references sink-groups.yaml (e.g., sink_asma)
    - target_service: target service/database in that sink group
"""

import re
from dataclasses import dataclass
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs

from .config import SERVICE_SCHEMAS_DIR, save_service_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SINK_KEY_SEPARATOR = "."
_SINK_KEY_PARTS = 2

# Valid unquoted PostgreSQL identifier: starts with letter or underscore,
# followed by letters, digits, underscores, or dollar signs. Max 63 chars.
_PG_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")
_PG_IDENTIFIER_MAX_LENGTH = 63


def validate_pg_schema_name(schema: str) -> str | None:
    """Validate that *schema* is a valid unquoted PostgreSQL identifier.

    Valid identifiers start with a letter or underscore, followed by
    letters, digits, underscores, or dollar signs (max 63 chars).
    Hyphens, spaces, and leading digits are NOT allowed.

    Args:
        schema: Schema name to validate.

    Returns:
        Error message if invalid, None if valid.

    Examples:
        >>> validate_pg_schema_name('public')  # None (valid)
        >>> validate_pg_schema_name('directory_clone')  # None (valid)
        >>> validate_pg_schema_name('directory-clone')  # error message
        >>> validate_pg_schema_name('123abc')  # error message
    """
    if not schema:
        return "Schema name cannot be empty"

    if len(schema) > _PG_IDENTIFIER_MAX_LENGTH:
        return (
            f"Schema name '{schema}' exceeds PostgreSQL maximum "
            f"of {_PG_IDENTIFIER_MAX_LENGTH} characters"
        )

    if not _PG_IDENTIFIER_PATTERN.match(schema):
        # Build a specific hint about what's wrong
        if "-" in schema:
            suggestion = schema.replace("-", "_")
            return (
                f"Schema name '{schema}' contains hyphens which are invalid "
                f"in PostgreSQL identifiers. Use underscores instead: '{suggestion}'"
            )
        if schema[0].isdigit():
            return (
                f"Schema name '{schema}' starts with a digit. "
                f"PostgreSQL identifiers must start with a letter or underscore"
            )
        return (
            f"Schema name '{schema}' contains invalid characters. "
            f"Only letters, digits, underscores, and dollar signs are allowed"
        )

    return None


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TableConfigOptions:
    """Options for building a sink table configuration."""

    target_exists: bool
    target: str | None = None
    target_schema: str | None = None
    include_columns: list[str] | None = None
    columns: dict[str, str] | None = None
    from_table: str | None = None
    replicate_structure: bool = False
    sink_schema: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers — parsing & validation
# ---------------------------------------------------------------------------


def _parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse 'sink_group.target_service' → (sink_group, target_service).

    Returns None if the format is invalid.
    """
    parts = sink_key.split(_SINK_KEY_SEPARATOR, 1)
    if len(parts) != _SINK_KEY_PARTS:
        return None
    return parts[0], parts[1]


def _validate_sink_group_exists(sink_group: str) -> bool:
    """Return True if *sink_group* exists in sink-groups.yaml."""
    sink_file = get_project_root() / "sink-groups.yaml"
    if not sink_file.exists():
        return False
    try:
        from cdc_generator.helpers.yaml_loader import load_yaml_file

        sink_groups = load_yaml_file(sink_file)
        return sink_group in sink_groups
    except (FileNotFoundError, ValueError):
        return False


def _validate_and_parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse *sink_key*, printing an error if the format is invalid."""
    parsed = _parse_sink_key(sink_key)
    if parsed is None:
        print_error(
            f"Invalid sink key '{sink_key}'. Expected format: "
            + "sink_group.target_service (e.g., sink_asma.chat)"
        )
    return parsed


# ---------------------------------------------------------------------------
# Internal helpers — schema validation
# ---------------------------------------------------------------------------


def _get_target_service_from_sink_key(sink_key: str) -> str | None:
    """Extract target_service from sink key 'sink_group.target_service'."""
    parsed = _parse_sink_key(sink_key)
    return parsed[1] if parsed else None


def _list_tables_in_service_schemas(target_service: str) -> list[str]:
    """List all tables for sink target across preferred/legacy schema roots.

    Returns:
        List of 'schema.table' strings.
    """
    tables: set[str] = set()
    for service_dir in get_service_schema_read_dirs(target_service, get_project_root()):
        if not service_dir.is_dir():
            continue
        for schema_dir in service_dir.iterdir():
            if not schema_dir.is_dir():
                continue
            for table_file in schema_dir.glob("*.yaml"):
                tables.add(f"{schema_dir.name}.{table_file.stem}")
    return sorted(tables)


def _validate_table_in_schemas(
    sink_key: str,
    table_key: str,
) -> bool:
    """Validate that table_key exists in service-schemas for the sink target.

    Prints friendly errors if schemas are missing or table not found.

    Returns:
        True if valid, False if validation failed.
    """
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return False

    has_service_dir = any(
        service_dir.is_dir()
        for service_dir in get_service_schema_read_dirs(target_service, get_project_root())
    )
    if not has_service_dir:
        print_error(f"No schemas found for sink target '{target_service}'")
        print_info(
            "To fetch schemas, run:\n"
            + "  cdc manage-service --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        print_info(
            "Or create manually: "
            + f"services/_schemas/{target_service}/<schema>/<Table>.yaml"
        )
        return False

    available = _list_tables_in_service_schemas(target_service)
    if not available:
        print_error(
            f"Schema directory for '{target_service}' exists but is empty"
        )
        print_info(
            "To populate schemas, run:\n"
            + "  cdc manage-service --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return False

    if table_key not in available:
        print_error(
            f"Table '{table_key}' not found in "
            + f"service-schemas/{target_service}/"
        )
        print_info(
            "Available tables:\n  "
            + "\n  ".join(available)
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Internal helpers — typed dict access on loaded YAML data
# ---------------------------------------------------------------------------


def _get_source_tables_dict(config: dict[str, object]) -> dict[str, object]:
    """Return source.tables dict (typed), or empty dict if absent."""
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return {}
    source = cast(dict[str, object], source_raw)
    tables_raw = source.get("tables")
    if not isinstance(tables_raw, dict):
        return {}
    return cast(dict[str, object], tables_raw)


def _get_source_table_keys(config: dict[str, object]) -> list[str]:
    """Return list of source table keys (e.g. ['public.users', …])."""
    return [str(k) for k in _get_source_tables_dict(config)]


def _get_sinks_dict(config: dict[str, object]) -> dict[str, object]:
    """Return the sinks section, creating it if absent.

    Returns a *mutable* reference into *config*.
    """
    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        config["sinks"] = {}
        return cast(dict[str, object], config["sinks"])
    return cast(dict[str, object], sinks_raw)


def _get_sink_tables(sink_cfg: dict[str, object]) -> dict[str, object]:
    """Return the tables dict inside a single sink config, creating if absent."""
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        sink_cfg["tables"] = {}
        return cast(dict[str, object], sink_cfg["tables"])
    return cast(dict[str, object], tables_raw)


def _resolve_sink_config(
    sinks: dict[str, object],
    sink_key: str,
) -> dict[str, object] | None:
    """Return typed sink config dict, or None (with error) if invalid."""
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Invalid sink configuration for '{sink_key}'")
        return None
    return cast(dict[str, object], sink_raw)


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
    parsed = _validate_and_parse_sink_key(sink_key)
    if parsed is None:
        return False

    sink_group, _target = parsed
    if not _validate_sink_group_exists(sink_group):
        print_error(f"Sink group '{sink_group}' not found in sink-groups.yaml")
        print_info("Run 'cdc manage-sink-groups --list' to see available groups")
        return False

    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks = _get_sinks_dict(config)
    if sink_key in sinks:
        print_warning(f"Sink '{sink_key}' already exists in service '{service}'")
        return False

    sinks[sink_key] = {"tables": {}}
    if not save_service_config(service, config):
        return False

    print_success(f"Added sink '{sink_key}' to service '{service}'")
    return True


def remove_sink_from_service(service: str, sink_key: str) -> bool:
    """Remove a sink destination from *service*.

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
        print_warning(f"Sink '{sink_key}' not found in service '{service}'")
        available = [str(k) for k in sinks]
        if available:
            print_info(f"Available sinks: {', '.join(available)}")
        return False

    del sinks[sink_key]
    if not save_service_config(service, config):
        return False

    print_success(f"Removed sink '{sink_key}' from service '{service}'")
    return True


# ---------------------------------------------------------------------------
# Public API — add / remove sink table
# ---------------------------------------------------------------------------


def _build_table_config(opts: TableConfigOptions) -> dict[str, object]:
    """Build the per-table config dict from the given options.

    target_exists is ALWAYS included in the output.
    """
    cfg: dict[str, object] = {"target_exists": opts.target_exists}

    # Add 'from' field if provided
    if opts.from_table is not None:
        cfg["from"] = opts.from_table

    # Add 'replicate_structure' if True
    if opts.replicate_structure:
        cfg["replicate_structure"] = True

    if opts.target_exists:
        if opts.target:
            cfg["target"] = opts.target
        if opts.columns:
            cfg["columns"] = opts.columns
    else:
        if opts.target_schema:
            cfg["target_schema"] = opts.target_schema
        if opts.include_columns:
            cfg["include_columns"] = opts.include_columns
    return cfg


def _validate_table_add(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object],
    skip_schema_validation: bool = False,
) -> tuple[dict[str, object] | None, str | None]:
    """Validate parameters for adding table to sink.

    Args:
        config: Service config dict.
        sink_key: Sink key.
        table_key: Table key to add.
        table_opts: Table options dict.
        skip_schema_validation: If True, skip checking if table exists in service-schemas
            (used for custom tables with --sink-schema).

    Returns:
        (sink_tables_dict, error_msg) — tables dict on success, or None + error.
    """
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key) if sink_key in sinks else None

    if not sink_cfg:
        return None, f"Sink '{sink_key}' not found"

    tables = _get_sink_tables(sink_cfg)

    if table_key in tables:
        print_warning(f"Table '{table_key}' already in sink '{sink_key}'")
        return None, None  # None error = soft failure (warning already shown)

    if "target_exists" not in table_opts:
        return (
            None,
            "Missing required parameter 'target_exists'. "
            + "Specify --target-exists true (map to existing table) or "
            + "--target-exists false (autocreate clone)",
        )

    # Validate 'from' field references a valid source table
    from_table = table_opts.get("from")
    if from_table is not None:
        source_tables = _get_source_table_keys(config)
        if str(from_table) not in source_tables:
            available = "\n  ".join(source_tables) if source_tables else "(none)"
            return (
                None,
                f"Source table '{from_table}' not found in service.\n"
                + f"Available source tables:\n  {available}",
            )

    # Validate table exists in service-schemas for the sink target
    # Skip for custom tables (when sink_schema is provided)
    if not skip_schema_validation and not _validate_table_in_schemas(sink_key, table_key):
        return None, None  # Error already printed

    return tables, None


def _save_custom_table_structure(
    sink_key: str,
    table_key: str,
    from_table: str,
    source_service: str,
) -> None:
    """Save minimal reference file to service-schemas/{target}/custom-tables/.

    Creates a lightweight YAML reference that points to the source table schema.
    Base structure (columns, PKs, types) is deduced from source at generation time.
    Only non-deducible content (column_templates, transforms) is stored here.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.notification').
        table_key: Target table key (e.g., 'notification.customer_user').
        from_table: Source table key (e.g., 'public.customer_user').
        source_service: Source service name to find original schema.
    """

    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        print_warning("Could not determine target service from sink key")
        return

    # Parse source and target table keys
    if "." not in from_table or "." not in table_key:
        print_warning(f"Invalid table format: {from_table} or {table_key}")
        return

    source_schema, source_table = from_table.split(".", 1)
    target_schema, target_table = table_key.split(".", 1)

    # Verify source schema exists
    source_file = (
        SERVICE_SCHEMAS_DIR
        / source_service
        / source_schema
        / f"{source_table}.yaml"
    )

    if not source_file.exists():
        print_warning(
            f"Source table schema not found: {source_file}\n"
            + "Custom table reference will not be saved. "
            + "Run inspect on source service first."
        )
        return

    # Create minimal reference file
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

    # Target directory and file
    target_dir = SERVICE_SCHEMAS_DIR / target_service / "custom-tables"
    target_dir.mkdir(parents=True, exist_ok=True)

    target_file = target_dir / f"{table_key.replace('/', '_')}.yaml"

    # Save minimal reference with comments as header
    try:
        target_file.write_text(
            "# Minimal reference file - base structure deduced from source at generation time\n"
            + "\n"
            + "# source_reference: Points to the source table schema\n"
            + "# sink_target: Defines the target schema/table in sink database\n"
            + "#\n"
            + "# Base structure (columns, types, PKs) is deduced from source at generation time.\n"
            + "# Only store non-deducible content here:\n"
            + "#\n"
            + "# column_templates:  (references column-templates.yaml)\n"
            + "#   - template: source_table\n"
            + "#   - template: environment\n"
            + "#     name: deploy_env\n"
            + "#\n"
            + "# transforms:  (references transform-rules.yaml)\n"
            + "#   - rule: user_class_splitter\n"
            + "\n",
            encoding="utf-8",
        )

        with target_file.open("a", encoding="utf-8") as f:
            import yaml

            yaml.dump(reference_data, f, default_flow_style=False, sort_keys=False)

        print_success(
            f"Saved custom table reference: {target_file.relative_to(SERVICE_SCHEMAS_DIR.parent)}"
        )
    except Exception as exc:
        print_warning(f"Failed to save custom table reference: {exc}")


def _is_required_sink_column(column: dict[str, Any]) -> bool:
    """Return True if sink column requires source input.

    Required means either primary key or non-nullable without default value.
    """
    is_pk = bool(column.get("primary_key", False))
    nullable = bool(column.get("nullable", True))
    has_default = column.get("default") is not None
    return (is_pk or not nullable) and not has_default


def _collect_named_column_types(
    columns: list[dict[str, Any]],
) -> dict[str, str]:
    """Return {column_name: type} for columns with both fields present."""
    result: dict[str, str] = {}
    for col in columns:
        name = col.get("name")
        col_type = col.get("type")
        if isinstance(name, str) and isinstance(col_type, str):
            result[name] = col_type
    return result


def _analyze_identity_coverage(
    source_types: dict[str, str],
    sink_types: dict[str, str],
    mapped_sink_columns: set[str],
) -> tuple[set[str], list[tuple[str, str, str]]]:
    """Return identity-compatible and identity-incompatible sink columns."""
    identity_covered: set[str] = set()
    incompatible_identity: list[tuple[str, str, str]] = []

    for sink_col, sink_type in sink_types.items():
        if sink_col in mapped_sink_columns:
            continue

        source_type = source_types.get(sink_col)
        if source_type is None:
            continue

        if _check_type_compatibility(source_type, sink_type):
            identity_covered.add(sink_col)
            continue

        incompatible_identity.append((sink_col, source_type, sink_type))

    return identity_covered, incompatible_identity


def _find_required_unmapped_sink_columns(
    sink_columns: list[dict[str, Any]],
    covered: set[str],
) -> list[str]:
    """List required sink columns not covered by explicit/identity mapping."""
    required_unmapped: list[str] = []
    for sink_col in sink_columns:
        col_name = sink_col.get("name")
        if not isinstance(col_name, str):
            continue
        if col_name in covered:
            continue
        if _is_required_sink_column(sink_col):
            required_unmapped.append(col_name)
    return required_unmapped


def _build_add_table_compatibility_guidance(
    source_table: str,
    sink_table: str,
    incompatible_identity: list[tuple[str, str, str]],
    required_unmapped: list[str],
    source_names: list[str],
) -> str:
    """Build a user-friendly compatibility error with mapping suggestions."""
    guidance_lines: list[str] = [
        "Source/sink column compatibility check failed.",
        f"Source table: {source_table}",
        f"Sink table: {sink_table}",
    ]

    if incompatible_identity:
        guidance_lines.append("Incompatible same-name columns:")
        for col_name, src_type, sink_type in incompatible_identity:
            guidance_lines.append(
                f"  - {col_name}: source={src_type}, sink={sink_type}"
            )
            guidance_lines.append(
                f"    Suggestion: --map-column {col_name} <sink_column>"
            )

    if required_unmapped:
        guidance_lines.append(
            "Required sink columns without compatible source mapping:"
        )
        for col_name in sorted(required_unmapped):
            guidance_lines.append(f"  - {col_name}")
            if source_names:
                candidates = ", ".join(source_names[:5])
                guidance_lines.append(
                    "    Suggestion: add --map-column "
                    + f"<source_column> {col_name} "
                    + f"(available source columns: {candidates})"
                )

    guidance_lines.append(
        "When columns match by name and type, mapping is applied implicitly."
    )
    return "\n".join(guidance_lines)


def _validate_add_table_schema_compatibility(
    service: str,
    sink_key: str,
    source_fallback_table: str,
    sink_table_key: str,
    opts: TableConfigOptions,
) -> str | None:
    """Validate source/sink column compatibility for add_sink_table flow.

    Applies only when mapping to an existing target table
    (target_exists=True) and replicate_structure=False.
    """
    if opts.replicate_structure or not opts.target_exists:
        return None

    source_table = opts.from_table if opts.from_table else source_fallback_table
    sink_table = opts.target if opts.target else sink_table_key

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        return f"Invalid sink key format: '{sink_key}'"

    source_columns = _load_table_columns(service, source_table)
    sink_columns = _load_table_columns(target_service, sink_table)
    if source_columns is None or sink_columns is None:
        print_warning(
            "Skipping add-time compatibility check because schema files are missing."
        )
        print_info(
            "Run inspect/save to enable strict checks: "
            + f"--inspect --all --save and --inspect-sink {sink_key} --all --save"
        )
        return None

    explicit_mappings = opts.columns or {}
    mapping_errors = _validate_column_mappings(
        list(explicit_mappings.items()),
        source_columns,
        sink_columns,
        source_table,
        sink_table,
    )
    if mapping_errors:
        details = "\n  - ".join(mapping_errors)
        return (
            "Invalid --map-column configuration:\n"
            + f"  - {details}"
        )

    source_types = _collect_named_column_types(source_columns)
    sink_types = _collect_named_column_types(sink_columns)
    mapped_sink_columns = set(explicit_mappings.values())
    identity_covered, incompatible_identity = _analyze_identity_coverage(
        source_types,
        sink_types,
        mapped_sink_columns,
    )
    covered = mapped_sink_columns | identity_covered
    required_unmapped = _find_required_unmapped_sink_columns(
        sink_columns,
        covered,
    )

    if not incompatible_identity and not required_unmapped:
        return None

    source_names = sorted(source_types.keys())
    return _build_add_table_compatibility_guidance(
        source_table,
        sink_table,
        incompatible_identity,
        required_unmapped,
        source_names,
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
            include_columns, columns, from, replicate_structure, sink_schema.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    opts = table_opts if table_opts is not None else {}

    # Handle sink_schema override - change table_key schema
    sink_schema = opts.get("sink_schema")
    final_table_key = table_key

    if sink_schema is not None:
        # Validate schema name before using it
        schema_error = validate_pg_schema_name(str(sink_schema))
        if schema_error:
            print_error(schema_error)
            return False

        # Override schema in table_key
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
        config, sink_key, final_table_key, opts, skip_schema_validation=sink_schema is not None
    )

    if error or tables is None:
        if error:
            print_error(error)
        return False

    target_exists = bool(opts.get("target_exists", False))
    target = opts.get("target")
    from_table = opts.get("from")
    replicate_structure = bool(opts.get("replicate_structure", False))

    # Validate target_schema if provided
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
    )

    compatibility_error = _validate_add_table_schema_compatibility(
        service,
        sink_key,
        table_key,
        final_table_key,
        config_opts,
    )
    if compatibility_error:
        print_error(compatibility_error)
        return False

    tables[final_table_key] = _build_table_config(config_opts)

    if not save_service_config(service, config):
        return False

    # Save custom table reference if replicate_structure is enabled
    # Use from_table if provided, otherwise source table is final_table_key
    if sink_schema is not None and replicate_structure:
        source_table = str(from_table) if from_table else table_key
        _save_custom_table_structure(
            sink_key, final_table_key, source_table, service
        )

    label = f"→ '{target}'" if target_exists and target else "(clone)"
    print_success(f"Added table '{final_table_key}' {label} to sink '{sink_key}'")
    return True


def remove_sink_table(service: str, sink_key: str, table_key: str) -> bool:
    """Remove *table_key* from a service sink.

    Also removes the related custom-table YAML file from
    ``service-schemas/{target_service}/custom-tables/`` if it exists.

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
        print_warning(f"Table '{table_key}' not found in sink '{sink_key}'")
        return False

    del tables[table_key]
    if not save_service_config(service, config):
        return False

    # Clean up custom-table YAML if it exists
    _remove_custom_table_file(sink_key, table_key)

    print_success(f"Removed table '{table_key}' from sink '{sink_key}'")
    return True


def _remove_custom_table_file(sink_key: str, table_key: str) -> None:
    """Remove the custom-table YAML reference file if it exists.

    Looks for ``service-schemas/{target_service}/custom-tables/{table_key}.yaml``.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Table key (e.g., 'directory-clone.customers').
    """
    target_service = _get_target_service_from_sink_key(sink_key)
    if not target_service:
        return

    custom_file = (
        SERVICE_SCHEMAS_DIR
        / target_service
        / "custom-tables"
        / f"{table_key.replace('/', '_')}.yaml"
    )

    if custom_file.is_file():
        custom_file.unlink()
        print_info(
            "Removed custom table file: "
            +f"service-schemas/{target_service}/custom-tables/"
            +f"{table_key.replace('/', '_')}.yaml"
        )


def _validate_schema_update_inputs(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Validate inputs for update_sink_table_schema.

    Returns:
        Tuple of (config, tables_dict) if valid, None on error.
    """
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

    return config, tables


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
    # Validate new schema name
    schema_error = validate_pg_schema_name(new_schema)
    if schema_error:
        print_error(schema_error)
        return False

    result = _validate_schema_update_inputs(service, sink_key, table_key)
    if result is None:
        return False

    config, tables = result

    # Parse current table key to get table name
    if "." not in table_key:
        print_error(
            f"Invalid table key '{table_key}': expected 'schema.table' format"
        )
        return False

    parts = table_key.split(".", 1)
    old_schema = parts[0]
    table_name = parts[1]
    new_table_key = f"{new_schema}.{table_name}"

    # Check if new table key already exists
    if new_table_key in tables:
        print_error(
            f"Table '{new_table_key}' already exists in sink '{sink_key}'"
        )
        return False

    # Move the table config to new key
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


def _load_table_columns(
    service: str,
    table_key: str,
) -> list[dict[str, Any]] | None:
    """Load column definitions from service-schemas/{service}/{schema}/{table}.yaml.

    Returns:
        List of column dicts (name, type, nullable, primary_key), or None.
    """
    if "." not in table_key:
        return None

    schema, table = table_key.split(".", 1)
    schema_file = SERVICE_SCHEMAS_DIR / service / schema / f"{table}.yaml"
    if not schema_file.exists():
        return None

    try:
        from cdc_generator.helpers.yaml_loader import load_yaml_file

        data = load_yaml_file(schema_file)
        columns = data.get("columns", [])
        if isinstance(columns, list):
            return cast(list[dict[str, Any]], columns)
    except (FileNotFoundError, ValueError):
        pass
    return None


def _get_column_type(
    columns: list[dict[str, Any]],
    column_name: str,
) -> str | None:
    """Find a column's type from a column definitions list."""
    for col in columns:
        if col.get("name") == column_name:
            raw_type = col.get("type")
            return str(raw_type) if raw_type is not None else None
    return None


def _check_type_compatibility(
    source_type: str,
    sink_type: str,
    source_engine: str = "pgsql",
    sink_engine: str = "pgsql",
) -> bool:
    """Check if source_type is compatible with sink_type.

    Uses TypeMapper to normalize types, then compares.
    """
    try:
        from cdc_generator.helpers.type_mapper import TypeMapper

        mapper = TypeMapper(source_engine, sink_engine)
        mapped = mapper.map_type(source_type)
        # Normalize both sides for comparison
        sink_mapper = TypeMapper(sink_engine, sink_engine)
        normalized_sink = sink_mapper.map_type(sink_type)
        normalized_mapped = sink_mapper.map_type(mapped)
        return normalized_mapped == normalized_sink
    except FileNotFoundError:
        # No mapping file — fall back to direct string comparison
        return source_type.lower() == sink_type.lower()


def _resolve_source_table_from_sink(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
) -> str | None:
    """Resolve the source table for a sink table entry.

    Checks the 'from' field first, then falls back to matching source tables.
    """
    sinks = _get_sinks_dict(config)
    sink_cfg = _resolve_sink_config(sinks, sink_key)
    if sink_cfg is None:
        return None

    tables = _get_sink_tables(sink_cfg)
    tbl_raw = tables.get(table_key)
    if not isinstance(tbl_raw, dict):
        return None

    tbl_cfg = cast(dict[str, object], tbl_raw)

    # Check explicit 'from' field
    from_table = tbl_cfg.get("from")
    if isinstance(from_table, str):
        return from_table

    # Fall back: check if sink table_key matches a source table
    source_tables = _get_source_table_keys(config)
    if table_key in source_tables:
        return table_key

    return None


def _validate_column_mappings(
    column_mappings: list[tuple[str, str]],
    source_columns: list[dict[str, Any]],
    sink_columns: list[dict[str, Any]],
    source_table: str,
    table_key: str,
) -> list[str]:
    """Validate column mapping pairs against source/sink schemas.

    Returns:
        List of error messages (empty = all valid).
    """
    source_col_names = {col["name"] for col in source_columns if "name" in col}
    sink_col_names = {col["name"] for col in sink_columns if "name" in col}

    errors: list[str] = []
    for src_col, tgt_col in column_mappings:
        if src_col not in source_col_names:
            errors.append(
                f"Source column '{src_col}' not found in "
                + f"'{source_table}' (available: "
                + f"{', '.join(sorted(source_col_names))})"
            )
            continue

        if tgt_col not in sink_col_names:
            errors.append(
                f"Sink column '{tgt_col}' not found in "
                + f"'{table_key}' (available: "
                + f"{', '.join(sorted(sink_col_names))})"
            )
            continue

        # Type compatibility check
        src_type = _get_column_type(source_columns, src_col)
        tgt_type = _get_column_type(sink_columns, tgt_col)
        if src_type and tgt_type and not _check_type_compatibility(src_type, tgt_type):
            errors.append(
                f"Type mismatch: '{src_col}' ({src_type}) "
                + f"→ '{tgt_col}' ({tgt_type})"
            )
    return errors


def _apply_column_mappings(
    tables: dict[str, object],
    table_key: str,
    column_mappings: list[tuple[str, str]],
) -> dict[str, str]:
    """Write column mapping pairs into the table config.

    Returns:
        The columns dict after applying.
    """
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

    for src_col, tgt_col in column_mappings:
        cols[src_col] = tgt_col
    return cols


def _warn_unmapped_required(
    sink_columns: list[dict[str, Any]],
    cols: dict[str, str],
    source_col_names: set[str],
    sink_col_names: set[str],
) -> None:
    """Warn about unmapped non-nullable sink columns."""
    mapped_sink_cols = set(cols.values())
    identity_mapped = source_col_names & sink_col_names
    all_covered = mapped_sink_cols | identity_mapped

    unmapped_required: list[str] = []
    for col in sink_columns:
        col_name = col.get("name", "")
        nullable = col.get("nullable", True)
        is_pk = col.get("primary_key", False)
        if (not nullable or is_pk) and col_name not in all_covered:
            unmapped_required.append(col_name)

    if unmapped_required:
        print_warning(
            "Unmapped required sink columns (non-nullable or PK): "
            + ", ".join(sorted(unmapped_required))
        )


def _load_schemas_for_mapping(
    source_service: str,
    source_table: str,
    target_service: str,
    table_key: str,
    service: str,
    sink_key: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    """Load source and sink column schemas for column mapping.

    Returns:
        (source_columns, sink_columns) tuple, or None on error.
    """
    source_columns = _load_table_columns(source_service, source_table)
    sink_columns = _load_table_columns(target_service, table_key)

    if source_columns is None:
        src_path = source_table.replace(".", "/")
        print_error(
            "Source table schema not found: "
            + f"service-schemas/{source_service}/{src_path}.yaml"
        )
        print_info(
            "Run: cdc manage-service --service "
            + f"{service} --inspect --all --save"
        )
        return None

    if sink_columns is None:
        tgt_path = table_key.replace(".", "/")
        print_error(
            "Sink table schema not found: "
            + f"service-schemas/{target_service}/{tgt_path}.yaml"
        )
        print_info(
            f"Run: cdc manage-service --service {service}"
            + f" --inspect-sink {sink_key} --all --save"
        )
        return None

    return source_columns, sink_columns


@dataclass
class _MappingContext:
    """Resolved context needed for column mapping."""

    config: dict[str, object]
    tables: dict[str, object]
    source_table: str
    source_columns: list[dict[str, Any]]
    sink_columns: list[dict[str, Any]]


def _resolve_mapping_context(
    service: str,
    sink_key: str,
    table_key: str,
) -> _MappingContext | None:
    """Resolve and validate all context needed for column mapping.

    Returns:
        _MappingContext on success, None on error (messages printed).
    """
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
        available = [str(k) for k in tables]
        if available:
            print_info(f"Available tables: {', '.join(available)}")
        return None

    source_table = _resolve_source_table_from_sink(config, sink_key, table_key)
    if source_table is None:
        print_error(
            f"Cannot determine source table for '{table_key}' in sink '{sink_key}'"
        )
        print_info(
            "Ensure the sink table has a 'from' field or matches a source table"
        )
        return None

    target_service = _get_target_service_from_sink_key(sink_key)
    if target_service is None:
        print_error(f"Invalid sink key format: '{sink_key}'")
        return None

    schemas = _load_schemas_for_mapping(
        service, source_table, target_service, table_key, service, sink_key,
    )
    if schemas is None:
        return None
    source_columns, sink_columns = schemas

    return _MappingContext(
        config=config,
        tables=tables,
        source_table=source_table,
        source_columns=source_columns,
        sink_columns=sink_columns,
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
        column_mappings, ctx.source_columns, ctx.sink_columns,
        ctx.source_table, table_key,
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


def _format_mapped_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single *mapped* table (target_exists=true)."""
    target = tbl_cfg.get("target", "?")
    cols_raw = tbl_cfg.get("columns", {})
    cols = cast(dict[str, str], cols_raw) if isinstance(cols_raw, dict) else {}
    col_count = len(cols)

    line = (
        f"  {Colors.YELLOW}→{Colors.RESET} "
        f"{Colors.CYAN}{tbl_key}{Colors.RESET} "
        f"→ {Colors.OKGREEN}{target}{Colors.RESET} "
        f"{Colors.DIM}(mapped, {col_count} columns){Colors.RESET}"
    )
    print(line)
    for src_col, tgt_col in cols.items():
        print(f"    {Colors.DIM}{src_col} → {tgt_col}{Colors.RESET}")


def _format_cloned_table(
    tbl_key: str,
    tbl_cfg: dict[str, object],
) -> None:
    """Print a single *cloned* table (target_exists=false/absent)."""
    target_schema = tbl_cfg.get("target_schema")
    inc_raw = tbl_cfg.get("include_columns", [])
    inc_cols = cast(list[str], inc_raw) if isinstance(inc_raw, list) else []

    extras: list[str] = []
    if target_schema:
        extras.append(f"schema: {target_schema}")
    if inc_cols:
        extras.append(f"{len(inc_cols)} columns")

    extra_str = f" ({', '.join(extras)})" if extras else ""
    line = (
        f"  {Colors.OKGREEN}≡{Colors.RESET} "
        f"{Colors.CYAN}{tbl_key}{Colors.RESET} "
        f"{Colors.DIM}(clone{extra_str}){Colors.RESET}"
    )
    print(line)


def _format_sink_entry(
    sink_key: str,
    sink_cfg: dict[str, object],
) -> None:
    """Print header + table rows for one sink entry."""
    # Header
    parsed = _parse_sink_key(sink_key)
    if parsed:
        sg, ts = parsed
        header = (
            f"\n{Colors.BOLD}{Colors.CYAN}{sink_key}{Colors.RESET}"
            f"  {Colors.DIM}(group: {sg}, target: {ts}){Colors.RESET}"
        )
        print(header)
    else:
        print(f"\n{Colors.BOLD}{Colors.CYAN}{sink_key}{Colors.RESET}")

    # Tables
    tables_raw = sink_cfg.get("tables", {})
    tables = cast(dict[str, object], tables_raw) if isinstance(tables_raw, dict) else {}
    if not tables:
        print(f"  {Colors.DIM}No tables configured{Colors.RESET}")
        return

    for tbl_key_raw, tbl_raw in tables.items():
        tbl_key = str(tbl_key_raw)
        tbl_cfg = cast(dict[str, object], tbl_raw) if isinstance(tbl_raw, dict) else {}
        if tbl_cfg.get("target_exists", False):
            _format_mapped_table(tbl_key, tbl_cfg)
        else:
            _format_cloned_table(tbl_key, tbl_cfg)

    # Databases
    db_raw = sink_cfg.get("databases", {})
    if isinstance(db_raw, dict) and db_raw:
        databases = cast(dict[str, object], db_raw)
        print(f"  {Colors.DIM}Databases:{Colors.RESET}")
        for env, db in databases.items():
            print(f"    {env}: {db}")


def list_sinks(service: str) -> bool:
    """List all sinks configured for *service*.

    Returns:
        True if sinks were found and displayed, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return False

    sinks = cast(dict[str, object], sinks_raw)
    print_header(f"Sinks for service '{service}'")

    for sk_raw, sc_raw in sinks.items():
        sk = str(sk_raw)
        sc = cast(dict[str, object], sc_raw) if isinstance(sc_raw, dict) else {}
        _format_sink_entry(sk, sc)

    src_count = len(_get_source_table_keys(config))
    print(f"\n{Colors.DIM}Source tables: {src_count}{Colors.RESET}")
    return True


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_single_sink(
    sink_key_str: str,
    sink_raw: object,
    source_tables: list[str],
) -> list[str]:
    """Validate one sink entry, returning a list of error messages."""
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

        # Validate schema portion of table key
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

        # REQUIRED: target_exists must be present
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


def validate_sinks(service: str) -> bool:
    """Validate sink configuration for *service*.

    Checks sink key format, sink group existence, source table presence,
    and required fields for target_exists=true tables.

    Returns:
        True if all validations pass, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        print_info(f"No sinks configured for service '{service}'")
        return True

    sinks = cast(dict[str, object], sinks_raw)
    source_tables = _get_source_table_keys(config)
    all_valid = True

    for sk_raw, sc_raw in sinks.items():
        for error in _validate_single_sink(str(sk_raw), sc_raw, source_tables):
            print_error(f"  ✗ {error}")
            all_valid = False

    if all_valid:
        print_success(f"Sink configuration for service '{service}' is valid")
    else:
        print_error(f"Sink validation failed for service '{service}'")

    return all_valid
