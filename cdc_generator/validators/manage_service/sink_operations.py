"""Sink operations for CDC service configuration.

Handles adding, removing, and listing sink configurations in service YAML files.
Sinks define WHERE source tables are sent and HOW they are mapped.

Sink key format: {sink_group}.{target_service}
    - sink_group: references sink-groups.yaml (e.g., sink_asma)
    - target_service: target service/database in that sink group
"""

from typing import cast

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config

from .config import SERVICE_SCHEMAS_DIR, save_service_config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SINK_KEY_SEPARATOR = "."
_SINK_KEY_PARTS = 2


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
    """List all tables in service-schemas/{target_service}/{schema}/*.yaml.

    Returns:
        List of 'schema.table' strings.
    """
    service_dir = SERVICE_SCHEMAS_DIR / target_service
    if not service_dir.is_dir():
        return []

    tables: list[str] = []
    for schema_dir in service_dir.iterdir():
        if schema_dir.is_dir():
            for table_file in schema_dir.glob("*.yaml"):
                tables.append(f"{schema_dir.name}.{table_file.stem}")
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

    service_dir = SERVICE_SCHEMAS_DIR / target_service
    if not service_dir.is_dir():
        print_error(f"No schemas found for sink target '{target_service}'")
        print_info(
            "To fetch schemas, run:\n"
            + "  cdc manage-service --service <SERVICE>"
            + f" --inspect-sink {sink_key} --all --save"
        )
        print_info(
            "Or create manually: "
            + f"service-schemas/{target_service}/<schema>/<Table>.yaml"
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


def _build_table_config(
    *,
    target_exists: bool,
    target: str | None,
    target_schema: str | None,
    include_columns: list[str] | None,
    columns: dict[str, str] | None = None,
) -> dict[str, object]:
    """Build the per-table config dict from the given options.

    target_exists is ALWAYS included in the output.
    """
    cfg: dict[str, object] = {"target_exists": target_exists}

    if target_exists:
        if target:
            cfg["target"] = target
        if columns:
            cfg["columns"] = columns
    else:
        if target_schema:
            cfg["target_schema"] = target_schema
        if include_columns:
            cfg["include_columns"] = include_columns
    return cfg


def _validate_table_add(
    config: dict[str, object],
    sink_key: str,
    table_key: str,
    table_opts: dict[str, object],
) -> tuple[dict[str, object] | None, str | None]:
    """Validate parameters for adding table to sink.

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

    # Validate table exists in service-schemas for the sink target
    if not _validate_table_in_schemas(sink_key, table_key):
        return None, None  # Error already printed

    return tables, None


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
            include_columns, columns.

    Returns:
        True on success, False otherwise.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return False

    opts = table_opts if table_opts is not None else {}
    tables, error = _validate_table_add(config, sink_key, table_key, opts)

    if error:
        print_error(error)
        return False
    if tables is None:
        return False

    target_exists = bool(opts.get("target_exists", False))
    target = opts.get("target")
    tables[table_key] = _build_table_config(
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
    )

    if not save_service_config(service, config):
        return False

    label = f"→ '{target}'" if target_exists and target else "(clone)"
    print_success(f"Added table '{table_key}' {label} to sink '{sink_key}'")
    return True


def remove_sink_table(service: str, sink_key: str, table_key: str) -> bool:
    """Remove *table_key* from a service sink.

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

    print_success(f"Removed table '{table_key}' from sink '{sink_key}'")
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
