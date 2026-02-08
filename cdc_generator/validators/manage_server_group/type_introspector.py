"""Introspect column types from database servers.

Queries the system catalog (pg_type / sys.types) and writes/updates
the type definition files under service-schemas/types/{engine}.yaml.

Used by:
    cdc manage-sink-groups --sink-group <name> --introspect-types [--server <name>]
    cdc manage-source-groups --introspect-types [--server <name>]
"""

from typing import Any

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root

try:
    from cdc_generator.helpers.yaml_loader import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TYPES_DIR = get_project_root() / "cdc_generator" / "service-schemas" / "types"

# PostgreSQL category mapping (pg_type.typcategory → human category)
_PG_CATEGORY_MAP: dict[str, str] = {
    "N": "numeric",
    "S": "text",
    "B": "boolean",
    "D": "date_time",
    "U": "uuid",      # user-defined, but uuid lives here
    "V": "bit_string",
    "G": "geometric",
    "I": "network",
    "A": "array",
    "R": "range",
    "E": "enum",
}

# PostgreSQL types that should be categorised as UUID despite typcategory='U'
_PG_UUID_TYPES = frozenset({"uuid"})

# PostgreSQL types that should be categorised as JSON
_PG_JSON_TYPES = frozenset({"json", "jsonb"})

# PostgreSQL types that should be categorised as binary
_PG_BINARY_TYPES = frozenset({"bytea"})

# MSSQL category mapping (based on system_type_id ranges and names)
_MSSQL_CATEGORY_MAP: dict[str, str] = {
    # Exact numeric
    "tinyint": "numeric",
    "smallint": "numeric",
    "int": "numeric",
    "bigint": "numeric",
    "decimal": "numeric",
    "numeric": "numeric",
    "float": "numeric",
    "real": "numeric",
    "money": "numeric",
    "smallmoney": "numeric",
    # Text
    "char": "text",
    "varchar": "text",
    "nchar": "text",
    "nvarchar": "text",
    "text": "text",
    "ntext": "text",
    # Boolean
    "bit": "boolean",
    # Date/Time
    "date": "date_time",
    "time": "date_time",
    "datetime": "date_time",
    "datetime2": "date_time",
    "datetimeoffset": "date_time",
    "smalldatetime": "date_time",
    # UUID
    "uniqueidentifier": "uuid",
    # Binary
    "binary": "binary",
    "varbinary": "binary",
    "image": "binary",
    # XML
    "xml": "xml",
    # Misc
    "sql_variant": "other",
    "geography": "spatial",
    "geometry": "spatial",
    "hierarchyid": "other",
    "timestamp": "binary",  # MSSQL timestamp = rowversion
}

_EXPECTED_PG_TYPE_COUNT = 5

# Max types to show in summary preview line
_TYPE_PREVIEW_LIMIT = 5


# ---------------------------------------------------------------------------
# PostgreSQL introspection
# ---------------------------------------------------------------------------


def _introspect_postgres_types(conn_params: dict[str, Any]) -> dict[str, list[str]] | None:
    """Query pg_type catalog and return categorised types.

    Args:
        conn_params: dict with host, port, user, password, database.

    Returns:
        {category: [type_name, ...]} or None on error.
    """
    try:
        import psycopg2  # type: ignore[import-not-found]
    except ImportError:
        print_error("psycopg2 not installed — use: pip install psycopg2-binary")
        return None

    query = """
    SELECT t.typname, t.typcategory
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    WHERE n.nspname = 'pg_catalog'
      AND t.typtype IN ('b', 'e', 'r')
      AND t.typname NOT LIKE E'\\\\_%'
      AND t.typname NOT IN ('void', 'trigger', 'event_trigger',
                            'internal', 'language_handler',
                            'fdw_handler', 'index_am_handler',
                            'tsm_handler', 'table_am_handler',
                            'any', 'anyelement', 'anyarray',
                            'anynonarray', 'anyenum', 'anyrange',
                            'record', 'cstring', 'unknown',
                            'opaque', 'pg_ddl_command')
    ORDER BY t.typcategory, t.typname
    """

    try:
        database = conn_params.get("database") or "postgres"
        print_info(
            "Connecting to PostgreSQL: "
            + f"{conn_params['host']}:{conn_params['port']}/{database}"
        )
        conn = psycopg2.connect(  # type: ignore[misc]
            host=conn_params["host"],
            port=conn_params["port"],
            database=database,
            user=conn_params["user"],
            password=conn_params["password"],
            connect_timeout=10,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows: list[tuple[str, str]] = cursor.fetchall()
        conn.close()
    except Exception as exc:
        print_error(f"Failed to query PostgreSQL types: {exc}")
        return None

    categories: dict[str, list[str]] = {}
    for typname, typcategory in rows:
        # Determine category
        if typname in _PG_UUID_TYPES:
            cat = "uuid"
        elif typname in _PG_JSON_TYPES:
            cat = "json"
        elif typname in _PG_BINARY_TYPES:
            cat = "binary"
        else:
            cat = _PG_CATEGORY_MAP.get(typcategory, "other")

        categories.setdefault(cat, []).append(typname)

    if len(categories) < _EXPECTED_PG_TYPE_COUNT:
        print_warning(
            f"Only {len(categories)} type categories found — "
            + "expected more. Check database connectivity."
        )

    return categories


# ---------------------------------------------------------------------------
# MSSQL introspection
# ---------------------------------------------------------------------------


def _introspect_mssql_types(conn_params: dict[str, Any]) -> dict[str, list[str]] | None:
    """Query sys.types catalog and return categorised types.

    Args:
        conn_params: dict with host, port, user, password.

    Returns:
        {category: [type_name, ...]} or None on error.
    """
    try:
        from cdc_generator.helpers.helpers_mssql import create_mssql_connection
    except ImportError:
        print_error("pymssql not installed — use: pip install pymssql")
        return None

    query = """
    SELECT name
    FROM sys.types
    WHERE is_user_defined = 0
    ORDER BY name
    """

    try:
        print_info(
            "Connecting to MSSQL: "
            + f"{conn_params['host']}:{conn_params['port']}"
        )
        conn = create_mssql_connection(
            host=conn_params["host"],
            port=conn_params["port"],
            database="master",
            user=conn_params["user"],
            password=conn_params["password"],
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
    except Exception as exc:
        print_error(f"Failed to query MSSQL types: {exc}")
        return None

    categories: dict[str, list[str]] = {}
    for row in rows:
        type_name = str(row[0])
        cat = _MSSQL_CATEGORY_MAP.get(type_name, "other")
        categories.setdefault(cat, []).append(type_name)

    return categories


# ---------------------------------------------------------------------------
# YAML generation
# ---------------------------------------------------------------------------


def _load_existing_type_file(engine: str) -> dict[str, Any] | None:
    """Load existing type definition YAML, or None if not found."""
    if yaml is None:
        return None
    type_file = _TYPES_DIR / f"{engine}.yaml"
    if not type_file.is_file():
        return None
    with type_file.open(encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]


def _build_type_yaml(
    _engine: str,
    categories: dict[str, list[str]],
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the type definition YAML structure.

    Merges introspected types with existing constraints/defaults
    from the static file (if it exists).
    """
    existing_categories: dict[str, Any] = (
        existing.get("categories", {}) if existing else {}
    )
    existing_constraints: dict[str, Any] = (
        existing.get("constraints", {}) if existing else {}
    )
    existing_defaults: dict[str, Any] = (
        existing.get("common_defaults", {}) if existing else {}
    )

    # Build categories — keep existing constraints/defaults, update types
    new_categories: dict[str, dict[str, Any]] = {}
    for cat_name, type_list in sorted(categories.items()):
        old_cat = existing_categories.get(cat_name, {})
        cat_entry: dict[str, Any] = {
            "types": sorted(type_list),
        }
        # Preserve constraints and defaults from existing file
        if old_cat.get("constraints"):
            cat_entry["constraints"] = old_cat["constraints"]
        if old_cat.get("defaults"):
            cat_entry["defaults"] = old_cat["defaults"]
        new_categories[cat_name] = cat_entry

    # Keep categories from existing file that weren't in introspection
    # (they might have been manually curated, e.g. array types)
    for cat_name, cat_data in existing_categories.items():
        if cat_name not in new_categories:
            new_categories[cat_name] = cat_data

    result: dict[str, Any] = {
        "categories": new_categories,
    }

    # Preserve constraints and common_defaults sections
    if existing_constraints:
        result["constraints"] = existing_constraints
    if existing_defaults:
        result["common_defaults"] = existing_defaults

    return result


def _save_type_file(engine: str, data: dict[str, Any]) -> bool:
    """Save type definition YAML file."""
    if yaml is None:
        print_error("PyYAML is required for type file generation")
        return False

    _TYPES_DIR.mkdir(parents=True, exist_ok=True)
    type_file = _TYPES_DIR / f"{engine}.yaml"

    header = (
        f"# {engine.upper()} column types for custom table definitions\n"
        "# Auto-generated by: cdc manage-sink-groups --introspect-types\n"
        "# Manual edits to constraints/defaults will be preserved on re-run.\n"
        "#\n"
        "# Categories group related types. The 'types' list provides autocomplete values.\n"
        "\n"
    )

    with type_file.open("w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(data, f)

    return True


def _print_type_summary(
    engine_label: str,
    categories: dict[str, list[str]],
) -> None:
    """Print a summary of discovered types."""
    total = sum(len(types) for types in categories.values())
    print_success(
        f"Discovered {total} {engine_label} types "
        + f"in {len(categories)} categories"
    )
    for cat_name, type_list in sorted(categories.items()):
        type_preview = ", ".join(type_list[:_TYPE_PREVIEW_LIMIT])
        extra = len(type_list) - _TYPE_PREVIEW_LIMIT
        suffix = f", ... (+{extra})" if extra > 0 else ""
        print_info(f"  {cat_name}: {type_preview}{suffix}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def introspect_types(
    engine: str,
    conn_params: dict[str, Any],
) -> bool:
    """Introspect types from a database server and save to YAML.

    Args:
        engine: Database engine ('postgres' or 'mssql').
        conn_params: Connection parameters (host, port, user, password, [database]).

    Returns:
        True on success.
    """
    if engine == "postgres":
        categories = _introspect_postgres_types(conn_params)
    elif engine == "mssql":
        categories = _introspect_mssql_types(conn_params)
    else:
        print_error(
            f"Unsupported engine '{engine}' for type introspection. "
            + "Supported: postgres, mssql"
        )
        return False

    if categories is None:
        return False

    # Load existing file to preserve constraints/defaults
    existing = _load_existing_type_file(engine)

    # Build and save
    type_data = _build_type_yaml(engine, categories, existing)
    if not _save_type_file(engine, type_data):
        return False

    _print_type_summary(engine.upper(), categories)

    type_file = _TYPES_DIR / f"{engine}.yaml"
    print_info(f"Saved to: {type_file.relative_to(get_project_root())}")

    if existing:
        print_info(
            "Constraints and defaults preserved from existing file"
        )
    else:
        print_warning(
            "No existing file found — constraints and defaults"
            + " sections are empty"
        )
        print_info(
            "Edit the generated file to add constraints and"
            + " default expressions per category"
        )

    return True
