"""Type definitions management for service schemas.

Generates and manages type definition files under
``service-schemas/definitions/{pgsql|mssql}.yaml`` in the
implementation repo (project root).

These definitions are used for:
- Autocomplete when defining custom table columns
- Validation of column type specs
- Future: type mapping between DB engines
"""

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.mssql_loader import has_pymssql
from cdc_generator.helpers.psycopg2_loader import (
    create_postgres_connection,
    has_psycopg2,
)
from cdc_generator.helpers.service_config import get_project_root

try:
    from cdc_generator.helpers.yaml_loader import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------

_DEFINITIONS_DIR_NAME = "definitions"


def _get_definitions_dir() -> Path:
    """Return path to service-schemas/definitions/ in project root."""
    return get_project_root() / "service-schemas" / _DEFINITIONS_DIR_NAME


# -------------------------------------------------------------------
# Internal: introspect types from DB
# -------------------------------------------------------------------


def _introspect_postgres_types(
    conn_params: dict[str, Any],
) -> dict[str, list[str]] | None:
    """Query pg_type catalog for all base types.

    Returns:
        {category: [type_name, ...]} or None on error.
    """
    if not has_psycopg2:
        print_error(
            "psycopg2 not installed — "
            + "use: pip install psycopg2-binary"
        )
        return None

    # Category mapping (pg_type.typcategory → human label)
    pg_category_map: dict[str, str] = {
        "N": "numeric",
        "S": "text",
        "B": "boolean",
        "D": "date_time",
        "U": "uuid",
        "V": "bit_string",
        "G": "geometric",
        "I": "network",
        "A": "array",
        "R": "range",
        "E": "enum",
    }
    uuid_types = frozenset({"uuid"})
    json_types = frozenset({"json", "jsonb"})
    binary_types = frozenset({"bytea"})

    query = """
    SELECT t.typname, t.typcategory
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    WHERE n.nspname = 'pg_catalog'
      AND t.typtype IN ('b', 'e', 'r')
      AND t.typname NOT LIKE E'\\\\_%'
      AND t.typname NOT IN (
            'void', 'trigger', 'event_trigger',
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
        conn = create_postgres_connection(
            host=conn_params["host"],
            port=int(conn_params["port"]),
            dbname=database,
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
        if typname in uuid_types:
            cat = "uuid"
        elif typname in json_types:
            cat = "json"
        elif typname in binary_types:
            cat = "binary"
        else:
            cat = pg_category_map.get(typcategory, "other")
        categories.setdefault(cat, []).append(typname)

    return categories


def _introspect_mssql_types(
    conn_params: dict[str, Any],
) -> dict[str, list[str]] | None:
    """Query sys.types catalog for all base types.

    Returns:
        {category: [type_name, ...]} or None on error.
    """
    if not has_pymssql:
        print_error(
            "pymssql not installed — use: pip install pymssql"
        )
        return None

    mssql_map: dict[str, str] = {
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
        "char": "text",
        "varchar": "text",
        "nchar": "text",
        "nvarchar": "text",
        "text": "text",
        "ntext": "text",
        "bit": "boolean",
        "date": "date_time",
        "time": "date_time",
        "datetime": "date_time",
        "datetime2": "date_time",
        "datetimeoffset": "date_time",
        "smalldatetime": "date_time",
        "uniqueidentifier": "uuid",
        "binary": "binary",
        "varbinary": "binary",
        "image": "binary",
        "xml": "xml",
        "sql_variant": "other",
        "geography": "spatial",
        "geometry": "spatial",
        "hierarchyid": "other",
        "timestamp": "binary",
    }

    try:
        conn = create_mssql_connection(
            host=conn_params["host"],
            port=conn_params["port"],
            database="master",
            user=conn_params["user"],
            password=conn_params["password"],
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sys.types "
            + "WHERE is_user_defined = 0 ORDER BY name"
        )
        rows = cursor.fetchall()
        conn.close()
    except Exception as exc:
        print_error(f"Failed to query MSSQL types: {exc}")
        return None

    categories: dict[str, list[str]] = {}
    for row in rows:
        type_name = str(row[0])
        cat = mssql_map.get(type_name, "other")
        categories.setdefault(cat, []).append(type_name)

    return categories


# -------------------------------------------------------------------
# YAML read / write
# -------------------------------------------------------------------


def load_type_definitions(
    engine: str,
) -> dict[str, list[str]] | None:
    """Load type definitions from definitions YAML file.

    Args:
        engine: Database engine ('pgsql' or 'mssql').

    Returns:
        {category: [type_name, ...]} or None if not found.
    """
    if yaml is None:
        return None

    defs_dir = _get_definitions_dir()
    type_file = defs_dir / f"{engine}.yaml"
    if not type_file.is_file():
        return None

    with type_file.open(encoding="utf-8") as f:
        raw = yaml.load(f)

    if not isinstance(raw, dict) or "categories" not in raw:
        return None

    data: dict[str, Any] = dict(raw)
    categories: dict[str, list[str]] = {}
    raw_cats: dict[str, Any] = data.get("categories", {})
    for cat_name, cat_data in raw_cats.items():
        types_list: list[Any]
        if isinstance(cat_data, dict):
            cat_dict = cast(dict[str, Any], cat_data)
            types_list = list(cat_dict.get("types", []))
        elif isinstance(cat_data, list):
            types_list = cast(list[Any], cat_data)
        else:
            continue
        categories[str(cat_name)] = [str(t) for t in types_list]

    return categories


def get_all_type_names(engine: str) -> list[str]:
    """Get flat list of all type names for an engine.

    Useful for autocomplete and validation.

    Args:
        engine: Database engine ('pgsql' or 'mssql').

    Returns:
        Sorted list of type names, or empty list.
    """
    categories = load_type_definitions(engine)
    if not categories:
        return []

    all_types: list[str] = []
    for types in categories.values():
        all_types.extend(types)

    return sorted(set(all_types))


def _build_type_yaml(
    categories: dict[str, list[str]],
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the type definition YAML structure.

    Merges introspected types with existing constraints/defaults.
    """
    existing_cats: dict[str, Any] = (
        existing.get("categories", {}) if existing else {}
    )

    new_cats: dict[str, dict[str, Any]] = {}
    for cat_name, type_list in sorted(categories.items()):
        old_cat_raw: Any = existing_cats.get(cat_name, {})
        entry: dict[str, Any] = {"types": sorted(type_list)}
        # Preserve manually-added constraints and defaults
        if isinstance(old_cat_raw, dict):
            old_cat = cast(dict[str, Any], old_cat_raw)
            if old_cat.get("constraints"):
                entry["constraints"] = old_cat["constraints"]
            if old_cat.get("defaults"):
                entry["defaults"] = old_cat["defaults"]
        new_cats[cat_name] = entry

    # Keep manually-added categories not in introspection
    for cat_name, cat_data in existing_cats.items():
        if cat_name not in new_cats:
            new_cats[cat_name] = cat_data

    result: dict[str, Any] = {"categories": new_cats}

    # Preserve top-level sections
    if existing:
        for key in ("constraints", "common_defaults"):
            if key in existing:
                result[key] = existing[key]

    return result


def _save_definitions_file(
    engine: str,
    data: dict[str, Any],
    *,
    source_label: str = "",
) -> bool:
    """Save type definitions to YAML file."""
    if yaml is None:
        print_error("PyYAML is required for type definitions")
        return False

    defs_dir = _get_definitions_dir()
    defs_dir.mkdir(parents=True, exist_ok=True)
    type_file = defs_dir / f"{engine}.yaml"

    source_line = (
        f"# Source: {source_label}\n" if source_label else ""
    )
    header = (
        f"# {engine.upper()} type definitions\n"
        +"# Auto-generated from database introspection\n"
        + source_line
        + "# Manual edits to constraints/defaults are "
        + "preserved on re-run.\n"
        +"#\n"
        +"# Categories group related types. "
        +"'types' lists provide autocomplete values.\n"
        +"\n"
    )

    with type_file.open("w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(data, f)

    return True


# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------

_TYPE_PREVIEW_LIMIT = 5


def generate_type_definitions(
    db_type: str,
    conn_params: dict[str, Any],
    *,
    source_label: str = "",
) -> bool:
    """Introspect DB types and save definitions YAML.

    Called automatically during ``--inspect --save`` and
    ``--inspect-sink --save``.

    Args:
        db_type: Database type ('postgres' or 'mssql').
        conn_params: Connection parameters.
        source_label: Label for the source (for YAML header).

    Returns:
        True on success.
    """
    engine = _db_type_to_engine(db_type)

    if db_type == "postgres":
        categories = _introspect_postgres_types(conn_params)
    elif db_type == "mssql":
        categories = _introspect_mssql_types(conn_params)
    else:
        print_error(
            f"Unsupported db_type '{db_type}' for type "
            + "definitions. Supported: postgres, mssql"
        )
        return False

    if categories is None:
        return False

    # Load existing to preserve manual edits
    existing = _load_existing(engine)

    # Build and save
    type_data = _build_type_yaml(categories, existing)
    if not _save_definitions_file(
        engine, type_data, source_label=source_label,
    ):
        return False

    # Print summary
    total = sum(len(t) for t in categories.values())
    print_success(
        f"Generated {engine}.yaml with {total} types "
        + f"in {len(categories)} categories"
    )
    for cat_name, type_list in sorted(categories.items()):
        preview = ", ".join(type_list[:_TYPE_PREVIEW_LIMIT])
        extra = len(type_list) - _TYPE_PREVIEW_LIMIT
        suffix = f", ... (+{extra})" if extra > 0 else ""
        print_info(f"  {cat_name}: {preview}{suffix}")

    defs_dir = _get_definitions_dir()
    type_file = defs_dir / f"{engine}.yaml"
    print_info(
        f"Saved to: {type_file.relative_to(get_project_root())}"
    )

    return True


def _db_type_to_engine(db_type: str) -> str:
    """Map database type string to engine file name."""
    if db_type == "postgres":
        return "pgsql"
    if db_type == "mssql":
        return "mssql"
    return db_type


def _load_existing(engine: str) -> dict[str, Any] | None:
    """Load existing definitions file if present."""
    if yaml is None:
        return None
    type_file = _get_definitions_dir() / f"{engine}.yaml"
    if not type_file.is_file():
        return None
    with type_file.open(encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]
