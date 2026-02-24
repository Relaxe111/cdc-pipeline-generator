"""Save detailed database schemas to YAML files."""

from pathlib import Path
from typing import Any, cast

import yaml

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.mssql_loader import has_pymssql
from cdc_generator.helpers.psycopg2_loader import (
    ensure_psycopg2,
    has_psycopg2,
)
from cdc_generator.helpers.service_schema_paths import (
    get_service_schema_write_dir,
)

from .db_inspector_common import get_connection_params, get_service_db_config

_TRACKED_TABLES_FILE = "tracked-tables.yaml"
_SCHEMA_TABLE_PARTS = 2


def _tracked_tables_file_path(service: str) -> Path:
    """Return tracked-tables.yaml path for a service under services/_schemas."""
    service_schema_dir = get_service_schema_write_dir(service)
    return service_schema_dir / _TRACKED_TABLES_FILE


def _normalize_table_ref(table_ref: str) -> tuple[str, str] | None:
    """Normalize ``schema.table`` into a tuple, or ``None`` if invalid."""
    normalized = table_ref.strip()
    parts = normalized.split(".", 1)
    if len(parts) != _SCHEMA_TABLE_PARTS:
        return None

    schema_name = parts[0].strip()
    table_name = parts[1].strip()
    if not schema_name or not table_name:
        return None

    return schema_name, table_name


def load_tracked_tables(service: str) -> dict[str, list[str]]:
    """Load tracked tables from ``services/_schemas/{service}/tracked-tables.yaml``."""
    tracked_file = _tracked_tables_file_path(service)
    if not tracked_file.exists():
        return {}

    with tracked_file.open() as f:
        raw_data = yaml.safe_load(f)

    if not isinstance(raw_data, dict):
        return {}

    raw_mapping = cast(dict[object, object], raw_data)

    tracked: dict[str, list[str]] = {}
    for schema_raw, tables_raw in raw_mapping.items():
        if not isinstance(schema_raw, str):
            continue
        schema_name = schema_raw.strip()
        if not schema_name or not isinstance(tables_raw, list):
            continue

        table_values = cast(list[object], tables_raw)

        table_names = [
            str(table_name).strip()
            for table_name in table_values
            if isinstance(table_name, str) and str(table_name).strip()
        ]
        if table_names:
            tracked[schema_name] = sorted(table_names)

    return tracked


def add_tracked_tables(
    service: str,
    table_refs: list[str],
) -> bool:
    """Add tracked ``schema.table`` refs to service tracked-tables.yaml."""
    tracked_file = _tracked_tables_file_path(service)
    existing = load_tracked_tables(service)

    merged: dict[str, set[str]] = {
        schema_name: set(table_names)
        for schema_name, table_names in existing.items()
    }

    added_count = 0
    for table_ref in table_refs:
        normalized = _normalize_table_ref(table_ref)
        if not normalized:
            continue
        schema_name, table_name = normalized
        merged.setdefault(schema_name, set())
        if table_name not in merged[schema_name]:
            merged[schema_name].add(table_name)
            added_count += 1

    if added_count == 0:
        return bool(existing)

    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        schema_name: sorted(table_names)
        for schema_name, table_names in sorted(merged.items())
        if table_names
    }

    with tracked_file.open("w") as f:
        yaml.dump(
            payload,
            f,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
        )

    print_success(
        f"✓ Updated tracked tables: {tracked_file} "
        + f"(+{added_count})"
    )
    return True


def filter_tables_by_tracked(
    service: str,
    tables: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter tables to tracked whitelist for a service.

    If no tracked-tables file (or empty file) exists, returns all tables.
    """
    tracked = load_tracked_tables(service)
    if not tracked:
        return tables

    tracked_refs = {
        f"{schema_name}.{table_name}".casefold()
        for schema_name, table_names in tracked.items()
        for table_name in table_names
    }

    filtered = [
        table
        for table in tables
        if f"{table.get('TABLE_SCHEMA')}.{table.get('TABLE_NAME')}".casefold()
        in tracked_refs
    ]

    print_info(
        f"Tracked table filter applied for '{service}': "
        + f"{len(filtered)}/{len(tables)} table(s) selected"
    )
    return filtered


def save_detailed_schema_mssql(
    service: str,
    schema: str,
    tables: list[dict[str, Any]],
    conn_params: dict[str, Any],
) -> dict[str, Any]:
    """Save detailed MSSQL table schema to YAML.

    Args:
        service: Service name
        schema: Database schema name
        tables: List of table dictionaries
        conn_params: Connection parameters

    Returns:
        Dictionary mapping table names to their schema data
    """
    if not has_pymssql:
        print_error(
            "pymssql not installed — "
            + "use: pip install pymssql"
        )
        return {}

    conn = create_mssql_connection(
        host=conn_params['host'],
        port=conn_params['port'],
        database=conn_params['database'],
        user=conn_params['user'],
        password=conn_params['password'],
    )
    cursor = conn.cursor()

    tables_data: dict[str, Any] = {}

    for i, table in enumerate(tables, 1):
        table_name = table['TABLE_NAME']
        # Use table's schema or fallback to parameter
        table_schema = table.get('TABLE_SCHEMA', schema)
        print(f"  [{i}/{len(tables)}] {table_schema}.{table_name}")

        # Get detailed column info
        cursor.execute(f"""
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = '{table_schema}'
                    AND tc.TABLE_NAME = '{table_name}'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = '{table_schema}'
                AND c.TABLE_NAME = '{table_name}'
            ORDER BY c.ORDINAL_POSITION
        """)

        columns: list[dict[str, Any]] = []
        primary_keys: list[str] = []
        for row in cursor.fetchall():
            col_name, data_type, _max_len, is_nullable, default_value, is_pk = row
            columns.append({
                'name': col_name,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'default_value': default_value,
                'primary_key': bool(is_pk),
            })
            if is_pk:
                primary_keys.append(col_name)

        tables_data[table_name] = {
            'database': conn_params['database'],
            'schema': table_schema,
            'service': service,
            'table': table_name,
            'columns': columns,
            'primary_key': (
                primary_keys[0]
                if len(primary_keys) == 1
                else primary_keys
            ),
        }

    conn.close()
    return tables_data


def save_detailed_schema_postgres(
    service: str,
    schema: str,
    tables: list[dict[str, Any]],
    conn_params: dict[str, Any],
) -> dict[str, Any]:
    """Save detailed PostgreSQL table schema to YAML.

    Args:
        service: Service name
        schema: Database schema name
        tables: List of table dictionaries
        conn_params: Connection parameters

    Returns:
        Dictionary mapping table names to their schema data
    """
    if not has_psycopg2:
        print_error(
            "psycopg2 not installed — "
            + "use: pip install psycopg2-binary"
        )
        return {}

    pg = ensure_psycopg2()

    conn = pg.connect(
        host=conn_params['host'],
        port=conn_params['port'],
        dbname=conn_params['database'],
        user=conn_params['user'],
        password=conn_params['password'],
    )
    cursor = conn.cursor(
        cursor_factory=pg.extras.RealDictCursor,
    )

    tables_data: dict[str, Any] = {}

    for i, table in enumerate(tables, 1):
        table_name = table['TABLE_NAME']
        # Use table's schema or fallback to parameter
        table_schema = table.get('TABLE_SCHEMA', schema)
        print(f"  [{i}/{len(tables)}] {table_schema}.{table_name}")

        # Get detailed column info with primary key detection
        cursor.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = %s
                AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (table_schema, table_name, table_schema, table_name))

        columns: list[dict[str, Any]] = []
        primary_keys: list[str] = []
        for row in cursor.fetchall():
            columns.append({
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES',
                'default_value': row.get('column_default'),
                'primary_key': row['is_primary_key'],
            })
            if row['is_primary_key']:
                primary_keys.append(row['column_name'])

        tables_data[table_name] = {
            'database': conn_params['database'],
            'schema': table_schema,
            'service': service,
            'table': table_name,
            'columns': columns,
            'primary_key': (
                primary_keys[0]
                if len(primary_keys) == 1
                else primary_keys
            ),
        }

    conn.close()
    return tables_data


def _save_tables_to_yaml(
    service: str,
    tables_data: dict[str, Any],
) -> bool:
    """Write table schema data to YAML files.

    Groups tables by schema and saves each table to its own
    YAML file under ``services/_schemas/{service}/{schema}/``.

    Args:
        service: Service name (used for output directory)
        tables_data: Mapping of table_name → table schema dict

    Returns:
        True if all tables saved successfully.
    """
    tables_by_schema: dict[str, dict[str, Any]] = {}
    for table_name, table_data in tables_data.items():
        table_schema = table_data.get("schema") or "unknown"
        if table_schema not in tables_by_schema:
            tables_by_schema[table_schema] = {}
        tables_by_schema[table_schema][table_name] = table_data

    total_saved = 0
    service_schema_dir = get_service_schema_write_dir(service)
    for schema_name, schema_tables in tables_by_schema.items():
        output_dir = service_schema_dir / schema_name
        output_dir.mkdir(parents=True, exist_ok=True)

        for table_name, table_data in schema_tables.items():
            output_file = output_dir / f"{table_name}.yaml"
            with output_file.open("w") as f:
                yaml.dump(
                    table_data,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                )

        total_saved += len(schema_tables)
        print_success(
            f"✓ Saved {len(schema_tables)} table schemas "
            + f"to {output_dir}/"
        )

    return total_saved > 0


def _fetch_and_save(
    service: str,
    schema: str,
    tables: list[dict[str, Any]],
    db_type: str,
    conn_params: dict[str, Any],
) -> bool:
    """Fetch column details and save table schemas.

    Shared logic used by both source and sink save paths.

    Args:
        service: Service name (used for output directory)
        schema: Database schema name
        tables: List of table dicts from inspection
        db_type: Database type ('mssql' or 'postgres')
        conn_params: Connection parameters for the target DB

    Returns:
        True if schemas saved successfully.
    """
    print_info(
        f"Saving detailed schema for {len(tables)} tables..."
    )

    if db_type == "mssql":
        tables_data = save_detailed_schema_mssql(
            service, schema, tables, conn_params,
        )
    elif db_type == "postgres":
        tables_data = save_detailed_schema_postgres(
            service, schema, tables, conn_params,
        )
    else:
        print_error(f"Unsupported database type: {db_type}")
        return False

    if not tables_data:
        return False

    return _save_tables_to_yaml(service, tables_data)


def save_detailed_schema(
    service: str,
    env: str,
    schema: str,
    tables: list[dict[str, Any]],
    db_type: str,
) -> bool:
    """Save detailed table schema for a *source* database.

    Resolves connection params from the service's source config.

    Args:
        service: Service name
        env: Environment name
        schema: Database schema name
        tables: List of table dicts from inspection
        db_type: Database type ('mssql' or 'postgres')

    Returns:
        True if schema saved successfully, False otherwise
    """
    try:
        db_config = get_service_db_config(service, env)
        if not db_config:
            return False

        conn_params = get_connection_params(db_config, db_type)
        if not conn_params:
            return False

        return _fetch_and_save(
            service, schema, tables, db_type, conn_params,
        )

    except Exception as e:
        print_error(f"Failed to save schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def save_sink_schema(
    target_service: str,
    sink_key: str,
    source_service: str,
    env: str,
    tables: list[dict[str, Any]],
) -> bool:
    """Save detailed table schema for a *sink* database.

    Resolves connection params from the sink group config
    instead of the source service config.

    Args:
        target_service: Target service name (for output path)
        sink_key: Sink key (e.g., 'sink_asma.activities')
        source_service: Source service requesting the save
        env: Environment name
        tables: List of table dicts from inspection

    Returns:
        True if schema saved successfully, False otherwise
    """
    try:
        from .db_inspector_common import get_sink_db_config

        sink_config = get_sink_db_config(
            source_service, sink_key, env,
        )
        if not sink_config:
            return False

        db_type: str = sink_config["db_type"]
        conn_params = get_connection_params(sink_config, db_type)
        if not conn_params:
            return False

        schema = ""
        return _fetch_and_save(
            target_service, schema, tables,
            db_type, conn_params,
        )

    except Exception as e:
        print_error(f"Failed to save sink schema: {e}")
        import traceback
        traceback.print_exc()
        return False
