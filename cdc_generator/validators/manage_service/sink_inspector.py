"""Sink database schema inspection for CDC pipeline.

Connects to a sink database and lists available tables,
similar to how postgres_inspector and mssql_inspector
inspect source databases.
"""

from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_info
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.mssql_loader import has_pymssql
from cdc_generator.helpers.psycopg2_loader import (
    ensure_psycopg2,
    has_psycopg2,
)

from .db_inspector_common import get_connection_params, get_sink_db_config


def inspect_sink_schema(
    service: str,
    sink_key: str,
    env: str = "nonprod",
) -> tuple[list[dict[str, Any]], str, list[str]] | None:
    """Inspect sink database schema to get available tables.

    Resolves the sink connection from sink-groups.yaml and the
    database name from source-groups.yaml, then queries the
    database for tables.

    Args:
        service: Service name
        sink_key: Sink key (e.g., 'sink_asma.calendar')
        env: Environment name (default: nonprod)

    Returns:
        Tuple of (tables, db_type, allowed_schemas) or None on error.
        Each table dict has TABLE_SCHEMA, TABLE_NAME, COLUMN_COUNT.
    """
    sink_config = get_sink_db_config(service, sink_key, env)
    if not sink_config:
        return None

    db_type: str = sink_config["db_type"]
    schemas: list[str] = sink_config.get("schemas", [])

    conn_params = get_connection_params(sink_config, db_type)
    if not conn_params:
        return None

    if db_type == "postgres":
        tables = _inspect_postgres_sink(conn_params)
    elif db_type == "mssql":
        tables = _inspect_mssql_sink(conn_params)
    else:
        print_error(f"Unsupported sink database type: {db_type}")
        return None

    if tables is None:
        return None

    return tables, db_type, schemas


def _inspect_postgres_sink(
    conn_params: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Inspect a PostgreSQL sink database for tables.

    Args:
        conn_params: Connection parameters (host, port, user, password, database)

    Returns:
        List of table dicts or None on error
    """
    if not has_psycopg2:
        print_error(
            "psycopg2 not installed — "
            + "use: pip install psycopg2-binary"
        )
        return None

    try:
        print_info(
            "Connecting to sink PostgreSQL: "
            + f"{conn_params['host']}:{conn_params['port']}"
            + f"/{conn_params['database']}"
        )

        pg = ensure_psycopg2()

        conn = pg.connect(
            host=conn_params["host"],
            port=conn_params["port"],
            dbname=conn_params["database"],
            user=conn_params["user"],
            password=conn_params["password"],
        )
        cursor = conn.cursor(
            cursor_factory=pg.extras.RealDictCursor,
        )

        query = """
        SELECT
            t.table_schema AS "TABLE_SCHEMA",
            t.table_name AS "TABLE_NAME",
            COUNT(c.column_name)::INTEGER AS "COLUMN_COUNT"
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_type = 'BASE TABLE'
            AND t.table_schema NOT IN (
                'pg_catalog', 'information_schema'
            )
        GROUP BY t.table_schema, t.table_name
        ORDER BY t.table_schema, t.table_name
        """

        cursor.execute(query)
        tables = cursor.fetchall()
        conn.close()

        return [dict(table) for table in tables]

    except Exception as e:
        print_error(
            f"Failed to inspect sink PostgreSQL schema: {e}"
        )
        return None


def _inspect_mssql_sink(
    conn_params: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Inspect an MSSQL sink database for tables.

    Args:
        conn_params: Connection parameters (host, port, user, password, database)

    Returns:
        List of table dicts or None on error
    """
    if not has_pymssql:
        print_error(
            "pymssql not installed — use: pip install pymssql"
        )
        return None

    try:
        print_info(
            "Connecting to sink MSSQL: "
            + f"{conn_params['host']}:{conn_params['port']}"
            + f"/{conn_params['database']}"
        )

        conn = create_mssql_connection(
            host=conn_params["host"],
            port=conn_params["port"],
            database=conn_params["database"],
            user=conn_params["user"],
            password=conn_params["password"],
        )
        cursor = conn.cursor(as_dict=True)

        query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            (SELECT COUNT(*)
             FROM INFORMATION_SCHEMA.COLUMNS c
             WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA
               AND c.TABLE_NAME = t.TABLE_NAME) AS COLUMN_COUNT
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """

        cursor.execute(query)
        tables: list[dict[str, Any]] = cursor.fetchall()
        conn.close()

        return tables

    except Exception as e:
        print_error(f"Failed to inspect sink MSSQL schema: {e}")
        return None
