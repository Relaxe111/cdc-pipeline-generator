"""PostgreSQL schema inspection for CDC pipeline."""

from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_info
from cdc_generator.helpers.psycopg2_loader import (
    ensure_psycopg2,
    has_psycopg2,
)

from .db_inspector_common import (
    ValidationEnvMissingError,
    get_connection_params,
    get_service_db_config,
)


def inspect_postgres_schema(service: str, env: str = 'nonprod') -> list[dict[str, Any]] | None:
    """Inspect PostgreSQL schema to get list of available tables.

    Args:
        service: Service name
        env: Environment name (default: nonprod)

    Returns:
        List of table dictionaries with TABLE_SCHEMA, TABLE_NAME, COLUMN_COUNT
    """
    if not has_psycopg2:
        print_error("psycopg2 not installed - use: pip install psycopg2-binary")
        return None

    try:
        # Get service configuration
        db_config = get_service_db_config(service, env)
        if not db_config:
            return None

        # Extract connection parameters
        conn_params = get_connection_params(db_config, 'postgres')
        if not conn_params:
            return None

        print_info(
            "Connecting to PostgreSQL: "
            + f"{conn_params['host']}:{conn_params['port']}/"
            + f"{conn_params['database']}"
        )

        pg = ensure_psycopg2()

        # Connect to PostgreSQL
        conn = pg.connect(
            host=conn_params['host'],
            port=conn_params['port'],
            dbname=conn_params['database'],
            user=conn_params['user'],
            password=conn_params['password']
        )

        cursor = conn.cursor(cursor_factory=pg.extras.RealDictCursor)

        # Get all tables with their schemas and column counts
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
            AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
        GROUP BY t.table_schema, t.table_name
        ORDER BY t.table_schema, t.table_name
        """

        cursor.execute(query)
        tables = cursor.fetchall()

        conn.close()

        # Convert RealDictRow to regular dict for consistency with MSSQL inspector
        return [dict(table) for table in tables]

    except ValidationEnvMissingError:
        # Re-raise to allow proper handling in CLI
        raise
    except Exception as e:
        print_error(f"Failed to inspect PostgreSQL schema: {e}")
        import traceback
        traceback.print_exc()
        return None
