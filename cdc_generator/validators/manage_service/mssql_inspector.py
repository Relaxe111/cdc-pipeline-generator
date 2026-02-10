"""MSSQL schema inspection for CDC pipeline."""

from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error, print_info
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.mssql_loader import has_pymssql

from .db_inspector_common import get_connection_params, get_service_db_config


def inspect_mssql_schema(service: str, env: str = 'nonprod') -> list[dict[str, Any]] | None:
    """Inspect MSSQL schema to get list of available tables.

    Args:
        service: Service name
        env: Environment name (default: nonprod)

    Returns:
        List of table dictionaries with TABLE_SCHEMA, TABLE_NAME, COLUMN_COUNT
    """
    if not has_pymssql:
        print_error("pymssql not installed - use: pip install pymssql")
        return None

    try:
        # Get service configuration
        db_config = get_service_db_config(service, env)
        if not db_config:
            return None

        # Extract connection parameters
        conn_params = get_connection_params(db_config, 'mssql')
        if not conn_params:
            return None

        print_info(
            "Connecting to MSSQL: "
            + f"{conn_params['host']}:{conn_params['port']}"
            + f"/{conn_params['database']}"
        )

        # Connect to MSSQL
        conn = create_mssql_connection(
            host=conn_params['host'],
            port=conn_params['port'],
            database=conn_params['database'],
            user=conn_params['user'],
            password=conn_params['password']
        )

        cursor = conn.cursor(as_dict=True)

        # Get all tables with their schemas
        query = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            (SELECT COUNT(*)
             FROM INFORMATION_SCHEMA.COLUMNS c
             WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA
               AND c.TABLE_NAME = t.TABLE_NAME) as COLUMN_COUNT
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """

        cursor.execute(query)
        tables = cast(list[dict[str, Any]], cursor.fetchall())

        conn.close()

        return tables

    except Exception as e:
        print_error(f"MSSQL inspection failed: {e}")
        return None
