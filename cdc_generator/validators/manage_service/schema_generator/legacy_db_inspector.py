"""Legacy MSSQL database inspector - saves detailed table schemas to YAML files.

NOTE: This module is kept for backward compatibility but is NOT used in the main
schema generation flow anymore. Schema generation now uses YAML files from
service-schemas/ directory instead of database introspection.
"""

from pathlib import Path
from typing import Any

import yaml  # type: ignore

from cdc_generator.helpers.helpers_logging import print_error, print_info, print_success
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.service_config import load_customer_config, load_service_config

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Check for pymssql availability
try:
    import pymssql  # type: ignore
    HAS_PYMSSQL = True
except ImportError:
    HAS_PYMSSQL = False  # type: ignore[misc]


def save_detailed_schema(service: str, env: str, schema: str, tables: list[dict[str, Any]]) -> bool:
    """Save detailed table schema information to YAML file.

    ⚠️ LEGACY FUNCTION - Not used in main schema generation flow.
    This was used for database introspection but has been replaced by
    YAML-based schema loading from service-schemas/ directory.

    Args:
        service: Service name
        env: Environment name
        schema: Database schema name
        tables: List of table dictionaries from MSSQL inspection

    Returns:
        True if schema saved successfully, False otherwise
    """
    if not HAS_PYMSSQL:
        print_error("pymssql not installed - use: pip install pymssql")
        return False

    try:
        print_info(f"Saving detailed schema for {len(tables)} tables...")
        output_dir = PROJECT_ROOT / 'generated' / 'schemas'
        output_dir.mkdir(parents=True, exist_ok=True)

        config = load_service_config(service)
        reference = config.get('reference', 'avansas')
        customer_config = load_customer_config(reference)
        env_config = customer_config.get('environments', {}).get(env, {})
        database = env_config.get('database_name', 'unknown')

        schema_data: dict[str, Any] = {
            'database': database,
            'schema': schema,
            'service': service,
            'tables': {}
        }

        # Connect and get detailed schema
        mssql = env_config.get('mssql', {})

        # Expand environment variables (support ${VAR} format)
        def expand_env(value: str | int | None) -> str | int | None:
            """Expand ${VAR} and $VAR patterns."""
            if not isinstance(value, str):
                return value
            value = value.replace('${', '$').replace('}', '')
            import os
            return os.path.expandvars(value)

        host_val = expand_env(mssql.get('host', 'localhost'))
        host = str(host_val) if host_val is not None else 'localhost'
        port_val = expand_env(mssql.get('port', '1433'))
        port = int(port_val) if port_val is not None else 1433
        user_val = expand_env(mssql.get('user', 'sa'))
        user = str(user_val) if user_val is not None else 'sa'
        password_val = expand_env(mssql.get('password', ''))
        password = str(password_val) if password_val is not None else ''

        conn = create_mssql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        for i, table in enumerate(tables, 1):
            table_name = table['TABLE_NAME']
            print(f"  [{i}/{len(tables)}] {table_name}")

            # Get detailed column info
            cursor.execute(f"""
                SELECT
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.IS_NULLABLE,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND tc.TABLE_SCHEMA = '{schema}'
                        AND tc.TABLE_NAME = '{table_name}'
                ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE c.TABLE_SCHEMA = '{schema}'
                    AND c.TABLE_NAME = '{table_name}'
                ORDER BY c.ORDINAL_POSITION
            """)

            columns: list[dict[str, Any]] = []
            primary_keys: list[str] = []
            for row in cursor:
                col_name, data_type, _max_len, is_nullable, is_pk = row
                columns.append({
                    'name': col_name,
                    'type': data_type,
                    'nullable': is_nullable == 'YES',
                    'primary_key': bool(is_pk)
                })
                if is_pk:
                    primary_keys.append(col_name)

            schema_data['tables'][table_name] = {
                'columns': columns,
                'primary_key': primary_keys[0] if len(primary_keys) == 1 else primary_keys if primary_keys else None
            }

        conn.close()

        # Save to YAML
        output_file = output_dir / f'{database}_{schema}.yaml'
        with open(output_file, 'w') as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print_success(f"\nSchema saved to {output_file}")
        print_info(f"  {len(tables)} tables documented")
        return True

    except Exception as e:
        print_error(f"Failed to save schema: {e}")
        import traceback
        traceback.print_exc()
        return False
