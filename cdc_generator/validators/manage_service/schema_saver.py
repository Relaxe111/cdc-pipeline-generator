"""Save detailed database schemas to YAML files."""

import os
import yaml
from pathlib import Path
from typing import List, Dict
from helpers_logging import print_info, print_error, print_success
from .db_inspector_common import get_service_db_config, get_connection_params

try:
    import pymssql
    HAS_PYMSSQL = True
except ImportError:
    HAS_PYMSSQL = False

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

PROJECT_ROOT = Path(__file__).parent.parent.parent


def save_detailed_schema_mssql(service: str, env: str, schema: str, tables: List[Dict], conn_params: Dict) -> Dict:
    """Save detailed MSSQL table schema to YAML.
    
    Args:
        service: Service name
        env: Environment name
        schema: Database schema name
        tables: List of table dictionaries
        conn_params: Connection parameters
        
    Returns:
        Dictionary mapping table names to their schema data
    """
    if not HAS_PYMSSQL:
        print_error("pymssql not installed")
        return {}
    
    conn = pymssql.connect(
        server=conn_params['host'],
        port=conn_params['port'],
        database=conn_params['database'],
        user=conn_params['user'],
        password=conn_params['password']
    )
    cursor = conn.cursor()
    
    tables_data = {}
    
    for i, table in enumerate(tables, 1):
        table_name = table['TABLE_NAME']
        table_schema = table.get('TABLE_SCHEMA', schema)  # Use table's schema or fallback to parameter
        print(f"  [{i}/{len(tables)}] {table_schema}.{table_name}")
        
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
                    AND tc.TABLE_SCHEMA = '{table_schema}'
                    AND tc.TABLE_NAME = '{table_name}'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = '{table_schema}' 
                AND c.TABLE_NAME = '{table_name}'
            ORDER BY c.ORDINAL_POSITION
        """)
        
        columns = []
        primary_keys = []
        for row in cursor:
            col_name, data_type, max_len, is_nullable, is_pk = row
            columns.append({
                'name': col_name,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'primary_key': bool(is_pk)
            })
            if is_pk:
                primary_keys.append(col_name)
        
        tables_data[table_name] = {
            'database': conn_params['database'],
            'schema': table_schema,  # Use table's actual schema
            'service': service,
            'table': table_name,
            'columns': columns,
            'primary_key': primary_keys[0] if len(primary_keys) == 1 else primary_keys
        }
    
    conn.close()
    return tables_data


def save_detailed_schema_postgres(service: str, env: str, schema: str, tables: List[Dict], conn_params: Dict) -> Dict:
    """Save detailed PostgreSQL table schema to YAML.
    
    Args:
        service: Service name
        env: Environment name
        schema: Database schema name
        tables: List of table dictionaries
        conn_params: Connection parameters
        
    Returns:
        Dictionary mapping table names to their schema data
    """
    if not HAS_PSYCOPG2:
        print_error("psycopg2 not installed")
        return {}
    
    conn = psycopg2.connect(
        host=conn_params['host'],
        port=conn_params['port'],
        database=conn_params['database'],
        user=conn_params['user'],
        password=conn_params['password']
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    tables_data = {}
    
    for i, table in enumerate(tables, 1):
        table_name = table['TABLE_NAME']
        table_schema = table.get('TABLE_SCHEMA', schema)  # Use table's schema or fallback to parameter
        print(f"  [{i}/{len(tables)}] {table_schema}.{table_name}")
        
        # Get detailed column info with primary key detection
        cursor.execute("""
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
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
        
        columns = []
        primary_keys = []
        for row in cursor:
            columns.append({
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES',
                'primary_key': row['is_primary_key']
            })
            if row['is_primary_key']:
                primary_keys.append(row['column_name'])
        
        tables_data[table_name] = {
            'database': conn_params['database'],
            'schema': table_schema,
            'service': service,
            'table': table_name,
            'columns': columns,
            'primary_key': primary_keys[0] if len(primary_keys) == 1 else primary_keys
        }
    
    conn.close()
    return tables_data


def save_detailed_schema(service: str, env: str, schema: str, tables: List[Dict], db_type: str) -> bool:
    """Save detailed table schema information to YAML file.
    
    Args:
        service: Service name
        env: Environment name
        schema: Database schema name (None for all schemas)
        tables: List of table dictionaries from inspection
        db_type: Database type ('mssql' or 'postgres')
    
    Returns:
        True if schema saved successfully, False otherwise
    """
    try:
        print_info(f"Saving detailed schema for {len(tables)} tables...")
        
        # Get database configuration
        db_config = get_service_db_config(service, env)
        if not db_config:
            return False
        
        # Get connection parameters
        conn_params = get_connection_params(db_config, db_type)
        if not conn_params:
            return False
        
        # Call appropriate saver based on database type
        if db_type == 'mssql':
            tables_data = save_detailed_schema_mssql(service, env, schema, tables, conn_params)
        elif db_type == 'postgres':
            tables_data = save_detailed_schema_postgres(service, env, schema, tables, conn_params)
        else:
            print_error(f"Unsupported database type: {db_type}")
            return False
        
        if not tables_data:
            return False
        
        # Group tables by schema (important when --all is used)
        tables_by_schema = {}
        for table_name, table_data in tables_data.items():
            table_schema = table_data.get('schema') or 'unknown'
            if table_schema not in tables_by_schema:
                tables_by_schema[table_schema] = {}
            tables_by_schema[table_schema][table_name] = table_data
        
        # Save each table to its own YAML file in /{schema}/ directory
        total_saved = 0
        for schema_name, schema_tables in tables_by_schema.items():
            output_dir = PROJECT_ROOT / 'service-schemas' / service / schema_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for table_name, table_data in schema_tables.items():
                output_file = output_dir / f'{table_name}.yaml'
                with open(output_file, 'w') as f:
                    yaml.dump(table_data, f, default_flow_style=False, sort_keys=False, indent=2)
            
            total_saved += len(schema_tables)
            print_success(f"✓ Saved {len(schema_tables)} table schemas to {output_dir}/")
        
        return True
        
    except Exception as e:
        print_error(f"Failed to save schema: {e}")
        import traceback
        traceback.print_exc()
        return False
