"""Database inspection for MSSQL and PostgreSQL servers."""

import os
import re
from typing import List, Dict, Optional, Any

try:
    import pymssql  # type: ignore[import-not-found]
    has_pymssql = True
except ImportError:
    pymssql = None  # type: ignore[assignment]
    has_pymssql = False

try:
    import psycopg2  # type: ignore[import-not-found]
    has_psycopg2 = True
except ImportError:
    psycopg2 = None  # type: ignore[assignment]
    has_psycopg2 = False

from .filters import should_ignore_database, should_include_database, should_exclude_schema
from cdc_generator.helpers.helpers_logging import print_info, print_warning
from cdc_generator.helpers.helpers_mssql import create_mssql_connection


class MissingEnvironmentVariableError(ValueError):
    """Raised when a required connection value still contains an unresolved env var."""


_ENV_REFERENCE_PATTERN = re.compile(r"\$(?:\{(?P<braced>[A-Za-z0-9_]+)\}|(?P<plain>[A-Za-z0-9_]+))")


def _collect_missing_env_vars(template: str) -> List[str]:
    """Return env var names referenced in template that are not exported."""
    missing: List[str] = []
    for match in _ENV_REFERENCE_PATTERN.finditer(template):
        var_name = match.group('braced') or match.group('plain')
        if var_name and os.environ.get(var_name) is None:
            missing.append(var_name)
    return missing


def _resolve_env_value(value: Any, field_name: str) -> str:
    """Resolve environment variables and ensure the result is usable."""
    if value is None:
        raise MissingEnvironmentVariableError(
            f"Server configuration is missing the '{field_name}' field."
        )

    value_str = str(value)
    missing_vars = _collect_missing_env_vars(value_str)
    if missing_vars:
        missing = ", ".join(sorted(set(missing_vars)))
        raise MissingEnvironmentVariableError(
            f"Environment variable(s) {missing} required for '{field_name}' are not set.\n"
            "Set them inside the dev container or replace the placeholder value in server_group.yaml."
        )

    expanded = os.path.expandvars(value_str)

    if not expanded.strip():
        raise MissingEnvironmentVariableError(
            f"Value for '{field_name}' is empty after resolving environment variables."
        )

    return expanded


def get_mssql_connection(server_config: Dict[str, Any]) -> Any:
    """Get MSSQL connection from server config."""
    if not has_pymssql:
        raise ImportError("pymssql not installed - run: pip install pymssql")
    
    host = _resolve_env_value(server_config.get('host'), 'host')
    user = _resolve_env_value(
        server_config.get('username', server_config.get('user')),
        'username'
    )
    password = _resolve_env_value(server_config.get('password'), 'password')
    port = int(_resolve_env_value(server_config.get('port', 1433), 'port'))
    
    return create_mssql_connection(
        host=host,
        port=port,
        database='',  # No database for server-level inspection
        user=user,
        password=password
    )


def get_postgres_connection(server_config: Dict[str, Any], database: str = 'postgres') -> Any:
    """Get PostgreSQL connection from server config."""
    if not has_psycopg2:
        raise ImportError("psycopg2 not installed - run: pip install psycopg2-binary")
    
    host = _resolve_env_value(server_config.get('host'), 'host')
    user = _resolve_env_value(
        server_config.get('username', server_config.get('user')),
        'username'
    )
    password = _resolve_env_value(server_config.get('password'), 'password')
    port = int(_resolve_env_value(server_config.get('port', 5432), 'port'))
    
    return psycopg2.connect(  # type: ignore[misc]
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connect_timeout=10  # 10 second timeout
    )


def list_mssql_databases(
    server_config: Dict[str, Any], 
    include_pattern: Optional[str] = None, 
    database_exclude_patterns: Optional[List[str]] = None,
    schema_exclude_patterns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """List all databases on MSSQL server."""
    print_info(f"Connecting to MSSQL server...")
    
    # Use provided patterns or empty lists
    ignore_patterns = database_exclude_patterns or []
    
    conn = get_mssql_connection(server_config)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name
        FROM sys.databases
        WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
        AND state = 0  -- Only ONLINE databases
        ORDER BY name
    """)
    
    databases: List[Dict[str, Any]] = []
    ignored_count = 0
    excluded_count = 0
    for row in cursor.fetchall():
        db_name = row[0]
        
        # Check if database should be ignored
        if should_ignore_database(db_name, ignore_patterns):
            ignored_count += 1
            continue
        
        # Check if database matches include pattern
        if not should_include_database(db_name, include_pattern):
            excluded_count += 1
            continue
        
        try:
            # Get schemas for this database
            cursor.execute(f"""
                USE [{db_name}];
                SELECT DISTINCT TABLE_SCHEMA 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA
            """)
            all_schemas = [r[0] for r in cursor.fetchall()]
            
            # Filter schemas based on provided exclude patterns
            schema_patterns = schema_exclude_patterns or []
            schemas = [s for s in all_schemas if not should_exclude_schema(s, schema_patterns)]
            
            # Get table count
            cursor.execute(f"""
                USE [{db_name}];
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()[0]
            
            databases.append({
                'name': db_name,
                'schemas': schemas if schemas else ['dbo'],
                'table_count': table_count
            })
        except Exception as e:
            print_warning(f"Could not inspect database {db_name}: {e}")
            continue
    
    if ignored_count > 0:
        print_info(f"Ignored {ignored_count} database(s) matching patterns: {ignore_patterns}")
    
    if excluded_count > 0:
        print_info(f"Excluded {excluded_count} database(s) not matching include pattern: {include_pattern}")
    
    conn.close()
    return databases


def list_postgres_databases(
    server_config: Dict[str, Any], 
    include_pattern: Optional[str] = None,
    database_exclude_patterns: Optional[List[str]] = None,
    schema_exclude_patterns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """List all databases on PostgreSQL server."""
    print_info(f"Connecting to PostgreSQL server...")
    
    # Use provided patterns or empty lists
    ignore_patterns = database_exclude_patterns or []
    
    conn = get_postgres_connection(server_config)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT datname 
        FROM pg_database 
        WHERE datistemplate = false 
        AND datname NOT IN ('postgres', 'template0', 'template1')
        ORDER BY datname
    """)
    
    db_names = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # Filter out ignored databases and check include pattern
    filtered_db_names: List[str] = []
    ignored_count = 0
    excluded_count = 0
    for db_name in db_names:
        if should_ignore_database(db_name, ignore_patterns):
            ignored_count += 1
            continue
        
        if not should_include_database(db_name, include_pattern):
            excluded_count += 1
            continue
        
        filtered_db_names.append(db_name)
    
    if ignored_count > 0:
        print_info(f"Ignored {ignored_count} database(s) matching patterns: {ignore_patterns}")
    
    if excluded_count > 0:
        print_info(f"Excluded {excluded_count} database(s) not matching include pattern: {include_pattern}")
    
    databases: List[Dict[str, Any]] = []
    for db_name in filtered_db_names:
        try:
            db_conn = get_postgres_connection(server_config, db_name)
            db_cursor = db_conn.cursor()
            
            # Get schemas (exclude temp schemas)
            db_cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND schema_name NOT LIKE 'pg_temp_%'
                AND schema_name NOT LIKE 'pg_toast_temp_%'
                ORDER BY schema_name
            """)
            all_schemas = [row[0] for row in db_cursor.fetchall()]
            
            # Filter schemas based on provided exclude patterns
            schema_patterns = schema_exclude_patterns or []
            schemas = [s for s in all_schemas if not should_exclude_schema(s, schema_patterns)]
            
            # Get table count (exclude temp schemas)
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND table_schema NOT LIKE 'pg_temp_%'
                AND table_schema NOT LIKE 'pg_toast_temp_%'
                AND table_type = 'BASE TABLE'
            """)
            table_count = db_cursor.fetchone()[0]
            
            databases.append({
                'name': db_name,
                'schemas': schemas,
                'table_count': table_count
            })
            
            db_conn.close()
        except Exception as e:
            print_warning(f"Could not inspect database {db_name}: {e}")
    
    return databases
