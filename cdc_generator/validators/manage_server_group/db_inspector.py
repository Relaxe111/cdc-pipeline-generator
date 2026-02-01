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


_INTERESTING_ENV_KEYWORDS = (
    "POSTGRES",
    "MSSQL",
    "SQLSERVER",
    "DATABASE",
    "SOURCE",
    "SINK",
    "CDC",
    "REDPANDA",
    "KAFKA",
)


_ENV_REFERENCE_PATTERN = re.compile(r"\$(?:\{(?P<braced>[A-Za-z0-9_]+)\}|(?P<plain>[A-Za-z0-9_]+))")


def extract_identifiers(db_name: str, server_group_config: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract identifiers (customer/service/env/suffix) from database name using configured patterns.
    
    Args:
        db_name: Database name to parse
        server_group_config: Server group configuration with extraction patterns
        
    Returns:
        Dictionary with extracted identifiers (customer, service, env, suffix)
    """
    pattern_type = server_group_config.get('pattern')
    extraction_pattern = server_group_config.get('extraction_pattern', '')
    
    # If extraction pattern is provided and not empty, use it
    if extraction_pattern:
        match = re.match(extraction_pattern, db_name)
        if match:
            groups = match.groupdict()
            
            # For db-per-tenant: extract customer
            if pattern_type == 'db-per-tenant':
                return {
                    'customer': groups.get('customer', db_name),
                    'service': server_group_config.get('name', ''),
                    'env': '',
                    'suffix': ''
                }
            
            # For db-shared: extract service, env, suffix
            elif pattern_type == 'db-shared':
                service_name = groups.get('service', '')
                suffix = groups.get('suffix', '')
                
                # Apply suffix transformation if present
                if suffix:
                    service_name = f"{suffix}_{service_name}"
                
                return {
                    'customer': '',
                    'service': service_name,
                    'env': groups.get('env', ''),
                    'suffix': suffix
                }
    
    # Fallback logic when no pattern or pattern doesn't match
    if pattern_type == 'db-per-tenant':
        # Use database name as customer
        return {'customer': db_name, 'service': server_group_config.get('name', ''), 'env': '', 'suffix': ''}
    
    elif pattern_type == 'db-shared':
        # Simple fallback: search for environment keywords and strip "db"
        env_match = re.search(r'(dev|test|stage|prod|nonprod)', db_name, re.IGNORECASE)
        env = env_match.group(1).lower() if env_match else ''
        
        # Remove environment keyword and "db" word, use what's left as service
        service = db_name.lower()
        if env:
            service = re.sub(rf'_{env}$|^{env}_', '', service)
        service = re.sub(r'_db_|_db$|^db_', '_', service).strip('_')
        
        return {
            'customer': '',
            'service': service or db_name,
            'env': env,
            'suffix': ''
        }
    
    # Default: use database name as service
    return {'customer': '', 'service': db_name, 'env': '', 'suffix': ''}


def _collect_missing_env_vars(template: str) -> List[str]:
    """Return env var names referenced in template that are not exported."""
    missing: List[str] = []
    for match in _ENV_REFERENCE_PATTERN.finditer(template):
        var_name = match.group('braced') or match.group('plain')
        if var_name and os.environ.get(var_name) is None:
            missing.append(var_name)
    return missing


def _list_available_env_vars() -> List[str]:
    """List docker env variables that look relevant for database connections."""
    available: List[str] = []
    for name in os.environ:
        upper = name.upper()
        if any(keyword in upper for keyword in _INTERESTING_ENV_KEYWORDS):
            available.append(name)
    return sorted(set(available))


def _format_env_lines(values: List[str]) -> str:
    """Format env variable names as indented bullet list."""
    if not values:
        return "        - None detected in this container session."
    return "\n".join(f"        - {value}" for value in values)


def _build_missing_env_message(field_name: str, missing_vars: List[str]) -> str:
    required_block = _format_env_lines(sorted(set(missing_vars)))
    available_block = _format_env_lines(_list_available_env_vars())
    return (
        "Missing environment variables detected for a connection field.\n"
        f"    Field: {field_name}\n"
        "    Required variables:\n"
        f"{required_block}\n"
        "\n"
        "Currently exported docker env variables:\n"
        f"{available_block}\n"
        "\n"
        "Next steps:\n"
        "    - Export the missing variables inside the dev container (set -x VAR_NAME value)\n"
        "    - Or update server_group.yaml to use literal credentials when appropriate\n"
    )


def _build_missing_field_message(field_name: str) -> str:
    available_block = _format_env_lines(_list_available_env_vars())
    return (
        "Server configuration is missing a required connection field.\n"
        f"    Field: {field_name}\n"
        "\n"
        "Currently exported docker env variables:\n"
        f"{available_block}\n"
    )


def _build_empty_value_message(field_name: str) -> str:
    available_block = _format_env_lines(_list_available_env_vars())
    return (
        "The resolved connection value is empty after expanding environment variables.\n"
        f"    Field: {field_name}\n"
        "\n"
        "Currently exported docker env variables:\n"
        f"{available_block}\n"
        "\n"
        "Next steps:\n"
        "    - Export the expected variable with a non-empty value\n"
        "    - Or update server_group.yaml with a literal fallback\n"
    )


def _resolve_env_value(value: Any, field_name: str) -> str:
    """Resolve environment variables and ensure the result is usable."""
    if value is None:
        raise MissingEnvironmentVariableError(_build_missing_field_message(field_name))

    value_str = str(value)
    missing_vars = _collect_missing_env_vars(value_str)
    if missing_vars:
        raise MissingEnvironmentVariableError(_build_missing_env_message(field_name, missing_vars))

    expanded = os.path.expandvars(value_str)

    if not expanded.strip():
        raise MissingEnvironmentVariableError(_build_empty_value_message(field_name))

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
    server_group_config: Dict[str, Any],
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
    ignored_schema_count = 0
    databases_with_ignored_schemas = 0
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
            ignored_schemas = len(all_schemas) - len(schemas)
            if ignored_schemas > 0:
                ignored_schema_count += ignored_schemas
                databases_with_ignored_schemas += 1
            
            # Get table count
            cursor.execute(f"""
                USE [{db_name}];
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()[0]
            
            # Extract identifiers using configured pattern
            identifiers = extract_identifiers(db_name, server_group_config)
            
            databases.append({
                'name': db_name,
                'service': identifiers.get('service', db_name),
                'environment': identifiers.get('env', ''),
                'customer': identifiers.get('customer', ''),
                'schemas': schemas if schemas else ['dbo'],
                'table_count': table_count
            })
        except Exception as e:
            print_warning(f"Could not inspect database {db_name}: {e}")
            continue
    
    if ignored_count > 0:
        patterns_text = ', '.join(ignore_patterns)
        print_info(f"🚫 Ignored {ignored_count} database(s) matching patterns: \033[31m{patterns_text}\033[0m")
    
    if excluded_count > 0:
        print_info(f"⊘ Excluded {excluded_count} database(s) not matching include pattern: {include_pattern}")
    
    if ignored_schema_count > 0:
        schema_patterns = schema_exclude_patterns or []
        patterns_text = ', '.join(schema_patterns)
        print_info(f"📊 Ignored {ignored_schema_count} schema(s) from {databases_with_ignored_schemas} database(s) matching patterns: \033[31m{patterns_text}\033[0m")
    
    conn.close()
    return databases


def list_postgres_databases(
    server_config: Dict[str, Any],
    server_group_config: Dict[str, Any],
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
        patterns_text = ', '.join(ignore_patterns)
        print_info(f"🚫 Ignored {ignored_count} database(s) matching patterns: \033[31m{patterns_text}\033[0m")
    
    if excluded_count > 0:
        print_info(f"⊘ Excluded {excluded_count} database(s) not matching include pattern: {include_pattern}")
    
    databases: List[Dict[str, Any]] = []
    ignored_schema_count = 0
    databases_with_ignored_schemas = 0
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
            ignored_schemas = len(all_schemas) - len(schemas)
            if ignored_schemas > 0:
                ignored_schema_count += ignored_schemas
                databases_with_ignored_schemas += 1
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND table_schema NOT LIKE 'pg_temp_%'
                AND table_schema NOT LIKE 'pg_toast_temp_%'
                AND table_type = 'BASE TABLE'
            """)
            table_count = db_cursor.fetchone()[0]
            
            # Extract identifiers using configured pattern
            identifiers = extract_identifiers(db_name, server_group_config)
            
            databases.append({
                'name': db_name,
                'service': identifiers.get('service', db_name),
                'environment': identifiers.get('env', ''),
                'customer': identifiers.get('customer', ''),
                'schemas': schemas,
                'table_count': table_count
            })
            
            db_conn.close()
        except Exception as e:
            print_warning(f"Could not inspect database {db_name}: {e}")
    
    if ignored_schema_count > 0:
        schema_patterns = schema_exclude_patterns or []
        patterns_text = ', '.join(schema_patterns)
        print_info(f"📊 Ignored {ignored_schema_count} schema(s) from {databases_with_ignored_schemas} database(s) matching patterns: \033[31m{patterns_text}\033[0m")
    
    return databases
