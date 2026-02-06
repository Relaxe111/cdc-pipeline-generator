"""Database inspection for MSSQL and PostgreSQL servers."""

import os
import re
from typing import List, Optional, Union, TYPE_CHECKING, Any

from .types import ServerConfig, ServerGroupConfig, DatabaseInfo, ExtractedIdentifiers

# Type checking imports for database connections
# These are untyped external libraries - use Any for connection objects
if TYPE_CHECKING:
    # Connection types would go here if stubs existed
    pass

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


class PostgresConnectionError(Exception):
    """Raised when PostgreSQL connection fails with user-friendly context."""
    
    def __init__(self, message: str, host: str, port: int, hint: str = ""):
        self.host = host
        self.port = port
        self.hint = hint
        super().__init__(message)


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


def extract_identifiers(
    db_name: str, 
    server_group_config: ServerGroupConfig, 
    server_name: str = "default"
) -> ExtractedIdentifiers:
    """
    Extract identifiers (customer/service/env/suffix) from database name using configured patterns.
    
    Priority order for db-shared pattern:
    1. extraction_patterns (per-server, ordered list) - NEW
    2. extraction_pattern (per-server, single pattern) - backward compat
    3. Fallback logic (parse db_name heuristically)
    
    Args:
        db_name: Database name to parse
        server_group_config: Server group configuration with extraction patterns
        server_name: Name of the server being scanned (default: "default")
        
    Returns:
        ExtractedIdentifiers with customer, service, env, suffix
    """
    from cdc_generator.helpers.helpers_pattern_matcher import (
        match_extraction_patterns,
        match_single_pattern,
    )
    
    pattern_type = server_group_config.get('pattern')
    servers = server_group_config.get('servers', {})
    server_config = servers.get(server_name, {})
    
    # For db-shared: try ordered extraction patterns first (NEW)
    if pattern_type == 'db-shared':
        extraction_patterns = server_config.get('extraction_patterns', [])
        if extraction_patterns:
            result = match_extraction_patterns(db_name, extraction_patterns, server_name)
            if result:
                service, env = result
                return {
                    'customer': '',
                    'service': service,
                    'env': env,
                    'suffix': ''
                }
        
        # Fallback to single extraction_pattern (backward compat)
        extraction_pattern = server_config.get('extraction_pattern', '')
        if not extraction_pattern:
            extraction_pattern = server_group_config.get('extraction_pattern', '')
        
        if extraction_pattern:
            result = match_single_pattern(db_name, extraction_pattern)
            if result:
                service, env = result
                return {
                    'customer': '',
                    'service': service,
                    'env': env,
                    'suffix': ''
                }
    
    # For db-per-tenant: use extraction_pattern for customer extraction
    elif pattern_type == 'db-per-tenant':
        extraction_pattern = server_config.get('extraction_pattern', '')
        if not extraction_pattern:
            extraction_pattern = server_group_config.get('extraction_pattern', '')
        
        if extraction_pattern:
            match = re.match(extraction_pattern, db_name)
            if match:
                groups = match.groupdict()
                return {
                    'customer': groups.get('customer', db_name),
                    'service': server_group_config.get('name', ''),
                    'env': '',
                    'suffix': ''
                }
    
    # Fallback logic when no pattern or pattern doesn't match
    if pattern_type == 'db-per-tenant':
        # Use database name as customer
        return {'customer': db_name, 'service': server_group_config.get('name', ''), 'env': '', 'suffix': ''}
    
    elif pattern_type == 'db-shared':
        # Fallback: no pattern matched
        # Use database name as service and server name as env
        return {
            'customer': '',
            'service': db_name,
            'env': server_name,
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


def _resolve_env_value(value: Union[str, int, None], field_name: str) -> str:
    """Resolve environment variables and ensure the result is usable.
    
    Args:
        value: YAML config value - can be str, int, or None
        field_name: Name of the field for error messages
    """
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


def get_mssql_connection(server_config: ServerConfig) -> Any:  # noqa: ANN401 - pymssql has no stubs
    """Get MSSQL connection from server config.
    
    Returns:
        pymssql connection object (typed as Any due to missing type stubs)
    """
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


def get_postgres_connection(server_config: ServerConfig, database: str = 'postgres') -> Any:  # noqa: ANN401 - psycopg2 has no stubs
    """Get PostgreSQL connection from server config.
    
    Returns:
        psycopg2 connection object (typed as Any due to missing type stubs)
    """
    if not has_psycopg2:
        raise ImportError("psycopg2 not installed - run: pip install psycopg2-binary")
    
    host = _resolve_env_value(server_config.get('host'), 'host')
    user = _resolve_env_value(
        server_config.get('username', server_config.get('user')),
        'username'
    )
    password = _resolve_env_value(server_config.get('password'), 'password')
    port = int(_resolve_env_value(server_config.get('port', 5432), 'port'))
    
    try:
        return psycopg2.connect(  # type: ignore[misc]
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10  # 10 second timeout
        )
    except psycopg2.OperationalError as e:  # type: ignore[union-attr]
        error_msg = str(e).lower()
        
        # DNS resolution failure
        if "could not translate host name" in error_msg or "name or service not known" in error_msg:
            raise PostgresConnectionError(
                f"Cannot resolve hostname '{host}'",
                host=host,
                port=port,
                hint=(
                    "Check that:\n"
                    f"  • The hostname '{host}' is correct\n"
                    "  • You have network connectivity (try: ping the host)\n"
                    "  • DNS is working (try: nslookup or dig the hostname)\n"
                    "  • If using VPN, ensure it's connected"
                )
            ) from e
        
        # Connection refused (server not running or wrong port)
        if "connection refused" in error_msg:
            raise PostgresConnectionError(
                f"Connection refused by {host}:{port}",
                host=host,
                port=port,
                hint=(
                    "Check that:\n"
                    "  • PostgreSQL server is running\n"
                    f"  • Port {port} is correct\n"
                    "  • Firewall allows connections to this port\n"
                    "  • Server is configured to accept remote connections (pg_hba.conf)"
                )
            ) from e
        
        # Connection timeout
        if "timeout" in error_msg or "timed out" in error_msg:
            raise PostgresConnectionError(
                f"Connection to {host}:{port} timed out",
                host=host,
                port=port,
                hint=(
                    "Check that:\n"
                    "  • The server is reachable (try: telnet or nc to host:port)\n"
                    "  • Network/firewall isn't blocking the connection\n"
                    "  • Server isn't overloaded"
                )
            ) from e
        
        # Authentication failure
        if "password authentication failed" in error_msg or "authentication failed" in error_msg:
            raise PostgresConnectionError(
                f"Authentication failed for user at {host}:{port}",
                host=host,
                port=port,
                hint=(
                    "Check that:\n"
                    "  • Username is correct\n"
                    "  • Password is correct\n"
                    "  • User has permission to connect to the database\n"
                    "  • Check pg_hba.conf authentication method"
                )
            ) from e
        
        # Database doesn't exist
        if "database" in error_msg and "does not exist" in error_msg:
            raise PostgresConnectionError(
                f"Database '{database}' does not exist on {host}:{port}",
                host=host,
                port=port,
                hint=f"Check that database '{database}' exists on the server"
            ) from e
        
        # SSL required
        if "ssl" in error_msg:
            raise PostgresConnectionError(
                f"SSL connection issue with {host}:{port}",
                host=host,
                port=port,
                hint=(
                    "Check that:\n"
                    "  • Server SSL configuration is correct\n"
                    "  • Client SSL settings match server requirements"
                )
            ) from e
        
        # Generic operational error
        raise PostgresConnectionError(
            f"Failed to connect to PostgreSQL at {host}:{port}",
            host=host,
            port=port,
            hint=f"Original error: {e}"
        ) from e


def list_mssql_databases(
    server_config: ServerConfig,
    server_group_config: ServerGroupConfig,
    include_pattern: Optional[str] = None, 
    database_exclude_patterns: Optional[List[str]] = None,
    schema_exclude_patterns: Optional[List[str]] = None,
    server_name: str = "default",
) -> List[DatabaseInfo]:
    """List all databases on MSSQL server.
    
    Args:
        server_config: Server connection configuration
        server_group_config: Full server group configuration for extraction patterns
        include_pattern: Regex pattern to include only matching databases
        database_exclude_patterns: Patterns to exclude databases
        schema_exclude_patterns: Patterns to exclude schemas
        server_name: Name of this server (for multi-server support)
    """
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
    
    databases: List[DatabaseInfo] = []
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
            
            # Extract identifiers using configured pattern (per-server or global)
            identifiers = extract_identifiers(db_name, server_group_config, server_name)
            
            databases.append({
                'name': db_name,
                'server': server_name,  # Tag with server name for multi-server
                'service': identifiers['service'] or db_name,
                'environment': identifiers['env'],
                'customer': identifiers['customer'],
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
    server_config: ServerConfig,
    server_group_config: ServerGroupConfig,
    include_pattern: Optional[str] = None,
    database_exclude_patterns: Optional[List[str]] = None,
    schema_exclude_patterns: Optional[List[str]] = None,
    server_name: str = "default",
) -> List[DatabaseInfo]:
    """List all databases on PostgreSQL server.
    
    Args:
        server_config: Server connection configuration
        server_group_config: Full server group configuration for extraction patterns
        include_pattern: Regex pattern to include only matching databases
        database_exclude_patterns: Patterns to exclude databases
        schema_exclude_patterns: Patterns to exclude schemas
        server_name: Name of this server (for multi-server support)
    """
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
    
    databases: List[DatabaseInfo] = []
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
            
            # Extract identifiers using configured pattern (per-server or global)
            identifiers = extract_identifiers(db_name, server_group_config, server_name)
            
            databases.append({
                'name': db_name,
                'server': server_name,  # Tag with server name for multi-server
                'service': identifiers['service'] or db_name,
                'environment': identifiers['env'],
                'customer': identifiers['customer'],
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
