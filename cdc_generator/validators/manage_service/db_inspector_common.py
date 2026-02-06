"""Common database inspection utilities for CDC pipeline."""

import os
import re
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error, print_warning
from cdc_generator.helpers.service_config import load_service_config
from cdc_generator.validators.manage_server_group.config import (
    get_server_group_for_service,
    load_server_groups,
)


def expand_env_vars(value: str | int | None) -> str | int | None:
    """Expand ${VAR} and $VAR patterns in environment variables.
    
    Args:
        value: Value to expand (can be string or other type)
        
    Returns:
        Expanded value if string, original value otherwise
    """
    if not isinstance(value, str):
        return value

    original_value = value
    # Replace ${VAR} with $VAR for os.path.expandvars
    value = value.replace('${', '$').replace('}', '')
    expanded = os.path.expandvars(value)

    # Check if expansion actually happened (variable was set)
    if expanded == value and '$' in value:
        # Variable wasn't expanded - it's not in the environment
        var_name = value.replace('$', '').split('/')[0].split(':')[0]  # Extract variable name

        print_warning(f"⚠️ Environment variable '{var_name}' not set.")
        print_warning(f"   - Using literal value: {original_value}")
        print_warning("   - 💡 In Docker, ensure variables are in .env and restart the container.")

        # Show available environment variables for debugging
        relevant_vars = {k: v for k, v in os.environ.items()
                        if any(keyword in k.upper() for keyword in ['MSSQL', 'POSTGRES', 'DB', 'DATABASE', 'HOST', 'PORT', 'USER', 'PASSWORD'])}

        if relevant_vars:
            print_warning("\n   Available database-related environment variables:")
            for k, v in sorted(relevant_vars.items()):
                # Mask password values
                display_value = '***' if 'PASSWORD' in k.upper() or 'PASS' in k.upper() else v
                print_warning(f"     - {k}={display_value}")
        else:
            print_warning("\n   No database-related environment variables found in the container.")

    return expanded


def get_service_db_config(service: str, env: str = 'nonprod') -> dict[str, Any] | None:
    """Get database connection configuration for a service.
    
    Args:
        service: Service name
        env: Environment name (default: nonprod)
        
    Returns:
        Dictionary with connection config or None if not found
    """
    try:
        config = load_service_config(service)

        # Get server group configuration using typed loaders
        server_groups_data = load_server_groups()
        server_group_name = get_server_group_for_service(service, server_groups_data)

        if not server_group_name:
            print_error(f"Could not find server group for service '{service}'")
            return None

        server_group = server_groups_data[server_group_name]
        db_type = server_group.get('type', 'postgres')  # 'mssql' or 'postgres'

        # Get validation database from service config
        validation_database = config.get('source', {}).get('validation_database')
        if not validation_database:
            print_error(f"No validation_database found in service config for '{service}'")
            return None

        # Find the environment with this database in server_group.yaml sources
        sources = server_group.get('sources', {})
        service_sources = sources.get(service, {})

        # Find which environment has this database
        env_config_found: dict[str, Any] | None = None
        for env_name, env_data in service_sources.items():
            if env_name == 'schemas':  # Skip schemas list
                continue
            env_data_dict = cast(dict[str, Any], env_data)
            if env_data_dict.get('database') == validation_database:
                # Get server configuration
                server_name = str(env_data_dict.get('server', 'default'))
                servers = server_group.get('servers', {})
                server_config = servers.get(server_name, {})

                # Build connection config based on database type
                if db_type == 'postgres':
                    env_config_found = {
                        'postgres': {
                            'host': str(expand_env_vars(server_config.get('host')) or 'localhost'),
                            'port': int(expand_env_vars(server_config.get('port')) or 5432),
                            'user': str(expand_env_vars(server_config.get('user') or server_config.get('username')) or 'postgres'),
                            'password': str(expand_env_vars(server_config.get('password')) or ''),
                            'database': validation_database
                        },
                        'database_name': validation_database
                    }
                else:  # mssql
                    env_config_found = {
                        'mssql': {
                            'host': str(expand_env_vars(server_config.get('host')) or 'localhost'),
                            'port': int(expand_env_vars(server_config.get('port')) or 1433),
                            'user': str(expand_env_vars(server_config.get('user') or server_config.get('username')) or 'sa'),
                            'password': str(expand_env_vars(server_config.get('password')) or '')
                        },
                        'database_name': validation_database
                    }
                break

        if not env_config_found:
            print_error(f"Could not find environment config for database '{validation_database}' in server_group.yaml")
            return None

        return {
            'env_config': env_config_found,
            'config': config
        }

    except Exception as e:
        print_error(f"Failed to load service config: {e}")
        return None
        return None


def get_connection_params(db_config: dict[str, Any], db_type: str) -> dict[str, Any] | None:
    """Extract connection parameters from database config.
    
    Args:
        db_config: Database configuration from get_service_db_config()
        db_type: Database type ('mssql' or 'postgres')
        
    Returns:
        Dictionary with connection parameters or None if not found
    """
    env_config = db_config.get('env_config', {})

    if db_type == 'mssql':
        mssql = env_config.get('mssql', {})
        database = env_config.get('database_name')

        port_val = expand_env_vars(mssql.get('port', '1433'))
        return {
            'host': str(expand_env_vars(mssql.get('host', 'localhost')) or 'localhost'),
            'port': int(port_val) if port_val and str(port_val).isdigit() else 1433,
            'user': str(expand_env_vars(mssql.get('user', 'sa')) or 'sa'),
            'password': str(expand_env_vars(mssql.get('password', '')) or ''),
            'database': database
        }

    if db_type == 'postgres':
        postgres = env_config.get('postgres', {})
        database = postgres.get('name') or postgres.get('database') or env_config.get('database_name')

        # Try to get explicit connection params first
        host = postgres.get('host')
        port = postgres.get('port')
        user = postgres.get('user')
        password = postgres.get('password')

        # If host not explicit, try to parse from URL
        if not host and 'url' in postgres:
            url = expand_env_vars(postgres['url'])
            url_str = str(url) if url else ''
            # Parse postgresql://user:password@host:port/database
            match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+?)(\?|$)', url_str)
            if match:
                user = str(match.group(1)) if not user else user
                password = str(match.group(2)) if not password else password
                host = str(match.group(3))
                port = int(match.group(4))
                database = str(match.group(5)) if not database else database

        # Expand environment variables
        host_val = expand_env_vars(host) if host else None
        host = str(host_val or 'localhost')
        port_val = expand_env_vars(port) if port else None
        port = int(port_val) if port_val and str(port_val).isdigit() else 5432
        user_val = expand_env_vars(user) if user else None
        user = str(user_val or 'postgres')
        password_val = expand_env_vars(password) if password else None
        password = str(password_val or '')

        return {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }

    print_error(f"Unsupported database type: {db_type}")
    return None
