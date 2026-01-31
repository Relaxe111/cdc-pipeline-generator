"""Common database inspection utilities for CDC pipeline."""

import os
from typing import Dict, Optional, Any
from helpers_logging import print_error
from service_config import load_service_config, load_customer_config


def expand_env_vars(value: Any) -> Any:
    """Expand ${VAR} and $VAR patterns in environment variables.
    
    Args:
        value: Value to expand (can be string or other type)
        
    Returns:
        Expanded value if string, original value otherwise
    """
    if not isinstance(value, str):
        return value
    # Replace ${VAR} with $VAR for os.path.expandvars
    value = value.replace('${', '$').replace('}', '')
    return os.path.expandvars(value)


def get_service_db_config(service: str, env: str = 'nonprod') -> Optional[Dict]:
    """Get database connection configuration for a service.
    
    Args:
        service: Service name
        env: Environment name (default: nonprod)
        
    Returns:
        Dictionary with connection config or None if not found
    """
    try:
        config = load_service_config(service)
        
        # For db-shared services (like directory), use direct environment config
        server_group = config.get('server_group')
        if server_group == 'asma':
            env_config = config.get('environments', {}).get(env, {})
            if not env_config:
                print_error(f"Environment '{env}' not found in service config")
                return None
            return {
                'env_config': env_config,
                'config': config
            }
        
        # For db-per-tenant services (like adopus), use reference customer
        reference_customer = config.get('reference', 'avansas')
        customer_config = load_customer_config(reference_customer)
        
        env_config = customer_config.get('environments', {}).get(env, {})
        if not env_config:
            print_error(f"Environment '{env}' not found for customer '{reference_customer}'")
            return None
        
        return {
            'env_config': env_config,
            'customer_config': customer_config,
            'config': config
        }
        
    except Exception as e:
        print_error(f"Failed to load service config: {e}")
        return None


def get_connection_params(db_config: Dict, db_type: str) -> Optional[Dict]:
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
        
        return {
            'host': expand_env_vars(mssql.get('host', 'localhost')),
            'port': int(expand_env_vars(mssql.get('port', '1433'))),
            'user': expand_env_vars(mssql.get('user', 'sa')),
            'password': expand_env_vars(mssql.get('password', '')),
            'database': database
        }
    
    elif db_type == 'postgres':
        postgres = env_config.get('postgres', {})
        database = postgres.get('name') or postgres.get('database') or env_config.get('database_name')
        
        # Try to get explicit connection params first
        host = postgres.get('host')
        port = postgres.get('port')
        user = postgres.get('user')
        password = postgres.get('password')
        
        # If host not explicit, try to parse from URL
        if not host and 'url' in postgres:
            import re
            url = expand_env_vars(postgres['url'])
            # Parse postgresql://user:password@host:port/database
            match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+?)(\?|$)', url)
            if match:
                user = match.group(1) if not user else user
                password = match.group(2) if not password else password
                host = match.group(3)
                port = int(match.group(4))
                database = match.group(5) if not database else database
        
        # Expand environment variables
        host = expand_env_vars(host) if host else 'localhost'
        port = int(expand_env_vars(port)) if port else 5432
        user = expand_env_vars(user) if user else 'postgres'
        password = expand_env_vars(password) if password else ''
        
        return {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
    
    else:
        print_error(f"Unsupported database type: {db_type}")
        return None
