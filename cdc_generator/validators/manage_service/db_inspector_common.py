"""Common database inspection utilities for CDC pipeline."""

import os
from typing import Any, Dict, Optional, Union
from cdc_generator.helpers.helpers_logging import print_error, print_warning
from cdc_generator.helpers.service_config import load_service_config, load_customer_config


def expand_env_vars(value: Union[str, int, None]) -> Union[str, int, None]:
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
        print_warning(f"   - 💡 In Docker, ensure variables are in .env and restart the container.")
        
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


def get_service_db_config(service: str, env: str = 'nonprod') -> Optional[Dict[str, Any]]:
    """Get database connection configuration for a service.
    
    Args:
        service: Service name
        env: Environment name (default: nonprod)
        
    Returns:
        Dictionary with connection config or None if not found
    """
    try:
        config = load_service_config(service)
        
        # Try to get connection info from server_group.yaml first (for inspection)
        from cdc_generator.helpers.service_config import get_project_root
        import yaml  # type: ignore
        
        server_groups_file = get_project_root() / 'server_group.yaml'
        if server_groups_file.exists():
            with open(server_groups_file) as f:
                server_groups_data = yaml.safe_load(f)
                
                # Find the server group for this service
                for sg_name, sg in server_groups_data.get('server_group', {}).items():
                    # For db-per-tenant: group name IS the service name
                    if sg.get('pattern') == 'db-per-tenant' and sg_name == service:
                        server_config = sg.get('server', {})
                        database_ref = sg.get('database_ref')
                        
                        # Build connection config from server_group.yaml
                        env_config: Dict[str, Any] = {
                            'mssql': {
                                'host': server_config.get('host'),
                                'port': server_config.get('port'),
                                'user': server_config.get('user') or server_config.get('username'),
                                'password': server_config.get('password')
                            },
                            'database_name': database_ref
                        }
                        
                        return {
                            'env_config': env_config,
                            'config': config
                        }
                    
                    # For db-shared: check database service names
                    elif sg.get('pattern') == 'db-shared':
                        for db in sg.get('databases', []):
                            if db.get('service') == service:
                                server_config = sg.get('server', {})
                                db_name = db.get('name')
                                
                                # Determine database type
                                if server_config.get('type') == 'postgres':
                                    env_config: Dict[str, Any] = {
                                        'postgres': {
                                            'host': server_config.get('host'),
                                            'port': server_config.get('port'),
                                            'user': server_config.get('user') or server_config.get('username'),
                                            'password': server_config.get('password'),
                                            'database': db_name
                                        },
                                        'database_name': db_name
                                    }
                                else:
                                    env_config: Dict[str, Any] = {
                                        'mssql': {
                                            'host': server_config.get('host'),
                                            'port': server_config.get('port'),
                                            'user': server_config.get('user') or server_config.get('username'),
                                            'password': server_config.get('password')
                                        },
                                        'database_name': db_name
                                    }
                                
                                return {
                                    'env_config': env_config,
                                    'config': config
                                }
        
        # Fallback to service config (old behavior)
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


def get_connection_params(db_config: Dict[str, Any], db_type: str) -> Optional[Dict[str, Any]]:
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
