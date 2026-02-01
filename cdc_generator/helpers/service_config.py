#!/usr/bin/env python3
"""
Shared module for loading service and customer configurations.
Supports both new service-based format (2-services/) and legacy format (2-customers/).
"""

from pathlib import Path
from ruamel.yaml import YAML
from typing import Any, Dict, List, Optional

# Initialize ruamel.yaml to preserve comments
yaml = YAML()
yaml.preserve_quotes = True
yaml.default_flow_style = False


def get_project_root() -> Path:
    """Get the project root directory of the implementation.
    
    Searches upwards from the current working directory for a directory containing
    either 'server_group.yaml' or a '2-services/' directory. This allows the
    tool to work correctly from any subdirectory within an implementation repo.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / 'server_group.yaml').exists() or (parent / '2-services').is_dir():
            return parent
    
    # Fallback or error
    raise FileNotFoundError(
        "Could not determine project root. Make sure you are inside an implementation "
        "directory (e.g., adopus-cdc-pipeline) that contains a 'server_group.yaml' file."
    )


def load_service_config(service_name: str = "adopus") -> Dict[str, Any]:
    """Load service configuration from 2-services/, preserving comments."""
    services_dir = get_project_root() / "2-services"
    service_path = services_dir / f"{service_name}.yaml"
    if not service_path.exists():
        raise FileNotFoundError(f"Service config not found: {service_path}")
    with open(service_path) as f:
        return yaml.load(f)


def merge_customer_config(service_config: dict, customer_name: str) -> dict:
    """Merge shared service config with customer-specific overrides.
    
    Returns a dict compatible with old customer YAML format for backward compatibility.
    """
    # Find customer in service config
    customers = service_config.get('customers', [])
    customer_data = None
    for c in customers:
        if c.get('name') == customer_name:
            customer_data = c
            break
    
    if not customer_data:
        raise ValueError(f"Customer '{customer_name}' not found in service config")
    
    # Flatten hierarchical source_tables structure to old format for backward compatibility
    # New format: [{schema: "dbo", tables: [{name: "Actor", primary_key: "actno"}]}]
    # Or simplified: [{schema: "dbo", tables: ["Actor", "Fraver"]}]  (when no extra properties)
    # Old format: [{schema: "dbo", table: "Actor", primary_key: "actno"}]
    source_tables_hierarchical = service_config.get('shared', {}).get('source_tables', [])
    ignore_tables = service_config.get('shared', {}).get('ignore_tables', [])
    
    source_tables_flat = []
    for schema_group in source_tables_hierarchical:
        schema_name = schema_group.get('schema')
        for table in schema_group.get('tables', []):
            # Handle both string format ("Actor") and object format ({name: "Actor", ...})
            if isinstance(table, str):
                table_name = table
                table_dict = {'name': table}
            else:
                table_name = table.get('name')
                table_dict = table
            
            # Check if table should be ignored (ignore_tables has priority)
            should_ignore = False
            for ignore_entry in ignore_tables:
                if isinstance(ignore_entry, str):
                    # Simple format: just table name (assumes dbo schema)
                    if table_name == ignore_entry and schema_name == 'dbo':
                        should_ignore = True
                        break
                elif isinstance(ignore_entry, dict):
                    # Advanced format: {schema: "dbo", table: "TableName"}
                    if (ignore_entry.get('table') == table_name and 
                        ignore_entry.get('schema', 'dbo') == schema_name):
                        should_ignore = True
                        break
            
            if not should_ignore:
                table_config = {
                    'schema': schema_name,
                    'table': table_name,
                    'primary_key': table_dict.get('primary_key')
                }
                
                # Handle column filtering (ignore_columns has priority over include_columns)
                ignore_cols = table_dict.get('ignore_columns')
                include_cols = table_dict.get('include_columns')
                
                if ignore_cols:
                    table_config['ignore_columns'] = ignore_cols
                elif include_cols:
                    table_config['include_columns'] = include_cols
                
                source_tables_flat.append(table_config)
    
    # Start with backward-compatible structure
    merged = {
        'customer': customer_name,
        'schema': customer_data.get('schema', customer_name),
        'customer_id': customer_data.get('customer_id'),
        'cdc_tables': source_tables_flat,
        'environments': {}
    }
    
    # Get shared environment defaults
    shared_envs = service_config.get('environments', {})
    
    # Get customer-specific environments
    customer_envs = customer_data.get('environments', {})
    
    # Merge each environment (customer overrides shared)
    for env_name, customer_env_config in customer_envs.items():
        # Start with shared environment config as base
        env_merged = shared_envs.get(env_name, {}).copy() if env_name in shared_envs else {}
        
        # Deep merge customer-specific config
        for key, value in customer_env_config.items():
            if isinstance(value, dict) and key in env_merged and isinstance(env_merged[key], dict):
                # Deep merge for nested dicts (like mssql, postgres, kafka)
                env_merged[key] = {**env_merged[key], **value}
            else:
                # Override for simple values
                env_merged[key] = value
        
        merged['environments'][env_name] = env_merged
    
    return merged


def load_customer_config(customer: str) -> Dict[str, Any]:
    """Load customer configuration - supports both new and legacy format.
    
    Priority:
    1. Try new service-based format (2-services/adopus.yaml)
    2. Fall back to legacy format (2-customers/{customer}.yaml)
    """
    # Try new service-based format first
    try:
        service_config = load_service_config("adopus")
        return merge_customer_config(service_config, customer)
    except (FileNotFoundError, ValueError):
        # Fall back to old format (individual customer files)
        customers_dir = get_project_root() / "2-customers"
        config_path = customers_dir / f"{customer}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Customer config not found in service or legacy format: {customer}")
        with open(config_path) as f:
            return yaml.safe_load(f)


def get_all_customers() -> List[str]:
    """Get list of all customers.
    
    Priority:
    1. Read from service config (2-services/adopus.yaml)
    2. Fall back to directory listing (2-customers/)
    """
    try:
        service_config = load_service_config("adopus")
        customers = service_config.get('customers', [])
        return [c['name'] for c in customers if 'name' in c]
    except FileNotFoundError:
        # Fall back to old format (directory listing)
        customers_dir = get_project_root() / "2-customers"
        if not customers_dir.exists():
            return []
        return [f.stem for f in customers_dir.glob("*.yaml")]


def get_customer_environments(customer: str) -> List[str]:
    """Get list of environments configured for a customer."""
    config = load_customer_config(customer)
    return list(config.get('environments', {}).keys())
