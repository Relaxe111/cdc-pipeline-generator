#!/usr/bin/env python3
"""
Shared module for loading service and customer configurations.
Supports both new service-based format (services/) and legacy format (2-customers/).
"""

from pathlib import Path
from typing import Any

from cdc_generator.helpers.yaml_loader import yaml


def get_project_root() -> Path:
    """Get the project root directory of the implementation.
    
    Searches upwards from the current working directory for a directory containing
    known markers: 'source-groups.yaml', 'services/', or '2-customers/'.
    This allows the tool to work correctly from any subdirectory within an implementation repo.
    
    Returns:
        Path to implementation root directory
        
    Note:
        As a fallback, returns current directory instead of raising error,
        so new files are created where the command is executed.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        server_group = parent / "source-groups.yaml"
        services_dir = parent / "services"
        customers_dir = parent / "2-customers"
        if server_group.exists() or services_dir.is_dir() or customers_dir.is_dir():
            return parent

    # Fallback to current directory (allows new implementation scaffolding)
    return current


def load_service_config(service_name: str = "adopus") -> dict[str, object]:
    """Load service configuration from services/, preserving comments.
    
    Supports both formats:
    - New: {service_name: {source: {...}, customers: [...]}}
    - Legacy: {service: service_name, source: {...}, customers: [...]}
    
    Always returns legacy format for backward compatibility.
    """
    services_dir = get_project_root() / "services"
    service_path = services_dir / f"{service_name}.yaml"
    if not service_path.exists():
        raise FileNotFoundError(f"Service config not found: {service_path}")
    with open(service_path) as f:
        raw_config = yaml.load(f)
    
    # Check if new format (service name as root key)
    if isinstance(raw_config, dict) and service_name in raw_config:
        # New format: extract service config and add 'service' field for backward compatibility
        service_config = raw_config[service_name]
        if isinstance(service_config, dict):
            config = service_config  # type: ignore[assignment]
            config['service'] = service_name  # type: ignore[index]
            return config  # type: ignore[return-value]
    
    # Legacy format: already has 'service' field
    return raw_config  # type: ignore[return-value]


def merge_customer_config(service_config: dict[str, object], customer_name: str) -> dict[str, object]:
    """Merge shared service config with customer-specific overrides.
    
    Returns a dict compatible with old customer YAML format for backward compatibility.
    """
    # Find customer in service config
    customers = service_config.get('customers', [])
    customer_data = None
    for c in customers:  # type: ignore[attr-defined]
        if c.get('name') == customer_name:  # type: ignore[attr-defined]
            customer_data = c  # type: ignore[assignment]
            break

    if not customer_data:
        raise ValueError(f"Customer '{customer_name}' not found in service config")

    # Flatten hierarchical source_tables structure to old format for backward compatibility
    # New format: [{schema: "dbo", tables: [{name: "Actor", primary_key: "actno"}]}]
    # Or simplified: [{schema: "dbo", tables: ["Actor", "Fraver"]}]  (when no extra properties)
    # Old format: [{schema: "dbo", table: "Actor", primary_key: "actno"}]
    source_tables_hierarchical = service_config.get('shared', {}).get('source_tables', [])  # type: ignore[attr-defined]
    ignore_tables = service_config.get('shared', {}).get('ignore_tables', [])  # type: ignore[attr-defined]

    source_tables_flat = []
    for schema_group in source_tables_hierarchical:  # type: ignore[attr-defined]
        schema_name = schema_group.get('schema')  # type: ignore[attr-defined]
        for table in schema_group.get('tables', []):  # type: ignore[attr-defined]
            # Handle both string format ("Actor") and object format ({name: "Actor", ...})
            if isinstance(table, str):
                table_name = table
                table_dict = {'name': table}
            else:
                table_name = table.get('name')  # type: ignore[attr-defined]
                table_dict = table  # type: ignore[assignment]

            # Check if table should be ignored (ignore_tables has priority)
            should_ignore = False
            for ignore_entry in ignore_tables:  # type: ignore[attr-defined]
                if isinstance(ignore_entry, str):
                    # Simple format: just table name (assumes dbo schema)
                    if table_name == ignore_entry and schema_name == 'dbo':
                        should_ignore = True
                        break
                elif isinstance(ignore_entry, dict):
                    # Advanced format: {schema: "dbo", table: "TableName"}
                    if (ignore_entry.get('table') == table_name and  # type: ignore[attr-defined]
                        ignore_entry.get('schema', 'dbo') == schema_name):  # type: ignore[attr-defined]
                        should_ignore = True
                        break

            if not should_ignore:
                table_config = {  # type: ignore[var-annotated]
                    'schema': schema_name,
                    'table': table_name,
                    'primary_key': table_dict.get('primary_key')  # type: ignore[attr-defined]
                }

                # Handle column filtering (ignore_columns has priority over include_columns)
                ignore_cols = table_dict.get('ignore_columns')  # type: ignore[attr-defined]
                include_cols = table_dict.get('include_columns')  # type: ignore[attr-defined]

                if ignore_cols:
                    table_config['ignore_columns'] = ignore_cols  # type: ignore[index]
                elif include_cols:
                    table_config['include_columns'] = include_cols  # type: ignore[index]

                source_tables_flat.append(table_config)  # type: ignore[arg-type]

    # Start with backward-compatible structure
    merged = {  # type: ignore[var-annotated]
        'customer': customer_name,
        'schema': customer_data.get('schema', customer_name),  # type: ignore[attr-defined]
        'customer_id': customer_data.get('customer_id'),  # type: ignore[attr-defined]
        'cdc_tables': source_tables_flat,
        'environments': {}
    }

    # Get shared environment defaults
    shared_envs = service_config.get('environments', {})  # type: ignore[attr-defined]

    # Get customer-specific environments
    customer_envs = customer_data.get('environments', {})  # type: ignore[attr-defined]

    # Merge each environment (customer overrides shared)
    for env_name, customer_env_config in customer_envs.items():  # type: ignore[attr-defined]
        # Start with shared environment config as base
        env_merged = shared_envs.get(env_name, {}).copy() if env_name in shared_envs else {}  # type: ignore[attr-defined,union-attr]

        # Deep merge customer-specific config
        for key, value in customer_env_config.items():  # type: ignore[attr-defined]
            if isinstance(value, dict) and key in env_merged and isinstance(env_merged[key], dict):
                # Deep merge for nested dicts (like mssql, postgres, kafka)
                env_merged[key] = {**env_merged[key], **value}
            else:
                # Override for simple values
                env_merged[key] = value  # type: ignore[index]

        merged['environments'][env_name] = env_merged  # type: ignore[index]

    return merged  # type: ignore[return-value]


def load_customer_config(customer: str) -> dict[str, Any]:
    """Load customer configuration - supports both new and legacy format.
    
    Priority:
    1. Try new service-based format (services/adopus.yaml)
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
            return yaml.safe_load(f)  # type: ignore[return-value,attr-defined]


def get_all_customers() -> list[str]:
    """Get list of all customers.
    
    Priority:
    1. Read from service config (services/adopus.yaml)
    2. Fall back to directory listing (2-customers/)
    """
    try:
        service_config = load_service_config("adopus")
        customers = service_config.get('customers', [])
        return [c['name'] for c in customers if 'name' in c]  # type: ignore[misc,attr-defined]
    except FileNotFoundError:
        # Fall back to old format (directory listing)
        customers_dir = get_project_root() / "2-customers"
        if not customers_dir.exists():
            return []
        return [f.stem for f in customers_dir.glob("*.yaml")]


def get_customer_environments(customer: str) -> list[str]:
    """Get list of environments configured for a customer."""
    config = load_customer_config(customer)
    return list(config.get('environments', {}).keys())
