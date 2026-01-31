"""Validation functions for CDC service configuration."""

import re
from pathlib import Path
from typing import Dict
from cdc_generator.helpers.helpers_logging import print_header, print_info, print_success, print_warning, print_error
from cdc_generator.helpers.service_config import load_service_config

PROJECT_ROOT = Path(__file__).parent.parent.parent


def validate_service_config(service: str) -> bool:
    """Validate service configuration for pipeline generation readiness.
    
    Checks:
    1. All required fields present (with hierarchical inheritance)
    2. Database connectivity prerequisites
    3. CDC table definitions
    4. Hierarchical duplicate check
    
    Returns:
        True if validation passes, False otherwise
    """
    config = load_service_config(service)
    
    # Support both server_group (new) and mode (legacy)
    server_group = config.get('server_group')
    if server_group:
        # Map server_group to mode
        mode = 'db-per-tenant' if server_group == 'adopus' else 'db-shared'
    else:
        mode = config.get('mode', 'db-per-tenant')
    
    errors = []
    warnings = []
    
    print_header(f"Validating configuration for {service} ({mode} mode)")
    
    # 1. Check basic structure
    if 'service' not in config:
        errors.append("Missing 'service' field")
    elif config['service'] != service:
        errors.append(f"Service name mismatch: config has '{config['service']}' but file is '{service}.yaml'")
    
    # Check for either server_group or mode
    if 'server_group' not in config and 'mode' not in config:
        errors.append("Missing 'server_group' or 'mode' field")
    elif 'server_group' in config:
        if config['server_group'] not in ['adopus', 'asma']:
            errors.append(f"Invalid server_group '{config['server_group']}' (must be 'adopus' or 'asma')")
    elif 'mode' in config:
        if config['mode'] not in ['db-per-tenant', 'shared-db']:
            errors.append(f"Invalid mode '{config['mode']}' (must be 'db-per-tenant' or 'shared-db')")
    
    if 'shared' not in config or 'source_tables' not in config.get('shared', {}):
        errors.append("Missing 'shared.source_tables' - no tables defined for CDC")
    else:
        source_tables = config['shared']['source_tables']
        if not source_tables:
            warnings.append("No CDC tables defined in shared.source_tables")
        else:
            # Check if validation schema exists for auto-detection
            schemas_dir = PROJECT_ROOT / '.vscode' / 'schemas'
            has_validation_schema = schemas_dir.exists() and list(schemas_dir.glob('*.service-validation.schema.json'))
            
            # Validate table definitions
            for idx, schema_group in enumerate(source_tables):
                if 'schema' not in schema_group:
                    errors.append(f"source_tables[{idx}]: Missing 'schema' field")
                if 'tables' not in schema_group:
                    errors.append(f"source_tables[{idx}]: Missing 'tables' array")
                else:
                    schema_name = schema_group.get('schema', '?')
                    for tidx, table in enumerate(schema_group.get('tables', [])):
                        # Handle both string ("Actor") and object ({name: "Actor", ...}) formats
                        if isinstance(table, str):
                            table_name = table
                            table_dict = {}
                        else:
                            table_name = table.get('name', '?')
                            table_dict = table
                        
                        if isinstance(table, dict) and 'name' not in table:
                            errors.append(f"source_tables[{idx}].tables[{tidx}]: Missing 'name' field")
                        
                        # Check primary_key availability (only for object format)
                        if isinstance(table, dict) and 'primary_key' not in table_dict:
                            if has_validation_schema:
                                # Will be auto-detected from schema - just info
                                pass  # Silent - will be detected automatically
                            else:
                                warnings.append(f"{schema_name}.{table_name}: No primary_key specified and no validation schema found. Run: cdc manage-service --service {service} --generate-validation --all")
    
    # 2. Mode-specific checks
    if mode == 'db-per-tenant':
        if 'environments' not in config:
            errors.append("DB-per-tenant mode requires 'environments' section")
        
        if 'customers' not in config or not config.get('customers'):
            errors.append("DB-per-tenant mode requires 'customers' array with at least one customer")
        else:
            customers = config['customers']
            for cidx, customer in enumerate(customers):
                if 'name' not in customer:
                    errors.append(f"customers[{cidx}]: Missing 'name' field")
                if 'customer_id' not in customer:
                    errors.append(f"customers[{cidx}]: Missing 'customer_id' field")
                if 'schema' not in customer:
                    errors.append(f"customers[{cidx}]: Missing 'schema' field")
                if 'environments' not in customer or not customer.get('environments'):
                    errors.append(f"customers[{cidx}] ({customer.get('name', '?')}): No environments defined")
    
    elif mode == 'shared-db':
        if 'environment' not in config:
            errors.append("Shared-db mode requires 'environment' section")
    
    # 3. Check required fields for each customer+environment (hierarchical)
    def deep_merge(base: dict, override: dict) -> dict:
        """Deep merge two dictionaries, with override taking precedence."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def get_resolved_config(env_name, customer_name=None):
        """Get fully resolved config for a customer+environment with inheritance."""
        env_root = config.get('environments', {})
        
        # Extract environment defaults (top-level properties, not environment keys)
        env_pattern = re.compile(r'^(local|nonprod|prod|prod-.+)$')
        env_defaults = {k: v for k, v in env_root.items() if not env_pattern.match(k)}
        
        # Get environment-specific config
        env_specific = env_root.get(env_name, {})
        
        # Merge: defaults → env-specific (deep merge for nested objects like mssql, postgres, kafka)
        resolved = deep_merge(env_defaults, env_specific)
        
        # For db-per-tenant, also merge customer-specific
        if customer_name and mode == 'db-per-tenant':
            customers = config.get('customers', [])
            for customer in customers:
                if customer.get('name') == customer_name:
                    customer_env = customer.get('environments', {}).get(env_name, {})
                    resolved = deep_merge(resolved, customer_env)
                    break
        
        return resolved
    
    # Required fields for pipeline generation
    required_fields = {
        'database_name': 'MSSQL database name',
        'mssql.host': 'MSSQL host',
        'mssql.port': 'MSSQL port',
        'mssql.user': 'MSSQL user',
        'mssql.password': 'MSSQL password',
        'postgres.url': 'PostgreSQL connection URL',
        'postgres.user': 'PostgreSQL user',
        'postgres.password': 'PostgreSQL password',
        'kafka.bootstrap_servers': 'Kafka bootstrap servers'
    }
    
    if mode == 'db-per-tenant':
        customers = config.get('customers', [])
        for customer in customers:
            customer_name = customer.get('name', 'unknown')
            customer_envs = customer.get('environments', {})
            
            for env_name in customer_envs.keys():
                resolved = get_resolved_config(env_name, customer_name)
                
                for field, description in required_fields.items():
                    parts = field.split('.')
                    value = resolved
                    for part in parts:
                        value = value.get(part) if isinstance(value, dict) else None
                        if value is None:
                            break
                    
                    if value is None:
                        errors.append(f"{customer_name}.{env_name}: Missing {description} ({field})")
    
    elif mode == 'shared-db':
        env_config = config.get('environment', {})
        for field, description in required_fields.items():
            parts = field.split('.')
            value = env_config
            for part in parts:
                value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    break
            
            if value is None:
                errors.append(f"environment: Missing {description} ({field})")
    
    # 4. Run hierarchy validation (no duplicates check)
    print_info("\nChecking hierarchical configuration...")
    hierarchy_valid = validate_hierarchy_no_duplicates(service)
    
    # Print results
    if errors:
        print_error(f"\n{'='*80}")
        print_error("Configuration Validation Errors")
        print_error(f"{'='*80}\n")
        for error in errors:
            print_error(f"  ❌ {error}")
        print_error(f"\n{'='*80}\n")
    
    if warnings:
        print_warning(f"\n{'='*80}")
        print_warning("Configuration Warnings")
        print_warning(f"{'='*80}\n")
        for warning in warnings:
            print_warning(f"  ⚠️  {warning}")
        print_warning(f"\n{'='*80}\n")
    
    if not errors and not warnings and hierarchy_valid:
        print_success(f"\n✓ All validation checks passed for {service}")
        print_success(f"✓ Configuration is ready for pipeline generation")
        return True
    elif not errors and hierarchy_valid:
        print_warning(f"\n⚠️  Validation passed with warnings for {service}")
        return True
    else:
        print_error(f"\n✗ Validation failed for {service}")
        return False


def validate_hierarchy_no_duplicates(service: str) -> bool:
    """Validate that there are no duplicate values in the configuration hierarchy.
    
    Checks that values at lower levels (environment-specific, customer-specific) 
    are actual overrides and not redundant duplicates of inherited values.
    
    Hierarchy: environments (root) → environments.<env> → customers[].environments.<env>
    
    Returns:
        True if validation passes, False if duplicates found
    """
    config = load_service_config(service)
    
    # Support both server_group (new) and mode (legacy)
    server_group = config.get('server_group')
    if server_group:
        mode = 'db-per-tenant' if server_group == 'adopus' else 'db-shared'
    else:
        mode = config.get('mode', 'db-per-tenant')
    
    if mode == 'shared-db':
        # Shared-db doesn't have hierarchy
        return True
    
    errors = []
    warnings = []
    
    # Get root-level defaults from environments
    env_root = config.get('environments', {})
    env_defaults = {
        'sink_tasks': env_root.get('sink_tasks'),
        'existing_mssql': env_root.get('existing_mssql'),
        'mssql': env_root.get('mssql'),
        'postgres': env_root.get('postgres'),
        'kafka': env_root.get('kafka')
    }
    
    # Check each named environment
    for env_name in ['local', 'nonprod', 'prod', 'prod-fretex']:
        if env_name not in env_root:
            continue
            
        env_config = env_root[env_name]
        if not isinstance(env_config, dict):
            continue
        
        # Check each property
        for prop in ['sink_tasks', 'existing_mssql', 'mssql', 'postgres', 'kafka']:
            env_value = env_config.get(prop)
            default_value = env_defaults.get(prop)
            
            # Skip if property not set at this level
            if env_value is None:
                continue
            
            # Check if it matches parent (redundant)
            if default_value is not None and env_value == default_value:
                errors.append(
                    f"environments.{env_name}.{prop} = {env_value} is redundant "
                    f"(same as environments.{prop}). Remove to inherit."
                )
    
    # Check customers
    customers = config.get('customers', [])
    for customer in customers:
        customer_name = customer.get('name', 'unknown')
        customer_envs = customer.get('environments', {})
        
        for env_name, customer_env_config in customer_envs.items():
            if not isinstance(customer_env_config, dict):
                continue
            
            # Build inheritance chain for this customer+environment
            # Inherited values: env_root defaults → env_root[env_name] → customer[env_name]
            env_specific = env_root.get(env_name, {})
            
            for prop in ['sink_tasks', 'existing_mssql', 'mssql', 'postgres', 'kafka']:
                customer_value = customer_env_config.get(prop)
                
                # Skip if not set at customer level
                if customer_value is None:
                    continue
                
                # Determine what value would be inherited
                inherited_value = env_specific.get(prop)
                if inherited_value is None:
                    inherited_value = env_defaults.get(prop)
                
                # Check if customer value matches inherited value
                if inherited_value is not None and customer_value == inherited_value:
                    errors.append(
                        f"customers[{customer_name}].environments.{env_name}.{prop} = {customer_value} "
                        f"is redundant (inherited from environments.{env_name}.{prop} or environments.{prop}). "
                        f"Remove to inherit."
                    )
    
    # Print results
    if errors:
        print_error(f"\n{'='*80}")
        print_error("Hierarchical Inheritance Validation Errors")
        print_error(f"{'='*80}\n")
        for error in errors:
            print_error(f"  ❌ {error}")
        print_error(f"\n{'='*80}\n")
        return False
    
    if warnings:
        print_warning(f"\n{'='*80}")
        print_warning("Hierarchical Inheritance Warnings")
        print_warning(f"{'='*80}\n")
        for warning in warnings:
            print_warning(f"  ⚠️  {warning}")
        print_warning(f"\n{'='*80}\n")
    
    print_success(f"✓ Hierarchical inheritance validation passed for {service}")
    return True
