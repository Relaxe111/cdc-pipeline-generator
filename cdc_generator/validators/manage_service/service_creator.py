"""Service creation and scaffolding."""

from typing import Any, cast

import yaml  # type: ignore

from cdc_generator.helpers.helpers_logging import print_header, print_success
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.validators.manage_server_group.config import load_server_groups


def create_service(service_name: str, server_group: str, server: str = "default") -> None:
    """Create a new service configuration file.
    
    Args:
        service_name: Name of the service to create
        server_group: Server group name (e.g., 'adopus', 'asma')
        server: Server name for multi-server setups (default: 'default')
    """
    project_root = get_project_root()
    services_dir = project_root / 'services'
    services_dir.mkdir(exist_ok=True)

    service_file = services_dir / f'{service_name}.yaml'

    # Load server_group.yaml using typed loader
    server_groups_data = load_server_groups()

    # Find the server group and get its type
    validation_database = None
    schemas = []
    pattern = None

    # New structure: server group at root level with sources
    if server_group in server_groups_data:
        group = server_groups_data[server_group]
        pattern = group.get('pattern')

        # Extract schemas and validation database from sources
        sources = group.get('sources', {})

        if pattern == 'db-per-tenant':
            # Use database_ref for validation
            database_ref = group.get('database_ref')
            if database_ref and database_ref in sources:
                source_config = sources[database_ref]
                schemas = source_config.get('schemas', ['dbo'])
                # Get actual database name from first environment with server='default'
                # or fall back to first available environment
                for env_name, env_config in source_config.items():
                    if env_name == 'schemas':
                        continue
                    env_config_dict = cast(dict[str, Any], env_config)
                    if isinstance(env_config, dict) and env_config_dict.get('server') == 'default':
                        validation_database = str(env_config_dict.get('database', ''))
                        break
                # Fallback: use first available environment
                if not validation_database:
                    for env_name, env_config in source_config.items():
                        if env_name == 'schemas':
                            continue
                        env_config_dict = cast(dict[str, Any], env_config)
                        if isinstance(env_config, dict) and env_config_dict.get('database'):
                            validation_database = str(env_config_dict.get('database', ''))
                            break

            if not validation_database:
                raise ValueError(
                    f"Could not find validation database for server group '{server_group}'.\n"
                    f"Expected: sources.{database_ref}.<env>.database in server_group.yaml"
                )

        elif pattern == 'db-shared':
            # Find the source that matches this service name
            if service_name in sources:
                source_config = sources[service_name]
                schemas = source_config.get('schemas', ['public'])
                # Get actual database name from first environment with server='default'
                # or fall back to first available environment
                for env_name, env_config in source_config.items():
                    if env_name == 'schemas':
                        continue
                    env_config_dict = cast(dict[str, Any], env_config)
                    if isinstance(env_config, dict) and env_config_dict.get('server') == 'default':
                        validation_database = str(env_config_dict.get('database', ''))
                        break
                # Fallback: use first available environment
                if not validation_database:
                    for env_name, env_config in source_config.items():
                        if env_name == 'schemas':
                            continue
                        env_config_dict = cast(dict[str, Any], env_config)
                        if isinstance(env_config, dict) and env_config_dict.get('database'):
                            validation_database = str(env_config_dict.get('database', ''))
                            break

            if not validation_database:
                raise ValueError(
                    f"Could not find database for service '{service_name}' in server group '{server_group}'.\n"
                    f"Expected: sources.{service_name}.<env>.database in server_group.yaml"
                )

    if not pattern:
        raise ValueError(f"Server group '{server_group}' not found in server_group.yaml")

    # Check if service exists - update mode
    update_mode = service_file.exists()

    if update_mode:
        print_header(f"Updating {pattern} service: {service_name}")
        # Load existing service file
        with open(service_file) as f:
            existing_service = yaml.safe_load(f)
    else:
        print_header(f"Creating new {pattern} service: {service_name}")
        existing_service = None

    if pattern == 'db-per-tenant':
        template: dict[str, Any] = {
            'service': service_name,
            'source': {
                'validation_database': validation_database,
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'dbo.Actor': {primary_key: 'actno', ignore_columns: []}
                    # 'dbo.User': {primary_key: 'userid'}
                }
            },
            'customers': [
                {
                    'name': 'customer1',
                    'customer_id': 1,
                    'schema': 'customer1',
                    'environments': {
                        'local': {
                            'database_name': 'customer1'
                        },
                        'nonprod': {
                            'database_name': 'Customer1Test'
                        },
                        'prod': {
                            'database_name': 'Customer1Prod'
                        }
                    }
                }
            ]
        }

    else:  # db-shared
        template: dict[str, Any] = {
            'service': service_name,
            'source': {
                'validation_database': validation_database,
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'public.users': {primary_key: 'id'}
                    # 'logs.events': {primary_key: 'event_id', ignore_columns: ['debug_data']}
                }
            }
        }

    # If updating, merge with existing configuration
    if update_mode and existing_service:
        # Update validation_database if found in server_group.yaml
        if validation_database and 'source' in existing_service:
            existing_service['source']['validation_database'] = validation_database

        # Update source with extracted schemas
        if schemas:
            # Ensure source exists
            if 'source' not in existing_service:
                existing_service['source'] = {}

            # Preserve existing tables (already in flat schema.table format)
            if 'tables' not in existing_service['source']:
                existing_service['source']['tables'] = {}

            # Migrate old source_tables structure if present
            if 'source_tables' in existing_service:
                migrated_tables = {}
                for schema_entry in existing_service['source_tables']:
                    schema_name = schema_entry.get('schema')
                    tables = schema_entry.get('tables', [])
                    for table in tables:
                        if isinstance(table, str):
                            table_name = table
                            migrated_tables[f"{schema_name}.{table_name}"] = {}
                        else:
                            table_name = table.get('name')
                            table_props = {k: v for k, v in table.items() if k != 'name'}
                            migrated_tables[f"{schema_name}.{table_name}"] = table_props

                # Merge migrated tables with existing flat tables
                existing_service['source']['tables'].update(migrated_tables)
                # Remove old structure
                del existing_service['source_tables']

        template = existing_service

    # Write YAML with header comment and proper formatting
    header_comment = f"""# ============================================================================
# CDC Service Configuration - Auto-managed
# ============================================================================
# ⚠️  This file is mostly READ-ONLY - modify only through CDC commands:
#
#   cdc manage-service --service {service_name} --add-source-table <schema.table>
#   cdc manage-service --service {service_name} --remove-table <schema.table>
#   cdc manage-service --create-service {service_name}
#
# 📝 MANUAL EDITS ALLOWED:
#   - source.tables - You can manually add/edit table entries (use schema.table format)
#   - Table properties: primary_key, ignore_columns, include_columns
#   - environments - Environment-specific settings (kafka, etc.)
#
# 🚫 DO NOT MANUALLY EDIT:
#   - service (service name)
#   - source.validation_database (auto-populated from server_group.yaml)
#
# ℹ️  NOTE:
#   - server_group: Auto-detected (only one per implementation)
#   - server: Determined by environment (from server_group.yaml)
#   - source.type: From server_group.yaml type field
#   - Database connections: From server_group.yaml servers configuration
# ============================================================================

"""

    with open(service_file, 'w') as f:
        f.write(header_comment)
        yaml.dump(template, f, default_flow_style=False, sort_keys=False, indent=2)

    action = "Updated" if update_mode else "Created"
    print_success(f"✓ {action} service configuration: {service_file}")

    if not update_mode:
        print_success("\nNext steps:")
        if pattern == 'db-per-tenant':
            print_success(f"  1. Edit {service_file} to configure customers and environments")
            print_success(f"  2. Add CDC tables: cdc manage-service --service {service_name} --add-source-table <schema.table>")
            print_success(f"  3. Validate: cdc manage-service --service {service_name} --validate-config")
            print_success("  4. Generate pipelines: cdc generate")
        else:
            print_success(f"  1. Add CDC tables: cdc manage-service --service {service_name} --add-source-table <schema.table>")
            print_success(f"  2. Validate: cdc manage-service --service {service_name} --validate-config")
            print_success("  3. Generate pipelines: cdc generate")
