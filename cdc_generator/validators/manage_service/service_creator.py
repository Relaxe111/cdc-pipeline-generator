"""Service creation and scaffolding."""

import yaml  # type: ignore
from typing import Dict, Any
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
    database_name = None
    schemas = []
    pattern = None
    
    # New structure: server group at root level with sources
    if server_group in server_groups_data:
        group = server_groups_data[server_group]
        pattern = group.get('pattern')
        
        # Extract database name and schemas based on pattern
        if pattern == 'db-per-tenant':
            # Use database_ref for validation
            validation_database = group.get('database_ref')
            database_name = validation_database
            # Find schemas from sources using database_ref
            sources = group.get('sources', {})
            if database_name and database_name in sources:
                source_config = sources[database_name]
                schemas = source_config.get('schemas', ['dbo'])
        elif pattern == 'db-shared':
            # Find the source that matches this service name
            sources = group.get('sources', {})
            if service_name in sources:
                source_config = sources[service_name]
                validation_database = service_name
                database_name = service_name
                schemas = source_config.get('schemas', ['public'])
    
    if not pattern:
        raise ValueError(f"Server group '{server_group}' not found in server_group.yaml")
    
    if pattern == 'db-shared' and not validation_database:
        raise ValueError(f"No database found for service '{service_name}' in server group '{server_group}'")
    
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
        template: Dict[str, Any] = {
            'service': service_name,
            'server_group': server_group,
            'server': server,  # Multi-server support: which server this service uses
            'source': {
                'type': 'mssql',
                'validation_database': validation_database or 'database_name',
                'validation_env': 'nonprod',
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'dbo.Actor': {primary_key: 'actno', ignore_columns: []}
                    # 'dbo.User': {primary_key: 'userid'}
                }
            },
            'environments': {
                # Root-level defaults (inherited by all environments)
                'sink_tasks': 8,
                'mssql': {
                    'host': '${MSSQL_HOST}',
                    'port': 1433,
                    'user': '${MSSQL_USER}',
                    'password': '${MSSQL_PASSWORD}'
                },
                'postgres': {
                    'url': '${POSTGRES_URL}',
                    'user': '${POSTGRES_USER}',
                    'password': '${POSTGRES_PASSWORD}'
                },
                'kafka': {
                    'bootstrap_servers': '${KAFKA_BOOTSTRAP_SERVERS}',
                    'sasl_username': '${KAFKA_SASL_USERNAME}',
                    'sasl_password': '${KAFKA_SASL_PASSWORD}'
                },
                
                # Environment-specific overrides
                'local': {
                    'database_name': 'customer1',
                    'mssql': {
                        'host': 'localhost',
                        'user': 'sa',
                        'password': 'YourStrong@Passw0rd'
                    },
                    'postgres': {
                        'url': 'postgres://localhost:5432/postgres',
                        'user': 'postgres',
                        'password': 'postgres'
                    },
                    'kafka': {
                        'bootstrap_servers': 'localhost:19092',
                        'sasl_username': '',
                        'sasl_password': ''
                    }
                },
                'nonprod': {
                    'database_name': '${DATABASE_NAME}',
                    # Inherits mssql, postgres, kafka from root
                },
                'prod': {
                    'database_name': '${DATABASE_NAME}',
                    # Inherits mssql, postgres, kafka from root
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
        template: Dict[str, Any] = {
            'service': service_name,
            'server_group': server_group,
            'server': server,  # Multi-server support: which server this service uses
            'source': {
                'type': 'postgres',  # or 'mssql'
                'validation_database': validation_database or 'database_name',  # Database to use for schema inspection
                'validation_env': 'nonprod',  # Environment to use for schema inspection
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'public.users': {primary_key: 'id'}
                    # 'logs.events': {primary_key: 'event_id', ignore_columns: ['debug_data']}
                }
            },
            'environments': {
                'nonprod': {
                    'postgres': {
                        'name': database_name or 'database_name',
                        'url': '${POSTGRES_URL}',
                        'user': '${POSTGRES_USER}',
                        'password': '${POSTGRES_PASSWORD}'
                    },
                    'kafka': {
                        'bootstrap_servers': '${KAFKA_BOOTSTRAP_SERVERS}'
                    }
                }
            }
        }
    
    # If updating, merge with existing configuration
    if update_mode and existing_service:
        # Update top-level fields
        existing_service['server_group'] = server_group
        
        # Update validation_database if found in server_group.yaml
        if validation_database and 'source' in existing_service:
            existing_service['source']['validation_database'] = validation_database
        
        # Update database name in environments if found
        if database_name and 'environments' in existing_service:
            for env_name, env_config in existing_service['environments'].items():
                if isinstance(env_config, dict) and 'postgres' in env_config:
                    existing_service['environments'][env_name]['postgres']['name'] = database_name
        
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
    header_comment = """# ============================================================================
# CDC Service Configuration - Auto-managed
# ============================================================================
# ⚠️  This file is mostly READ-ONLY - modify only through CDC commands:
#
#   cdc manage-service --service {service_name} --add-source-table <schema.table>
#   cdc manage-service --service {service_name} --remove-table <schema.table>
#   cdc manage-service --create-service --service {service_name} --server-group <group_name>
#
# 📝 MANUAL EDITS ALLOWED:
#   - source.tables - You can manually add/edit table entries (use schema.table format)
#   - Table properties: primary_key, ignore_columns, include_columns
#
# 🚫 DO NOT MANUALLY EDIT:
#   - service, server_group, reference (use --create-service to update)
#   - environments (auto-populated from server_group.yaml)
#   - Database connection details (managed via server_group.yaml)
# ============================================================================

""".format(service_name=service_name)
    
    with open(service_file, 'w') as f:
        f.write(header_comment)
        yaml.dump(template, f, default_flow_style=False, sort_keys=False, indent=2)
    
    action = "Updated" if update_mode else "Created"
    print_success(f"✓ {action} service configuration: {service_file}")
    
    if not update_mode:
        print_success(f"\nNext steps:")
        if pattern == 'db-per-tenant':
            print_success(f"  1. Edit {service_file} to configure your service")
            print_success(f"  2. Add CDC tables to shared.source_tables")
            print_success(f"  3. Configure customers and environments")
            print_success(f"  4. Run: cdc manage-service --service {service_name} --validate-config")
            print_success(f"  5. Generate validation schema: cdc manage-service --service {service_name} --generate-validation --all")
            print_success(f"  6. Generate pipelines: cdc generate")
        else:
            print_success(f"  1. Edit {service_file}:")
            print_success(f"     - Set source.type (postgres or mssql)")
            print_success(f"     - Set source.validation_database (database name for schema inspection)")
            print_success(f"     - Set postgres.name (PostgreSQL database name)")
            print_success(f"     - Update environment variables (url, user, password)")
            print_success(f"  2. Add CDC tables to source_tables[].tables array")
        print_success(f"  3. Run: cdc manage-service --service {service_name} --validate-config")
        print_success(f"  4. Generate validation schema: cdc manage-service --service {service_name} --generate-validation --all")
        print_success(f"  5. Generate pipelines: cdc generate")
