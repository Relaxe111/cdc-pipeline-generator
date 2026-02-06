"""JSON Schema generation for service validation based on MSSQL database introspection."""

import json
import os
from pathlib import Path

import yaml

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_mssql import create_mssql_connection
from cdc_generator.helpers.service_config import load_customer_config, load_service_config

PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVICES_DIR = PROJECT_ROOT / 'services'

# Check for pymssql availability
try:
    import pymssql
    HAS_PYMSSQL = True
except ImportError:
    HAS_PYMSSQL = False


def load_schemas_from_yaml(service: str, schema_filter: str = None) -> dict:
    """Load table schemas from service-schemas/{service}/ YAML files.
    
    Args:
        service: Service name (e.g., 'adopus', 'directory')
        schema_filter: Optional schema name to filter by
    
    Returns:
        Dict[schema_name, Dict[table_name, table_metadata]]
    """
    service_schemas_dir = PROJECT_ROOT / 'service-schemas' / service

    if not service_schemas_dir.exists():
        return {}

    schemas_data = {}

    # Iterate through schema directories
    for schema_dir in service_schemas_dir.iterdir():
        if not schema_dir.is_dir():
            continue

        schema_name = schema_dir.name

        # Apply schema filter if provided
        if schema_filter and schema_name != schema_filter:
            continue

        schemas_data[schema_name] = {}

        # Load all table YAML files in this schema
        for table_file in schema_dir.glob('*.yaml'):
            try:
                with open(table_file) as f:
                    table_data = yaml.safe_load(f)

                table_name = table_data.get('table')
                if not table_name:
                    continue

                columns = table_data.get('columns', [])

                # Extract primary key(s) from columns
                primary_keys = [col['name'] for col in columns if col.get('primary_key')]
                primary_key = primary_keys[0] if len(primary_keys) == 1 else primary_keys if primary_keys else None

                # Store in schema data structure (matches database query format)
                schemas_data[schema_name][table_name] = {
                    'columns': columns,
                    'primary_key': primary_key
                }
            except Exception as e:
                print_error(f"Failed to load {table_file}: {e}")

    return schemas_data


def generate_service_validation_schema(service: str, env: str = 'nonprod', schema_filter: str | None = None) -> bool:
    """Generate JSON Schema for service YAML validation based on database schema.
    
    Args:
        service: Service name
        env: Environment name (default: nonprod)
        schema_filter: Optional schema name to filter (None = all schemas)
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        config = load_service_config(service)

        # Check if this is a PostgreSQL service - use YAML-based generation instead of database introspection
        server_group = config.get('server_group')
        use_yaml_schemas = False
        if server_group:
            server_groups_file = PROJECT_ROOT / 'server_group.yaml'
            if server_groups_file.exists():
                with open(server_groups_file) as f:
                    server_groups_data = yaml.safe_load(f)
                    for sg in server_groups_data.get('server_groups', []):
                        if sg.get('name') == server_group:
                            server_type = sg.get('server', {}).get('type')
                            pattern = sg.get('pattern')

                            # Use YAML-based generation for PostgreSQL services
                            if server_type == 'postgres':
                                use_yaml_schemas = True
                                print_info(f"Using YAML-based schema generation for PostgreSQL service '{service}'")

        # For YAML-based generation (PostgreSQL), skip MSSQL dependencies
        if not use_yaml_schemas:
            if not HAS_PYMSSQL:
                print_error("pymssql not installed - use: pip install pymssql")
                return False

            reference_customer = config.get('reference', 'avansas')
            customer_config = load_customer_config(reference_customer)

            # Get the specific environment config
            env_config = customer_config.get('environments', {}).get(env, {})
            if not env_config:
                print_error(f"Environment '{env}' not found for customer '{reference_customer}'")
                return False

        # Load all server groups from server_group.yaml
        server_groups_file = PROJECT_ROOT / 'server_group.yaml'
        all_server_groups = {}
        all_database_names_by_group = {}

        if server_groups_file.exists():
            with open(server_groups_file) as f:
                server_groups_data = yaml.safe_load(f)
                server_group_dict = server_groups_data.get('server_group', {})
                for group_name, sg in server_group_dict.items():
                    all_server_groups[group_name] = sg
                    # Extract all database names from this server group
                    db_names = set()
                    databases = sg.get('databases', [])
                    if databases:  # Check if databases is not None
                        for db in databases:
                            # Handle both string and dict formats
                            if isinstance(db, str):
                                db_names.add(db)
                            elif isinstance(db, dict):
                                db_names.add(db.get('name'))
                    all_database_names_by_group[group_name] = db_names

        # Get current server group and mode
        server_group = config.get('server_group')
        if server_group:
            mode = all_server_groups.get(server_group, {}).get('pattern', 'db-per-tenant')
        else:
            mode = config.get('mode', 'db-per-tenant')

        # Get database names for current server group
        database_names = all_database_names_by_group.get(server_group, set())

        # For YAML-based generation, skip database connection
        if use_yaml_schemas:
            schema_msg = f"schema: {schema_filter}" if schema_filter else "all schemas"
            print_header(f"Generating validation schema from service-schemas/{service} ({schema_msg})")

            # Load schemas from YAML files
            schemas_data = load_schemas_from_yaml(service, schema_filter)

            if not schemas_data:
                print_error(f"No schemas found in service-schemas/{service}" + (f" matching '{schema_filter}'" if schema_filter else ""))
                print_info(f"Make sure YAML files exist in service-schemas/{service}/{{schema}}/{{table}}.yaml")
                return False

            # Display loaded schemas
            for schema_name in sorted(schemas_data.keys()):
                print_info(f"Processing schema: {schema_name}")
                total_tables = len(schemas_data[schema_name])
                for idx, table_name in enumerate(sorted(schemas_data[schema_name].keys()), 1):
                    print(f"  {Colors.CYAN}[{idx}/{total_tables}]{Colors.RESET} {table_name}")

            # Get schema names for validation
            schema_names = sorted(schemas_data.keys())
            database = config.get('source', {}).get('validation_database', service)

        else:
            # MSSQL database introspection path
            # Extract connection info
            mssql = env_config.get('mssql', {})

            # Get database name from source.validation_database or fallback to env_config.database_name
            source_config = config.get('source', {})
            database = source_config.get('validation_database') or env_config.get('database_name')

            if not database:
                print_error("No validation_database found in source config and no database_name in environment config")
                return False

            # Expand environment variables (support ${VAR} format)
            def expand_env(value):
                """Expand ${VAR} and $VAR patterns."""
                if not isinstance(value, str):
                    return value
                # Replace ${VAR} with $VAR for os.path.expandvars
                value = value.replace('${', '$').replace('}', '')
                return os.path.expandvars(value)

            host = expand_env(mssql.get('host', 'localhost'))
            port = int(expand_env(mssql.get('port', '1433')))
            user = expand_env(mssql.get('user', 'sa'))
            password = expand_env(mssql.get('password', ''))

            schema_msg = f"schema: {schema_filter}" if schema_filter else "all schemas"
            print_header(f"Generating validation schema from service-schemas/{service} ({schema_msg})")

            # Load schemas from YAML files instead of querying database
            schemas_data = load_schemas_from_yaml(service, schema_filter)

            if not schemas_data:
                print_error(f"No schemas found in service-schemas/{service}" + (f" matching '{schema_filter}'" if schema_filter else ""))
                print_info(f"Make sure YAML files exist in service-schemas/{service}/{{schema}}/{{table}}.yaml")
                return False

            # Display loaded schemas
            for schema_name in sorted(schemas_data.keys()):
                print_info(f"Processing schema: {schema_name}")
                total_tables = len(schemas_data[schema_name])
                for idx, table_name in enumerate(sorted(schemas_data[schema_name].keys()), 1):
                    print(f"  {Colors.CYAN}[{idx}/{total_tables}]{Colors.RESET} {table_name}")

            # Get schema names for validation
            schema_names = sorted(schemas_data.keys())

        # Database introspection no longer needed - using YAML files
        # conn = pymssql.connect(server=host, port=port, database=database, user=user, password=password)
        # cursor = conn.cursor(as_dict=True)

        # Commented out - no longer querying database
        # # Get schemas (filtered if specified)
        # if schema_filter:
        #     cursor.execute(f"""
        #         SELECT DISTINCT TABLE_SCHEMA
        #         FROM INFORMATION_SCHEMA.TABLES
        #         WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema_filter}'
        #         ORDER BY TABLE_SCHEMA
        #     """)
        # else:
        #     cursor.execute("""
        #         SELECT DISTINCT TABLE_SCHEMA
        #         FROM INFORMATION_SCHEMA.TABLES
        #         WHERE TABLE_TYPE = 'BASE TABLE'
        #         ORDER BY TABLE_SCHEMA
        #     """)
        # schemas = [row['TABLE_SCHEMA'] for row in cursor.fetchall()]

        # if not schemas:
        #     print_error(f"No schemas found" + (f" matching '{schema_filter}'" if schema_filter else ""))
        #     return False

        # # Build schema data per database schema
        # schemas_data = {}

        # for schema_name in schemas:
        #     print_info(f"Processing schema: {schema_name}")
        #
        #     # Get tables in this schema
        #     cursor.execute(f"""
        #         SELECT TABLE_NAME
        #         FROM INFORMATION_SCHEMA.TABLES
        #         WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_TYPE = 'BASE TABLE'
        #         ORDER BY TABLE_NAME
        #     """)
        #
        #     table_rows = cursor.fetchall()
        #     total_tables = len(table_rows)
        #
        #     tables = {}
        #     for idx, row in enumerate(table_rows, 1):
        #         table_name = row['TABLE_NAME']
        #         print(f"  {Colors.CYAN}[{idx}/{total_tables}]{Colors.RESET} {table_name}")
        #
        #         # Get columns for this table
        #         cursor.execute(f"""
        #             SELECT
        #                 c.COLUMN_NAME,
        #                 c.DATA_TYPE,
        #                 c.IS_NULLABLE,
        #                 CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
        #             FROM INFORMATION_SCHEMA.COLUMNS c
        #             LEFT JOIN (
        #                 SELECT ku.COLUMN_NAME
        #                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        #                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
        #                     ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
        #                 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        #                     AND tc.TABLE_SCHEMA = '{schema_name}'
        #                     AND tc.TABLE_NAME = '{table_name}'
        #             ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
        #             WHERE c.TABLE_SCHEMA = '{schema_name}' AND c.TABLE_NAME = '{table_name}'
        #             ORDER BY c.ORDINAL_POSITION
        #         """)
        #
        #         columns = []
        #         primary_keys = []
        #         for col_row in cursor.fetchall():
        #             col_name = col_row['COLUMN_NAME']
        #             columns.append({
        #                 'name': col_name,
        #                 'type': col_row['DATA_TYPE'],
        #                 'nullable': col_row['IS_NULLABLE'] == 'YES',
        #                 'primary_key': bool(col_row['IS_PRIMARY_KEY'])
        #             })
        #             if col_row['IS_PRIMARY_KEY']:
        #                 primary_keys.append(col_name)
        #
        #         tables[table_name] = {
        #             'columns': columns,
        #             'primary_key': primary_keys[0] if len(primary_keys) == 1 else primary_keys if primary_keys else None
        #         }
        #
        #     schemas_data[schema_name] = tables

        # conn.close()  # No longer needed - using YAML files

        # Generate mini schemas for keys (BEFORE determining schema_ref so shared schemas exist)
        generate_service_enum_schema()
        generate_server_group_enum_schema()
        generate_database_name_schemas()
        generate_schema_name_schemas()
        generate_table_names_enum_schema(service, schemas_data)

        # Determine schema reference (use shared if schema list has shared version)
        schema_names = sorted(schemas_data.keys())

        # Try to find shared schema for this schema list
        if len(schema_names) == 1:
            shared_name = schema_names[0]
        else:
            shared_name = '_'.join(schema_names)

        shared_schema_file = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys' / 'schema_name' / 'shared' / f'{shared_name}.schema.json'

        if shared_schema_file.exists():
            schema_ref = f"keys/schema_name/shared/{shared_name}.schema.json"
        else:
            schema_ref = f"keys/schema_name/{database}.schema.json"

        # Generate comprehensive JSON Schema with both structural and table validation
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": f"{service.title()} Service Validation Schema",
            "description": f"Comprehensive validation schema for {service} service: structure + table content validation (from service-schemas/{service})",
            "type": "object",
            "required": ["service"],
            "definitions": {},
            "properties": {
                "service": {
                    "$ref": "keys/service.schema.json"
                },
                "server_group": {
                    "$ref": "keys/server_group.schema.json"
                },
                "reference": {
                    "type": "string",
                    "description": "Reference customer/database for schema validation (db-per-tenant only)"
                },
                "mode": {
                    "type": "string",
                    "enum": ["db-per-tenant", "shared-db"],
                    "description": "⚠️ LEGACY: Service mode - use server_group instead. db-per-tenant (database per customer) or shared-db (customer_id column)"
                },
                "source": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["mssql", "postgres"],
                            "description": "⚠️ LEGACY: Source database type (auto-detected from server_group)"
                        },
                        "validation_database": {
                            "$ref": f"keys/database_name/{server_group}.schema.json",
                            "description": f"Database name for schema validation (from server_group: {server_group})"
                        },
                        "validation_env": {
                            "type": "string",
                            "enum": ["local", "nonprod", "prod", "prod-fretex"],
                            "description": "Environment for schema validation"
                        }
                    }
                },
                "source_tables": {
                    "type": "array",
                    "description": "Source table definitions grouped by schema",
                    "items": {
                        "type": "object",
                        "required": ["schema", "tables"],
                        "properties": {
                            "schema": {
                                "$ref": schema_ref,
                                "description": f"Database schema name (from validation_database: {database})"
                            },
                            "tables": {
                                "type": "array",
                                "description": "List of tables in this schema",
                                "items": {
                                    "anyOf": []
                                }
                            }
                        }
                    }
                },
                "environments": {
                    "type": "object",
                    "description": "Environment-specific configurations with hierarchical inheritance. Properties at root level serve as defaults.",
                    "properties": {
                        "sink_tasks": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Default sink tasks (inherited by all environments unless overridden)"
                        },
                        "existing_mssql": {
                            "type": "boolean",
                            "description": "Default existing_mssql flag (inherited unless overridden)"
                        },
                        "mssql": {
                            "type": "object",
                            "description": "Default MSSQL connection (inherited unless overridden)"
                        },
                        "postgres": {
                            "type": "object",
                            "description": "Default PostgreSQL connection (inherited unless overridden)"
                        },
                        "kafka": {
                            "type": "object",
                            "description": "Default Kafka connection (inherited unless overridden)"
                        }
                    },
                    "patternProperties": {
                        "^(local|nonprod|prod|prod-.+)$": {
                            "type": "object",
                            "description": "Environment-specific overrides (inherits from root level)",
                            "properties": {
                                "sink_tasks": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "description": "Override sink tasks for this environment"
                                },
                                "existing_mssql": {
                                    "type": "boolean",
                                    "description": "Override existing_mssql for this environment"
                                },
                                "mssql": {
                                    "type": "object",
                                    "description": "Override MSSQL connection for this environment"
                                },
                                "postgres": {
                                    "type": "object",
                                    "description": "Override PostgreSQL connection for this environment"
                                },
                                "kafka": {
                                    "type": "object",
                                    "description": "Override Kafka connection for this environment"
                                }
                            }
                        }
                    }
                },
                "environment": {
                    "type": "object",
                    "description": "Single environment configuration (shared-db)",
                    "properties": {
                        "database_name": {
                            "type": "string",
                            "enum": [
                                "directory_dev", "directory_stage", "directory_test",
                                "adopus_db_directory_dev", "adopus_db_directory_stage", "adopus_db_directory_test",
                                "adpraksis_db_dev", "adpraksis_db_stage", "adpraksis_db_test",
                                "activities_db_dev", "activities_db_stage",
                                "adopus_wrapper_db_dev", "adopus_wrapper_db_stage",
                                "wrapper_db_dev_adcuris", "wrapper_db_stage_adcuris", "wrapper_db_test_adcuris",
                                "auth_dev", "auth_stage", "auth_test",
                                "calendar_dev", "calendar_stage", "calendar_test",
                                "chat_dev", "chat_stage", "chat_test",
                                "datalog_db_dev", "datalog_db_stage", "datalog_db_test",
                                "notification_db_dev", "notification_db_stage", "notification_db_test",
                                "proxy_dev", "proxy_stage", "proxy_test",
                                "tracing_dev", "tracing_stage", "tracing_test",
                                "exper_video_module_dev", "exper_video_module_stage", "exper_video_module_test",
                                "postgres", "ddn_test_db"
                            ],
                            "description": "Database name from available nonprod PostgreSQL databases"
                        },
                        "sink_tasks": {"type": "integer", "minimum": 1},
                        "existing_mssql": {"type": "boolean"},
                        "mssql": {"type": "object"},
                        "postgres": {"type": "object"}
                    }
                },
                "customers": {
                    "type": "array",
                    "description": "Customer-specific configurations (db-per-tenant only). Inherits from environments root and specific environments.",
                    "items": {
                        "type": "object",
                        "required": ["name", "customer_id", "schema"],
                        "properties": {
                            "name": {"type": "string"},
                            "customer_id": {"type": "integer"},
                            "schema": {"type": "string"},
                            "environments": {
                                "type": "object",
                                "description": "Customer-specific environment overrides (inherits from global environments)",
                                "additionalProperties": False,
                                "properties": {
                                    "local": {
                                        "type": "object",
                                        "description": "Customer+environment-specific overrides",
                                        "properties": {
                                            "database": {
                                                "type": "object",
                                                "properties": {
                                                    "name": {
                                                        "type": "string",
                                                        "description": "Database name"
                                                    }
                                                }
                                            },
                                            "database_name": {
                                                "type": "string",
                                                "description": "Database name (deprecated, use database.name instead)"
                                            },
                                            "sink_tasks": {
                                                "type": "integer",
                                                "minimum": 1,
                                                "description": "Override sink tasks for this customer+environment"
                                            },
                                            "existing_mssql": {
                                                "type": "boolean",
                                                "description": "Override existing_mssql for this customer+environment"
                                            },
                                            "mssql": {
                                                "type": "object",
                                                "description": "Override MSSQL connection for this customer+environment"
                                            },
                                            "postgres": {
                                                "type": "object",
                                                "description": "Override PostgreSQL connection for this customer+environment"
                                            },
                                            "kafka": {
                                                "type": "object",
                                                "description": "Override Kafka connection for this customer+environment"
                                            },
                                            "topic_prefix": {
                                                "type": "string",
                                                "description": "Override Kafka topic prefix for this customer+environment"
                                            }
                                        }
                                    },
                                    "nonprod": {
                                        "type": "object",
                                        "description": "Customer+environment-specific overrides",
                                        "properties": {
                                            "database": {
                                                "type": "object",
                                                "properties": {
                                                    "name": {
                                                        "type": "string",
                                                        "description": "Database name"
                                                    }
                                                }
                                            },
                                            "database_name": {
                                                "type": "string",
                                                "description": "Database name (deprecated, use database.name instead)"
                                            },
                                            "sink_tasks": {
                                                "type": "integer",
                                                "minimum": 1,
                                                "description": "Override sink tasks for this customer+environment"
                                            },
                                            "existing_mssql": {
                                                "type": "boolean",
                                                "description": "Override existing_mssql for this customer+environment"
                                            },
                                            "mssql": {
                                                "type": "object",
                                                "description": "Override MSSQL connection for this customer+environment"
                                            },
                                            "postgres": {
                                                "type": "object",
                                                "description": "Override PostgreSQL connection for this customer+environment"
                                            },
                                            "kafka": {
                                                "type": "object",
                                                "description": "Override Kafka connection for this customer+environment"
                                            },
                                            "topic_prefix": {
                                                "type": "string",
                                                "description": "Override Kafka topic prefix for this customer+environment"
                                            }
                                        }
                                    },
                                    "prod": {
                                        "type": "object",
                                        "description": "Customer+environment-specific overrides",
                                        "properties": {
                                            "database": {
                                                "type": "object",
                                                "properties": {
                                                    "name": {
                                                        "type": "string",
                                                        "description": "Database name"
                                                    }
                                                }
                                            },
                                            "database_name": {
                                                "type": "string",
                                                "description": "Database name (deprecated, use database.name instead)"
                                            },
                                            "sink_tasks": {
                                                "type": "integer",
                                                "minimum": 1,
                                                "description": "Override sink tasks for this customer+environment"
                                            },
                                            "existing_mssql": {
                                                "type": "boolean",
                                                "description": "Override existing_mssql for this customer+environment"
                                            },
                                            "mssql": {
                                                "type": "object",
                                                "description": "Override MSSQL connection for this customer+environment"
                                            },
                                            "postgres": {
                                                "type": "object",
                                                "description": "Override PostgreSQL connection for this customer+environment"
                                            },
                                            "kafka": {
                                                "type": "object",
                                                "description": "Override Kafka connection for this customer+environment"
                                            },
                                            "topic_prefix": {
                                                "type": "string",
                                                "description": "Override Kafka topic prefix for this customer+environment"
                                            }
                                        }
                                    },
                                    "prod-fretex": {
                                        "type": "object",
                                        "description": "Customer+environment-specific overrides",
                                        "properties": {
                                            "database": {
                                                "type": "object",
                                                "properties": {
                                                    "name": {
                                                        "type": "string",
                                                        "description": "Database name"
                                                    }
                                                }
                                            },
                                            "database_name": {
                                                "type": "string",
                                                "description": "Database name (deprecated, use database.name instead)"
                                            },
                                            "sink_tasks": {
                                                "type": "integer",
                                                "minimum": 1,
                                                "description": "Override sink tasks for this customer+environment"
                                            },
                                            "existing_mssql": {
                                                "type": "boolean",
                                                "description": "Override existing_mssql for this customer+environment"
                                            },
                                            "mssql": {
                                                "type": "object",
                                                "description": "Override MSSQL connection for this customer+environment"
                                            },
                                            "postgres": {
                                                "type": "object",
                                                "description": "Override PostgreSQL connection for this customer+environment"
                                            },
                                            "kafka": {
                                                "type": "object",
                                                "description": "Override Kafka connection for this customer+environment"
                                            },
                                            "topic_prefix": {
                                                "type": "string",
                                                "description": "Override Kafka topic prefix for this customer+environment"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "allOf": [
                {
                    "if": {
                        "anyOf": [
                            {
                                "properties": {"mode": {"const": "db-per-tenant"}},
                                "required": ["mode"]
                            },
                            {
                                "properties": {"server_group": {"const": "adopus"}},
                                "required": ["server_group"]
                            }
                        ]
                    },
                    "then": {
                        "required": ["environments", "customers", "source", "reference"]
                    }
                },
                {
                    "if": {
                        "anyOf": [
                            {
                                "properties": {"mode": {"const": "shared-db"}},
                                "required": ["mode"]
                            },
                            {
                                "properties": {"server_group": {"const": "asma"}},
                                "required": ["server_group"]
                            }
                        ]
                    },
                    "then": {
                        "required": ["environments", "source"],
                        "not": {
                            "anyOf": [
                                {"required": ["customers"]},
                                {"required": ["reference"]}
                            ]
                        }
                    }
                }
            ]
        }

        # Create table definitions
        all_table_refs = []
        all_table_names = []
        for schema_name, tables in schemas_data.items():
            for table_name, table_info in tables.items():
                all_table_names.append(table_name)
                # Deduplicate column names
                column_names = list(dict.fromkeys([col['name'] for col in table_info['columns']]))
                primary_key = table_info['primary_key']

                # Create unique definition name
                def_name = f"{schema_name}_{table_name}_table"

                # Create table definition
                json_schema["definitions"][def_name] = {
                    "type": "object",
                    "required": ["name"],
                    "additionalProperties": False,
                    "properties": {
                        "name": {
                            "const": table_name,
                            "description": f"{schema_name}.{table_name}"
                        },
                        "primary_key": {
                            "type": "string" if isinstance(primary_key, str) else "array",
                            "description": f"Primary key: {primary_key}"
                        },
                        "ignore_columns": {
                            "type": "array",
                            "description": f"Columns to exclude from CDC (available: {len(column_names)} columns)",
                            "items": {
                                "type": "string",
                                "enum": column_names
                            }
                        },
                        "include_columns": {
                            "type": "array",
                            "description": f"Columns to include in CDC (available: {len(column_names)} columns)",
                            "items": {
                                "type": "string",
                                "enum": column_names
                            }
                        }
                    },
                    "not": {
                        "required": ["ignore_columns", "include_columns"]
                    }
                }

                # Add reference to anyOf list
                all_table_refs.append({"$ref": f"#/definitions/{def_name}"})

        # Add all table references to anyOf, plus allow simple string format with enum
        # Deduplicate table names (tables can exist in multiple schemas)
        all_table_refs.append({
            "type": "string",
            "description": "Table name (simplified format when no additional properties needed)",
            "enum": sorted(list(set(all_table_names)))
        })
        # Set anyOf for source_tables (used by both db-per-tenant and db-shared modes)
        json_schema["properties"]["source_tables"]["items"]["properties"]["tables"]["items"]["anyOf"] = all_table_refs

        # Save JSON Schema
        output_dir = PROJECT_ROOT / '.vscode' / 'schemas'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f'{database}.service-validation.schema.json'

        with open(output_file, 'w') as f:
            json.dump(json_schema, f, indent=2)

        print_success(f"\nGenerated validation schema: {output_file}")
        print_info(f"  {len(schemas_data)} schemas: {', '.join(schemas_data.keys())}")
        print_info(f"  Total tables: {sum(len(t) for t in schemas_data.values())}")

        # Detect case-variant column names across all tables
        all_columns = set()
        case_variants = {}
        for schema_name, tables in schemas_data.items():
            for table_name, table_info in tables.items():
                for col in table_info['columns']:
                    col_name = col['name']
                    col_lower = col_name.lower()
                    if col_lower not in case_variants:
                        case_variants[col_lower] = set()
                    case_variants[col_lower].add(col_name)

        # Find columns with multiple case variations
        overlapping_columns = {col_lower: sorted(variants) for col_lower, variants in case_variants.items() if len(variants) > 1}
        has_case_variants = len(overlapping_columns) > 0

        # Add schema comment to service YAML
        service_yaml_path = SERVICES_DIR / f'{service}.yaml'
        schema_comment = f"# yaml-language-server: $schema=../.vscode/schemas/{database}.service-validation.schema.json"

        # Build informational header
        info_header = f"""# The redhat.vscode-yaml extension shows ALL possible column names from ALL tables
# in autocomplete suggestions, not just columns for the specific table being edited.
# This is a known limitation of the extension's JSON Schema support.
#
# ✅ NOTE: Despite this limitation, the schema DOES validate that column names
#    you specify actually exist in the specific table. Invalid columns will be
#    marked as errors.
#
# 💡 HIERARCHICAL CONFIGURATION
# Properties set at higher levels are inherited by lower levels unless overridden.
# Inheritance chain: environments (root) → environments.<env> → customers[].environments.<env>
# To validate hierarchical configuration:
#   cdc manage-tables --service {service} --validate-hierarchy
# To validate complete configuration for pipeline generation:
#   cdc manage-tables --service {service} --validate-config
#
# 🛡️  existing_mssql FLAG
# Controls database initialization behavior:
#   • false: Fresh setup - can create database and tables (for local development)
#   • true:  Existing DB - only enable CDC, never modify database (nonprod/prod)
#
"""

        # Build warning comment if case variants exist
        warning_comment = ""
        if has_case_variants:
            # Create the overlapping columns section - show ALL of them
            overlap_list = []
            for col_lower, variants in sorted(overlapping_columns.items()):
                overlap_list.append(f"#   - {', '.join(variants)}")
            overlap_section = '\n'.join(overlap_list)

            warning_comment = f"""#
# ⚠️  CASE SENSITIVITY WARNING
# The following columns exist with different casing across tables:
{overlap_section}
#
# Always verify the exact column name casing for your specific table.
"""

        final_comment = f"# 📝 To verify column names for a specific table:\n#    cdc manage-service --service {service} --inspect --schema {{schema_name}}\n# ==================================================================================\n"

        full_header = schema_comment + '\n' + info_header + warning_comment + final_comment

        with open(service_yaml_path) as f:
            content = f.read()

        # Remove ALL old header comments (everything before first non-comment line)
        lines = content.split('\n')
        first_non_comment_idx = 0
        for i, line in enumerate(lines):
            if line and not line.startswith('#'):
                first_non_comment_idx = i
                break

        # Keep only the actual YAML content
        yaml_content = '\n'.join(lines[first_non_comment_idx:])

        # Write new header + content
        with open(service_yaml_path, 'w') as f:
            f.write(full_header + '\n' + yaml_content)

        print_success(f"\n✓ Updated schema validation in {service}.yaml")

        return True

    except Exception as e:
        print_error(f"Failed to generate validation schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def save_detailed_schema(service: str, env: str, schema: str, tables: list[dict]) -> bool:
    """Save detailed table schema information to YAML file.
    
    Args:
        service: Service name
        env: Environment name
        schema: Database schema name
        tables: List of table dictionaries from MSSQL inspection
    
    Returns:
        True if schema saved successfully, False otherwise
    """
    if not HAS_PYMSSQL:
        print_error("pymssql not installed - use: pip install pymssql")
        return False

    try:
        print_info(f"Saving detailed schema for {len(tables)} tables...")
        output_dir = PROJECT_ROOT / 'generated' / 'schemas'
        output_dir.mkdir(parents=True, exist_ok=True)

        config = load_service_config(service)
        reference = config.get('reference', 'avansas')
        customer_config = load_customer_config(reference)
        env_config = customer_config.get('environments', {}).get(env, {})
        database = env_config.get('database_name', 'unknown')

        schema_data = {
            'database': database,
            'schema': schema,
            'service': service,
            'tables': {}
        }

        # Connect and get detailed schema
        mssql = env_config.get('mssql', {})

        # Expand environment variables (support ${VAR} format)
        def expand_env(value):
            """Expand ${VAR} and $VAR patterns."""
            if not isinstance(value, str):
                return value
            value = value.replace('${', '$').replace('}', '')
            return os.path.expandvars(value)

        host = expand_env(mssql.get('host', 'localhost'))
        port = int(expand_env(mssql.get('port', '1433')))
        user = expand_env(mssql.get('user', 'sa'))
        password = expand_env(mssql.get('password', ''))

        conn = create_mssql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        for i, table in enumerate(tables, 1):
            table_name = table['TABLE_NAME']
            print(f"  [{i}/{len(tables)}] {table_name}")

            # Get detailed column info
            cursor.execute(f"""
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.IS_NULLABLE,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                        ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND tc.TABLE_SCHEMA = '{schema}'
                        AND tc.TABLE_NAME = '{table_name}'
                ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE c.TABLE_SCHEMA = '{schema}' 
                    AND c.TABLE_NAME = '{table_name}'
                ORDER BY c.ORDINAL_POSITION
            """)

            columns = []
            primary_keys = []
            for row in cursor:
                col_name, data_type, max_len, is_nullable, is_pk = row
                columns.append({
                    'name': col_name,
                    'type': data_type,
                    'nullable': is_nullable == 'YES',
                    'primary_key': bool(is_pk)
                })
                if is_pk:
                    primary_keys.append(col_name)

            schema_data['tables'][table_name] = {
                'columns': columns,
                'primary_key': primary_keys[0] if len(primary_keys) == 1 else primary_keys if primary_keys else None
            }

        conn.close()

        # Save to YAML
        output_file = output_dir / f'{database}_{schema}.yaml'
        with open(output_file, 'w') as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        print_success(f"\nSchema saved to {output_file}")
        print_info(f"  {len(tables)} tables documented")
        return True

    except Exception as e:
        print_error(f"Failed to save schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_table_names_enum_schema(service: str, schemas_data: dict) -> bool:
    """Generate mini schema for table names (schema.table format) for a service.
    
    Args:
        service: Service name (e.g., 'adopus', 'proxy')
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]
    
    Saves to .vscode/schemas/keys/table_name/{service}.schema.json
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        # Build list of schema.table qualified names
        table_names = []
        for schema_name, tables in schemas_data.items():
            for table_name in tables.keys():
                qualified_name = f"{schema_name}.{table_name}"
                table_names.append(qualified_name)

        if not table_names:
            print_info(f"No tables found for service '{service}'")
            return True

        # Create the mini schema
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": f"table_name/{service}.schema.json",
            "title": f"Table Name Validation ({service})",
            "description": f"Valid schema-qualified table names for service '{service}' (schema.table format, auto-generated from service-schemas/{service})",
            "type": "string",
            "enum": sorted(table_names)
        }

        # Save to keys/table_name directory
        keys_dir = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys' / 'table_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / f'{service}.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated table_name mini schema: {output_file}")
        print_info(f"  {len(table_names)} tables: {', '.join(sorted(table_names)[:5])}{'...' if len(table_names) > 5 else ''}")

        return True

    except Exception as e:
        print_error(f"Failed to generate table_name mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_service_enum_schema() -> bool:
    """Generate mini schema for 'service' key from server_group.yaml.
    
    Extracts service names based on pattern:
    - db-per-tenant: uses top-level 'service' field
    - db-shared: uses 'databases[].service' values
    
    Saves to .vscode/schemas/keys/service.schema.json
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        server_groups_file = PROJECT_ROOT / 'server_group.yaml'
        if not server_groups_file.exists():
            print_error(f"server_group.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        services = set()

        for group_name, group in data.get('server_group', {}).items():
            group_type = group.get('server_group_type')

            if group_type == 'db-per-tenant':
                # For db-per-tenant: group name IS the service name
                services.add(group_name)
            elif group_type == 'db-shared':
                # Collect all unique service names from databases
                for db in group.get('databases', []):
                    service_name = db.get('service')
                    if service_name:
                        services.add(service_name)

        # Create the mini schema for service key only
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "service.schema.json",
            "title": "Service Name Validation",
            "description": "Valid service names from server_group.yaml (auto-generated)",
            "type": "string",
            "enum": sorted(list(services))
        }

        # Save to keys directory
        keys_dir = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / 'service.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated service mini schema: {output_file}")
        print_info(f"  {len(services)} services: {', '.join(sorted(services))}")

        return True

    except Exception as e:
        print_error(f"Failed to generate service mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_server_group_enum_schema() -> bool:
    """Generate mini schema for 'server_group' key from server_group.yaml.
    
    Extracts all server group names.
    
    Saves to .vscode/schemas/keys/server_group.schema.json
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        server_groups_file = PROJECT_ROOT / 'server_group.yaml'
        if not server_groups_file.exists():
            print_error(f"server_group.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        server_groups = list(data.get('server_group', {}).keys())

        # Create the mini schema for server_group key only
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "server_group.schema.json",
            "title": "Server Group Validation",
            "description": "Valid server group names from server_group.yaml (auto-generated)",
            "type": "string",
            "enum": sorted(server_groups)
        }

        # Save to keys directory
        keys_dir = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / 'server_group.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated server_group mini schema: {output_file}")
        print_info(f"  {len(server_groups)} groups: {', '.join(sorted(server_groups))}")

        return True

    except Exception as e:
        print_error(f"Failed to generate server_group mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_database_name_schemas() -> bool:
    """Generate mini schemas for 'validation_database' per server group.
    
    Creates one mini schema per server group with database names from that group.
    
    Saves to .vscode/schemas/keys/database_name/{server_group}.schema.json
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        server_groups_file = PROJECT_ROOT / 'server_group.yaml'
        if not server_groups_file.exists():
            print_error(f"server_group.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        keys_dir = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys' / 'database_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        generated_count = 0

        for group_name, group in data.get('server_group', {}).items():

            # Extract database names from this server group
            db_names = []

            # For db-per-tenant, include database_ref if it exists
            group_type = group.get('pattern')
            database_ref = group.get('database_ref')
            if group_type == 'db-per-tenant' and database_ref:
                db_names.append(database_ref)

            for db in group.get('databases', []):
                db_name = db.get('name')
                if db_name:
                    db_names.append(db_name)

            if not db_names:
                continue

            # Create mini schema for this server group's databases
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"database_name/{group_name}.schema.json",
                "title": f"Database Name Validation ({group_name})",
                "description": f"Valid database names from server group '{group_name}' (auto-generated)",
                "type": "string",
                "enum": sorted(set(db_names))  # Use set to deduplicate if database_ref is also in databases list
            }

            output_file = keys_dir / f'{group_name}.schema.json'
            with open(output_file, 'w') as f:
                json.dump(schema, f, indent=2)

            print_success(f"Generated database_name/{group_name} mini schema: {output_file}")
            print_info(f"  {len(db_names)} databases")
            generated_count += 1

        return generated_count > 0

    except Exception as e:
        print_error(f"Failed to generate database_name mini schemas: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_schema_name_schemas() -> bool:
    """Generate mini schemas for 'schema' field per database.
    
    For db-per-tenant: Only generates from database_ref (all customer DBs have same schema)
    For db-shared: Generates for all databases
    Optimization: Databases with identical schema lists share a schema file
    
    Saves to .vscode/schemas/keys/schema_name/{database}.schema.json or shared variants
    
    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        server_groups_file = PROJECT_ROOT / 'server_group.yaml'
        if not server_groups_file.exists():
            print_error(f"server_group.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        keys_dir = PROJECT_ROOT / '.vscode' / 'schemas' / 'keys' / 'schema_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        # Group databases by their schema list (sorted tuple for consistent matching)
        schema_groups = {}  # tuple(sorted_schemas) -> [db_names]
        databases_to_generate = []

        # First pass: collect all databases and group by schema list
        for group_name, group in data.get('server_group', {}).items():
            group_type = group.get('pattern')
            database_ref = group.get('database_ref')

            for db in group.get('databases', []):
                db_name = db.get('name')
                schemas = db.get('schemas', [])

                if not db_name or not schemas:
                    continue

                # Group by schema list (include ALL databases for shared detection)
                schema_key = tuple(sorted(schemas))
                if schema_key not in schema_groups:
                    schema_groups[schema_key] = []
                schema_groups[schema_key].append(db_name)

                # For db-per-tenant, only process database_ref for file generation
                if group_type == 'db-per-tenant' and database_ref:
                    if db_name != database_ref:
                        continue

                databases_to_generate.append((db_name, schemas))

        # Create shared schemas for schema lists used by 2+ databases
        shared_schemas_created = {}  # schema_key -> shared_filename
        shared_dir = keys_dir / 'shared'
        shared_dir.mkdir(parents=True, exist_ok=True)

        for schema_key, db_names in schema_groups.items():
            if len(db_names) >= 2:  # Shared if used by 2+ databases
                # Create a readable filename from schema list
                schemas_list = list(schema_key)
                if len(schemas_list) == 1:
                    shared_name = schemas_list[0]
                else:
                    shared_name = '_'.join(schemas_list)

                shared_file = shared_dir / f'{shared_name}.schema.json'
                schema = {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "$id": f"schema_name/shared/{shared_name}.schema.json",
                    "title": f"Schema Name Validation (shared: {', '.join(schemas_list)})",
                    "description": f"Shared schema for databases with schemas: {', '.join(schemas_list)} (used by {len(db_names)} databases)",
                    "type": "string",
                    "enum": schemas_list
                }

                with open(shared_file, 'w') as f:
                    json.dump(schema, f, indent=2)

                shared_schemas_created[schema_key] = shared_name

        # Second pass: generate individual schemas or skip if shared exists
        generated_count = 0
        skipped_for_shared = []

        for db_name, schemas in databases_to_generate:
            schema_key = tuple(sorted(schemas))

            # If shared version exists, skip individual file creation
            if schema_key in shared_schemas_created:
                skipped_for_shared.append(db_name)
                # Remove old individual file if it exists
                old_file = keys_dir / f'{db_name}.schema.json'
                if old_file.exists():
                    old_file.unlink()
                continue

            # Create mini schema for this database's schemas (unique schema list)
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"schema_name/{db_name}.schema.json",
                "title": f"Schema Name Validation ({db_name})",
                "description": f"Valid schema names from database '{db_name}' (auto-generated)",
                "type": "string",
                "enum": sorted(schemas)
            }

            output_file = keys_dir / f'{db_name}.schema.json'
            with open(output_file, 'w') as f:
                json.dump(schema, f, indent=2)

            generated_count += 1

        if generated_count > 0 or shared_schemas_created:
            msg = f"Generated {generated_count} schema_name mini schemas"
            if shared_schemas_created:
                shared_names = [name for name in shared_schemas_created.values()]
                msg += f" + {len(shared_schemas_created)} shared ({', '.join(sorted(shared_names))})"
                if skipped_for_shared:
                    msg += f" [replaced {len(skipped_for_shared)} individual schemas]"
            print_success(msg)

        return generated_count > 0 or len(shared_schemas_created) > 0

    except Exception as e:
        print_error(f"Failed to generate schema_name mini schemas: {e}")
        import traceback
        traceback.print_exc()
        return False
