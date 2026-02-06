"""JSON Schema property builders for different configuration patterns."""

from typing import Any


def build_environments_schema() -> dict[str, Any]:
    """Build environments schema (db-per-tenant pattern)."""
    return {
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
    }


def build_single_environment_schema() -> dict[str, Any]:
    """Build single environment schema (db-shared pattern)."""
    return {
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
    }


def build_customers_schema() -> dict[str, Any]:
    """Build customers array schema (db-per-tenant pattern)."""
    env_override_schema: dict[str, Any] = {
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

    return {
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
                        "local": env_override_schema,
                        "nonprod": env_override_schema,
                        "prod": env_override_schema,
                        "prod-fretex": env_override_schema
                    }
                }
            }
        }
    }


def build_conditional_requirements() -> list[dict[str, Any]]:
    """Build conditional validation rules (allOf)."""
    return [
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


def add_table_definitions(json_schema: dict[str, Any], schemas_data: dict[str, Any]) -> None:
    """Add table definitions to JSON Schema.
    
    Modifies json_schema in place to add:
    - Table definitions in #/definitions
    - anyOf references in source_tables.items.properties.tables.items.anyOf
    
    Args:
        json_schema: The JSON Schema dict to modify
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]
    """
    all_table_refs: list[dict[str, Any]] = []
    all_table_names: list[str] = []

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
