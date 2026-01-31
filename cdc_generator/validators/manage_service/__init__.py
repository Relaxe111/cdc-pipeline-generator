"""Manage Tables package - modular CDC service and table management."""

from .config import (
    get_available_services,
    load_service_schema_tables,
    get_table_schema_definition,
    save_service_config,
    detect_service_mode
)

from .mssql_inspector import inspect_mssql_schema
from .postgres_inspector import inspect_postgres_schema

from .table_operations import (
    add_table_to_service,
    remove_table_from_service
)

from .validation import (
    validate_service_config,
    validate_hierarchy_no_duplicates
)

from .service_creator import create_service

from .schema_generator import (
    generate_service_validation_schema,
    save_detailed_schema
)

from .interactive import (
    prompt_select,
    prompt_multiselect,
    prompt_mappings,
    validate_table_compatibility
)

from .interactive_mode import run_interactive_mode

__all__ = [
    # Config operations
    "get_available_services",
    "load_service_schema_tables",
    "get_table_schema_definition",
    "save_service_config",
    "detect_service_mode",
    # MSSQL inspection
    "inspect_mssql_schema",
    # PostgreSQL inspection
    "inspect_postgres_schema",
    # Table operations
    "add_table_to_service",
    "remove_table_from_service",
    # Validation
    "validate_service_config",
    "validate_hierarchy_no_duplicates",
    # Service creation
    "create_service",
    # Schema generation
    "generate_service_validation_schema",
    "save_detailed_schema",
    # Interactive
    "prompt_select",
    "prompt_multiselect",
    "prompt_mappings",
    "validate_table_compatibility",
    "run_interactive_mode"
]
