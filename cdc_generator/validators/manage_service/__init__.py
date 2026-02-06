"""Manage Tables package - modular CDC service and table management."""

from .config import (
    detect_service_mode,
    get_available_services,
    get_table_schema_definition,
    load_service_schema_tables,
    save_service_config,
)
from .interactive import (
    prompt_mappings,
    prompt_multiselect,
    prompt_select,
    validate_table_compatibility,
)
from .interactive_mode import run_interactive_mode
from .mssql_inspector import inspect_mssql_schema
from .postgres_inspector import inspect_postgres_schema
from .schema_generator import generate_service_validation_schema, save_detailed_schema
from .service_creator import create_service
from .table_operations import add_table_to_service, remove_table_from_service
from .validation import validate_hierarchy_no_duplicates, validate_service_config

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
