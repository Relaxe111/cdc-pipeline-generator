"""Server group management package - modular components."""

from .config import (
    load_server_groups,
    get_server_group_by_name,
    load_database_exclude_patterns,
    load_schema_exclude_patterns,
    save_database_exclude_patterns,
    save_schema_exclude_patterns,
)

from .filters import (
    should_ignore_database,
    should_include_database,
    should_exclude_schema,
    infer_service_name,
)

from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases,
)

from .yaml_writer import (
    parse_existing_comments,
    update_server_group_yaml,
)

from .utils import (
    regenerate_all_validation_schemas,
    update_vscode_schema,
    update_completions,
)

from .cli_handlers import (
    list_server_groups,
    handle_add_group,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_update,
)

__all__ = [
    # Config
    'load_server_groups',
    'get_server_group_by_name',
    'load_database_exclude_patterns',
    'load_schema_exclude_patterns',
    'save_database_exclude_patterns',
    'save_schema_exclude_patterns',
    # Filters
    'should_ignore_database',
    'should_include_database',
    'should_exclude_schema',
    'infer_service_name',
    # DB Inspector
    'list_mssql_databases',
    'list_postgres_databases',
    # YAML Writer
    'parse_existing_comments',
    'update_server_group_yaml',
    # Utils
    'regenerate_all_validation_schemas',
    'update_vscode_schema',
    'update_completions',
    # CLI Handlers
    'list_server_groups',
    'handle_add_group',
    'handle_add_ignore_pattern',
    'handle_add_schema_exclude',
    'handle_update',
]
