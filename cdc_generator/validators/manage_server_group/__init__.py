"""Server group management package - modular components."""

from .cli_handlers import (
    handle_add_env_mapping,
    handle_add_extraction_pattern,
    handle_add_group,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_add_server,
    handle_info,
    handle_list_extraction_patterns,
    handle_list_servers,
    handle_remove_extraction_pattern,
    handle_remove_server,
    handle_set_extraction_pattern,
    handle_set_kafka_topology,
    handle_update,
    list_server_groups,
    validate_multi_server_config,
)
from .config import (
    PROJECT_ROOT,
    SERVER_GROUPS_FILE,
    get_single_server_group,
    load_database_exclude_patterns,
    load_env_mappings,
    load_schema_exclude_patterns,
    load_server_groups,
    save_database_exclude_patterns,
    save_env_mappings,
    save_schema_exclude_patterns,
    save_server_group_preserving_comments,
)
from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases,
)
from .filters import (
    infer_service_name,
    should_exclude_schema,
    should_ignore_database,
    should_include_database,
)
from .metadata_comments import (
    add_metadata_stats_comments,
    ensure_file_header_exists,
    get_file_header_comments,
    get_update_timestamp_comment,
    validate_output_has_metadata,
)
from .scaffolding import (
    scaffold_project_structure,
    update_scaffold,
)
from .type_introspector import (
    introspect_types,
)
from .utils import (
    regenerate_all_validation_schemas,
    update_completions,
    update_vscode_schema,
)
from .yaml_writer import (
    parse_existing_comments,
    update_server_group_yaml,
)

__all__ = [
    'PROJECT_ROOT',
    'SERVER_GROUPS_FILE',
    'add_metadata_stats_comments',
    'ensure_file_header_exists',
    # Metadata Comments
    'get_file_header_comments',
    'get_single_server_group',
    'get_update_timestamp_comment',
    'handle_add_env_mapping',
    'handle_add_extraction_pattern',
    'handle_add_group',
    'handle_add_ignore_pattern',
    'handle_add_schema_exclude',
    'handle_add_server',
    'handle_info',
    'handle_list_extraction_patterns',
    'handle_list_servers',
    'handle_remove_extraction_pattern',
    'handle_remove_server',
    'handle_set_extraction_pattern',
    'handle_set_kafka_topology',
    'handle_update',
    'infer_service_name',
    # Type Introspection
    'introspect_types',
    # DB Inspector
    'list_mssql_databases',
    'list_postgres_databases',
    # CLI Handlers
    'list_server_groups',
    'load_database_exclude_patterns',
    'load_env_mappings',
    'load_schema_exclude_patterns',
    # Config
    'load_server_groups',
    # YAML Writer
    'parse_existing_comments',
    # Utils
    'regenerate_all_validation_schemas',
    'save_database_exclude_patterns',
    'save_env_mappings',
    'save_schema_exclude_patterns',
    'save_server_group_preserving_comments',
    # Scaffolding
    'scaffold_project_structure',
    'should_exclude_schema',
    # Filters
    'should_ignore_database',
    'should_include_database',
    'update_completions',
    'update_scaffold',
    'update_server_group_yaml',
    'update_vscode_schema',
    'validate_multi_server_config',
    'validate_output_has_metadata',
]
