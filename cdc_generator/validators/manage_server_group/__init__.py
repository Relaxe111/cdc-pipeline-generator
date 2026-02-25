"""Server group management package - modular components."""

from .cli_handlers import (
    handle_add_env_mapping,
    handle_add_extraction_pattern,
    handle_add_group,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    handle_add_server,
    handle_add_table_include,
    handle_add_table_exclude,
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
from .comment_processor import parse_existing_comments
from .config import (
    PROJECT_ROOT,
    SERVER_GROUPS_FILE,
    get_single_server_group,
    load_database_exclude_patterns,
    load_env_mappings,
    load_schema_exclude_patterns,
    load_server_groups,
    load_table_include_patterns,
    load_table_exclude_patterns,
    save_database_exclude_patterns,
    save_env_mappings,
    save_schema_exclude_patterns,
    save_server_group_preserving_comments,
    save_table_include_patterns,
    save_table_exclude_patterns,
)
from .db_inspector import (
    list_mssql_databases,
    list_postgres_databases,
)
from .filters import (
    infer_service_name,
    should_exclude_schema,
    should_exclude_table,
    should_include_table,
    should_ignore_database,
    should_include_database,
)
from .handlers_validation_env import (
    get_available_envs,
    handle_list_envs,
    handle_set_validation_env,
    update_envs_list,
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
from .yaml_writer import update_server_group_yaml

__all__ = [
    'PROJECT_ROOT',
    'SERVER_GROUPS_FILE',
    'add_metadata_stats_comments',
    'ensure_file_header_exists',
    'get_available_envs',
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
    'handle_add_table_include',
    'handle_add_table_exclude',
    'handle_info',
    'handle_list_envs',
    'handle_list_extraction_patterns',
    'handle_list_servers',
    'handle_remove_extraction_pattern',
    'handle_remove_server',
    'handle_set_extraction_pattern',
    'handle_set_kafka_topology',
    'handle_set_validation_env',
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
    'load_table_include_patterns',
    'load_table_exclude_patterns',
    # YAML Writer
    'parse_existing_comments',
    # Utils
    'regenerate_all_validation_schemas',
    'save_database_exclude_patterns',
    'save_env_mappings',
    'save_schema_exclude_patterns',
    'save_server_group_preserving_comments',
    'save_table_include_patterns',
    'save_table_exclude_patterns',
    # Scaffolding
    'scaffold_project_structure',
    'should_exclude_schema',
    'should_exclude_table',
    'should_include_table',
    # Filters
    'should_ignore_database',
    'should_include_database',
    'update_completions',
    'update_envs_list',
    'update_scaffold',
    'update_server_group_yaml',
    'update_vscode_schema',
    'validate_multi_server_config',
    'validate_output_has_metadata',
]
