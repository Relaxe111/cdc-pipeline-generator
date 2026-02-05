"""Server group management package - modular components."""

from .config import (
    load_server_groups,
    get_single_server_group,
    load_database_exclude_patterns,
    load_schema_exclude_patterns,
    save_database_exclude_patterns,
    save_schema_exclude_patterns,
    load_env_mappings,
    save_env_mappings,
    save_server_group_preserving_comments,
    PROJECT_ROOT,
    SERVER_GROUPS_FILE,
)

from .metadata_comments import (
    get_file_header_comments,
    get_update_timestamp_comment,
    ensure_file_header_exists,
    validate_output_has_metadata,
    add_metadata_stats_comments,
)

from .scaffolding import (
    scaffold_project_structure,
    update_scaffold,
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
    handle_add_env_mapping,
    handle_update,
    handle_info,
    handle_add_server,
    handle_list_servers,
    handle_remove_server,
    handle_set_kafka_topology,
    validate_multi_server_config,
)

__all__ = [
    # Config
    'load_server_groups',
    'get_single_server_group',
    'load_database_exclude_patterns',
    'load_schema_exclude_patterns',
    'save_database_exclude_patterns',
    'save_schema_exclude_patterns',
    'load_env_mappings',
    'save_env_mappings',
    'save_server_group_preserving_comments',
    'PROJECT_ROOT',
    'SERVER_GROUPS_FILE',
    # Metadata Comments
    'get_file_header_comments',
    'get_update_timestamp_comment',
    'ensure_file_header_exists',
    'validate_output_has_metadata',
    'add_metadata_stats_comments',
    # Scaffolding
    'scaffold_project_structure',
    'update_scaffold',
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
    'handle_add_env_mapping',
    'handle_update',
    'handle_info',
    'handle_add_server',
    'handle_list_servers',
    'handle_remove_server',
    'handle_set_kafka_topology',
    'validate_multi_server_config',
]
