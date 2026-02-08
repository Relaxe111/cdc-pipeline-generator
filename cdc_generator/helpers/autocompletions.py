#!/usr/bin/env python3
"""Shell autocompletion helpers for CDC CLI.

DEPRECATED: This module is maintained for backward compatibility only.
All functionality has been refactored into the autocompletions/ package.

New code should import from:
    from cdc_generator.helpers.autocompletions.services import list_existing_services
    from cdc_generator.helpers.autocompletions.server_groups import list_servers_from_server_group
    etc.
"""

# Re-export all functions from the new modular structure for backward compatibility
from cdc_generator.helpers.autocompletions.scaffold import (
    scaffold_flag_completions,
)
from cdc_generator.helpers.autocompletions.schemas import (
    list_schemas_for_service,
)
from cdc_generator.helpers.autocompletions.server_groups import (
    list_databases_from_server_group,
    list_non_inherited_sink_group_names,
    list_server_group_names,
    list_servers_for_sink_group,
    list_servers_from_server_group,
    list_sink_group_names,
)
from cdc_generator.helpers.autocompletions.services import (
    list_available_services_from_server_group,
    list_existing_services,
)
from cdc_generator.helpers.autocompletions.sinks import (
    get_default_sink_for_service,
    list_available_sink_keys,
    list_custom_table_columns_for_autocomplete,
    list_custom_tables_for_service_sink,
    list_sink_keys_for_service,
    list_tables_for_sink_target,
    list_target_columns_for_sink_table,
    list_target_tables_for_sink,
)
from cdc_generator.helpers.autocompletions.tables import (
    list_columns_for_table,
    list_source_tables_for_service,
    list_tables_for_service,
)
from cdc_generator.helpers.autocompletions.types import list_pg_column_types
from cdc_generator.helpers.autocompletions.utils import (
    find_directory_upward,
    find_file_upward,
)

# Private helper function (kept for backward compatibility)
from cdc_generator.helpers.autocompletions.sinks import (
    load_sink_tables_for_autocomplete,
)

__all__ = [
    # Utilities
    "find_file_upward",
    "find_directory_upward",
    # Services
    "list_existing_services",
    "list_available_services_from_server_group",
    # Server groups
    "list_servers_from_server_group",
    "list_server_group_names",
    "list_sink_group_names",
    "list_non_inherited_sink_group_names",
    "list_servers_for_sink_group",
    "list_databases_from_server_group",
    # Schemas
    "list_schemas_for_service",
    # Tables
    "list_tables_for_service",
    "list_columns_for_table",
    "list_source_tables_for_service",
    # Sinks
    "list_sink_keys_for_service",
    "list_available_sink_keys",
    "list_target_tables_for_sink",
    "list_tables_for_sink_target",
    "get_default_sink_for_service",
    "list_target_columns_for_sink_table",
    "list_custom_tables_for_service_sink",
    "list_custom_table_columns_for_autocomplete",
    # Types
    "list_pg_column_types",
    # Scaffold
    "scaffold_flag_completions",
]


if __name__ == '__main__':
    # Delegate to the new modular implementation
    from cdc_generator.helpers.autocompletions import main
    import sys
    sys.exit(main())

