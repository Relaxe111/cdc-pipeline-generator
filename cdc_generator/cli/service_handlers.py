"""Facade exports for manage-service handlers.

Split into smaller modules to keep file size manageable and improve
maintainability. Import from this module to preserve legacy import paths.
"""

from cdc_generator.cli.service_handlers_create import (
    handle_create_service,
)
from cdc_generator.cli.service_handlers_extra import (
    handle_add_extra_column,
    handle_add_transform,
    handle_list_column_templates,
    handle_list_extra_columns,
    handle_list_transform_rules,
    handle_list_transforms,
    handle_remove_extra_column,
    handle_remove_transform,
)
from cdc_generator.cli.service_handlers_inspect import (
    handle_inspect,
)
from cdc_generator.cli.service_handlers_inspect_sink import (
    handle_inspect_sink,
)
from cdc_generator.cli.service_handlers_list_source import (
    handle_list_source_tables,
)
from cdc_generator.cli.service_handlers_misc import (
    handle_interactive,
    handle_no_service,
)
from cdc_generator.cli.service_handlers_sink import (
    handle_modify_custom_table,
    handle_sink_add,
    handle_sink_add_custom_table,
    handle_sink_add_table,
    handle_sink_list,
    handle_sink_map_column_error,
    handle_sink_remove,
    handle_sink_remove_table,
    handle_sink_update_schema,
    handle_sink_validate,
)
from cdc_generator.cli.service_handlers_source import (
    handle_add_source_table,
    handle_add_source_tables,
    handle_remove_table,
)
from cdc_generator.cli.service_handlers_validation import (
    handle_generate_validation,
    handle_validate_config,
    handle_validate_hierarchy,
)

__all__ = [
    "handle_add_extra_column",
    "handle_add_source_table",
    "handle_add_source_tables",
    "handle_add_transform",
    "handle_create_service",
    "handle_generate_validation",
    "handle_inspect",
    "handle_inspect_sink",
    "handle_interactive",
    "handle_list_column_templates",
    "handle_list_extra_columns",
    "handle_list_source_tables",
    "handle_list_transform_rules",
    "handle_list_transforms",
    "handle_modify_custom_table",
    "handle_no_service",
    "handle_remove_extra_column",
    "handle_remove_table",
    "handle_remove_transform",
    "handle_sink_add",
    "handle_sink_add_custom_table",
    "handle_sink_add_table",
    "handle_sink_list",
    "handle_sink_map_column_error",
    "handle_sink_remove",
    "handle_sink_remove_table",
    "handle_sink_update_schema",
    "handle_sink_validate",
    "handle_validate_config",
    "handle_validate_hierarchy",
]
