"""CLI command handlers for server group management.

This module re-exports all handlers from the split modules for backward compatibility.
"""

# Re-export all handlers for backward compatibility
from .handlers_config import (
    handle_add_env_mapping,
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    parse_env_mapping,
)
from .handlers_group import (
    ensure_project_structure,
    handle_add_group,
    list_server_groups,
    validate_multi_server_config,
)
from .handlers_info import (
    handle_info,
)
from .handlers_server import (
    default_connection_placeholders,
    handle_add_extraction_pattern,
    handle_add_server,
    handle_list_extraction_patterns,
    handle_list_servers,
    handle_remove_extraction_pattern,
    handle_remove_server,
    handle_set_extraction_pattern,
    handle_set_kafka_topology,
)
from .handlers_update import (
    handle_update,
)

__all__ = [
    "default_connection_placeholders",
    "ensure_project_structure",
    "handle_add_env_mapping",
    "handle_add_extraction_pattern",
    "handle_add_group",
    "handle_add_ignore_pattern",
    "handle_add_schema_exclude",
    "handle_add_server",
    "handle_info",
    "handle_list_extraction_patterns",
    "handle_list_servers",
    "handle_remove_extraction_pattern",
    "handle_remove_server",
    "handle_set_extraction_pattern",
    "handle_set_kafka_topology",
    "handle_update",
    "list_server_groups",
    "parse_env_mapping",
    "validate_multi_server_config",
]
