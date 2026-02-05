"""CLI command handlers for server group management.

This module re-exports all handlers from the split modules for backward compatibility.
"""

# Re-export all handlers for backward compatibility
from .handlers_group import (
    ensure_project_structure,
    validate_multi_server_config,
    list_server_groups,
    handle_add_group,
)

from .handlers_config import (
    handle_add_ignore_pattern,
    handle_add_schema_exclude,
    parse_env_mapping,
    handle_add_env_mapping,
)

from .handlers_update import (
    handle_update,
)

from .handlers_info import (
    handle_info,
)

from .handlers_server import (
    default_connection_placeholders,
    handle_add_server,
    handle_list_servers,
    handle_remove_server,
    handle_set_kafka_topology,
)

__all__ = [
    "ensure_project_structure",
    "validate_multi_server_config",
    "list_server_groups",
    "handle_add_group",
    "handle_add_ignore_pattern",
    "handle_add_schema_exclude",
    "parse_env_mapping",
    "handle_add_env_mapping",
    "handle_update",
    "handle_info",
    "default_connection_placeholders",
    "handle_add_server",
    "handle_list_servers",
    "handle_remove_server",
    "handle_set_kafka_topology",
]
