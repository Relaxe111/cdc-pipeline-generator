"""Schema-related autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.utils import find_file_upward
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_schemas_for_service(service_name: str) -> list[str]:
    """List available schemas for a specific service from source-groups.yaml sources.

    Args:
        service_name: Name of the service.

    Returns:
        List of schema names.

    Expected YAML structure:
        server_group_name:
          sources:
            service_name:
              schemas:
                - schema1
                - schema2

    Example:
        >>> list_schemas_for_service('chat')
        ['public', 'logs', 'monitoring']
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        data = load_yaml_file(server_group_file)
        if not data:
            return []

        # Find first server group (should only be one)
        for server_group_data in data.values():
            if not isinstance(server_group_data, dict) or 'sources' not in server_group_data:
                continue

            group_dict = cast(dict[str, Any], server_group_data)
            sources = group_dict.get('sources', {})
            if not isinstance(sources, dict) or service_name not in sources:
                continue

            sources_dict = cast(dict[str, Any], sources)
            service_config = sources_dict[service_name]
            if not isinstance(service_config, dict):
                continue

            service_dict = cast(dict[str, Any], service_config)
            schemas = service_dict.get('schemas', [])
            if isinstance(schemas, list):
                schemas_list = cast(list[Any], schemas)
                return sorted(str(s) for s in schemas_list if isinstance(s, str))
            return []

        return []
    except Exception:
        return []
