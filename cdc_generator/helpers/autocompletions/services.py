"""Service-related autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.utils import (
    find_directory_upward,
    find_file_upward,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_existing_services() -> list[str]:
    """List existing service files from services/*.yaml.

    Used for --service flag autocompletion (shows created services).

    Returns:
        List of service names (without .yaml extension).

    Example:
        >>> list_existing_services()
        ['chat', 'directory']
    """
    services_dir = find_directory_upward('services')
    if not services_dir:
        return []

    services: list[str] = []
    for yaml_file in services_dir.glob('*.yaml'):
        if yaml_file.is_file():
            services.append(yaml_file.stem)

    return sorted(services)


def list_available_services_from_server_group() -> list[str]:
    """List sources defined in source-groups.yaml (sources: section).

    Used for --create-service flag autocompletion (shows sources that can be created).

    Returns:
        List of source names from source-groups.yaml.

    Expected YAML structure:
        server_group_name:
          pattern: "..."
          sources:
            service1: {...}
            service2: {...}

    Example:
        >>> list_available_services_from_server_group()
        ['chat', 'directory', 'calendar']
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not config:
            return []

        # Extract sources from server_group structure (flat format)
        # Look for root key with 'pattern' field (server group marker)
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_dict = cast(dict[str, Any], group_data)
                # Found server group - check for 'sources' key
                sources_obj = group_dict.get('sources', {})
                if not sources_obj:
                    # Fallback to legacy 'services' key
                    sources_obj = group_dict.get('services', {})

                if isinstance(sources_obj, dict):
                    sources_dict = cast(dict[str, Any], sources_obj)
                    return sorted(sources_dict.keys())
                break

        return []

    except Exception:
        return []
