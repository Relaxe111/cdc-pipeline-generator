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
    """List sources defined in source-groups.yaml that don't have service files yet.

    Used for --create-service flag autocompletion (shows sources that can be created).
    Filters out services that already have a services/<name>.yaml file.

    Returns:
        List of source names from source-groups.yaml that aren't created yet.

    Expected YAML structure:
        server_group_name:
          pattern: "..."
          sources:
            service1: {...}
            service2: {...}

    Example:
        >>> list_available_services_from_server_group()
        ['calendar', 'notification']  # chat, directory already exist
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not config:
            return []

        # Extract sources from server_group structure (flat format)
        # Look for root key with server-group markers.
        all_source_names: set[str] = set()
        for group_data in config.values():
            if isinstance(group_data, dict) and (
                'pattern' in group_data or 'server_group_type' in group_data
            ):
                group_dict = cast(dict[str, Any], group_data)
                # Found server group - check for 'sources' key
                sources_obj = group_dict.get('sources', {})
                if not sources_obj:
                    # Fallback to legacy 'services' key
                    sources_obj = group_dict.get('services', {})

                if isinstance(sources_obj, dict):
                    sources_dict = cast(dict[str, Any], sources_obj)
                    all_source_names.update(sources_dict.keys())

        all_sources = sorted(all_source_names)

        if not all_sources:
            return []

        # Filter out services that already have YAML files in the SAME project
        # as source-groups.yaml. This avoids accidentally picking services/
        # from parent folders in multi-workspace layouts.
        project_services_dir = server_group_file.parent / "services"
        existing: set[str] = set()
        if project_services_dir.is_dir():
            existing = {
                yaml_file.stem
                for yaml_file in project_services_dir.glob("*.yaml")
                if yaml_file.is_file()
            }

        return [svc for svc in all_sources if svc not in existing]

    except Exception:
        return []
