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
                    pattern: "db-shared" | "db-per-tenant"

                    sources:
                        service1: {...}
                        service2: {...}

        Rules:
        - db-shared: each source is a candidate service
        - db-per-tenant: server-group name is the single candidate service

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

        # Extract candidate service names from server-group structure.
        all_source_names: set[str] = set()
        for group_name, group_data in config.items():
            if isinstance(group_data, dict) and (
                'pattern' in group_data
            ):
                group_dict = cast(dict[str, Any], group_data)
                group_pattern = _normalize_group_pattern(group_dict)

                if group_pattern == "db-per-tenant":
                    all_source_names.add(str(group_name))
                    continue

                # db-shared (or unknown): check for 'sources' key
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


def _normalize_group_pattern(group_dict: dict[str, Any]) -> str:
    """Resolve server group pattern."""
    raw_pattern = group_dict.get("pattern", "")
    return str(raw_pattern).strip().lower()


def list_available_validation_databases(
    service_name: str | None = None,
) -> list[str]:
    """List candidate validation databases from source-groups.yaml.

    Rules:
    - If ``service_name`` is provided:
      - db-per-tenant: match by server-group name
      - db-shared: match by source/service key
    - If not provided: return all discovered databases across groups.
    """
    server_group_file = find_file_upward('source-groups.yaml')
    if not server_group_file:
        return []

    try:
        config = load_yaml_file(server_group_file)
        if not isinstance(config, dict):
            return []

        requested_service = ""
        if service_name is not None:
            requested_service = str(service_name).strip()

        database_names: set[str] = set()
        for group_name, group_data in config.items():
            if not isinstance(group_data, dict):
                continue

            group_dict = cast(dict[str, Any], group_data)
            group_pattern = _normalize_group_pattern(group_dict)

            sources_obj = group_dict.get('sources', {})
            if not isinstance(sources_obj, dict):
                continue
            sources_dict = cast(dict[str, Any], sources_obj)

            if requested_service:
                if group_pattern == 'db-per-tenant':
                    if str(group_name) != requested_service:
                        continue
                    _collect_databases_from_sources(sources_dict, database_names)
                    continue

                source_entry = sources_dict.get(requested_service)
                if not isinstance(source_entry, dict):
                    continue
                _collect_databases_from_source_entry(
                    cast(dict[str, Any], source_entry),
                    database_names,
                )
                continue

            _collect_databases_from_sources(sources_dict, database_names)

        return sorted(database_names)
    except Exception:
        return []


def _collect_databases_from_sources(
    sources_dict: dict[str, Any],
    output: set[str],
) -> None:
    """Collect databases from all source entries in a sources map."""
    for source_entry in sources_dict.values():
        if not isinstance(source_entry, dict):
            continue
        _collect_databases_from_source_entry(cast(dict[str, Any], source_entry), output)


def _collect_databases_from_source_entry(
    source_entry: dict[str, Any],
    output: set[str],
) -> None:
    """Collect environment-level database values from a single source entry."""
    direct_database = source_entry.get('database', source_entry.get('database_name'))
    if direct_database is not None:
        direct_name = str(direct_database).strip()
        if direct_name:
            output.add(direct_name)

    for env_name, env_config in source_entry.items():
        if env_name == 'schemas' or not isinstance(env_config, dict):
            continue

        env_dict = cast(dict[str, Any], env_config)
        raw_database = env_dict.get('database', env_dict.get('database_name'))
        if raw_database is None:
            continue

        database_name = str(raw_database).strip()
        if database_name:
            output.add(database_name)
