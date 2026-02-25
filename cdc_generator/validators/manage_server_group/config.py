"""Configuration loading and management for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from collections.abc import Callable
from typing import Any, cast

from cdc_generator.helpers.service_config import get_project_root

from .types import ServerGroupConfig, ServerGroupFile

PROJECT_ROOT = get_project_root()
SERVER_GROUPS_FILE = PROJECT_ROOT / "source-groups.yaml"


# ============================================================================
# Comment-Preserving YAML Save
# ============================================================================
# IMPORTANT: All modifications to source-groups.yaml MUST use this function
# to preserve header comments and metadata.

def save_server_group_preserving_comments(
    updater: Callable[[dict[str, Any]], None],
    error_context: str = "update server group"
) -> None:
    """Save source-groups.yaml while preserving all header comments.

    This is the ONLY function that should write to source-groups.yaml.
    All other save functions must use this to preserve metadata comments.

    Args:
        updater: Function that receives the parsed config dict and modifies it in-place.
                 The updater should find the server group and update the desired field.
        error_context: Description of operation for error messages.

    Raises:
        RuntimeError: If save fails or no server group found.

    Example:
        def update_env_mappings(config: Dict[str, Any]) -> None:
            for group_data in config.values():
                if isinstance(group_data, dict) and 'pattern' in group_data:
                    group_data['env_mappings'] = {'staging': 'stage'}

        save_server_group_preserving_comments(update_env_mappings, "save env mappings")
    """
    try:
        # Read entire file to preserve comments
        with SERVER_GROUPS_FILE.open() as f:
            file_content = f.read()

        # Split into comment header and YAML content
        lines = file_content.split('\n')

        # Find where the YAML data starts (first non-comment, non-blank line)
        yaml_start_idx = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                yaml_start_idx = i
                break

        # Preserve all lines before YAML starts (header comments)
        header_lines = lines[:yaml_start_idx]

        # Parse YAML content
        yaml_content = '\n'.join(lines[yaml_start_idx:])
        config: dict[str, Any] = yaml.safe_load(yaml_content) or {}  # type: ignore[misc]

        # Apply the update
        updater(config)

        # Regenerate YAML content
        yaml_output = yaml.dump(  # type: ignore[misc]
            config,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
            allow_unicode=True,
        )

        # Combine header with updated YAML
        final_content = '\n'.join(header_lines) + '\n' + yaml_output

        with SERVER_GROUPS_FILE.open('w') as f:
            f.write(final_content)

    except FileNotFoundError:
        raise RuntimeError(
            f"Failed to {error_context}: source-groups.yaml not found"
        ) from None
    except Exception as e:
        raise RuntimeError(f"Failed to {error_context}: {e}") from e


def load_server_groups() -> ServerGroupFile:
    """Load server groups configuration from YAML file.

    Returns:
        ServerGroupFile: Dict mapping server group names to their configs

    Raises:
        FileNotFoundError: If source-groups.yaml not found in current directory or parents
    """
    if not SERVER_GROUPS_FILE.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {SERVER_GROUPS_FILE}"
        )

    with SERVER_GROUPS_FILE.open() as f:
        return cast(ServerGroupFile, yaml.safe_load(f) or {})  # type: ignore[misc]


def validate_server_group_structure(group_data: object, name: str) -> None:
    """Validate server group has expected structure.

    Raises ValueError with helpful message if structure is invalid.
    This provides runtime validation beyond TypedDict static checking.

    Args:
        group_data: Server group configuration dict
        name: Server group name (for error messages)

    Raises:
        ValueError: If structure is invalid
    """
    if not isinstance(group_data, dict):
        raise ValueError(f"Server group '{name}' must be a dict, got {type(group_data).__name__}")

    # Required fields
    if 'pattern' not in group_data:
        raise ValueError(f"Server group '{name}' missing required field 'pattern'")

    if group_data['pattern'] not in ('db-shared', 'db-per-tenant'):
        raise ValueError(
            f"Server group '{name}' has invalid pattern "
            + f"'{group_data['pattern']}' "
            + "(must be 'db-shared' or 'db-per-tenant')"
        )

    # New structure requires 'sources' at root level
    if 'sources' not in group_data:
        raise ValueError(
            f"Server group '{name}' missing required field 'sources'.\\n" +
            f"Expected structure: {name}.sources (not {name}.server_group.databases)"
        )

    if not isinstance(group_data['sources'], dict):
        raise ValueError(f"Server group '{name}' field 'sources' must be a dict")


def get_single_server_group(config: ServerGroupFile) -> ServerGroupConfig | None:
    """Get the single server group from configuration.

    Format: server_group_name as root key (e.g., asma1: {...})

    Since each implementation has only one server group, this returns the first one found.
    Adds 'name' field to the returned dict for compatibility.
    Validates structure at runtime.

    Args:
        config: Loaded server groups configuration (ServerGroupFile)

    Returns:
        ServerGroupConfig with 'name' field injected, or None if no server group exists

    Raises:
        ValueError: If server group structure is invalid
    """
    # Flat format: name as root key with 'pattern' field
    # Look for any top-level key that has a 'pattern' field (server group marker)
    for name, group_data in config.items():
        if 'pattern' in group_data:
            # Validate structure before returning
            validate_server_group_structure(group_data, name)

            # Create a mutable copy with the name injected
            result = dict(group_data)
            result['name'] = name
            return cast(ServerGroupConfig, result)

    return None


def get_server_group_for_service(
    service_name: str,
    config: ServerGroupFile | None = None,
) -> str | None:
    """Find which server group a service belongs to.

    Args:
        service_name: Service/source name to look up
        config: Optional pre-loaded config (will load if not provided)

    Returns:
        Server group name if found, None otherwise
    """
    if config is None:
        try:
            config = load_server_groups()
        except FileNotFoundError:
            return None

    for sg_name, sg_data in config.items():
        if not isinstance(sg_data, dict):
            continue

        sources_obj = sg_data.get('sources', {})
        sources = sources_obj if isinstance(sources_obj, dict) else {}
        if service_name in sources:
            return sg_name

        # db-per-tenant: service can be modeled as the server-group root key,
        # while sources contain customer entries only.
        pattern = str(sg_data.get('server_group_type', sg_data.get('pattern', '')))
        if pattern == 'db-per-tenant' and sg_name == service_name:
            return sg_name

    return None


def get_all_defined_services(config: ServerGroupFile | None = None) -> set[str]:
    """Get set of all services defined in source-groups.yaml.

    Args:
        config: Optional pre-loaded config (will load if not provided)

    Returns:
        Set of service names from all server groups
    """
    if config is None:
        try:
            config = load_server_groups()
        except FileNotFoundError:
            return set()

    services: set[str] = set()
    for sg_data in config.values():
        if 'sources' in sg_data:
            services.update(sg_data.get('sources', {}).keys())

    return services


def load_database_exclude_patterns() -> list[str]:
    """Load database exclude patterns from source-groups.yaml.

    Format: server_group_name as root key with database_exclude_patterns field.
    """
    try:
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        # Flat format: server_group_name as root key with 'pattern' field
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(list[str] | None, group_data.get('database_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns

        return []
    except Exception:
        return []


def load_schema_exclude_patterns() -> list[str]:
    """Load schema exclude patterns from source-groups.yaml.

    Format: server_group_name as root key with schema_exclude_patterns field.
    """
    try:
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        # Flat format: server_group_name as root key with 'pattern' field
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(list[str] | None, group_data.get('schema_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns

        return []
    except Exception:
        return []


def save_database_exclude_patterns(patterns: list[str]) -> None:
    """Save database exclude patterns to source-groups.yaml.

    Uses centralized save function to preserve header comments.
    """
    def updater(config: dict[str, Any]) -> None:
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['database_exclude_patterns'] = patterns
                return
        raise RuntimeError("No server group found to update")

    save_server_group_preserving_comments(updater, "save database exclude patterns")


def save_schema_exclude_patterns(patterns: list[str]) -> None:
    """Save schema exclude patterns to source-groups.yaml.

    Uses centralized save function to preserve header comments.
    """
    def updater(config: dict[str, Any]) -> None:
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['schema_exclude_patterns'] = patterns
                return
        raise RuntimeError("No server group found to update")

    save_server_group_preserving_comments(updater, "save schema exclude patterns")


def load_table_exclude_patterns() -> list[str]:
    """Load table exclude patterns from source-groups.yaml.

    Format: server_group_name as root key with table_exclude_patterns field.
    """
    try:
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(list[str] | None, group_data.get('table_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns

        return []
    except Exception:
        return []


def load_table_include_patterns() -> list[str]:
    """Load table include patterns from source-groups.yaml.

    Format: server_group_name as root key with table_include_patterns field.
    """
    try:
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(list[str] | None, group_data.get('table_include_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns

        return []
    except Exception:
        return []


def save_table_exclude_patterns(patterns: list[str]) -> None:
    """Save table exclude patterns to source-groups.yaml.

    Uses centralized save function to preserve header comments.
    """
    def updater(config: dict[str, Any]) -> None:
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['table_exclude_patterns'] = patterns
                return
        raise RuntimeError("No server group found to update")

    save_server_group_preserving_comments(updater, "save table exclude patterns")


def save_table_include_patterns(patterns: list[str]) -> None:
    """Save table include patterns to source-groups.yaml.

    Uses centralized save function to preserve header comments.
    """
    def updater(config: dict[str, Any]) -> None:
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['table_include_patterns'] = patterns
                return
        raise RuntimeError("No server group found to update")

    save_server_group_preserving_comments(updater, "save table include patterns")


def load_env_mappings() -> dict[str, str]:
    """Load environment mappings from source-groups.yaml.

    Format: server_group_name as root key with env_mappings field.
    env_mappings is a dict mapping source env suffix to target env name.
    Example: {"staging": "stage", "production": "prod"}
    """
    try:
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        # Flat format: server_group_name as root key with 'pattern' field
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                mappings = cast(dict[str, str] | None, group_data.get('env_mappings'))  # type: ignore[misc]
                if mappings:
                    return mappings

        return {}
    except Exception:
        return {}


def save_env_mappings(mappings: dict[str, str]) -> None:
    """Save environment mappings to source-groups.yaml.

    Uses centralized save function to preserve header comments.
    """
    def updater(config: dict[str, Any]) -> None:
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['env_mappings'] = mappings
                return
        raise RuntimeError("No server group found to update")

    save_server_group_preserving_comments(updater, "save env mappings")
