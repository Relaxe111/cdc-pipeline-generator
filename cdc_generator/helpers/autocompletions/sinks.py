"""Sink-related autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.tables import list_tables_for_service
from cdc_generator.helpers.autocompletions.utils import (
    find_directory_upward,
    find_file_upward,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]

# Constant for schema.table format validation
SCHEMA_TABLE_PARTS = 2


def list_sink_keys_for_service(service_name: str) -> list[str]:
    """List configured sink keys for a service from services/{service}.yaml.

    Format: sink_group.target_service

    Args:
        service_name: Name of the service.

    Returns:
        List of sink keys (e.g., ['sink_asma.chat', 'sink_asma.directory']).

    Expected YAML structure:
        service_name:
          sinks:
            sink_group.target_service:
              tables: {...}

    Example:
        >>> list_sink_keys_for_service('chat')
        ['sink_asma.chat', 'sink_test.monitoring']
    """
    if not yaml:
        return []

    services_dir = find_directory_upward('services')
    service_file = services_dir / f'{service_name}.yaml' if services_dir else None
    if not service_file or not service_file.is_file():
        return []

    try:
        data = load_yaml_file(service_file)
        if not data or not isinstance(data, dict):
            return []

        data_dict = cast(dict[str, Any], data)

        # Support new format (service name as root key)
        config = (
            cast(dict[str, Any], data_dict[service_name])
            if service_name in data_dict and isinstance(data_dict[service_name], dict)
            else data_dict
        )

        sinks = config.get('sinks', {})
        return sorted(cast(dict[str, Any], sinks).keys()) if isinstance(sinks, dict) else []

    except Exception:
        return []


def list_available_sink_keys() -> list[str]:
    """Generate possible sink keys from sink-groups.yaml.

    For inherited sinks: uses inherited_sources list.
    For standalone sinks: uses sources dict keys.

    Returns:
        List of possible sink key suggestions (sink_group.source_name).

    Expected YAML structure:
        sink_group_name:
          inherits: true
          inherited_sources:
            - source1
            - source2
        # OR
        sink_group_name:
          inherits: false
          sources:
            source1: {...}
            source2: {...}

    Example:
        >>> list_available_sink_keys()
        ['sink_asma.chat', 'sink_asma.directory']
    """
    if not yaml:
        return []

    sink_file = find_file_upward('sink-groups.yaml')
    if not sink_file:
        return []

    try:
        config = load_yaml_file(sink_file)
        if not config or not isinstance(config, dict):
            return []

        suggestions: list[str] = []
        sink_config = cast(dict[str, Any], config)

        for group_name, group_data in sink_config.items():
            if not isinstance(group_data, dict):
                continue
            group_dict = cast(dict[str, Any], group_data)

            if group_dict.get("inherits"):
                # Inherited sink: suggest from inherited_sources
                sources = group_dict.get("inherited_sources", [])
                if isinstance(sources, list):
                    for src in sources:
                        if isinstance(src, str):
                            suggestions.append(f"{group_name}.{src}")
            else:
                # Standalone sink: suggest from sources keys
                sources = group_dict.get("sources", {})
                if isinstance(sources, dict):
                    sources_dict = cast(dict[str, Any], sources)
                    for src in sources_dict:
                        suggestions.append(f"{group_name}.{src}")

        return sorted(suggestions)

    except Exception:
        return []


def list_target_tables_for_sink(
    service_name: str,
    sink_key: str,
) -> list[str]:
    """List available target tables from service-schemas for sink's target service.

    Parses sink_key to extract target_service, then lists tables
    from service-schemas/{target_service}/.

    Args:
        service_name: Source service name (unused, kept for API consistency).
        sink_key: Sink key in format 'sink_group.target_service'.

    Returns:
        List of target table options in format 'schema.table'.

    Example:
        >>> list_target_tables_for_sink('chat', 'sink_asma.directory')
        ['public.customers', 'public.users']
    """
    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []

    target_service = parts[1]
    return list_tables_for_service(target_service)


def list_tables_for_sink_target(sink_key: str) -> list[str]:
    """List available tables from service-schemas for a sink's target service.

    Parses sink_key to extract target_service, then lists tables
    from service-schemas/{target_service}/.

    This is used for --add-sink-table autocomplete: given --sink sink_asma.chat,
    it returns tables from service-schemas/chat/ (e.g., public.users, public.rooms).

    Args:
        sink_key: Sink key in format 'sink_group.target_service'.

    Returns:
        List of tables in format 'schema.table'.

    Example:
        >>> list_tables_for_sink_target('sink_asma.chat')
        ['public.users', 'public.rooms', 'logs.activity']
    """
    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []

    target_service = parts[1]
    return list_tables_for_service(target_service)


def get_default_sink_for_service(service_name: str) -> str:
    """Return the only sink key if a service has exactly one sink.

    Used to auto-default --sink when there's only one option.

    Args:
        service_name: Name of the service.

    Returns:
        The single sink key, or empty string if zero or multiple sinks.

    Example:
        >>> get_default_sink_for_service('chat')
        'sink_asma.chat'
    """
    sinks = list_sink_keys_for_service(service_name)
    if len(sinks) == 1:
        return sinks[0]
    return ""


def list_target_columns_for_sink_table(
    sink_key: str,
    target_table: str,
) -> list[str]:
    """List columns for a target table from service-schemas.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.chat').
        target_table: Target table in format 'schema.table'.

    Returns:
        List of column names.

    Expected YAML structure (service-schemas/{target_service}/{schema}/{table}.yaml):
        columns:
          - name: col1
          - name: col2

    Example:
        >>> list_target_columns_for_sink_table('sink_asma.chat', 'public.users')
        ['id', 'username', 'email']
    """
    if not yaml:
        return []

    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []

    target_service = parts[1]
    table_parts = target_table.split('.', 1)
    if len(table_parts) != SCHEMA_TABLE_PARTS:
        return []

    schema, table = table_parts
    schemas_dir = find_directory_upward('service-schemas')
    table_file = schemas_dir / target_service / schema / f'{table}.yaml' if schemas_dir else None
    if not table_file or not table_file.is_file():
        return []

    try:
        table_schema = load_yaml_file(table_file)
        if not table_schema or not isinstance(table_schema, dict):
            return []

        columns = table_schema.get('columns', [])
        return (
            sorted(
                str(col.get('name', ''))
                for col in columns
                if isinstance(col, dict) and col.get('name')
            )
            if isinstance(columns, list)
            else []
        )

    except Exception:
        return []


def _load_sink_tables_for_autocomplete(
    service_name: str,
    sink_key: str,
) -> dict[str, object] | None:
    """Load tables dict from a service sink config for autocompletion.

    Returns:
        Tables dict or None if not loadable.

    Expected YAML structure:
        service_name:
          sinks:
            sink_key:
              tables:
                table1: {...}
                table2: {...}
    """
    if not yaml:
        return None

    services_dir = find_directory_upward('services')
    service_file = (
        services_dir / f'{service_name}.yaml' if services_dir else None
    )
    if not service_file or not service_file.is_file():
        return None

    try:
        with service_file.open(encoding='utf-8') as f:
            data = yaml.safe_load(f)
        if not data or not isinstance(data, dict):
            return None
    except Exception:
        return None

    config = (
        data[service_name]
        if service_name in data and isinstance(data[service_name], dict)
        else data
    )

    sinks = config.get('sinks', {})
    sink_cfg = sinks.get(sink_key, {}) if isinstance(sinks, dict) else {}
    tables = sink_cfg.get('tables', {}) if isinstance(sink_cfg, dict) else {}

    if not isinstance(tables, dict):
        return None

    return cast(dict[str, object], tables)


def list_custom_tables_for_service_sink(
    service_name: str,
    sink_key: str,
) -> list[str]:
    """List custom tables (custom: true) for a service sink.

    Used for --modify-custom-table autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.

    Returns:
        List of table keys where custom=true.

    Example:
        >>> list_custom_tables_for_service_sink('chat', 'sink_asma.chat')
        ['custom.audit_log', 'custom.stats']
    """
    tables = _load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    return sorted(
        str(k) for k, v in tables.items()
        if isinstance(v, dict) and v.get('custom')
    )


def list_custom_table_columns_for_autocomplete(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List column names for a custom table.

    Used for --remove-column autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key (schema.table).

    Returns:
        List of column names.

    Example:
        >>> list_custom_table_columns_for_autocomplete('chat', 'sink_asma.chat', 'custom.audit')
        ['id', 'timestamp', 'action']
    """
    tables = _load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    tbl_cfg = tables.get(table_key, {})
    if not isinstance(tbl_cfg, dict) or not tbl_cfg.get('custom'):
        return []

    columns = tbl_cfg.get('columns', {})
    if not isinstance(columns, dict):
        return []

    return sorted(str(k) for k in columns)
