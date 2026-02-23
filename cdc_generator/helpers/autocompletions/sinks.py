"""Sink-related autocompletion functions."""

from typing import Any, cast

from cdc_generator.helpers.autocompletions.tables import list_tables_for_service
from cdc_generator.helpers.autocompletions.utils import (
    find_directory_upward,
    find_file_upward,
)
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
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
        if not data or not data:
            return []

        data_dict = cast(dict[str, object], data)

        # Support new format (service name as root key)
        config = (
            cast(dict[str, object], data_dict[service_name])
            if service_name in data_dict and isinstance(data_dict[service_name], dict)
            else data_dict
        )

        sinks = config.get('sinks', {})
        return sorted(cast(dict[str, object], sinks).keys()) if isinstance(sinks, dict) else []

    except Exception:
        return []


def _process_sink_group(group_name: str, group_dict: dict[str, Any]) -> list[str]:
    """Process a single sink group and return list of sink keys.

    Args:
        group_name: Name of the sink group.
        group_dict: Configuration dictionary for the group.

    Returns:
        List of sink keys for this group.
    """
    suggestions: list[str] = []

    if group_dict.get("inherits"):
        # Inherited sink: suggest from inherited_sources
        sources = group_dict.get("inherited_sources", [])
        if isinstance(sources, list):
            sources_list = cast(list[Any], sources)
            for src in sources_list:
                if isinstance(src, str):
                    suggestions.append(f"{group_name}.{src}")
    else:
        # Standalone sink: suggest from sources keys
        sources = group_dict.get("sources", {})
        if isinstance(sources, dict):
            sources_dict = cast(dict[str, Any], sources)
            for src in sources_dict:
                suggestions.append(f"{group_name}.{src}")

    return suggestions


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
        if not config or not config:
            return []

        suggestions: list[str] = []
        sink_config = cast(dict[str, Any], config)

        for group_name, group_data in sink_config.items():
            if not isinstance(group_data, dict):
                continue
            group_dict = cast(dict[str, Any], group_data)
            suggestions.extend(_process_sink_group(group_name, group_dict))

        return sorted(suggestions)

    except Exception:
        return []


def list_target_tables_for_sink(
    _service_name: str,
    sink_key: str,
) -> list[str]:
    """List available target tables from service-schemas for sink's target service.

    Parses sink_key to extract target_service, then lists tables
    from service-schemas/{target_service}/.

    Args:
        _service_name: Source service name (unused, kept for API consistency).
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


def list_custom_table_definitions_for_sink_target(sink_key: str) -> list[str]:
    """List custom-table definitions for a sink target service.

    Reads from ``services/_schemas/{target_service}/custom-tables/*.yaml``
    with legacy fallback to ``service-schemas/{target_service}/custom-tables/*.yaml``.

    Args:
        sink_key: Sink key in format ``sink_group.target_service``.

    Returns:
        Sorted list of ``schema.table`` references.
    """
    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []

    target_service = parts[1]

    from cdc_generator.validators.manage_service_schema.custom_table_ops import (
        list_custom_tables,
    )

    try:
        return list_custom_tables(target_service)
    except Exception:
        return []


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
    """List columns for a target table from schema files.

    Args:
        sink_key: Sink key (e.g., 'sink_asma.chat').
        target_table: Target table in format 'schema.table'.

    Returns:
        List of column names.

    Expected YAML structure (services/_schemas/{target_service}/{schema}/{table}.yaml):
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
    table_parts = target_table.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS or len(table_parts) != SCHEMA_TABLE_PARTS:
        return []

    target_service = parts[1]
    schema, table = table_parts
    for service_dir in get_service_schema_read_dirs(target_service):
        table_file = service_dir / schema / f"{table}.yaml"
        if not table_file.is_file():
            continue

        try:
            table_schema = load_yaml_file(table_file)

            columns = table_schema.get("columns", [])
            return (
                sorted(
                    str(col.get("name", ""))
                    for col in columns
                    if isinstance(col, dict) and col.get("name")
                )
                if isinstance(columns, list)
                else []
            )
        except Exception:
            continue

    return []


def load_sink_tables_for_autocomplete(
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

    data_dict = cast(dict[str, Any], data)
    config = (
        data_dict[service_name]
        if service_name in data_dict and isinstance(data_dict[service_name], dict)
        else data_dict
    )
    config_dict = cast(dict[str, object], config)

    sinks = config_dict.get('sinks', {})
    sinks_dict = cast(dict[str, object], sinks) if isinstance(sinks, dict) else {}
    sink_cfg = cast(dict[str, object], sinks_dict.get(sink_key, {}))

    return cast(dict[str, object], sink_cfg.get('tables', {}))


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
    tables = load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    return sorted(
        str(k) for k, v in tables.items()
        if isinstance(v, dict) and cast(dict[str, Any], v).get('custom')
    )


def list_sink_tables_for_service(service_name: str, sink_key: str) -> list[str]:
    """List all tables configured in a sink.

    Used for --sink-table autocompletion in update operations.

    Args:
        service_name: Service name.
        sink_key: Sink key.

    Returns:
        List of all table keys in the sink (schema.table format).

    Example:
        >>> list_sink_tables_for_service('directory', 'sink_asma.calendar')
        ['public.customer_user', 'calendar.events']
    """
    tables = load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    return sorted(str(k) for k in tables)


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
    tables = load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    tbl_cfg = tables.get(table_key, {})
    if not isinstance(tbl_cfg, dict):
        return []
    tbl_cfg_dict = cast(dict[str, Any], tbl_cfg)
    if not tbl_cfg_dict.get('custom'):
        return []

    columns = tbl_cfg_dict.get('columns', {})
    if isinstance(columns, dict):
        columns_dict = cast(dict[str, Any], columns)
        return sorted(str(k) for k in columns_dict)

    sink_parts = sink_key.split('.', 1)
    if len(sink_parts) != 2:  # noqa: PLR2004
        return []
    target_service = sink_parts[1]

    if "." not in table_key:
        return []
    schema_name, table_name = table_key.split(".", 1)

    for service_dir in get_service_schema_read_dirs(target_service):
        schema_file = service_dir / schema_name / f"{table_name}.yaml"
        if not schema_file.exists():
            continue
        data = load_yaml_file(schema_file)
        cols_raw = data.get("columns")
        if not isinstance(cols_raw, list):
            continue
        col_names = [
            str(col["name"]) for col in cols_raw
            if isinstance(col, dict) and isinstance(col.get("name"), str)
        ]
        return sorted(col_names)

    return []


def list_source_columns_for_sink_table(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List source columns for a sink table (from the sink table's 'from' field).

    Resolves the source table from the sink table's 'from' field, then loads
    columns from services/_schemas/{source_service}/{schema}/{table}.yaml.

    Args:
        service_name: Source service name.
        sink_key: Sink key (e.g., 'sink_asma.proxy').
        table_key: Sink table key (e.g., 'public.directory_user_name').

    Returns:
        List of source column names.

    Example:
        >>> list_source_columns_for_sink_table(
        ...     'directory', 'sink_asma.proxy', 'public.directory_user_name',
        ... )
        ['brukerBrukerNavn', 'customer_id', 'email', 'user_id', ...]
    """
    tables = load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return []

    tbl_cfg = tables.get(table_key, {})
    if not isinstance(tbl_cfg, dict):
        return []
    tbl_cfg_dict = cast(dict[str, Any], tbl_cfg)

    # Resolve source table from 'from' field
    from_table = tbl_cfg_dict.get('from')
    if not isinstance(from_table, str):
        # Fall back to table_key if no 'from' field
        from_table = table_key

    table_parts = from_table.split('.', 1)
    if len(table_parts) != SCHEMA_TABLE_PARTS:
        return []

    schema, table = table_parts
    for service_dir in get_service_schema_read_dirs(service_name):
        table_file = service_dir / schema / f"{table}.yaml"
        if not table_file.is_file():
            continue

        try:
            table_schema = load_yaml_file(table_file)

            columns = table_schema.get("columns", [])
            return (
                sorted(
                    str(col.get("name", ""))
                    for col in columns
                    if isinstance(col, dict) and col.get("name")
                )
                if isinstance(columns, list)
                else []
            )
        except Exception:
            continue

    return []


def _load_column_type_map(service_name: str, table_key: str) -> dict[str, str]:
    """Load ``{column_name: type}`` from schema YAML for ``schema.table``."""
    table_parts = table_key.split('.', 1)
    if len(table_parts) != SCHEMA_TABLE_PARTS:
        return {}

    schema, table = table_parts
    for service_dir in get_service_schema_read_dirs(service_name):
        table_file = service_dir / schema / f"{table}.yaml"
        if not table_file.is_file():
            continue

        try:
            table_schema = load_yaml_file(table_file)
            columns = table_schema.get("columns", [])
            if not isinstance(columns, list):
                return {}

            result: dict[str, str] = {}
            for col in columns:
                if not isinstance(col, dict):
                    continue
                name = col.get("name")
                col_type = col.get("type")
                if isinstance(name, str) and isinstance(col_type, str):
                    result[name] = col_type
            return result
        except Exception:
            continue

    return {}


def list_compatible_target_columns_for_source_column(
    service_name: str,
    sink_key: str,
    source_table: str,
    target_table: str,
    source_column: str,
) -> list[str]:
    """List target columns that are type-compatible with the source column."""
    from cdc_generator.validators.manage_service.sink_operations import (
        check_type_compatibility,
    )

    source_types = _load_column_type_map(service_name, source_table)
    if source_column not in source_types:
        return []

    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []
    target_service = parts[1]

    target_types = _load_column_type_map(target_service, target_table)
    source_type = source_types[source_column]
    return sorted(
        target_col
        for target_col, target_type in target_types.items()
        if check_type_compatibility(source_type, target_type)
    )


def list_compatible_source_columns_for_target_table(
    service_name: str,
    sink_key: str,
    source_table: str,
    target_table: str,
) -> list[str]:
    """List source columns that have at least one compatible target column."""
    source_types = _load_column_type_map(service_name, source_table)
    if not source_types:
        return []

    return sorted(
        src_col
        for src_col in source_types
        if list_compatible_target_columns_for_source_column(
            service_name,
            sink_key,
            source_table,
            target_table,
            src_col,
        )
    )


def list_compatible_target_prefixes_for_map_column(
    service_name: str,
    sink_key: str,
    source_table: str,
    target_table: str,
    limit: int = 40,
) -> list[str]:
    """Return up to ``limit`` unique ``target:`` prefixes from target schema."""
    del service_name
    del source_table  # source filtering happens in target:source step

    if limit <= 0:
        return []

    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []
    target_service = parts[1]

    target_types = _load_column_type_map(target_service, target_table)
    if not target_types:
        return []

    return [
        f"{target_column}:"
        for target_column in sorted(target_types)
    ][:limit]


def list_compatible_map_column_pairs_for_target_prefix(
    service_name: str,
    sink_key: str,
    source_table: str,
    target_table: str,
    target_prefix: str,
    source_prefix: str,
    limit: int = 40,
) -> list[str]:
    """Return up to ``limit`` ``target:source`` pairs for a target prefix."""
    from cdc_generator.validators.manage_service.sink_operations import (
        check_type_compatibility,
    )

    if limit <= 0:
        return []

    target_prefix_normalized = target_prefix.casefold()
    source_prefix_normalized = source_prefix.casefold()

    source_types = _load_column_type_map(service_name, source_table)
    if not source_types:
        return []

    parts = sink_key.split('.', 1)
    if len(parts) != SCHEMA_TABLE_PARTS:
        return []
    target_service = parts[1]

    target_types = _load_column_type_map(target_service, target_table)
    if not target_types:
        return []

    compat_cache: dict[tuple[str, str], bool] = {}
    results: list[str] = []

    for target_column in sorted(target_types):
        if (
            target_prefix_normalized
            and not target_column.casefold().startswith(target_prefix_normalized)
        ):
            continue

        target_type = target_types[target_column]
        for source_column in sorted(source_types):
            if (
                source_prefix_normalized
                and not source_column.casefold().startswith(source_prefix_normalized)
            ):
                continue

            source_type = source_types[source_column]
            cache_key = (source_type, target_type)
            if cache_key not in compat_cache:
                compat_cache[cache_key] = check_type_compatibility(
                    source_type,
                    target_type,
                )
            if not compat_cache[cache_key]:
                continue

            results.append(f"{target_column}:{source_column}")
            if len(results) >= limit:
                return results

    return results
