"""Autocompletion helpers for column templates and transform rules.

Provides dynamic completion data for --add-extra-column, --remove-extra-column,
--add-transform, and --remove-transform CLI flags.
"""

from cdc_generator.core.column_templates import list_template_keys
from cdc_generator.core.extra_columns import list_extra_columns, list_transforms
from cdc_generator.core.transform_rules import list_rule_keys

from .sinks import load_sink_tables_for_autocomplete


def list_column_template_keys() -> list[str]:
    """List all available column template keys.

    Returns:
        Sorted list of template keys for autocompletion.

    Example:
        >>> list_column_template_keys()
        ['cdc_lsn', 'cdc_operation', 'environment', ...]
    """
    return list_template_keys()


def list_transform_rule_keys() -> list[str]:
    """List all available transform rule keys.

    Returns:
        Sorted list of rule keys for autocompletion.

    Example:
        >>> list_transform_rule_keys()
        ['active_users_only', 'priority_label', 'user_class_splitter']
    """
    return list_rule_keys()


def list_extra_columns_for_table(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List extra column template keys on a specific sink table.

    Used for --remove-extra-column autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key (schema.table).

    Returns:
        List of template keys configured on the table.

    Example:
        >>> list_extra_columns_for_table(
        ...     'directory', 'sink_asma.notification',
        ...     'notification.customer_user',
        ... )
        ['environment', 'source_table']
    """
    table_cfg = _load_table_config(service_name, sink_key, table_key)
    if table_cfg is None:
        return []
    return sorted(list_extra_columns(table_cfg))


def list_transforms_for_table(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List transform rule keys on a specific sink table.

    Used for --remove-transform autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key (schema.table).

    Returns:
        List of rule keys configured on the table.

    Example:
        >>> list_transforms_for_table(
        ...     'directory', 'sink_asma.notification',
        ...     'notification.customer_user',
        ... )
        ['user_class_splitter']
    """
    table_cfg = _load_table_config(service_name, sink_key, table_key)
    if table_cfg is None:
        return []
    return sorted(list_transforms(table_cfg))


def _load_table_config(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> dict[str, object] | None:
    """Load a specific table config from service YAML.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key.

    Returns:
        Table config dict, or None if not found.
    """
    from typing import cast

    tables = load_sink_tables_for_autocomplete(service_name, sink_key)
    if tables is None:
        return None

    raw = tables.get(table_key)
    if not isinstance(raw, dict):
        return None

    return cast(dict[str, object], raw)
