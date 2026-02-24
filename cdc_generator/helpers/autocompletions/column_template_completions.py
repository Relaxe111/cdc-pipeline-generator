"""Autocompletion helpers for column templates and transforms.

Provides dynamic completion data for --add-column-template, --remove-column-template,
--add-transform, and --remove-transform CLI flags.
"""

from cdc_generator.core.bloblang_refs import list_bloblang_refs
from cdc_generator.core.column_template_operations import (
    list_column_templates,
    list_transforms,
)
from cdc_generator.core.column_templates import list_template_keys

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
    """List all available transform Bloblang refs.

    Returns:
        Sorted list of Bloblang refs for autocompletion.

    Example:
        >>> list_transform_rule_keys()
        ['services/_bloblang/adopus/user_class.blobl']
    """
    return list_bloblang_refs()


def list_column_templates_for_table(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List column template keys on a specific sink table.

    Used for --remove-column-template autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key (schema.table).

    Returns:
        List of template keys configured on the table.

    Example:
        >>> list_column_templates_for_table(
        ...     'directory', 'sink_asma.notification',
        ...     'notification.customer_user',
        ... )
        ['environment', 'source_table']
    """
    table_cfg = _load_table_config(service_name, sink_key, table_key)
    if table_cfg is None:
        return []
    return sorted(list_column_templates(table_cfg))


def list_transforms_for_table(
    service_name: str,
    sink_key: str,
    table_key: str,
) -> list[str]:
    """List transform Bloblang refs on a specific sink table.

    Used for --remove-transform autocompletion.

    Args:
        service_name: Service name.
        sink_key: Sink key.
        table_key: Table key (schema.table).

    Returns:
        List of Bloblang refs configured on the table.

    Example:
        >>> list_transforms_for_table(
        ...     'directory', 'sink_asma.notification',
        ...     'notification.customer_user',
        ... )
        ['services/_bloblang/adopus/user_class.blobl']
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
