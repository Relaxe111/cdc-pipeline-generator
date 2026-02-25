"""Autocompletion helpers for column templates and transforms.

Provides dynamic completion data for --add-column-template, --remove-column-template,
--add-transform, and --remove-transform CLI flags.
"""

from cdc_generator.core.bloblang_refs import list_bloblang_refs
from cdc_generator.core.column_template_operations import (
    list_column_templates,
    list_transforms,
)
from cdc_generator.core.column_templates import get_templates, list_template_keys
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file

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


def _load_target_column_types(
    sink_key: str,
    target_table: str,
) -> dict[str, str]:
    """Load target table column types for a sink target service."""
    sink_parts = sink_key.split(".", 1)
    table_parts = target_table.split(".", 1)
    if len(sink_parts) != 2 or len(table_parts) != 2:
        return {}

    target_service = sink_parts[1]
    schema_name, table_name = table_parts

    for service_dir in get_service_schema_read_dirs(target_service):
        schema_file = service_dir / schema_name / f"{table_name}.yaml"
        if not schema_file.is_file():
            continue

        try:
            schema_data = load_yaml_file(schema_file)
            columns_raw = schema_data.get("columns", [])
            if not isinstance(columns_raw, list):
                return {}

            column_types: dict[str, str] = {}
            for column in columns_raw:
                if not isinstance(column, dict):
                    continue
                column_name = column.get("name")
                column_type = column.get("type")
                if isinstance(column_name, str) and isinstance(column_type, str):
                    column_types[column_name] = column_type
            return column_types
        except Exception:
            continue

    return {}


def _is_template_compatible_with_target_type(
    template_type: str,
    target_type: str,
) -> bool:
    """Return True if template type is compatible with a target column type."""
    from cdc_generator.validators.manage_service.sink_operations import (
        check_type_compatibility,
    )

    return check_type_compatibility(template_type, target_type)


def list_compatible_target_prefixes_for_column_template(
    sink_key: str,
    target_table: str,
    limit: int = 40,
) -> list[str]:
    """Return up to ``limit`` target column prefixes ``col:`` with compatible templates."""
    if limit <= 0:
        return []

    target_types = _load_target_column_types(sink_key, target_table)
    if not target_types:
        return []

    templates = get_templates()
    if not templates:
        return []

    compatible_prefixes: list[str] = []
    for target_col in sorted(target_types):
        target_type = target_types[target_col]
        has_compatible = any(
            _is_template_compatible_with_target_type(
                template.column_type,
                target_type,
            )
            for template in templates.values()
        )
        if has_compatible:
            compatible_prefixes.append(f"{target_col}:")
            if len(compatible_prefixes) >= limit:
                break

    return compatible_prefixes


def list_compatible_column_template_pairs_for_target_prefix(
    sink_key: str,
    target_table: str,
    target_prefix: str,
    template_prefix: str,
    limit: int = 40,
) -> list[str]:
    """Return up to ``limit`` ``target:template`` compatibility pairs."""
    if limit <= 0:
        return []

    target_types = _load_target_column_types(sink_key, target_table)
    if not target_types:
        return []

    target_prefix_normalized = target_prefix.casefold()
    template_prefix_normalized = template_prefix.casefold()

    templates = get_templates()
    if not templates:
        return []

    results: list[str] = []
    for target_col in sorted(target_types):
        if (
            target_prefix_normalized
            and not target_col.casefold().startswith(target_prefix_normalized)
        ):
            continue

        target_type = target_types[target_col]
        for template_key in sorted(templates):
            if (
                template_prefix_normalized
                and not template_key.casefold().startswith(template_prefix_normalized)
            ):
                continue

            template = templates[template_key]
            if not _is_template_compatible_with_target_type(
                template.column_type,
                target_type,
            ):
                continue

            results.append(f"{target_col}:{template_key}")
            if len(results) >= limit:
                return results

    return results


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
