"""Sink table extra columns and transforms operations.

Handles adding/removing extra_columns and transforms on sink tables
in service YAML files. These reference templates/rules by name only.

All template/rule definitions live in:
  - service-schemas/column-templates.yaml
  - service-schemas/transform-rules.yaml
"""

from typing import cast

from cdc_generator.core.extra_columns import (
    add_extra_column,
    add_transform,
    list_extra_columns,
    list_transforms,
    remove_extra_column,
    remove_transform,
)
from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
)
from cdc_generator.helpers.service_config import load_service_config

from .config import save_service_config

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_table_config(
    service: str,
    sink_key: str,
    table_key: str,
) -> tuple[dict[str, object], dict[str, object]] | None:
    """Load service config and resolve to the specific table config dict.

    Args:
        service: Service name.
        sink_key: Sink key (e.g., 'sink_asma.notification').
        table_key: Table key (e.g., 'notification.customer_user').

    Returns:
        Tuple of (service_config, table_config) on success, None on error.
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError as exc:
        print_error(f"Service not found: {exc}")
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        print_error(f"No sinks configured for service '{service}'")
        return None

    sinks = cast(dict[str, object], sinks_raw)
    sink_raw = sinks.get(sink_key)
    if not isinstance(sink_raw, dict):
        print_error(f"Sink '{sink_key}' not found in service '{service}'")
        return None

    sink_cfg = cast(dict[str, object], sink_raw)
    tables_raw = sink_cfg.get("tables")
    if not isinstance(tables_raw, dict):
        print_error(f"No tables in sink '{sink_key}'")
        return None

    tables = cast(dict[str, object], tables_raw)
    table_raw = tables.get(table_key)
    if not isinstance(table_raw, dict):
        print_error(f"Table '{table_key}' not found in sink '{sink_key}'")
        _show_available_tables(tables)
        return None

    return config, cast(dict[str, object], table_raw)


def _show_available_tables(tables: dict[str, object]) -> None:
    """Print available table keys as help.

    Args:
        tables: Tables dict from sink config.
    """
    available = sorted(str(k) for k in tables)
    if available:
        print_info(f"Available tables: {', '.join(available)}")


# ---------------------------------------------------------------------------
# Public API — extra columns
# ---------------------------------------------------------------------------


def add_extra_column_to_table(
    service: str,
    sink_key: str,
    table_key: str,
    template_key: str,
    name_override: str | None = None,
) -> bool:
    """Add an extra column template reference to a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        template_key: Column template key from column-templates.yaml.
        name_override: Optional column name override.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not add_extra_column(table_cfg, template_key, name_override):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def remove_extra_column_from_table(
    service: str,
    sink_key: str,
    table_key: str,
    template_key: str,
) -> bool:
    """Remove an extra column template reference from a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        template_key: Column template key to remove.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not remove_extra_column(table_cfg, template_key):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def list_extra_columns_on_table(
    service: str,
    sink_key: str,
    table_key: str,
) -> bool:
    """List all extra columns on a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.

    Returns:
        True if any columns found, False otherwise.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    _config, table_cfg = resolved
    columns = list_extra_columns(table_cfg)

    print_header(f"Extra columns on '{table_key}' in sink '{sink_key}'")
    if not columns:
        print_info("No extra columns configured")
        return False

    for tpl_key in columns:
        from cdc_generator.core.column_templates import get_template

        template = get_template(tpl_key)
        if template:
            print(
                f"  {Colors.CYAN}{tpl_key}{Colors.RESET}"
                + f" → {Colors.OKGREEN}{template.name}{Colors.RESET}"
                + f" ({template.column_type})"
                + f"  {Colors.DIM}{template.description}{Colors.RESET}"
            )
        else:
            print(f"  {Colors.YELLOW}{tpl_key}{Colors.RESET} (unknown template)")

    return True


# ---------------------------------------------------------------------------
# Public API — transforms
# ---------------------------------------------------------------------------


def add_transform_to_table(
    service: str,
    sink_key: str,
    table_key: str,
    rule_key: str,
) -> bool:
    """Add a transform rule reference to a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        rule_key: Transform rule key from transform-rules.yaml.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not add_transform(table_cfg, rule_key):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def remove_transform_from_table(
    service: str,
    sink_key: str,
    table_key: str,
    rule_key: str,
) -> bool:
    """Remove a transform rule reference from a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.
        rule_key: Transform rule key to remove.

    Returns:
        True on success, False on error.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    config, table_cfg = resolved
    if not remove_transform(table_cfg, rule_key):
        return False

    if not save_service_config(service, config):
        return False

    print_info("Run 'cdc generate' to update pipelines")
    return True


def list_transforms_on_table(
    service: str,
    sink_key: str,
    table_key: str,
) -> bool:
    """List all transforms on a sink table.

    Args:
        service: Service name.
        sink_key: Sink key.
        table_key: Table key.

    Returns:
        True if any transforms found, False otherwise.
    """
    resolved = _resolve_table_config(service, sink_key, table_key)
    if resolved is None:
        return False

    _config, table_cfg = resolved
    rules = list_transforms(table_cfg)

    print_header(f"Transforms on '{table_key}' in sink '{sink_key}'")
    if not rules:
        print_info("No transforms configured")
        return False

    for rule_key in rules:
        from cdc_generator.core.transform_rules import get_rule

        rule = get_rule(rule_key)
        if rule:
            print(
                f"  {Colors.CYAN}{rule_key}{Colors.RESET}"
                + f" ({rule.rule_type})"
                + f"  {Colors.DIM}{rule.description}{Colors.RESET}"
            )
        else:
            print(f"  {Colors.YELLOW}{rule_key}{Colors.RESET} (unknown rule)")

    return True
