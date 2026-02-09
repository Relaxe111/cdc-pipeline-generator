"""CLI handlers for column templates and transforms on sink tables."""

import argparse

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
)
from cdc_generator.validators.manage_service.sink_template_ops import (
    add_column_template_to_table,
    add_transform_to_table,
    list_column_templates_on_table,
    list_transforms_on_table,
    remove_column_template_from_table,
    remove_transform_from_table,
)


def _resolve_sink_and_table(
    args: argparse.Namespace,
) -> tuple[str, str, str] | None:
    """Resolve service, sink_key, and sink_table from args.

    Returns:
        Tuple of (service, sink_key, sink_table) or None on error.
    """
    service = args.service
    if not service:
        print_error("--service is required")
        return None

    sink_key = _get_sink_key(args)
    if not sink_key:
        return None

    sink_table = getattr(args, "sink_table", None)
    if not sink_table:
        print_error("--sink-table is required for this operation")
        print_info(
            "Example: --sink-table notification.customer_user"
        )
        return None

    return service, sink_key, str(sink_table)


def _get_sink_key(args: argparse.Namespace) -> str | None:
    """Get sink key from args, with auto-default for single-sink services.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Sink key string, or None if not resolvable.
    """
    if args.sink:
        return str(args.sink)

    from typing import cast

    from cdc_generator.helpers.service_config import load_service_config

    try:
        config = load_service_config(args.service)
    except FileNotFoundError:
        print_error(f"Service '{args.service}' not found")
        return None

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        print_error("No sinks configured")
        return None

    sinks = cast(dict[str, object], sinks_raw)
    sink_keys = list(sinks.keys())
    if len(sink_keys) == 1:
        key = str(sink_keys[0])
        print_info(f"Auto-selected sink: {key}")
        return key

    print_error(
        "Multiple sinks found — use --sink to specify"
    )
    print_info(
        f"Available sinks: {', '.join(str(k) for k in sink_keys)}"
    )
    return None


# ---------------------------------------------------------------------------
# Extra column handlers
# ---------------------------------------------------------------------------


def handle_add_column_template(args: argparse.Namespace) -> int:
    """Handle --add-column-template flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    template_key = args.add_column_template

    name_override = getattr(args, "column_name", None)

    if add_column_template_to_table(
        service, sink_key, sink_table, template_key, name_override,
    ):
        return 0
    return 1


def handle_remove_column_template(args: argparse.Namespace) -> int:
    """Handle --remove-column-template flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    template_key = args.remove_column_template

    if remove_column_template_from_table(
        service, sink_key, sink_table, template_key,
    ):
        return 0
    return 1


def handle_list_column_templates(args: argparse.Namespace) -> int:
    """Handle --list-column-templates flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    list_column_templates_on_table(service, sink_key, sink_table)
    return 0


# ---------------------------------------------------------------------------
# Transform handlers
# ---------------------------------------------------------------------------


def handle_add_transform(args: argparse.Namespace) -> int:
    """Handle --add-transform flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    rule_key = args.add_transform

    if add_transform_to_table(
        service, sink_key, sink_table, rule_key,
    ):
        return 0
    return 1


def handle_remove_transform(args: argparse.Namespace) -> int:
    """Handle --remove-transform flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    rule_key = args.remove_transform

    if remove_transform_from_table(
        service, sink_key, sink_table, rule_key,
    ):
        return 0
    return 1


def handle_list_transforms(args: argparse.Namespace) -> int:
    """Handle --list-transforms flag."""
    resolved = _resolve_sink_and_table(args)
    if resolved is None:
        return 1

    service, sink_key, sink_table = resolved
    list_transforms_on_table(service, sink_key, sink_table)
    return 0


# ---------------------------------------------------------------------------
# Template & rule listing handlers
# ---------------------------------------------------------------------------


def handle_list_column_templates(_args: argparse.Namespace) -> int:
    """Handle --list-column-templates flag."""
    from cdc_generator.core.column_templates import get_templates

    templates = get_templates()
    print_header("Available column templates")

    if not templates:
        print_info("No column templates found")
        return 0

    for key in sorted(templates):
        tpl = templates[key]
        print(
            f"  {Colors.CYAN}{key}{Colors.RESET}"
            + f" → {Colors.OKGREEN}{tpl.name}{Colors.RESET}"
            + f" ({tpl.column_type})"
            + f"  {Colors.DIM}{tpl.description}{Colors.RESET}"
        )

    return 0


def handle_list_transform_rules(_args: argparse.Namespace) -> int:
    """Handle --list-transform-rules flag."""
    from cdc_generator.core.transform_rules import get_rules

    rules = get_rules()
    print_header("Available transform rules")

    if not rules:
        print_info("No transform rules found")
        return 0

    for key in sorted(rules):
        rule = rules[key]
        conditions_count = len(rule.conditions)
        print(
            f"  {Colors.CYAN}{key}{Colors.RESET}"
            + f" ({rule.rule_type}, {conditions_count} conditions)"
            + f"  {Colors.DIM}{rule.description}{Colors.RESET}"
        )

    return 0
