"""Extra columns and transforms manager for sink tables.

Handles adding/removing extra_columns and transforms to sink table configs.
Extra columns reference column-templates.yaml by name.
Transforms reference transform-rules.yaml by name.

Service YAML structure:
    sinks:
      sink_asma.notification:
        tables:
          notification.customer_user:
            target_exists: false
            from: public.customer_user
            extra_columns:
              - template: source_table
              - template: environment
                name: deploy_env
            transforms:
              - rule: user_class_splitter
              - rule: active_users_only
"""

from dataclasses import dataclass
from typing import cast

from cdc_generator.core.column_templates import (
    ColumnTemplate,
    get_template,
    validate_template_reference,
)
from cdc_generator.core.transform_rules import (
    TransformRule,
    get_rule,
    validate_rule_reference,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedExtraColumn:
    """An extra column resolved from a template reference.

    Attributes:
        template_key: Reference to column-templates.yaml key.
        name: Final column name (may be overridden from template default).
        template: Resolved ColumnTemplate.
    """

    template_key: str
    name: str
    template: ColumnTemplate


@dataclass(frozen=True)
class ResolvedTransform:
    """A transform resolved from a rule reference.

    Attributes:
        rule_key: Reference to transform-rules.yaml key.
        rule: Resolved TransformRule.
    """

    rule_key: str
    rule: TransformRule


# ---------------------------------------------------------------------------
# Extra column operations
# ---------------------------------------------------------------------------


def _get_extra_columns_list(
    table_cfg: dict[str, object],
) -> list[object]:
    """Get or create the extra_columns list from table config.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        Mutable list reference for extra_columns.
    """
    raw = table_cfg.get("extra_columns")
    if not isinstance(raw, list):
        table_cfg["extra_columns"] = []
        return cast(list[object], table_cfg["extra_columns"])
    return cast(list[object], raw)


def _find_extra_column_index(
    extra_columns: list[object],
    template_key: str,
) -> int | None:
    """Find the index of an extra column by template key.

    Args:
        extra_columns: List of extra column dicts.
        template_key: Template key to find.

    Returns:
        Index if found, None otherwise.
    """
    for idx, item in enumerate(extra_columns):
        if isinstance(item, dict):
            item_dict = cast(dict[str, object], item)
            if item_dict.get("template") == template_key:
                return idx
    return None


def add_extra_column(
    table_cfg: dict[str, object],
    template_key: str,
    name_override: str | None = None,
) -> bool:
    """Add an extra column (template reference) to a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        template_key: Template key from column-templates.yaml.
        name_override: Optional column name override.

    Returns:
        True on success, False on error.
    """
    # Validate template exists
    error = validate_template_reference(template_key)
    if error is not None:
        print_error(error)
        return False

    extra_columns = _get_extra_columns_list(table_cfg)

    # Check for duplicate
    existing_idx = _find_extra_column_index(extra_columns, template_key)
    if existing_idx is not None:
        print_warning(f"Extra column template '{template_key}' already exists on this table")
        return False

    # Build entry
    entry: dict[str, object] = {"template": template_key}
    if name_override is not None:
        entry["name"] = name_override

    extra_columns.append(entry)

    template = get_template(template_key)
    default_name = template.name if template else template_key
    final_name = name_override if name_override is not None else default_name
    print_success(f"Added extra column '{final_name}' (template: {template_key})")
    return True


def remove_extra_column(
    table_cfg: dict[str, object],
    template_key: str,
) -> bool:
    """Remove an extra column (template reference) from a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        template_key: Template key to remove.

    Returns:
        True on success, False if not found.
    """
    extra_columns = _get_extra_columns_list(table_cfg)

    idx = _find_extra_column_index(extra_columns, template_key)
    if idx is None:
        print_warning(f"Extra column template '{template_key}' not found on this table")
        _list_existing_extra_columns(extra_columns)
        return False

    extra_columns.pop(idx)

    # Remove empty list from config
    if not extra_columns:
        table_cfg.pop("extra_columns", None)

    print_success(f"Removed extra column template '{template_key}'")
    return True


def _list_existing_extra_columns(extra_columns: list[object]) -> None:
    """Print existing extra columns for help messages.

    Args:
        extra_columns: List of extra column dicts.
    """
    templates = [
        str(cast(dict[str, object], item).get("template", "?"))
        for item in extra_columns
        if isinstance(item, dict)
    ]
    if templates:
        print_info(f"Existing extra columns: {', '.join(templates)}")


def list_extra_columns(table_cfg: dict[str, object]) -> list[str]:
    """List all extra column template keys on a sink table.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of template keys.
    """
    raw = table_cfg.get("extra_columns")
    if not isinstance(raw, list):
        return []

    result: list[str] = []
    for item in cast(list[object], raw):
        if isinstance(item, dict):
            tpl = cast(dict[str, object], item).get("template")
            if isinstance(tpl, str):
                result.append(tpl)
    return result


# ---------------------------------------------------------------------------
# Transform operations
# ---------------------------------------------------------------------------


def _get_transforms_list(
    table_cfg: dict[str, object],
) -> list[object]:
    """Get or create the transforms list from table config.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        Mutable list reference for transforms.
    """
    raw = table_cfg.get("transforms")
    if not isinstance(raw, list):
        table_cfg["transforms"] = []
        return cast(list[object], table_cfg["transforms"])
    return cast(list[object], raw)


def _find_transform_index(
    transforms: list[object],
    rule_key: str,
) -> int | None:
    """Find the index of a transform by rule key.

    Args:
        transforms: List of transform dicts.
        rule_key: Rule key to find.

    Returns:
        Index if found, None otherwise.
    """
    for idx, item in enumerate(transforms):
        if isinstance(item, dict):
            item_dict = cast(dict[str, object], item)
            if item_dict.get("rule") == rule_key:
                return idx
    return None


def add_transform(
    table_cfg: dict[str, object],
    rule_key: str,
) -> bool:
    """Add a transform (rule reference) to a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        rule_key: Rule key from transform-rules.yaml.

    Returns:
        True on success, False on error.
    """
    # Validate rule exists
    error = validate_rule_reference(rule_key)
    if error is not None:
        print_error(error)
        return False

    transforms = _get_transforms_list(table_cfg)

    # Check for duplicate
    existing_idx = _find_transform_index(transforms, rule_key)
    if existing_idx is not None:
        print_warning(f"Transform rule '{rule_key}' already exists on this table")
        return False

    transforms.append({"rule": rule_key})

    rule = get_rule(rule_key)
    desc = f" ({rule.description})" if rule and rule.description else ""
    print_success(f"Added transform rule '{rule_key}'{desc}")
    return True


def remove_transform(
    table_cfg: dict[str, object],
    rule_key: str,
) -> bool:
    """Remove a transform (rule reference) from a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        rule_key: Rule key to remove.

    Returns:
        True on success, False if not found.
    """
    transforms = _get_transforms_list(table_cfg)

    idx = _find_transform_index(transforms, rule_key)
    if idx is None:
        print_warning(f"Transform rule '{rule_key}' not found on this table")
        _list_existing_transforms(transforms)
        return False

    transforms.pop(idx)

    # Remove empty list from config
    if not transforms:
        table_cfg.pop("transforms", None)

    print_success(f"Removed transform rule '{rule_key}'")
    return True


def _list_existing_transforms(transforms: list[object]) -> None:
    """Print existing transforms for help messages.

    Args:
        transforms: List of transform dicts.
    """
    rules = [
        str(cast(dict[str, object], item).get("rule", "?"))
        for item in transforms
        if isinstance(item, dict)
    ]
    if rules:
        print_info(f"Existing transforms: {', '.join(rules)}")


def list_transforms(table_cfg: dict[str, object]) -> list[str]:
    """List all transform rule keys on a sink table.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of rule keys.
    """
    raw = table_cfg.get("transforms")
    if not isinstance(raw, list):
        return []

    result: list[str] = []
    for item in cast(list[object], raw):
        if isinstance(item, dict):
            rule = cast(dict[str, object], item).get("rule")
            if isinstance(rule, str):
                result.append(rule)
    return result


# ---------------------------------------------------------------------------
# Resolution — resolve references to actual objects
# ---------------------------------------------------------------------------


def resolve_extra_columns(
    table_cfg: dict[str, object],
) -> list[ResolvedExtraColumn]:
    """Resolve all extra column template references to ColumnTemplate objects.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of resolved extra columns (skips invalid references).

    Example:
        >>> resolved = resolve_extra_columns(table_cfg)
        >>> resolved[0].template.column_type
        'text'
    """
    raw = table_cfg.get("extra_columns")
    if not isinstance(raw, list):
        return []

    result: list[ResolvedExtraColumn] = []
    for item in cast(list[object], raw):
        if not isinstance(item, dict):
            continue

        entry = cast(dict[str, object], item)
        template_key = entry.get("template")
        if not isinstance(template_key, str):
            continue

        template = get_template(template_key)
        if template is None:
            print_warning(f"Extra column references unknown template: '{template_key}'")
            continue

        name_override = entry.get("name")
        final_name = str(name_override) if isinstance(name_override, str) else template.name

        result.append(ResolvedExtraColumn(
            template_key=template_key,
            name=final_name,
            template=template,
        ))

    return result


def resolve_transforms(
    table_cfg: dict[str, object],
) -> list[ResolvedTransform]:
    """Resolve all transform rule references to TransformRule objects.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of resolved transforms (skips invalid references).

    Example:
        >>> resolved = resolve_transforms(table_cfg)
        >>> resolved[0].rule.rule_type
        'row_multiplier'
    """
    raw = table_cfg.get("transforms")
    if not isinstance(raw, list):
        return []

    result: list[ResolvedTransform] = []
    for item in cast(list[object], raw):
        if not isinstance(item, dict):
            continue

        entry = cast(dict[str, object], item)
        rule_key = entry.get("rule")
        if not isinstance(rule_key, str):
            continue

        rule = get_rule(rule_key)
        if rule is None:
            print_warning(f"Transform references unknown rule: '{rule_key}'")
            continue

        result.append(ResolvedTransform(rule_key=rule_key, rule=rule))

    return result
