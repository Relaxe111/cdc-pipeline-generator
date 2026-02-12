"""Column template and transform operations for sink tables.

Manages adding/removing/listing column_templates and transforms in sink table configs.
Column templates reference column-templates.yaml by name.
Transforms reference transform-rules.yaml by name.

Service YAML structure:
    sinks:
      sink_asma.notification:
        tables:
          notification.customer_user:
            target_exists: false
            from: public.customer_user
            column_templates:
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
class ResolvedColumnTemplate:
    """A column template resolved from a template reference.

    Attributes:
        template_key: Reference to column-templates.yaml key.
        name: Final column name (may be overridden from template default).
        value: Final Bloblang expression (may be overridden from template default).
            Can be a source-group reference like ``{asma.sources.*.customer_id}``.
        template: Resolved ColumnTemplate.
    """

    template_key: str
    name: str
    value: str
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
# Column template operations
# ---------------------------------------------------------------------------


def _get_column_templates_list(
    table_cfg: dict[str, object],
) -> list[object]:
    """Get or create the column_templates list from table config.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        Mutable list reference for column_templates.
    """
    raw = table_cfg.get("column_templates")
    if not isinstance(raw, list):
        table_cfg["column_templates"] = []
        return cast(list[object], table_cfg["column_templates"])
    return cast(list[object], raw)


def _find_column_template_index(
    column_templates: list[object],
    template_key: str,
) -> int | None:
    """Find the index of a column template by template key.

    Args:
        column_templates: List of column template dicts.
        template_key: Template key to find.

    Returns:
        Index if found, None otherwise.
    """
    for idx, item in enumerate(column_templates):
        if isinstance(item, dict):
            item_dict = cast(dict[str, object], item)
            if item_dict.get("template") == template_key:
                return idx
    return None


def add_column_template(
    table_cfg: dict[str, object],
    template_key: str,
    name_override: str | None = None,
    value_override: str | None = None,
    table_key: str | None = None,
) -> bool:
    """Add a column template reference to a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        template_key: Template key from column-templates.yaml.
        name_override: Optional column name override.
        value_override: Optional value override (Bloblang expression or
            source-group reference like ``{asma.sources.*.customer_id}``).
        table_key: Optional table identifier for applies_to validation (schema.table).

    Returns:
        True on success, False on error.
    """
    # Validate template exists and can be applied to this table
    if table_key is not None:
        from cdc_generator.core.column_templates import validate_template_for_table

        error = validate_template_for_table(template_key, table_key)
    else:
        from cdc_generator.core.column_templates import validate_template_reference

        error = validate_template_reference(template_key)

    if error is not None:
        print_error(error)
        return False

    column_templates = _get_column_templates_list(table_cfg)

    # Check for duplicate
    existing_idx = _find_column_template_index(column_templates, template_key)
    if existing_idx is not None:
        print_warning(f"Column template '{template_key}' already exists on this table")
        return False

    # Build entry
    entry: dict[str, object] = {"template": template_key}
    if name_override is not None:
        entry["name"] = name_override
    if value_override is not None:
        # Validate source-group reference syntax (if applicable)
        from cdc_generator.core.source_ref_resolver import (
            is_source_ref,
            parse_source_ref,
        )

        if is_source_ref(value_override):
            ref = parse_source_ref(value_override)
            if ref is None:
                print_error(
                    f"Invalid source-group reference: {value_override}\n"
                    + "Expected format: {group.sources.*.key}"
                )
                return False
        entry["value"] = value_override

    column_templates.append(entry)

    template = get_template(template_key)
    default_name = template.name if template else template_key
    final_name = name_override if name_override is not None else default_name
    value_info = f", value: {value_override}" if value_override else ""
    print_success(f"Added column template '{final_name}' (key: {template_key}{value_info})")
    return True


def remove_column_template(
    table_cfg: dict[str, object],
    template_key: str,
) -> bool:
    """Remove a column template reference from a sink table config.

    Args:
        table_cfg: Sink table configuration dict (mutated in place).
        template_key: Template key to remove.

    Returns:
        True on success, False if not found.
    """
    column_templates = _get_column_templates_list(table_cfg)

    idx = _find_column_template_index(column_templates, template_key)
    if idx is None:
        print_warning(f"Column template '{template_key}' not found on this table")
        _list_existing_column_templates(column_templates)
        return False

    column_templates.pop(idx)

    # Remove empty list from config
    if not column_templates:
        table_cfg.pop("column_templates", None)

    print_success(f"Removed column template '{template_key}'")
    return True


def _list_existing_column_templates(column_templates: list[object]) -> None:
    """Print existing column templates for help messages.

    Args:
        column_templates: List of column template dicts.
    """
    templates = [
        str(cast(dict[str, object], item).get("template", "?"))
        for item in column_templates
        if isinstance(item, dict)
    ]
    if templates:
        print_info(f"Existing column templates: {', '.join(templates)}")


def list_column_templates(table_cfg: dict[str, object]) -> list[str]:
    """List all column template keys on a sink table.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of template keys.
    """
    raw = table_cfg.get("column_templates")
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


def resolve_column_templates(
    table_cfg: dict[str, object],
) -> list[ResolvedColumnTemplate]:
    """Resolve all column template references to ColumnTemplate objects.

    Args:
        table_cfg: Sink table configuration dict.

    Returns:
        List of resolved column templates (skips invalid references).

    Example:
        >>> resolved = resolve_column_templates(table_cfg)
        >>> resolved[0].template.column_type
        'text'
    """
    raw = table_cfg.get("column_templates")
    if not isinstance(raw, list):
        return []

    result: list[ResolvedColumnTemplate] = []
    for item in cast(list[object], raw):
        if not isinstance(item, dict):
            continue

        entry = cast(dict[str, object], item)
        template_key = entry.get("template")
        if not isinstance(template_key, str):
            continue

        template = get_template(template_key)
        if template is None:
            print_warning(f"Column template references unknown template: '{template_key}'")
            continue

        name_override = entry.get("name")
        final_name = str(name_override) if isinstance(name_override, str) else template.name

        value_override = entry.get("value")
        final_value = str(value_override) if isinstance(value_override, str) else template.value

        result.append(ResolvedColumnTemplate(
            template_key=template_key,
            name=final_name,
            value=final_value,
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
