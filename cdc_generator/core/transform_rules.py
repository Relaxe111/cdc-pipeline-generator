"""Transform rules loader for CDC pipeline generator.

Loads reusable row transformation rules from service-schemas/transform-rules.yaml.
Rules are referenced by name in service YAML transforms.

Rule types:
    conditional_column - First-match conditional (1 row → 1 row, adds column)
    row_multiplier     - All-match multiplication (1 row → N rows)
    filter             - Drop non-matching rows (1 row → 0 or 1 row)

Example transform-rules.yaml:
    rules:
      user_class_splitter:
        type: row_multiplier
        output_column:
          name: _user_class
          type: text
          not_null: true
        conditions:
          - when: this.Patient == true
            value: '"Patient"'
        on_no_match: drop

Example service YAML usage:
    transforms:
      - rule: user_class_splitter
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from cdc_generator.helpers.helpers_logging import print_error, print_warning
from cdc_generator.helpers.yaml_loader import load_yaml_file

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_RULE_TYPES = ("conditional_column", "row_multiplier", "filter")
RuleType = Literal["conditional_column", "row_multiplier", "filter"]

VALID_ON_NO_MATCH = ("drop", "keep", "default")
OnNoMatch = Literal["drop", "keep", "default"]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransformCondition:
    """A single condition within a transform rule.

    Attributes:
        when: Bloblang boolean expression (e.g., 'this.Patient == true').
        value: Bloblang expression for output value (optional for filter type).
    """

    when: str
    value: str | None = None


@dataclass(frozen=True)
class OutputColumn:
    """Output column definition for conditional_column and row_multiplier.

    Attributes:
        name: Column name in sink table.
        column_type: PostgreSQL column type.
        not_null: Whether the column is NOT NULL.
    """

    name: str
    column_type: str
    not_null: bool = False


@dataclass(frozen=True)
class TransformRule:
    """Immutable transform rule definition.

    Attributes:
        key: Rule identifier (lookup key in YAML).
        rule_type: One of: conditional_column, row_multiplier, filter.
        description: Human-readable description.
        conditions: List of conditions to evaluate.
        output_column: Column definition (required for conditional/multiplier).
        on_no_match: Behavior when no condition matches.
        default_value: Default Bloblang expression (for on_no_match='default').
    """

    key: str
    rule_type: RuleType
    description: str
    conditions: tuple[TransformCondition, ...]
    output_column: OutputColumn | None = None
    on_no_match: OnNoMatch = "drop"
    default_value: str | None = None


# ---------------------------------------------------------------------------
# Module-level cache (lazy-loaded singleton)
# ---------------------------------------------------------------------------

_cached_rules: dict[str, TransformRule] | None = None
_rules_file: Path | None = None


def _get_rules_path() -> Path:
    """Return the path to transform-rules.yaml."""
    if _rules_file is not None:
        return _rules_file
    from cdc_generator.helpers.service_config import get_project_root

    return get_project_root() / "service-schemas" / "transform-rules.yaml"


def set_rules_path(path: Path) -> None:
    """Override rules file path (for testing).

    Args:
        path: Path to the transform-rules.yaml file.
    """
    global _rules_file, _cached_rules  # noqa: PLW0603
    _rules_file = path
    _cached_rules = None


def clear_cache() -> None:
    """Clear the cached rules (for testing)."""
    global _cached_rules  # noqa: PLW0603
    _cached_rules = None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_output_column(key: str, raw: object) -> OutputColumn | None:
    """Parse output_column from raw YAML.

    Args:
        key: Parent rule key (for error messages).
        raw: Raw YAML dict for output_column.

    Returns:
        Parsed OutputColumn, or None if invalid.
    """
    if not isinstance(raw, dict):
        print_warning(f"Rule '{key}': output_column must be a dict")
        return None

    data = cast(dict[str, object], raw)
    name = data.get("name")
    col_type = data.get("type")

    if not isinstance(name, str) or not isinstance(col_type, str):
        print_warning(f"Rule '{key}': output_column requires 'name' and 'type' as strings")
        return None

    not_null_raw = data.get("not_null", False)
    not_null = bool(not_null_raw) if isinstance(not_null_raw, bool) else False

    return OutputColumn(name=name, column_type=col_type, not_null=not_null)


def _parse_conditions(
    key: str,
    raw: object,
    require_value: bool,
) -> tuple[TransformCondition, ...] | None:
    """Parse conditions list from raw YAML.

    Args:
        key: Parent rule key (for error messages).
        raw: Raw YAML list of conditions.
        require_value: Whether 'value' is required in each condition.

    Returns:
        Tuple of TransformCondition, or None if invalid.
    """
    if not isinstance(raw, list):
        print_warning(f"Rule '{key}': conditions must be a list")
        return None

    conditions: list[TransformCondition] = []
    for idx, item in enumerate(cast(list[object], raw)):
        if not isinstance(item, dict):
            print_warning(f"Rule '{key}': condition[{idx}] must be a dict")
            return None

        cond_data = cast(dict[str, object], item)
        when = cond_data.get("when")
        if not isinstance(when, str):
            print_warning(f"Rule '{key}': condition[{idx}] missing 'when' string")
            return None

        value_raw = cond_data.get("value")
        if require_value and not isinstance(value_raw, str):
            print_warning(f"Rule '{key}': condition[{idx}] missing 'value' string")
            return None

        value = str(value_raw) if isinstance(value_raw, str) else None
        conditions.append(TransformCondition(when=when, value=value))

    if not conditions:
        print_warning(f"Rule '{key}': conditions list is empty")
        return None

    return tuple(conditions)


def _validate_rule_type(key: str, raw: object) -> RuleType | None:
    """Validate and return rule type.

    Args:
        key: Rule key (for error messages).
        raw: Raw type value from YAML.

    Returns:
        Validated RuleType, or None if invalid.
    """
    if not isinstance(raw, str):
        print_warning(f"Rule '{key}': 'type' must be a string")
        return None

    if raw not in VALID_RULE_TYPES:
        print_warning(
            f"Rule '{key}': invalid type '{raw}'. "
            + f"Must be one of: {', '.join(VALID_RULE_TYPES)}"
        )
        return None

    return raw  # Already narrowed to RuleType by 'in' check


def _validate_on_no_match(key: str, raw: object) -> OnNoMatch:
    """Validate and return on_no_match value.

    Args:
        key: Rule key (for error messages).
        raw: Raw on_no_match value from YAML.

    Returns:
        Validated OnNoMatch value (defaults to 'drop').
    """
    if raw is None:
        return "drop"

    if not isinstance(raw, str) or raw not in VALID_ON_NO_MATCH:
        print_warning(
            f"Rule '{key}': invalid on_no_match '{raw}'. "
            + f"Must be one of: {', '.join(VALID_ON_NO_MATCH)}. Defaulting to 'drop'."
        )
        return "drop"

    return raw  # Already narrowed to OnNoMatch by 'in' check


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


def _parse_single_rule(key: str, raw: object) -> TransformRule | None:
    """Parse a single transform rule from raw YAML data.

    Args:
        key: Rule key (e.g., 'user_class_splitter').
        raw: Raw YAML dict for this rule.

    Returns:
        Parsed TransformRule, or None if invalid.
    """
    if not isinstance(raw, dict):
        print_warning(f"Transform rule '{key}': expected dict, got {type(raw).__name__}")
        return None

    data = cast(dict[str, object], raw)

    # Validate rule type
    rule_type = _validate_rule_type(key, data.get("type"))
    if rule_type is None:
        return None

    # Description
    desc_raw = data.get("description", "")
    description = str(desc_raw) if desc_raw is not None else ""

    # on_no_match
    on_no_match = _validate_on_no_match(key, data.get("on_no_match"))

    # default_value (for conditional_column with on_no_match=default)
    default_raw = data.get("default_value")
    default_value = str(default_raw) if isinstance(default_raw, str) else None

    # Output column (required for conditional_column and row_multiplier)
    output_column: OutputColumn | None = None
    needs_output = rule_type in ("conditional_column", "row_multiplier")
    if needs_output:
        output_column = _parse_output_column(key, data.get("output_column"))
        if output_column is None:
            return None

    # Conditions (value required for conditional_column and row_multiplier)
    require_value = rule_type in ("conditional_column", "row_multiplier")
    conditions = _parse_conditions(key, data.get("conditions"), require_value)
    if conditions is None:
        return None

    return TransformRule(
        key=key,
        rule_type=rule_type,
        description=description,
        conditions=conditions,
        output_column=output_column,
        on_no_match=on_no_match,
        default_value=default_value,
    )


def _load_all_rules(path: Path) -> dict[str, TransformRule]:
    """Load and parse all rules from YAML file.

    Args:
        path: Path to transform-rules.yaml.

    Returns:
        Dict of rule key → TransformRule.
    """
    try:
        raw_data = load_yaml_file(path)
    except FileNotFoundError:
        print_error(f"Transform rules file not found: {path}")
        return {}

    rules_raw = raw_data.get("rules")
    if not isinstance(rules_raw, dict):
        print_error("Transform rules file missing 'rules' root key")
        return {}

    rules_dict = cast(dict[str, object], rules_raw)
    result: dict[str, TransformRule] = {}

    for key_raw, value in rules_dict.items():
        key = str(key_raw)
        rule = _parse_single_rule(key, value)
        if rule is not None:
            result[key] = rule

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_rules() -> dict[str, TransformRule]:
    """Get all transform rules (lazy-loaded, cached).

    Returns:
        Dict of rule key → TransformRule.

    Example:
        >>> rules = get_rules()
        >>> rules["user_class_splitter"].rule_type
        'row_multiplier'
    """
    global _cached_rules  # noqa: PLW0603
    if _cached_rules is None:
        path = _get_rules_path()
        _cached_rules = _load_all_rules(path)
    return _cached_rules


def get_rule(key: str) -> TransformRule | None:
    """Get a single transform rule by key.

    Args:
        key: Rule identifier (e.g., 'user_class_splitter').

    Returns:
        TransformRule if found, None otherwise.

    Example:
        >>> rule = get_rule("active_users_only")
        >>> rule.rule_type
        'filter'
    """
    rules = get_rules()
    return rules.get(key)


def list_rule_keys() -> list[str]:
    """List all available rule keys (sorted).

    Returns:
        Sorted list of rule keys.

    Example:
        >>> list_rule_keys()
        ['active_users_only', 'priority_label', 'user_class_splitter']
    """
    rules = get_rules()
    return sorted(rules.keys())


def validate_rule_reference(key: str) -> str | None:
    """Validate that a rule key exists.

    Args:
        key: Rule key to validate.

    Returns:
        Error message if invalid, None if valid.

    Example:
        >>> validate_rule_reference("nonexistent")
        "Transform rule 'nonexistent' not found. Available: ..."
    """
    rule = get_rule(key)
    if rule is not None:
        return None

    available = list_rule_keys()
    available_str = ", ".join(available) if available else "(none)"
    return (
        f"Transform rule '{key}' not found. "
        + f"Available rules: {available_str}"
    )
