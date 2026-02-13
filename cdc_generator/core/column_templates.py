"""Column templates loader for CDC pipeline generator.

Loads reusable column definitions from service-schemas/column-templates.yaml.
Templates are referenced by name in service YAML extra_columns.

Example column-templates.yaml:
    templates:
      source_table:
        name: _source_table
        type: text
        not_null: true
        description: Source table name
        value: meta("table")

Example service YAML usage:
    extra_columns:
      - template: source_table
      - template: environment
        name: deploy_env         # override name
"""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from cdc_generator.helpers.helpers_logging import print_error, print_warning
from cdc_generator.helpers.yaml_loader import load_yaml_file

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnTemplate:
    """Immutable column template definition.

    Attributes:
        key: Template identifier (lookup key in YAML).
        name: Default column name in sink table.
        column_type: PostgreSQL column type (e.g., text, timestamptz).
        not_null: Whether the column is NOT NULL.
        description: Human-readable description.
        value: Bloblang expression for pipeline runtime.
        default: SQL default expression for DDL (e.g., now()).
        applies_to: Optional list of table patterns this template can be applied to.
            Supports glob patterns (e.g., "*.user", "public.*").
            If None or empty, template can be applied to any table.
    """

    key: str
    name: str
    column_type: str
    not_null: bool
    description: str
    value: str
    default: str | None = None
    applies_to: list[str] | None = None


# ---------------------------------------------------------------------------
# Module-level cache (lazy-loaded singleton)
# ---------------------------------------------------------------------------

_cached_templates: dict[str, ColumnTemplate] | None = None
_templates_file: Path | None = None


def get_templates_path() -> Path:
    """Return the path to column-templates.yaml.

    Respects any override set via ``set_templates_path()``.

    Returns:
        Path to the column-templates.yaml file.
    """
    if _templates_file is not None:
        return _templates_file
    from cdc_generator.helpers.service_config import get_project_root

    return get_project_root() / "service-schemas" / "column-templates.yaml"


def set_templates_path(path: Path) -> None:
    """Override templates file path (for testing).

    Args:
        path: Path to the column-templates.yaml file.
    """
    global _templates_file, _cached_templates  # noqa: PLW0603
    _templates_file = path
    _cached_templates = None


def clear_cache() -> None:
    """Clear the cached templates (for testing)."""
    global _cached_templates  # noqa: PLW0603
    _cached_templates = None


# ---------------------------------------------------------------------------
# Parsing & validation
# ---------------------------------------------------------------------------

_REQUIRED_FIELDS = ("name", "type", "value")


def _resolve_file_reference(value: str) -> tuple[str, str | None]:
    """Resolve file:// reference to actual Bloblang content.

    Args:
        value: Either inline Bloblang or file:// reference.

    Returns:
        tuple: (resolved_bloblang, file_path_or_none)
            - resolved_bloblang: The actual Bloblang expression
            - file_path_or_none: Path to file if it was a reference, None otherwise

    Example:
        "meta('table')" → ("meta('table')", None)
        "file://services/_schemas/_bloblang/mapper.blobl"
            → ("<file content>", "services/_schemas/_bloblang/...")
    """
    if not value.startswith("file://"):
        return value, None

    # Extract file path (remove file:// prefix)
    file_path = value[7:]  # len("file://") == 7

    # Resolve relative to project root
    from cdc_generator.helpers.service_config import get_project_root

    full_path = get_project_root() / file_path

    if not full_path.exists():
        print_error(f"Bloblang file not found: {file_path}")
        print_error(f"  Full path: {full_path}")
        return value, file_path  # Return original, let validation catch it

    try:
        content = full_path.read_text(encoding="utf-8").strip()
        return content, file_path
    except Exception as e:
        print_error(f"Failed to read Bloblang file {file_path}: {e}")
        return value, file_path


def _parse_single_template(
    key: str,
    raw: object,
) -> ColumnTemplate | None:
    """Parse a single template entry from raw YAML data.

    Args:
        key: Template key (e.g., 'source_table').
        raw: Raw YAML dict for this template.

    Returns:
        Parsed ColumnTemplate, or None if invalid.
    """
    if not isinstance(raw, dict):
        print_warning(f"Column template '{key}': expected dict, got {type(raw).__name__}")
        return None

    data = cast(dict[str, object], raw)

    # Validate required fields
    for field in _REQUIRED_FIELDS:
        if field not in data:
            print_warning(f"Column template '{key}': missing required field '{field}'")
            return None

    name = data.get("name")
    col_type = data.get("type")
    value = data.get("value")

    if not isinstance(name, str) or not isinstance(col_type, str) or not isinstance(value, str):
        print_warning(f"Column template '{key}': name, type, and value must be strings")
        return None

    # Resolve file:// references to actual Bloblang content
    resolved_value, _file_ref = _resolve_file_reference(value)

    not_null_raw = data.get("not_null", False)
    not_null = bool(not_null_raw) if isinstance(not_null_raw, bool) else False

    description_raw = data.get("description", "")
    description = str(description_raw) if description_raw is not None else ""

    default_raw = data.get("default")
    default = str(default_raw) if default_raw is not None else None

    # Parse optional applies_to field
    applies_to_raw = data.get("applies_to")
    applies_to: list[str] | None = None
    if applies_to_raw is not None:
        if isinstance(applies_to_raw, list):
            items = cast(list[object], applies_to_raw)
            applies_to = [str(p) for p in items if p is not None]
        elif isinstance(applies_to_raw, str):
            applies_to = [applies_to_raw]  # Convert single string to list

    return ColumnTemplate(
        key=key,
        name=name,
        column_type=col_type,
        not_null=not_null,
        description=description,
        value=resolved_value,  # Use resolved content, not original reference
        default=default,
        applies_to=applies_to,
    )


def _load_all_templates(path: Path) -> dict[str, ColumnTemplate]:
    """Load and parse all templates from YAML file.

    Args:
        path: Path to column-templates.yaml.

    Returns:
        Dict of template key → ColumnTemplate.
    """
    try:
        raw_data = load_yaml_file(path)
    except FileNotFoundError:
        print_error(f"Column templates file not found: {path}")
        return {}

    templates_raw = raw_data.get("templates")
    if not isinstance(templates_raw, dict):
        print_error("Column templates file missing 'templates' root key")
        return {}

    templates_dict = cast(dict[str, object], templates_raw)
    result: dict[str, ColumnTemplate] = {}

    for key_raw, value in templates_dict.items():
        key = str(key_raw)
        template = _parse_single_template(key, value)
        if template is not None:
            result[key] = template

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_templates() -> dict[str, ColumnTemplate]:
    """Get all column templates (lazy-loaded, cached).

    Returns:
        Dict of template key → ColumnTemplate.

    Example:
        >>> templates = get_templates()
        >>> templates["source_table"].name
        '_source_table'
    """
    global _cached_templates  # noqa: PLW0603
    if _cached_templates is None:
        path = get_templates_path()
        _cached_templates = _load_all_templates(path)
    return _cached_templates


def get_template(key: str) -> ColumnTemplate | None:
    """Get a single column template by key.

    Args:
        key: Template identifier (e.g., 'source_table').

    Returns:
        ColumnTemplate if found, None otherwise.

    Example:
        >>> tpl = get_template("sync_timestamp")
        >>> tpl.column_type
        'timestamptz'
    """
    templates = get_templates()
    return templates.get(key)


def list_template_keys() -> list[str]:
    """List all available template keys (sorted).

    Returns:
        Sorted list of template keys.

    Example:
        >>> list_template_keys()
        ['cdc_lsn', 'cdc_operation', 'environment', ...]
    """
    templates = get_templates()
    return sorted(templates.keys())


def validate_template_reference(key: str) -> str | None:
    """Validate that a template key exists.

    Args:
        key: Template key to validate.

    Returns:
        Error message if invalid, None if valid.

    Example:
        >>> validate_template_reference("nonexistent")
        "Column template 'nonexistent' not found. Available: ..."
    """
    template = get_template(key)
    if template is not None:
        return None

    available = list_template_keys()
    available_str = ", ".join(available) if available else "(none)"
    return (
        f"Column template '{key}' not found. "
        + f"Available templates: {available_str}"
    )


def validate_template_for_table(
    template_key: str,
    table_key: str,
) -> str | None:
    """Validate that a template can be applied to a specific table.

    Checks both template existence and applies_to pattern matching.

    Args:
        template_key: Template key to validate.
        table_key: Table identifier (schema.table format).

    Returns:
        Error message if invalid, None if valid.

    Example:
        >>> validate_template_for_table("source_table", "public.users")
        None
        >>> validate_template_for_table("user_specific", "logs.audit_log")
        "Template 'user_specific' cannot be applied to 'logs.audit_log'..."
    """
    # First check if template exists
    error = validate_template_reference(template_key)
    if error is not None:
        return error

    template = get_template(template_key)
    if template is None:
        return f"Template '{template_key}' not found"

    # If no applies_to restriction, template can be used anywhere
    if not template.applies_to:
        return None

    # Check if table matches any pattern
    from fnmatch import fnmatch

    for pattern in template.applies_to:
        if fnmatch(table_key, pattern):
            return None

    # No match found
    patterns_str = ", ".join(f"'{p}'" for p in template.applies_to)
    return (
        f"Template '{template_key}' cannot be applied to '{table_key}'. "
        + f"Allowed patterns: {patterns_str}"
    )

