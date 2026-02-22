"""CRUD operations for column template definitions in column-templates.yaml.

Manages the template library itself — adding, removing, editing, and
listing template definitions. Separate from column_template_operations.py
which manages template *references* on sink tables.

Usage:
    cdc manage-column-templates --add tenant_id \\
        --type text --not-null --value '${TENANT_ID}' \\
        --description "Tenant identifier"
    cdc manage-column-templates --remove tenant_id
    cdc manage-column-templates --list
    cdc manage-column-templates --show tenant_id
    cdc manage-column-templates --edit tenant_id --value '{asma.sources.*.customer_id}'
"""

from __future__ import annotations

import re
from typing import Any, cast

from cdc_generator.core.column_templates import (
    ColumnTemplate,
    clear_cache,
    get_template,
    get_templates_path,
    list_template_keys,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file, save_yaml_file

# ---------------------------------------------------------------------------
# YAML I/O (uses column_templates module's path for consistency)
# ---------------------------------------------------------------------------


def _load_raw_yaml() -> dict[str, Any]:
    """Load column-templates.yaml as raw YAML (preserving comments)."""
    path = get_templates_path()
    if not path.exists():
        return {"templates": {}}
    return cast(dict[str, Any], load_yaml_file(path))


def _save_raw_yaml(data: dict[str, Any]) -> None:
    """Save raw YAML back to column-templates.yaml."""
    path = get_templates_path()
    save_yaml_file(data, path)
    clear_cache()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_TYPES = frozenset({
    "text", "integer", "bigint", "boolean", "numeric",
    "timestamptz", "timestamp", "date", "time",
    "uuid", "jsonb", "json", "bytea", "smallint",
    "real", "double precision", "varchar", "char",
    "inet", "cidr", "macaddr",
})

_VALID_VALUE_SOURCES = frozenset({"bloblang", "source_ref", "sql"})
_VALUE_SOURCE_ALIASES: dict[str, str] = {
    "source-ref": "source_ref",
    "source_ref": "source_ref",
    "bloblang": "bloblang",
    "sql": "sql",
}

_SQL_EXPR_ALLOWED_RE = re.compile(
    r"^[a-zA-Z0-9_\s().,'\"+\-*/%:<>=!?|&\[\]{}$]+$"
)


def _normalize_value_source(raw_source: str) -> str | None:
    """Normalize value source aliases to canonical names."""
    normalized = raw_source.strip().lower()
    return _VALUE_SOURCE_ALIASES.get(normalized)


def _infer_value_source(value: str) -> str:
    """Infer value source for backward compatibility when unspecified."""
    from cdc_generator.core.source_ref_resolver import is_source_ref

    if is_source_ref(value):
        return "source_ref"
    return "bloblang"


def validate_value_source(value_source: str) -> str | None:
    """Validate value_source option for template value semantics."""
    normalized = _normalize_value_source(value_source)
    if normalized is not None:
        return None

    allowed = ", ".join(sorted(_VALID_VALUE_SOURCES))
    return (
        f"Unsupported value source '{value_source}'. "
        + f"Allowed values: {allowed}"
    )


def validate_template_value(value: str, value_source: str) -> str | None:
    """Validate template value according to its declared value_source."""
    from cdc_generator.core.source_ref_resolver import (
        parse_source_ref,
    )
    from cdc_generator.validators.manage_service.bloblang_validator import (
        check_rpk_available,
        validate_bloblang_expression,
    )

    normalized_source = _normalize_value_source(value_source)
    if normalized_source is None:
        return validate_value_source(value_source)

    stripped_value = value.strip()
    if not stripped_value:
        return "Template value cannot be empty"

    if normalized_source == "source_ref":
        ref = parse_source_ref(stripped_value)
        if ref is None:
            return (
                "Invalid source reference format. "
                + "Expected: {group.sources.*.key}"
            )
        return None

    if normalized_source == "sql":
        lowered = stripped_value.lower()
        if stripped_value.startswith("{") and stripped_value.endswith("}"):
            return "SQL value cannot use source-ref syntax"
        if "this." in lowered or "meta(" in lowered:
            return (
                "SQL value cannot contain Bloblang tokens "
                + "(this., meta(...))"
            )
        if not _SQL_EXPR_ALLOWED_RE.fullmatch(stripped_value):
            return (
                "SQL value contains unsupported characters. "
                + "Use a SQL expression like now(), gen_random_uuid(), "
                + "current_timestamp, cast(...), etc."
            )
        return None

    is_valid = True
    error_msg: str | None = None
    if check_rpk_available():
        is_valid, error_msg = validate_bloblang_expression(
            stripped_value,
            "column_template_value",
        )

    if not is_valid:
        detail = error_msg if error_msg else "unknown Bloblang parser error"
        return f"Invalid Bloblang expression: {detail}"

    return None


def validate_template_key(key: str) -> str | None:
    """Validate that a template key is well-formed.

    Args:
        key: Template key to validate.

    Returns:
        Error message if invalid, None if valid.
    """
    if not key:
        return "Template key cannot be empty"

    if not key.replace("_", "").replace("-", "").isalnum():
        return (
            f"Template key '{key}' contains invalid characters. "
            + "Use only alphanumeric, underscores, and hyphens."
        )

    if key[0].isdigit():
        return f"Template key '{key}' cannot start with a digit"

    return None


def validate_column_type(col_type: str) -> str | None:
    """Validate PostgreSQL column type.

    Args:
        col_type: Column type string.

    Returns:
        Error message if invalid, None if valid.
    """
    # Allow parameterized types like varchar(255)
    base_type = col_type.split("(")[0].strip().lower()
    if base_type not in _VALID_TYPES:
        available = ", ".join(sorted(_VALID_TYPES))
        return (
            f"Unknown column type '{col_type}'. "
            + f"Available types: {available}"
        )
    return None


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------


def add_template_definition(
    key: str,
    name: str,
    col_type: str,
    value: str,
    description: str = "",
    not_null: bool = False,
    default: str | None = None,
    applies_to: list[str] | None = None,
    value_source: str | None = None,
) -> bool:
    """Add a new template definition to column-templates.yaml.

    Args:
        key: Template identifier (e.g., 'tenant_id').
        name: Default column name (e.g., '_tenant_id').
        col_type: PostgreSQL column type (e.g., 'text').
        value: Bloblang expression or env var reference.
        description: Human-readable description.
        not_null: Whether column is NOT NULL.
        default: SQL default expression for DDL.
        applies_to: Optional list of table patterns (glob).
        value_source: Value generation mode: bloblang, source_ref, or sql.

    Returns:
        True on success, False on error.
    """
    # Validate key
    key_error = validate_template_key(key)
    if key_error is not None:
        print_error(key_error)
        return False

    # Validate type
    type_error = validate_column_type(col_type)
    if type_error is not None:
        print_error(type_error)
        return False

    effective_value_source = value_source or _infer_value_source(value)
    source_error = validate_value_source(effective_value_source)
    if source_error is not None:
        print_error(source_error)
        return False

    value_error = validate_template_value(value, effective_value_source)
    if value_error is not None:
        print_error(value_error)
        return False

    # Check for duplicate
    existing = get_template(key)
    if existing is not None:
        print_error(
            f"Template '{key}' already exists "
            + f"(column: {existing.name}, type: {existing.column_type})"
        )
        print_info("Use --edit to modify an existing template")
        return False

    # Build YAML entry
    entry: dict[str, Any] = {
        "name": name,
        "type": col_type,
    }
    if not_null:
        entry["not_null"] = True
    if description:
        entry["description"] = description
    entry["value"] = value
    normalized_source = _normalize_value_source(effective_value_source)
    if normalized_source is not None:
        entry["value_source"] = normalized_source
    if default is not None:
        entry["default"] = default
    if applies_to:
        entry["applies_to"] = applies_to

    # Load, insert, save
    raw = _load_raw_yaml()
    templates = raw.get("templates")
    if not isinstance(templates, dict):
        raw["templates"] = {}
        templates = raw["templates"]

    templates_dict = cast(dict[str, Any], templates)
    templates_dict[key] = entry
    _save_raw_yaml(raw)

    print_success(f"Added template '{key}' (column: {name}, type: {col_type})")
    return True


def remove_template_definition(key: str) -> bool:
    """Remove a template definition from column-templates.yaml.

    Args:
        key: Template key to remove.

    Returns:
        True on success, False if not found.
    """
    existing = get_template(key)
    if existing is None:
        print_error(f"Template '{key}' not found")
        _show_available_templates()
        return False

    raw = _load_raw_yaml()
    templates = raw.get("templates")
    if not isinstance(templates, dict):
        print_error("No templates section found in column-templates.yaml")
        return False

    templates_dict = cast(dict[str, Any], templates)
    if key not in templates_dict:
        print_error(f"Template '{key}' not found in file")
        return False

    del templates_dict[key]
    _save_raw_yaml(raw)

    print_success(
        f"Removed template '{key}' "
        + f"(was: column {existing.name}, type {existing.column_type})"
    )
    return True


def edit_template_definition(
    key: str,
    name: str | None = None,
    col_type: str | None = None,
    value: str | None = None,
    description: str | None = None,
    not_null: bool | None = None,
    default: str | None = None,
    value_source: str | None = None,
) -> bool:
    """Edit an existing template definition in column-templates.yaml.

    Only provided fields are updated; others remain unchanged.

    Args:
        key: Template key to edit.
        name: New column name (or None to keep).
        col_type: New column type (or None to keep).
        value: New Bloblang value expression (or None to keep).
        description: New description (or None to keep).
        not_null: New NOT NULL setting (or None to keep).
        default: New SQL default (or None to keep).
        value_source: New value generation mode (or None to keep/infer).

    Returns:
        True on success, False on error.
    """
    existing = get_template(key)
    if existing is None:
        print_error(f"Template '{key}' not found")
        _show_available_templates()
        return False

    # Validate new type if provided
    if col_type is not None:
        type_error = validate_column_type(col_type)
        if type_error is not None:
            print_error(type_error)
            return False

    raw = _load_raw_yaml()
    templates = raw.get("templates")
    if not isinstance(templates, dict):
        print_error("No templates section found")
        return False

    templates_dict = cast(dict[str, Any], templates)
    entry = templates_dict.get(key)
    if not isinstance(entry, dict):
        print_error(f"Template '{key}' entry is malformed")
        return False

    entry_dict = cast(dict[str, Any], entry)

    current_value_raw = entry_dict.get("value")
    current_value = str(current_value_raw) if current_value_raw is not None else ""
    existing_source_raw = entry_dict.get("value_source")
    existing_source = (
        _normalize_value_source(existing_source_raw)
        if isinstance(existing_source_raw, str)
        else None
    )

    effective_value = value if value is not None else current_value
    effective_value_source = (
        value_source
        if value_source is not None
        else (existing_source or _infer_value_source(effective_value))
    )

    source_error = validate_value_source(effective_value_source)
    if source_error is not None:
        print_error(source_error)
        return False

    value_error = validate_template_value(effective_value, effective_value_source)
    if value_error is not None:
        print_error(value_error)
        return False

    # Update provided fields
    changes: list[str] = []
    if name is not None:
        entry_dict["name"] = name
        changes.append(f"name → {name}")
    if col_type is not None:
        entry_dict["type"] = col_type
        changes.append(f"type → {col_type}")
    if value is not None:
        entry_dict["value"] = value
        changes.append(f"value → {value}")
    if description is not None:
        entry_dict["description"] = description
        changes.append(f"description → {description}")
    if not_null is not None:
        entry_dict["not_null"] = not_null
        changes.append(f"not_null → {not_null}")
    if default is not None:
        entry_dict["default"] = default
        changes.append(f"default → {default}")
    if value_source is not None:
        normalized_source = _normalize_value_source(value_source)
        if normalized_source is not None:
            entry_dict["value_source"] = normalized_source
            changes.append(f"value_source → {normalized_source}")

    if "value_source" not in entry_dict:
        normalized_source = _normalize_value_source(effective_value_source)
        if normalized_source is not None:
            entry_dict["value_source"] = normalized_source
            changes.append(f"value_source → {normalized_source}")

    if not changes:
        print_warning("No changes specified")
        return False

    _save_raw_yaml(raw)

    changes_str = ", ".join(changes)
    print_success(f"Updated template '{key}': {changes_str}")
    return True


def show_template_definition(key: str) -> ColumnTemplate | None:
    """Get detailed info about a template definition.

    Args:
        key: Template key to show.

    Returns:
        ColumnTemplate if found, None if not found.
    """
    template = get_template(key)
    if template is None:
        print_error(f"Template '{key}' not found")
        _show_available_templates()
        return None
    return template


def list_template_definitions() -> list[ColumnTemplate]:
    """List all template definitions with full details.

    Returns:
        List of all ColumnTemplate objects, sorted by key.
    """
    keys = list_template_keys()
    result: list[ColumnTemplate] = []
    for key in keys:
        template = get_template(key)
        if template is not None:
            result.append(template)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _show_available_templates() -> None:
    """Print available template keys."""
    keys = list_template_keys()
    if keys:
        print_info(f"Available templates: {', '.join(keys)}")
    else:
        print_info("No templates defined yet")
