"""Bloblang expression parser for extracting column references.

Extracts `this.column_name` references from Bloblang expressions to enable
database schema validation.
"""

from __future__ import annotations

import re


def strip_bloblang_comments(bloblang: str) -> str:
    """Remove line comments (``#`` and ``//``) outside quoted strings."""
    stripped_lines: list[str] = []

    for line in bloblang.splitlines():
        in_single = False
        in_double = False
        escaped = False
        comment_start: int | None = None

        for index, char in enumerate(line):
            if escaped:
                escaped = False
                continue

            if char == "\\":
                escaped = True
                continue

            if char == "'" and not in_double:
                in_single = not in_single
                continue

            if char == '"' and not in_single:
                in_double = not in_double
                continue

            if in_single or in_double:
                continue

            if char == "#":
                comment_start = index
                break

            if char == "/" and index + 1 < len(line) and line[index + 1] == "/":
                comment_start = index
                break

        content = line if comment_start is None else line[:comment_start]
        stripped_lines.append(content)

    return "\n".join(stripped_lines)


def extract_column_references(bloblang: str) -> set[str]:
    """Extract all column references from a Bloblang expression.

    Finds patterns:
    - this.column_name
    - this.nested.field (extracts 'nested')
    - this["column_name"]
    - this['column_name']

    Args:
        bloblang: Bloblang expression to parse.

    Returns:
        Set of column names referenced in the expression.

    Examples:
        >>> extract_column_references('this.customer_id')
        {'customer_id'}

        >>> extract_column_references('this.user.name')
        {'user'}

        >>> extract_column_references('this["status"] == "A"')
        {'status'}

        >>> extract_column_references('match this.type { "A" => this.value_a, _ => this.value_b }')
        {'type', 'value_a', 'value_b'}
    """
    columns: set[str] = set()
    normalized_bloblang = strip_bloblang_comments(bloblang)

    # Pattern 1: this.column_name (dot notation)
    # Matches: this.column_name, this.nested.field
    # Captures only the first level: customer_id, nested
    dot_pattern = r'\bthis\.([a-zA-Z_][a-zA-Z0-9_]*)'
    columns.update(re.findall(dot_pattern, normalized_bloblang))

    # Pattern 2: this["column"] or this['column'] (bracket notation)
    bracket_pattern = r'\bthis\[(["\'])([^"\']+)\1\]'
    bracket_matches = re.findall(bracket_pattern, normalized_bloblang)
    columns.update(match[1] for match in bracket_matches)

    return set(columns)


def is_static_expression(bloblang: str) -> bool:
    """Check if Bloblang expression references no database columns.

    Static expressions use only functions, literals, and metadata:
    - meta("table")
    - now()
    - uuid_v4()
    - "literal_string"
    - 42
    - ${ENVIRONMENT}

    Args:
        bloblang: Bloblang expression to check.

    Returns:
        True if expression is static (no column references).

    Examples:
        >>> is_static_expression('meta("table")')
        True

        >>> is_static_expression('this.customer_id')
        False

        >>> is_static_expression('now()')
        True

        >>> is_static_expression('this.created_at.format_timestamp("2006-01-02")')
        False
    """
    return len(extract_column_references(bloblang)) == 0


def extract_metadata_references(bloblang: str) -> set[str]:
    """Extract metadata field references from Bloblang.

    Finds patterns like:
    - meta("table")
    - meta("kafka_topic")
    - meta("operation")

    Args:
        bloblang: Bloblang expression to parse.

    Returns:
        Set of metadata field names.

    Examples:
        >>> extract_metadata_references('meta("table")')
        {'table'}

        >>> extract_metadata_references('meta("kafka_topic") + "_" + meta("partition")')
        {'kafka_topic', 'partition'}
    """
    # Match meta("field_name") or meta('field_name')
    pattern = r'meta\((["\'])([^"\']+)\1\)'
    matches = re.findall(pattern, strip_bloblang_comments(bloblang))
    return {match[1] for match in matches}


def uses_environment_variables(bloblang: str) -> set[str]:
    """Extract environment variable references from Bloblang.

    Finds patterns:
    - ${VAR_NAME}
    - ${ENVIRONMENT}

    Args:
        bloblang: Bloblang expression to parse.

    Returns:
        Set of environment variable names.

    Examples:
        >>> uses_environment_variables('${ENVIRONMENT}')
        {'ENVIRONMENT'}

        >>> uses_environment_variables('${DB_HOST}:${DB_PORT}')
        {'DB_HOST', 'DB_PORT'}
    """
    pattern = r'\$\{([A-Z_][A-Z0-9_]*)\}'
    return set(re.findall(pattern, strip_bloblang_comments(bloblang)))


def extract_root_assignments(bloblang: str) -> set[str]:
    """Extract top-level root field assignments from Bloblang.

    Finds patterns like:
    - root.field = ...
    - root.field.subfield = ... (captures ``field``)

    Args:
        bloblang: Bloblang expression to parse.

    Returns:
        Set of root field names assigned in the expression.

    Examples:
        >>> extract_root_assignments('root._priority = "high"')
        {'_priority'}

        >>> extract_root_assignments('root.profile.name = this.name')
        {'profile'}
    """
    pattern = r'\broot\.([a-zA-Z_][a-zA-Z0-9_]*)\b\s*='
    return set(re.findall(pattern, strip_bloblang_comments(bloblang)))
