"""Database and schema filtering logic."""

import re
from fnmatch import fnmatch

_REGEX_META_CHARS = set(".^$*+?{}[]\\|()")
_DB_PARTS_MIN_LEN = 2


def _looks_like_regex(pattern: str) -> bool:
    return any(char in pattern for char in _REGEX_META_CHARS)


def _matches_pattern(value: str, pattern: str) -> bool:
    cleaned = pattern.strip()
    if not cleaned:
        return False

    if _looks_like_regex(cleaned):
        try:
            return re.search(cleaned, value, flags=re.IGNORECASE) is not None
        except re.error:
            pass

    return cleaned.lower() in value.lower()


def should_ignore_database(db_name: str, ignore_patterns: list[str]) -> bool:
    """Check if database name matches any ignore pattern."""
    return any(_matches_pattern(db_name, pattern) for pattern in ignore_patterns)


def should_include_database(db_name: str, include_pattern: str | None) -> bool:
    """Check if database name matches include pattern (glob-style wildcard)."""
    if not include_pattern:
        return True  # No include pattern = include all
    return fnmatch(db_name, include_pattern)


def should_exclude_schema(schema_name: str, exclude_patterns: list[str] | None) -> bool:
    """Check if schema name matches any exclude pattern."""
    if not exclude_patterns:
        return False  # No exclude patterns = include all

    return any(_matches_pattern(schema_name, pattern) for pattern in exclude_patterns)


def should_exclude_table(table_name: str, exclude_patterns: list[str] | None) -> bool:
    """Check if table name matches any exclude pattern."""
    if not exclude_patterns:
        return False

    return any(_matches_pattern(table_name, pattern) for pattern in exclude_patterns)


def infer_service_name(database_name: str) -> str:
    """Infer service name from database name following pattern: {service}_db_{environment}

    Examples:
        activities_db_dev -> activities
        adopus_db_directory_dev -> directory
        auth_dev -> auth
        directory_dev -> directory
    """
    # Remove common environment suffixes
    for suffix in ['_dev', '_prod', '_test', '_staging']:
        if database_name.endswith(suffix):
            database_name = database_name[:-len(suffix)]
            break

    # Handle {service}_db_ pattern
    if '_db_' in database_name:
        parts = database_name.split('_db_')
        if len(parts) >= _DB_PARTS_MIN_LEN and parts[1]:
            return parts[1].lower()
        # Otherwise use the first part
        return parts[0].lower()

    # No _db_ pattern, use the full name
    return database_name.lower()
