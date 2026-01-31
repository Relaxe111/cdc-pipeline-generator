"""Database and schema filtering logic."""

from typing import List, Optional
from fnmatch import fnmatch


def should_ignore_database(db_name: str, ignore_patterns: List[str]) -> bool:
    """Check if database name matches any ignore pattern."""
    for pattern in ignore_patterns:
        if pattern.lower() in db_name.lower():
            return True
    return False


def should_include_database(db_name: str, include_pattern: Optional[str]) -> bool:
    """Check if database name matches include pattern (glob-style wildcard)."""
    if not include_pattern:
        return True  # No include pattern = include all
    return fnmatch(db_name, include_pattern)


def should_exclude_schema(schema_name: str, exclude_patterns: Optional[List[str]]) -> bool:
    """Check if schema name matches any exclude pattern."""
    if not exclude_patterns:
        return False  # No exclude patterns = include all
    
    for pattern in exclude_patterns:
        if pattern.lower() in schema_name.lower():
            return True
    return False


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
        # Check if first part looks like a namespace (e.g., 'adopus_db_directory' -> 'directory')
        if len(parts) >= 2:
            # If the part after _db_ is not empty, that's the service
            if parts[1]:
                return parts[1].lower()
        # Otherwise use the first part
        return parts[0].lower()
    
    # No _db_ pattern, use the full name
    return database_name.lower()
