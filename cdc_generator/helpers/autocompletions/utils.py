"""Shared utilities for autocompletion functions."""

from pathlib import Path


def find_file_upward(filename: str, max_depth: int = 3) -> Path | None:
    """Search for a file by walking up the directory tree.

    Args:
        filename: Name of the file to search for.
        max_depth: Maximum levels to search upward.

    Returns:
        Path to the file if found, None otherwise.

    Example:
        >>> find_file_upward('source-groups.yaml')
        Path('/workspace/source-groups.yaml')
    """
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / filename
        if candidate.exists():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def find_directory_upward(dirname: str, max_depth: int = 3) -> Path | None:
    """Search for a directory by walking up the directory tree.

    Args:
        dirname: Name of the directory to search for.
        max_depth: Maximum levels to search upward.

    Returns:
        Path to the directory if found, None otherwise.

    Example:
        >>> find_directory_upward('services')
        Path('/workspace/services')
    """
    current = Path.cwd()
    for _ in range(max_depth):
        candidate = current / dirname
        if candidate.is_dir():
            return candidate
        if current == current.parent:
            break
        current = current.parent
    return None


def find_service_schemas_dir_upward(max_depth: int = 3) -> Path | None:
    """Find service schema root with preferred/legacy compatibility.

    Search order per directory level:
    1) services/_schemas (preferred)
    2) service-schemas (legacy)
    """
    current = Path.cwd()
    for _ in range(max_depth):
        preferred = current / "services" / "_schemas"
        if preferred.is_dir():
            return preferred

        legacy = current / "service-schemas"
        if legacy.is_dir():
            return legacy

        if current == current.parent:
            break
        current = current.parent
    return None
