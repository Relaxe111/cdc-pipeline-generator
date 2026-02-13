"""Helpers for resolving service schema storage paths.

Preferred layout:
- services/_schemas/<service>/...

Legacy layout (read compatibility):
- service-schemas/<service>/...
"""

from __future__ import annotations

from pathlib import Path

from cdc_generator.helpers.service_config import get_project_root

PREFERRED_SCHEMAS_RELATIVE_PATH = Path("services") / "_schemas"
LEGACY_SCHEMAS_DIRNAME = "service-schemas"


def get_schema_roots(project_root: Path | None = None) -> list[Path]:
    """Return schema root candidates in priority order.

    Priority is preferred path first, then legacy path.
    Existing paths are returned first, then missing paths.
    """
    root = project_root if project_root is not None else get_project_root()
    preferred = root / PREFERRED_SCHEMAS_RELATIVE_PATH
    legacy = root / LEGACY_SCHEMAS_DIRNAME

    existing: list[Path] = []
    missing: list[Path] = []
    for candidate in [preferred, legacy]:
        if candidate.exists():
            existing.append(candidate)
        else:
            missing.append(candidate)

    return [*existing, *missing]


def get_schema_write_root(project_root: Path | None = None) -> Path:
    """Return canonical schema write root (preferred path)."""
    root = project_root if project_root is not None else get_project_root()
    return root / PREFERRED_SCHEMAS_RELATIVE_PATH


def get_service_schema_read_dirs(
    service: str,
    project_root: Path | None = None,
) -> list[Path]:
    """Return service schema directories for reads (preferred, then legacy)."""
    return [schema_root / service for schema_root in get_schema_roots(project_root)]


def get_service_schema_write_dir(
    service: str,
    project_root: Path | None = None,
) -> Path:
    """Return canonical service schema directory for writes."""
    return get_schema_write_root(project_root) / service
