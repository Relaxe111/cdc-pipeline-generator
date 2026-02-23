"""Helpers for Bloblang transform file references.

Canonical transform refs point to files under ``services/_bloblang/``.
Legacy fallback supports ``services/_schemas/_bloblang/`` reads.
"""

from __future__ import annotations

from pathlib import Path

from cdc_generator.helpers.service_config import get_project_root

_CANONICAL_PREFIX = Path("services") / "_bloblang"
_LEGACY_PREFIX = Path("services") / "_schemas" / "_bloblang"
_VALID_SUFFIXES = (".blobl", ".bloblang")
_FILE_PREFIX = "file://"


def _normalize_ref(value: str) -> str:
    """Normalize supported ref formats to project-relative path form."""
    if value.startswith(_FILE_PREFIX):
        return value[len(_FILE_PREFIX):]
    return value


def is_bloblang_ref(value: str) -> bool:
    """Return True when *value* is a valid project-relative Bloblang file ref."""
    normalized = _normalize_ref(value)
    if not normalized:
        return False

    ref_path = Path(normalized)
    suffix = ref_path.suffix.casefold()
    if suffix not in _VALID_SUFFIXES:
        return False

    return str(ref_path).startswith(str(_CANONICAL_PREFIX))


def list_bloblang_refs() -> list[str]:
    """List available Bloblang refs under canonical and legacy locations."""
    project_root = get_project_root()
    refs: set[str] = set()

    for root in (_CANONICAL_PREFIX, _LEGACY_PREFIX):
        full_root = project_root / root
        if not full_root.exists() or not full_root.is_dir():
            continue

        for pattern in ("**/*.blobl", "**/*.bloblang"):
            for file_path in full_root.glob(pattern):
                if not file_path.is_file():
                    continue
                try:
                    rel = file_path.relative_to(project_root)
                except ValueError:
                    continue

                if str(rel).startswith(str(_LEGACY_PREFIX)):
                    relative_tail = rel.relative_to(_LEGACY_PREFIX)
                    rel = _CANONICAL_PREFIX / relative_tail

                refs.add(rel.as_posix())

    return sorted(refs)


def resolve_bloblang_ref(value: str) -> Path | None:
    """Resolve a Bloblang ref to an existing file path.

    Looks in canonical location first, then legacy fallback.
    """
    normalized = _normalize_ref(value)
    if not normalized:
        return None

    project_root = get_project_root()
    ref_path = Path(normalized)
    if ref_path.suffix.casefold() not in _VALID_SUFFIXES:
        return None

    canonical = project_root / ref_path
    if canonical.exists() and canonical.is_file():
        return canonical

    if str(ref_path).startswith(str(_CANONICAL_PREFIX)):
        legacy_rel = _LEGACY_PREFIX / ref_path.relative_to(_CANONICAL_PREFIX)
        legacy = project_root / legacy_rel
        if legacy.exists() and legacy.is_file():
            return legacy

    return None


def read_bloblang_ref(value: str) -> str | None:
    """Read Bloblang content for a file reference, returning None if missing."""
    resolved = resolve_bloblang_ref(value)
    if resolved is None:
        return None
    return resolved.read_text(encoding="utf-8").strip()
