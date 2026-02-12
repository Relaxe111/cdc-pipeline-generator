"""Source-group reference resolver for column template values.

Resolves ``{group.sources.*.key}`` references against source-groups.yaml
at generation time. The ``*`` wildcard resolves to the current source name
during pipeline generation.

Syntax:
    {group.sources.*.key}

Where:
    group   - Server group name (e.g., 'asma', 'adopus')
    sources - Literal keyword
    *       - Wildcard: resolved to the source name at generation time
    key     - Any key defined under the source entry or its environment entries

Examples:
    {asma.sources.*.database}       → "directory_dev" (for source=directory, env=dev)
    {adopus.sources.*.customer_id}  → "3" (for source=AVProd)

Resolution rules:
    1. Look for 'key' directly under sources.{source} (source-level key)
    2. If not found, look under sources.{source}.{env} (env-level key)
    3. If key is missing for ANY environment, fail with clear error

Service YAML usage:
    column_templates:
      - template: tenant_id
        value: "{asma.sources.*.customer_id}"
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error

# Pattern: {group.sources.*.key}
_REF_PATTERN = re.compile(r"^\{([^.]+)\.sources\.\*\.([^}]+)\}$")


@dataclass(frozen=True)
class SourceRef:
    """Parsed source-group reference.

    Attributes:
        group: Server group name (e.g., 'asma').
        key: Key to look up under the source entry (e.g., 'customer_id').
        raw: Original reference string.
    """

    group: str
    key: str
    raw: str


def is_source_ref(value: str) -> bool:
    """Check if a value string is a source-group reference.

    Args:
        value: Template value string.

    Returns:
        True if value matches ``{group.sources.*.key}`` pattern.
    """
    return _REF_PATTERN.match(value) is not None


def parse_source_ref(value: str) -> SourceRef | None:
    """Parse a source-group reference string.

    Args:
        value: Reference string like ``{asma.sources.*.customer_id}``.

    Returns:
        Parsed SourceRef, or None if not a valid reference.
    """
    match = _REF_PATTERN.match(value)
    if match is None:
        return None

    return SourceRef(
        group=match.group(1),
        key=match.group(2),
        raw=value,
    )


def resolve_source_ref(
    ref: SourceRef,
    source_name: str,
    env: str | None = None,
    config: dict[str, Any] | None = None,
) -> str:
    """Resolve a source-group reference to a concrete value.

    Looks up ``source-groups.yaml`` → ``{ref.group}.sources.{source_name}``
    and finds ``ref.key`` either at source level or env level.

    Args:
        ref: Parsed source reference.
        source_name: Current source name (replaces ``*`` wildcard).
        env: Environment name for env-level key lookup (e.g., 'dev', 'prod').
            If None, only source-level keys are checked.
        config: Pre-loaded source-groups config. Loaded from disk if None.

    Returns:
        Resolved string value.

    Raises:
        SourceRefError: If group, source, or key is not found.
    """
    if config is None:
        config = _load_source_groups()

    # Validate group exists
    group_data = config.get(ref.group)
    if group_data is None:
        available = ", ".join(sorted(config.keys()))
        raise SourceRefError(
            f"Source group '{ref.group}' not found in source-groups.yaml.\n"
            + f"  Reference: {ref.raw}\n"
            + f"  Available groups: {available}"
        )

    # Validate sources section exists
    sources = group_data.get("sources")
    if not isinstance(sources, dict):
        raise SourceRefError(
            f"Source group '{ref.group}' has no 'sources' section.\n"
            + f"  Reference: {ref.raw}"
        )

    sources_dict = cast(dict[str, object], sources)

    # Validate source exists
    source_data = sources_dict.get(source_name)
    if not isinstance(source_data, dict):
        available = ", ".join(sorted(str(k) for k in sources_dict))
        raise SourceRefError(
            f"Source '{source_name}' not found in {ref.group}.sources.\n"
            + f"  Reference: {ref.raw}\n"
            + f"  Available sources: {available}"
        )

    source_dict = cast(dict[str, Any], source_data)

    # Step 1: Check source-level key
    if ref.key in source_dict:
        return str(source_dict[ref.key])

    # Step 2: Check env-level key (if env provided)
    if env is not None:
        env_data = source_dict.get(env)
        if isinstance(env_data, dict):
            env_dict = cast(dict[str, Any], env_data)
            if ref.key in env_dict:
                return str(env_dict[ref.key])

    # Build helpful error message
    _raise_key_not_found(ref, source_name, env, source_dict)
    # _raise_key_not_found always raises, but return for type checker
    return ""  # pragma: no cover


def validate_source_ref_for_all_envs(
    ref: SourceRef,
    source_name: str,
    config: dict[str, Any] | None = None,
) -> list[str]:
    """Validate that a source ref key exists for ALL environments of a source.

    Checks every environment entry under ``sources.{source_name}`` to ensure
    the referenced key is available. Returns a list of error messages (empty = valid).

    Args:
        ref: Parsed source reference.
        source_name: Source name to validate.
        config: Pre-loaded source-groups config. Loaded from disk if None.

    Returns:
        List of error messages. Empty list means all environments have the key.
    """
    if config is None:
        config = _load_source_groups()

    group_data = config.get(ref.group)
    if group_data is None:
        return [f"Source group '{ref.group}' not found in source-groups.yaml"]

    sources = group_data.get("sources")
    if not isinstance(sources, dict):
        return [f"Source group '{ref.group}' has no 'sources' section"]

    sources_dict = cast(dict[str, object], sources)

    source_data = sources_dict.get(source_name)
    if not isinstance(source_data, dict):
        return [f"Source '{source_name}' not found in {ref.group}.sources"]

    source_dict = cast(dict[str, Any], source_data)

    # If key is at source level, it's available for all envs
    if ref.key in source_dict:
        return []

    # Check each environment entry
    errors: list[str] = []
    env_keys = _get_env_keys(source_dict)

    if not env_keys:
        errors.append(
            f"Key '{ref.key}' not found at source level in "
            + f"{ref.group}.sources.{source_name}, "
            + "and no environment entries found"
        )
        return errors

    for env_key in env_keys:
        env_data = source_dict.get(env_key)
        if not isinstance(env_data, dict):
            continue
        env_dict = cast(dict[str, Any], env_data)
        if ref.key not in env_dict:
            errors.append(
                f"Key '{ref.key}' missing in "
                + f"{ref.group}.sources.{source_name}.{env_key}"
            )

    return errors


def validate_all_sources_have_key(
    ref: SourceRef,
    config: dict[str, Any] | None = None,
) -> list[str]:
    """Validate that ALL sources in a group have the referenced key.

    Used at generation time to ensure the reference is valid for every
    source that will generate a pipeline.

    Args:
        ref: Parsed source reference.
        config: Pre-loaded source-groups config. Loaded from disk if None.

    Returns:
        List of error messages. Empty list means all sources have the key.
    """
    if config is None:
        config = _load_source_groups()

    group_data = config.get(ref.group)
    if group_data is None:
        return [f"Source group '{ref.group}' not found in source-groups.yaml"]

    sources = group_data.get("sources")
    if not isinstance(sources, dict):
        return [f"Source group '{ref.group}' has no 'sources' section"]

    sources_dict = cast(dict[str, object], sources)

    errors: list[str] = []
    for src_name in sorted(sources_dict):
        src_errors = validate_source_ref_for_all_envs(
            ref, str(src_name), config,
        )
        errors.extend(src_errors)

    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Known non-environment keys at source level
_NON_ENV_KEYS = frozenset({
    "schemas", "customer_id", "name", "schema",
    "description", "table_count",
})


def _get_env_keys(source_dict: dict[str, Any]) -> list[str]:
    """Extract environment keys from a source dict.

    Environment keys are those that map to dict values and aren't
    known non-environment fields like 'schemas'.

    Args:
        source_dict: Source configuration dict.

    Returns:
        Sorted list of environment key names.
    """
    env_keys: list[str] = []
    for k, v in source_dict.items():
        if isinstance(v, dict) and k not in _NON_ENV_KEYS:
            env_keys.append(k)
    return sorted(env_keys)


def _raise_key_not_found(
    ref: SourceRef,
    source_name: str,
    env: str | None,
    source_dict: dict[str, Any],
) -> None:
    """Raise SourceRefError with helpful context about available keys."""
    # Collect available keys
    source_level_keys = sorted(
        k for k in source_dict
        if not isinstance(source_dict[k], dict) and k != "schemas"
    )

    env_level_keys: list[str] = []
    if env is not None:
        env_data = source_dict.get(env)
        if isinstance(env_data, dict):
            env_level_keys = sorted(cast(dict[str, Any], env_data).keys())

    parts = [
        f"Key '{ref.key}' not found for source '{source_name}'.",
        f"  Reference: {ref.raw}",
    ]
    if source_level_keys:
        parts.append(f"  Source-level keys: {', '.join(source_level_keys)}")
    if env_level_keys:
        parts.append(f"  Env-level keys ({env}): {', '.join(env_level_keys)}")
    if not source_level_keys and not env_level_keys:
        parts.append("  No keys found at source or environment level")

    raise SourceRefError("\n".join(parts))


def _load_source_groups() -> dict[str, Any]:
    """Load source-groups.yaml using the server group config loader."""
    from cdc_generator.validators.manage_server_group.config import (
        load_server_groups,
    )

    return cast(dict[str, Any], load_server_groups())


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class SourceRefError(Exception):
    """Error resolving a source-group reference."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def print_error(self) -> None:
        """Print the error message using the logging helper."""
        print_error(self.message)
