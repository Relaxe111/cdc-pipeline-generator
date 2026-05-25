"""Helpers for resolving CLI configuration values from .env and process env."""

from __future__ import annotations

import os
import re
from pathlib import Path

_MIN_QUOTED_VALUE_LENGTH = 2
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def build_env_lookup(project_root: Path) -> dict[str, str]:
    """Build an environment lookup using process env with .env fallbacks."""
    env_lookup = dict(os.environ)
    env_path = project_root / ".env"
    if not env_path.exists():
        return env_lookup

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        env_lookup.setdefault(key.strip(), _strip_env_value(value.strip()))

    return env_lookup


def resolve_config_value(
    raw_value: object,
    env_lookup: dict[str, str],
    resolve_env_values: bool,
    field_name: str,
) -> str:
    """Resolve a config value, expanding ${VAR} placeholders when enabled."""
    if raw_value is None:
        raise ValueError(f"{field_name} is missing")

    value = str(raw_value).strip()
    if not value:
        raise ValueError(f"{field_name} is empty")
    if not resolve_env_values:
        return value

    missing_vars = [match.group(1) for match in _ENV_VAR_PATTERN.finditer(value) if not env_lookup.get(match.group(1), "").strip()]
    if missing_vars:
        missing_list = ", ".join(sorted(set(missing_vars)))
        raise ValueError(f"{field_name} uses missing environment variable(s): {missing_list}")

    resolved_value = _ENV_VAR_PATTERN.sub(
        lambda match: env_lookup.get(match.group(1), ""),
        value,
    )
    if not resolved_value.strip():
        raise ValueError(f"{field_name} resolves to an empty value")
    return resolved_value


def _strip_env_value(value: str) -> str:
    if len(value) >= _MIN_QUOTED_VALUE_LENGTH and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value
