"""Shared validation helpers for migration CLI commands.

These helpers keep sink/environment validation consistent across
``manage-migrations`` commands that operate on generated manifests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml


def list_sink_dirs(migrations_dir: Path) -> list[Path]:
    """Return sink directories containing a manifest file."""
    if not migrations_dir.exists() or not migrations_dir.is_dir():
        return []

    return sorted(
        d
        for d in migrations_dir.iterdir()
        if d.is_dir() and (d / "manifest.yaml").exists()
    )


def resolve_sink_filter(
    *,
    migrations_dir: Path,
    sink_filter: str | None,
) -> str:
    """Resolve sink selection for env-bound commands.

    Rules:
    - If ``sink_filter`` is provided, it must exist.
    - If exactly one sink exists, auto-select it.
    - If multiple sinks exist and no filter is provided, raise.
    """
    sink_dirs = list_sink_dirs(migrations_dir)
    sink_names = [d.name for d in sink_dirs]

    if not sink_names:
        raise ValueError(
            f"No sink manifests found under: {migrations_dir}. "
            + "Run 'cdc manage-migrations generate' first.",
        )

    if sink_filter:
        if sink_filter not in sink_names:
            available = ", ".join(sink_names)
            raise ValueError(
                f"Unknown sink '{sink_filter}'. Available sinks: {available}",
            )
        return sink_filter

    if len(sink_names) == 1:
        return sink_names[0]

    available = ", ".join(sink_names)
    raise ValueError(
        "--sink is required when multiple sink targets exist. "
        + f"Available sinks: {available}",
    )


def list_manifest_envs(
    *,
    migrations_dir: Path,
    sink_filter: str | None,
) -> list[str]:
    """List environment keys from manifest ``sink_target.databases``.

    If ``sink_filter`` is omitted and exactly one sink exists, that sink is used.
    If multiple sinks exist and no filter is provided, returns a sorted union.
    """
    sink_dirs = list_sink_dirs(migrations_dir)
    if not sink_dirs:
        return []

    if sink_filter:
        selected = [d for d in sink_dirs if d.name == sink_filter]
    elif len(sink_dirs) == 1:
        selected = sink_dirs
    else:
        selected = sink_dirs

    envs: set[str] = set()
    for sink_dir in selected:
        manifest_path = sink_dir / "manifest.yaml"
        raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        manifest = cast(dict[str, Any], raw) if isinstance(raw, dict) else {}

        sink_target = manifest.get("sink_target")
        sink_target_dict = (
            cast(dict[str, Any], sink_target)
            if isinstance(sink_target, dict)
            else {}
        )
        databases = sink_target_dict.get("databases")
        databases_dict = (
            cast(dict[str, Any], databases)
            if isinstance(databases, dict)
            else {}
        )
        envs.update(str(key) for key in databases_dict)

    return sorted(envs)


def validate_env_for_sink(
    *,
    migrations_dir: Path,
    sink_name: str,
    env: str,
) -> None:
    """Validate that ``env`` exists in the selected sink manifest databases."""
    valid_envs = list_manifest_envs(
        migrations_dir=migrations_dir,
        sink_filter=sink_name,
    )
    if env in valid_envs:
        return

    if valid_envs:
        available = ", ".join(valid_envs)
        raise ValueError(
            f"Environment '{env}' is not configured for sink '{sink_name}'. "
            + f"Available environments from manifest: {available}",
        )

    raise ValueError(
        f"Sink '{sink_name}' manifest has no 'sink_target.databases' entries.",
    )
