#!/usr/bin/env python3
"""Find and optionally remove orphaned generated pipeline files.

Usage:
    cdc manage-pipelines prune
    cdc manage-pipelines prune --confirm
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.service_config import (
    get_all_customers,
    get_project_root,
    load_customer_config,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines prune",
        description="Detect and remove orphaned generated pipeline files",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Delete orphaned files (default is dry-run)",
    )
    return parser.parse_args()


def _expected_files(project_root: Path) -> set[str]:
    expected: set[str] = set()
    environments: set[str] = set()

    for customer in get_all_customers():
        config: dict[str, Any] = load_customer_config(customer)
        envs_raw = config.get("environments", {})
        envs = cast(dict[str, Any], envs_raw) if isinstance(envs_raw, dict) else {}

        for env_name in envs:
            env = str(env_name)
            environments.add(env)
            expected.add(f"sources/{env}/{customer}/source-pipeline.yaml")

    for env in environments:
        expected.add(f"sinks/{env}/sink-pipeline.yaml")

    _ = project_root
    return expected


def _existing_files(generated_root: Path) -> set[str]:
    if not generated_root.exists():
        return set()

    existing: set[str] = set()
    for file_path in generated_root.rglob("*.yaml"):
        existing.add(file_path.relative_to(generated_root).as_posix())
    return existing


def _cleanup_empty_dirs(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        return

    for root, dirs, files in os.walk(path, topdown=False):
        if files:
            continue
        for _ in dirs:
            break
        try:
            Path(root).rmdir()
        except OSError:
            continue


def main() -> int:
    args = _parse_args()
    project_root = get_project_root()
    generated_root = project_root / "pipelines" / "generated"

    expected = _expected_files(project_root)
    existing = _existing_files(generated_root)
    orphans = sorted(existing - expected)

    if not orphans:
        print("✅ No orphaned pipeline files found")
        return 0

    print("⚠️ Orphaned generated files:")
    for rel_path in orphans:
        print(f"  - {rel_path}")

    if not args.confirm:
        print("\nDry-run only. Re-run with --confirm to delete these files.")
        return 1

    removed = 0
    for rel_path in orphans:
        target = generated_root / rel_path
        if target.exists():
            target.unlink()
            removed += 1

    _cleanup_empty_dirs(generated_root / "sources")
    _cleanup_empty_dirs(generated_root / "sinks")

    print(f"\n✅ Removed {removed} orphaned files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
