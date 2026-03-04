#!/usr/bin/env python3
"""Detect drift between config/templates and generated pipeline files.

Usage:
    cdc manage-pipelines diff
"""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from cdc_generator.helpers.service_config import get_project_root


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines diff",
        description="Detect config drift in generated pipeline files",
    )
    return parser.parse_args()


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_yaml(root: Path) -> dict[str, str]:
    if not root.exists():
        return {}
    snapshot: dict[str, str] = {}
    for file_path in sorted(root.rglob("*.yaml")):
        rel = file_path.relative_to(root).as_posix()
        snapshot[rel] = _hash_file(file_path)
    return snapshot


def _run_generation(project_root: Path) -> tuple[int, str, str]:
    generator_script = Path(__file__).resolve().parents[1] / "core" / "pipeline_generator.py"
    result = subprocess.run(
        [sys.executable, str(generator_script), "--all"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def _run_with_loading(
    message: str,
    operation: Callable[[], tuple[int, str, str]],
) -> tuple[int, str, str]:
    """Run operation with interactive loading indicator when attached to a TTY."""
    if not sys.stdout.isatty():
        print(f"⏳ {message}...")
        return operation()

    frames = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
    stop_event = threading.Event()

    def _spinner() -> None:
        index = 0
        while not stop_event.is_set():
            frame = frames[index % len(frames)]
            print(f"\r{frame} {message}...", end="", flush=True)
            index += 1
            time.sleep(0.1)

    thread = threading.Thread(target=_spinner, daemon=True)
    thread.start()

    try:
        return operation()
    finally:
        stop_event.set()
        thread.join(timeout=0.3)
        print(f"\r✅ {message}...done")


def main() -> int:
    _parse_args()
    project_root = get_project_root()
    generated_root = project_root / "pipelines" / "generated"

    print("🔍 Running pipeline drift check")

    before = _snapshot_yaml(generated_root)
    code, stdout, stderr = _run_with_loading(
        "Generating pipelines for comparison",
        lambda: _run_generation(project_root),
    )

    if code != 0:
        print("❌ Failed to generate pipelines while calculating diff")
        if stdout.strip():
            print("\nstdout:\n" + stdout.strip())
        if stderr.strip():
            print("\nstderr:\n" + stderr.strip())
        return 2

    after = _snapshot_yaml(generated_root)

    before_keys = set(before)
    after_keys = set(after)

    added = sorted(after_keys - before_keys)
    removed = sorted(before_keys - after_keys)
    changed = sorted(
        key for key in (before_keys & after_keys)
        if before[key] != after[key]
    )

    if not added and not removed and not changed:
        print("✅ No pipeline drift detected")
        return 0

    print("⚠️ Pipeline drift detected")
    if added:
        print("\nAdded files:")
        for path in added:
            print(f"  + {path}")
    if changed:
        print("\nChanged files:")
        for path in changed:
            print(f"  ~ {path}")
    if removed:
        print("\nRemoved files:")
        for path in removed:
            print(f"  - {path}")

    print("\nRun `cdc manage-pipelines generate --all` to refresh all outputs.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
