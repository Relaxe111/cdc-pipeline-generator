#!/usr/bin/env python3
"""List available customers and generated pipeline artifacts.

Usage:
    cdc manage-pipelines list
    cdc manage-pipelines list --status
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

from cdc_generator.helpers.service_config import get_all_customers, get_project_root


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines list",
        description="List available customers and generated pipeline artifacts",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show readiness/freshness status summary",
    )
    return parser.parse_args()


def _count_yaml_files(path: Path) -> int:
    if not path.exists():
        return 0
    return len(list(path.rglob("*.yaml")))


def _latest_mtime(paths: list[Path]) -> datetime | None:
    latest: datetime | None = None
    for path in paths:
        if not path.exists():
            continue
        candidates = [path] if path.is_file() else list(path.rglob("*.yaml"))
        for candidate in candidates:
            ts = datetime.fromtimestamp(candidate.stat().st_mtime, tz=UTC)
            if latest is None or ts > latest:
                latest = ts
    return latest


def _render_status(project_root: Path, source_count: int, sink_count: int) -> None:
    templates_dir = project_root / "pipelines" / "templates"
    services_dir = project_root / "services"
    generated_dir = project_root / "pipelines" / "generated"

    config_time = _latest_mtime([templates_dir, services_dir, project_root / "source-groups.yaml"])
    generated_time = _latest_mtime([generated_dir])
    generated_ok = source_count > 0 and sink_count > 0
    bento_ok = shutil.which("bento") is not None

    freshness = "unknown"
    if config_time and generated_time:
        freshness = "up-to-date" if generated_time >= config_time else "stale"
    elif not generated_ok:
        freshness = "missing"

    print("\n🩺 Status")
    print("-" * 60)
    print(f"Bento binary: {'ready' if bento_ok else 'missing'}")
    print(f"Generated outputs: {'ready' if generated_ok else 'incomplete'}")
    print(f"Freshness: {freshness}")


def main() -> int:
    """Print customer list and generated pipeline summary."""
    args = _parse_args()
    project_root = get_project_root()
    customers = sorted(get_all_customers())

    print("📋 Pipeline Inventory")
    print("=" * 60)
    print(f"Project: {project_root}")
    print(f"Customers: {len(customers)}")

    if customers:
        for customer in customers:
            print(f"  - {customer}")
    else:
        print("  ⚠️  No customers discovered in services/*.yaml")

    generated_root = project_root / "pipelines" / "generated"
    source_dir = generated_root / "sources"
    sink_dir = generated_root / "sinks"
    source_count = _count_yaml_files(source_dir)
    sink_count = _count_yaml_files(sink_dir)

    print("\n📦 Generated Artifacts")
    print("-" * 60)
    print(f"Root: {generated_root}")
    print(f"Source pipeline files: {source_count}")
    print(f"Sink pipeline files:   {sink_count}")

    if args.status:
        _render_status(project_root, source_count, sink_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
