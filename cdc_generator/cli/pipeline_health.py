#!/usr/bin/env python3
"""Health checks for pipeline runtime readiness.

Usage:
    cdc manage-pipelines health
    cdc manage-pipelines health --url http://localhost:4195/ready
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from cdc_generator.helpers.service_config import get_project_root

HTTP_STATUS_OK_MIN = 200
HTTP_STATUS_FAILURE_MIN = 500
URL_TIMEOUT_SECONDS = 2
DOCKER_BENTO_IMAGES = (
    "ghcr.io/warpstreamlabs/bento",
    "jeffail/bento:latest",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines health",
        description="Check Bento availability and pipeline output readiness",
    )
    parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="Optional runtime endpoint URL to probe (can be repeated)",
    )
    return parser.parse_args()


def _count_yaml(path: Path) -> int:
    if not path.exists():
        return 0
    return len(list(path.rglob("*.yaml")))


def _check_bento() -> tuple[bool, str]:
    bento_bin = shutil.which("bento")
    if bento_bin is None:
        docker_bin = shutil.which("docker")
        if docker_bin is None:
            return False, "bento binary not found in PATH (and docker unavailable)"

        image_override = os.environ.get("BENTO_DOCKER_IMAGE", "").strip()
        candidate_images = (
            [image_override, *DOCKER_BENTO_IMAGES]
            if image_override
            else list(DOCKER_BENTO_IMAGES)
        )

        failures: list[str] = []
        for image in candidate_images:
            image_failures: list[str] = []
            for invocation in (("--version",), ("bento", "--version")):
                docker_result = subprocess.run(
                    [docker_bin, "run", "--rm", image, *invocation],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if docker_result.returncode == 0:
                    version_line = docker_result.stdout.strip().splitlines()
                    version = version_line[0] if version_line else "unknown"
                    return True, f"{version} (via docker image {image})"

                error_line = docker_result.stderr.strip().splitlines()
                short_error = error_line[-1] if error_line else "unknown docker error"
                image_failures.append(f"{' '.join(invocation)}: {short_error}")

            failures.append(f"{image} [{'; '.join(image_failures)}]")

        return False, "bento not found in PATH and docker fallback failed: " + "; ".join(failures)

    result = subprocess.run(
        [bento_bin, "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False, "bento --version failed"

    version_line = result.stdout.strip().splitlines()
    version = version_line[0] if version_line else "unknown"
    return True, f"{version} ({bento_bin})"


def _check_url(url: str) -> tuple[bool, str]:
    try:
        with urlopen(url, timeout=URL_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", 200)
            if HTTP_STATUS_OK_MIN <= status < HTTP_STATUS_FAILURE_MIN:
                return True, f"HTTP {status}"
            return False, f"HTTP {status}"
    except URLError as exc:
        return False, str(exc)


def main() -> int:
    args = _parse_args()
    project_root = get_project_root()

    failures = 0
    print("🏥 Pipeline health checks")
    print("=" * 60)

    bento_ok, bento_msg = _check_bento()
    print(f"[{'OK' if bento_ok else 'FAIL'}] bento: {bento_msg}")
    if not bento_ok:
        failures += 1

    generated_root = project_root / "pipelines" / "generated"
    sources = generated_root / "sources"
    sinks = generated_root / "sinks"

    sources_count = _count_yaml(sources)
    sinks_count = _count_yaml(sinks)

    sources_ok = sources.exists() and sources_count > 0
    sinks_ok = sinks.exists() and sinks_count > 0

    print(f"[{'OK' if sources_ok else 'FAIL'}] sources: {sources_count} yaml files")
    print(f"[{'OK' if sinks_ok else 'FAIL'}] sinks:   {sinks_count} yaml files")

    if not sources_ok:
        failures += 1
    if not sinks_ok:
        failures += 1

    for url in args.url:
        ok, msg = _check_url(url)
        print(f"[{'OK' if ok else 'FAIL'}] endpoint {url}: {msg}")
        if not ok:
            failures += 1

    if failures:
        print("\n❌ Health check failed")
        return 1

    print("\n✅ Health check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
