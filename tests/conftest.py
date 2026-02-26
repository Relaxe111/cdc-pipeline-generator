"""Shared fixtures and helpers for manage-service test suite.

Provides a composable ``make_project_dir`` factory that replaces the
6+ inline ``project_dir`` fixtures duplicated across test files.

Also re-exports ``make_namespace`` from ``_namespace_defaults`` so
legacy callers keep working.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from tests._namespace_defaults import make_namespace  # noqa: F401  — re-export

# ---------------------------------------------------------------------------
# Composable project-dir factory
# ---------------------------------------------------------------------------

# Default YAML content blocks used by the factory.
_DEFAULT_SOURCE_GROUPS = (
    "asma:\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  sources:\n"
    "    proxy:\n"
    "      schemas:\n"
    "        - public\n"
    "      nonprod:\n"
    "        server: default\n"
    "        database: proxy_db\n"
)

_DEFAULT_SINK_GROUPS = (
    "sink_asma:\n  type: postgres\n  server: sink-pg\n"
)


def _make_project_dir(
    tmp_path: Path,
    *,
    source_groups: str | None = _DEFAULT_SOURCE_GROUPS,
    sink_groups: str | None = _DEFAULT_SINK_GROUPS,
    service_schemas: bool = True,
    patch_service_schemas: bool = False,
    patch_server_groups: bool = True,
) -> Iterator[Path]:
    """Create an isolated project directory with configurable layout.

    Args:
        tmp_path: pytest ``tmp_path`` fixture.
        source_groups: Content for ``source-groups.yaml``, or None to skip.
        sink_groups: Content for ``sink-groups.yaml``, or None to skip.
        service_schemas: Whether to create the ``service-schemas/`` dir.
        patch_service_schemas: Whether to patch ``SERVICE_SCHEMAS_DIR``.
        patch_server_groups: Whether to patch ``SERVER_GROUPS_FILE``.

    Yields:
        The project root ``Path``.
    """
    services_dir = tmp_path / "services"
    services_dir.mkdir()

    if source_groups:
        (tmp_path / "source-groups.yaml").write_text(source_groups)
    if sink_groups:
        (tmp_path / "sink-groups.yaml").write_text(sink_groups)

    schemas_dir = tmp_path / "service-schemas"
    if service_schemas:
        schemas_dir.mkdir(exist_ok=True)

    original_cwd = Path.cwd()
    os.chdir(tmp_path)

    patches: list[Any] = [
        patch(
            "cdc_generator.validators.manage_service.config.SERVICES_DIR",
            services_dir,
        ),
    ]
    if patch_server_groups and source_groups:
        patches.append(
            patch(
                "cdc_generator.validators.manage_server_group.config.SERVER_GROUPS_FILE",
                tmp_path / "source-groups.yaml",
            ),
        )
    if patch_service_schemas and service_schemas:
        patches.extend([
            patch(
                "cdc_generator.validators.manage_service.config.SERVICE_SCHEMAS_DIR",
                schemas_dir,
            ),
            patch(
                "cdc_generator.cli.service_handlers_sink_custom.SERVICE_SCHEMAS_DIR",
                schemas_dir,
            ),
        ])

    for p in patches:
        p.start()
    try:
        yield tmp_path
    finally:
        os.chdir(original_cwd)
        for p in patches:
            p.stop()


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Default shared project_dir: services/, source-groups, sink-groups.

    Patches SERVICES_DIR and SERVER_GROUPS_FILE.
    For tests that need SERVICE_SCHEMAS_DIR patched or custom source-groups
    content, use the per-file fixture or call ``_make_project_dir`` directly.
    """
    yield from _make_project_dir(
        tmp_path,
        patch_service_schemas=True,
        patch_server_groups=True,
    )
