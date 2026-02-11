"""Unit tests for inspect handler error paths.

Tests the validation / error branches of handle_inspect and
handle_inspect_sink that don't require a real database connection.
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_inspect import (
    _resolve_inspect_db_type,
    handle_inspect,
)
from cdc_generator.cli.service_handlers_inspect_sink import (
    handle_inspect_sink,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project for inspect tests."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n"
        "  pattern: db-shared\n"
        "  type: postgres\n"
        "  sources:\n"
        "    proxy:\n"
        "      schemas:\n"
        "        - public\n"
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_asma:\n  type: postgres\n  server: sink-pg\n"
    )
    sf = services_dir / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables: {}\n"
    )
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    with patch(
        "cdc_generator.validators.manage_service.config.SERVICES_DIR",
        services_dir,
    ), patch(
        "cdc_generator.validators.manage_server_group.config.SERVER_GROUPS_FILE",
        tmp_path / "source-groups.yaml",
    ):
        try:
            yield tmp_path
        finally:
            os.chdir(original_cwd)


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "proxy",
        "inspect": False,
        "inspect_sink": None,
        "schema": None,
        "all": False,
        "env": "nonprod",
        "save": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# _resolve_inspect_db_type
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveInspectDbType:
    """Tests for _resolve_inspect_db_type."""

    def test_resolves_postgres(
        self, project_dir: Path,
    ) -> None:
        """Finds postgres type from source-groups.yaml."""
        db_type, sg, _ = _resolve_inspect_db_type("proxy")
        assert db_type == "postgres"
        assert sg == "asma"

    def test_returns_none_unknown_service(
        self, project_dir: Path,
    ) -> None:
        """Returns None for unknown service."""
        db_type, _sg, _ = _resolve_inspect_db_type("nonexistent")
        assert db_type is None


# ═══════════════════════════════════════════════════════════════════════════
# handle_inspect error paths
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleInspectErrors:
    """Tests for handle_inspect error conditions."""

    def test_requires_all_or_schema(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when neither --all nor --schema provided."""
        args = _ns(inspect=True)
        result = handle_inspect(args)
        assert result == 1

    def test_disallowed_schema_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when requested schema not in allowed list."""
        args = _ns(inspect=True, schema="dbo")
        result = handle_inspect(args)
        assert result == 1

    def test_unknown_service_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 for unknown service (no DB type)."""
        args = _ns(service="nonexistent", inspect=True, all=True)
        result = handle_inspect(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_inspect_sink error paths
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleInspectSinkErrors:
    """Tests for handle_inspect_sink error conditions."""

    def test_requires_all_or_schema(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when neither --all nor --schema provided."""
        args = _ns(inspect_sink="sink_asma.chat")
        result = handle_inspect_sink(args)
        assert result == 1

    def test_invalid_sink_key_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when sink key not found."""
        args = _ns(
            inspect_sink="sink_asma.nonexistent",
            all=True,
        )
        result = handle_inspect_sink(args)
        assert result == 1
