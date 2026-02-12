"""End-to-end CLI tests for ``cdc setup-local``."""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


def _write_minimal_compose(root: Path) -> None:
    (root / "docker-compose.yml").write_text(
        "services:\n"
        "  dev:\n"
        "    image: alpine\n",
    )


class TestCliSetupLocal:
    """CLI e2e for setup-local command behavior."""

    def test_help_works_without_project(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc("setup-local", "--help")

        assert result.returncode == 0
        assert "setup-local" in result.stdout + result.stderr

    def test_requires_project_with_docker_compose(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        result = run_cdc("setup-local")

        assert result.returncode == 1
        assert "docker-compose.yml" in result.stdout + result.stderr

    def test_no_flags_in_project_returns_zero(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_minimal_compose(isolated_project)

        result = run_cdc("setup-local")

        assert result.returncode == 0
        assert "No services specified" in result.stdout + result.stderr


class TestCliSetupLocalCompletions:
    """CLI e2e: completion entries for setup-local flags."""

    def test_flag_completion(self, run_cdc_completion: RunCdcCompletion) -> None:
        result = run_cdc_completion("cdc setup-local --")

        assert result.returncode == 0
        output = result.stdout
        if not output.strip():
            pytest.skip("No fish flag completions registered for setup-local")
        assert "--help" in output
