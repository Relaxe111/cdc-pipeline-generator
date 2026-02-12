"""CLI tests for manage-service remove-service flow."""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc

pytestmark = pytest.mark.cli


def _create_project(root: Path, service: str = "proxy") -> None:
    """Create minimal project structure for remove-service tests."""
    (root / "docker-compose.yml").write_text(
        "services:\n"
        "  dev:\n"
        "    image: busybox\n"
    )

    services_dir = root / "services"
    services_dir.mkdir(exist_ok=True)
    (services_dir / f"{service}.yaml").write_text(
        f"{service}:\n"
        "  source:\n"
        "    tables:\n"
        "      public.users: {}\n"
    )

    (root / "source-groups.yaml").write_text(
        "asma:\n"
        "  pattern: db-shared\n"
        "  type: postgres\n"
        "  servers:\n"
        "    default:\n"
        "      host: ${POSTGRES_SOURCE_HOST}\n"
        "      port: ${POSTGRES_SOURCE_PORT}\n"
        "      user: ${POSTGRES_SOURCE_USER}\n"
        "      password: ${POSTGRES_SOURCE_PASSWORD}\n"
        "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
        "  sources:\n"
        f"    {service}:\n"
        "      schemas:\n"
        "        - public\n"
    )


class TestCliRemoveService:
    """CLI e2e: --remove-service."""

    def test_remove_existing_service_cleans_local_config(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)

        schemas_dir = isolated_project / "service-schemas" / "proxy"
        schemas_dir.mkdir(parents=True)
        (schemas_dir / "placeholder.yaml").write_text("x: 1\n")

        result = run_cdc(
            "manage-service", "--remove-service", "proxy",
        )
        assert result.returncode == 0

        assert not (isolated_project / "services" / "proxy.yaml").exists()

        source_groups = (isolated_project / "source-groups.yaml").read_text()
        assert "proxy:" not in source_groups

        assert not schemas_dir.exists()

    def test_remove_nonexistent_service_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--remove-service", "ghost",
        )
        assert result.returncode == 1
