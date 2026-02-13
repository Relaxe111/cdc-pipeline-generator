"""Tests for autocompletion helper functions."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace with source-groups.yaml and services/."""
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)

        # Create source-groups.yaml with multiple services
        source_groups = workspace / "source-groups.yaml"
        source_groups.write_text(
            """
asma:
  pattern: db-shared
  source_type: postgres
  sources:
    directory:
      database_name: asma_directory
    chat:
      database_name: asma_chat
    calendar:
      database_name: asma_calendar
    notification:
      database_name: asma_notification
"""
        )

        # Create services directory
        services_dir = workspace / "services"
        services_dir.mkdir()

        # Create some existing service files (directory and chat exist)
        (services_dir / "directory.yaml").write_text("# directory service\n")
        (services_dir / "chat.yaml").write_text("# chat service\n")

        yield workspace


class TestListExistingServices:
    """Tests for list_existing_services()."""

    def test_returns_created_services(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns services that have YAML files."""
        monkeypatch.chdir(temp_workspace)

        from cdc_generator.helpers.autocompletions.services import (
            list_existing_services,
        )

        result = list_existing_services()
        assert result == ["chat", "directory"]

    def test_empty_when_no_services_dir(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns empty list when services/ directory doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.chdir(tmpdir)

            from cdc_generator.helpers.autocompletions.services import (
                list_existing_services,
            )

            result = list_existing_services()
            assert result == []


class TestListAvailableServicesFromServerGroup:
    """Tests for list_available_services_from_server_group()."""

    def test_filters_out_existing_services(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only returns services from source-groups.yaml that don't have YAML files yet."""
        monkeypatch.chdir(temp_workspace)

        from cdc_generator.helpers.autocompletions.services import (
            list_available_services_from_server_group,
        )

        result = list_available_services_from_server_group()

        # Should return calendar and notification (directory and chat already exist)
        assert result == ["calendar", "notification"]
        assert "directory" not in result
        assert "chat" not in result

    def test_returns_all_when_no_services_created(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns all services when services/ directory is empty."""
        monkeypatch.chdir(temp_workspace)

        # Remove existing service files
        services_dir = temp_workspace / "services"
        for yaml_file in services_dir.glob("*.yaml"):
            yaml_file.unlink()

        from cdc_generator.helpers.autocompletions.services import (
            list_available_services_from_server_group,
        )

        result = list_available_services_from_server_group()

        # Should return all 4 services
        assert result == ["calendar", "chat", "directory", "notification"]

    def test_returns_empty_when_all_services_created(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns empty list when all services have been created."""
        monkeypatch.chdir(temp_workspace)

        # Create remaining service files
        services_dir = temp_workspace / "services"
        (services_dir / "calendar.yaml").write_text("# calendar service\n")
        (services_dir / "notification.yaml").write_text("# notification service\n")

        from cdc_generator.helpers.autocompletions.services import (
            list_available_services_from_server_group,
        )

        result = list_available_services_from_server_group()

        # Should return empty - all services created
        assert result == []

    def test_empty_when_no_source_groups_file(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns empty list when source-groups.yaml doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            monkeypatch.chdir(tmpdir)

            from cdc_generator.helpers.autocompletions.services import (
                list_available_services_from_server_group,
            )

            result = list_available_services_from_server_group()
            assert result == []

    def test_ignores_parent_services_directory_when_local_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Uses only the services/ folder next to source-groups.yaml for filtering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            # Parent workspace with unrelated services files
            parent_services = root / "services"
            parent_services.mkdir()
            (parent_services / "chat.yaml").write_text("# unrelated parent chat\n")
            (parent_services / "directory.yaml").write_text(
                "# unrelated parent directory\n"
            )

            # Child project contains source-groups.yaml but no local services/
            child = root / "project"
            child.mkdir()
            (child / "source-groups.yaml").write_text(
                """
asma:
  pattern: db-shared
  source_type: postgres
  sources:
    directory:
      database_name: asma_directory
    chat:
      database_name: asma_chat
    proxy:
      database_name: asma_proxy
"""
            )

            monkeypatch.chdir(child)

            from cdc_generator.helpers.autocompletions.services import (
                list_available_services_from_server_group,
            )

            result = list_available_services_from_server_group()

            # Local project has no created services, so all sources are available
            assert result == ["chat", "directory", "proxy"]

    def test_aggregates_sources_from_multiple_server_groups(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns available services from all server groups in source-groups.yaml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            (workspace / "source-groups.yaml").write_text(
                """
alpha:
  pattern: db-shared
  source_type: postgres
  sources:
    chat:
      database_name: chat_dev
    directory:
      database_name: directory_dev

beta:
  pattern: db-shared
  source_type: postgres
  sources:
    proxy:
      database_name: proxy_dev
    tracing:
      database_name: tracing_dev
"""
            )

            services_dir = workspace / "services"
            services_dir.mkdir()
            (services_dir / "chat.yaml").write_text("# created\n")

            monkeypatch.chdir(workspace)

            from cdc_generator.helpers.autocompletions.services import (
                list_available_services_from_server_group,
            )

            result = list_available_services_from_server_group()

            assert result == ["directory", "proxy", "tracing"]


class TestCreateServiceCompletion:
    """Tests for --create-service flag completion."""

    @staticmethod
    def _complete(partial_cmd: str) -> list[str]:
        """Return completion values for *partial_cmd* via Click API."""
        import shlex

        from click.shell_completion import ShellComplete

        from cdc_generator.cli.commands import _click_cli

        cli = _click_cli
        parts = shlex.split(partial_cmd)
        if parts and parts[0] == "cdc":
            parts = parts[1:]
        incomplete = parts.pop() if parts else ""
        comp = ShellComplete(cli, {}, "cdc", "_CDC_COMPLETE")
        return [c.value for c in comp.get_completions(parts, incomplete)]

    def test_create_service_shows_uncreated_only(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--create-service completion only shows services not yet created."""
        monkeypatch.chdir(temp_workspace)

        # Trigger value completion by putting incomplete value after the option
        completions = self._complete("cdc manage-service --create-service c")

        # Should show calendar (starts with 'c'), not chat (already exists)
        assert "calendar" in completions
        assert "chat" not in completions

    def test_create_service_shows_notification_uncreated(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--create-service shows notification which isn't created yet."""
        monkeypatch.chdir(temp_workspace)

        # Check for notification (starts with 'n')
        completions = self._complete("cdc manage-service --create-service n")

        assert "notification" in completions

    def test_create_service_filters_existing_directory(
        self, temp_workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--create-service filters out directory which already exists."""
        monkeypatch.chdir(temp_workspace)

        # Try 'd' - directory exists but shouldn't show
        completions = self._complete("cdc manage-service --create-service d")

        assert "directory" not in completions
