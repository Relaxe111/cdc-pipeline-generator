"""Unit tests for selected ``manage-sink-groups`` handler functions."""

from argparse import Namespace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from cdc_generator.cli.sink_group import (
    _build_server_config,
    _check_server_references,
    _load_sink_group_for_server_op,
    _validate_single_sink_group,
    handle_add_server_command,
    handle_remove_server_command,
    handle_remove_sink_group_command,
)


@pytest.fixture
def standalone_sink_group() -> dict[str, Any]:
    """Standalone sink group fixture."""
    return {
        "source_group": "asma",
        "pattern": "db-shared",
        "type": "postgres",
        "servers": {
            "default": {
                "host": "localhost",
                "port": "5432",
                "user": "postgres",
                "password": "secret",
            },
        },
        "sources": {},
    }


@pytest.fixture
def inherited_sink_group() -> dict[str, Any]:
    """Inherited sink group fixture."""
    return {
        "inherits": True,
        "servers": {
            "default": {
                "source_ref": "default",
            },
        },
        "inherited_sources": ["directory"],
    }


def _ns(**kwargs: Any) -> Namespace:
    defaults: dict[str, Any] = {
        "sink_group": None,
        "add_server": None,
        "remove_server": None,
        "host": None,
        "port": None,
        "user": None,
        "password": None,
        "extraction_patterns": None,
        "remove": None,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


class TestBuildServerConfig:
    """Tests for ``_build_server_config``."""

    def test_build_server_config_with_explicit_values(
        self, standalone_sink_group: dict[str, Any],
    ) -> None:
        args = _ns(
            sink_group="sink_analytics",
            add_server="reporting",
            host="reporting.local",
            port="5432",
            user="report_user",
            password="report_secret",
        )

        config = _build_server_config(args, standalone_sink_group)

        assert config["host"] == "reporting.local"
        assert config["port"] == "5432"
        assert config["user"] == "report_user"
        assert config["password"] == "report_secret"

    def test_build_server_config_uses_env_placeholders(
        self, standalone_sink_group: dict[str, Any],
    ) -> None:
        args = _ns(sink_group="sink_asma", add_server="nonprod")

        config = _build_server_config(args, standalone_sink_group)

        assert str(config["host"]).startswith("${POSTGRES_SINK_HOST_ASMA_NONPROD")
        assert str(config["port"]).startswith("${POSTGRES_SINK_PORT_ASMA_NONPROD")


class TestCheckServerReferences:
    """Tests for ``_check_server_references``."""

    def test_detects_source_reference(self) -> None:
        sink_group = {
            "servers": {"default": {}, "reporting": {}},
            "sources": {
                "directory": {
                    "nonprod": {
                        "server": "reporting",
                        "database": "dir_db",
                    },
                },
            },
        }

        refs = _check_server_references("reporting", sink_group)

        assert refs == ["directory.nonprod"]

    def test_no_references_returns_empty(self) -> None:
        sink_group = {
            "servers": {"default": {}},
            "sources": {},
        }

        refs = _check_server_references("default", sink_group)

        assert refs == []


class TestLoadSinkGroupForServerOp:
    """Tests for ``_load_sink_group_for_server_op``."""

    @patch("cdc_generator.cli.sink_group.load_sink_groups")
    def test_missing_sink_group_returns_error(self, mock_load: Mock) -> None:
        mock_load.return_value = {"sink_other": {"servers": {}}}

        result = _load_sink_group_for_server_op(
            _ns(sink_group="sink_analytics"), "--add-server",
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group.load_sink_groups")
    @patch("cdc_generator.cli.sink_group.get_sink_file_path")
    def test_success_returns_tuple(self, mock_path: Mock, mock_load: Mock) -> None:
        sink_file = Path("/tmp/sink-groups.yaml")
        sink_group = {"servers": {"default": {}}}
        mock_path.return_value = sink_file
        mock_load.return_value = {"sink_analytics": sink_group}

        result = _load_sink_group_for_server_op(
            _ns(sink_group="sink_analytics"), "--add-server",
        )

        assert isinstance(result, tuple)
        sink_groups, loaded_group, group_name, loaded_file = result
        assert group_name == "sink_analytics"
        assert loaded_group == sink_group
        assert sink_groups["sink_analytics"] == sink_group
        assert loaded_file == sink_file


class TestHandleAddServerCommand:
    """Tests for ``handle_add_server_command``."""

    def test_requires_sink_group_and_server_name(self) -> None:
        result = handle_add_server_command(_ns(sink_group=None, add_server=None))
        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_fails_when_sink_group_load_fails(self, mock_load: Mock) -> None:
        mock_load.return_value = 1

        result = handle_add_server_command(
            _ns(sink_group="sink_analytics", add_server="reporting"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_inherited_group_cannot_add_server(
        self,
        mock_load: Mock,
        inherited_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = (
            {"sink_asma": inherited_sink_group},
            inherited_sink_group,
            "sink_asma",
            Path("/tmp/sink-groups.yaml"),
        )

        result = handle_add_server_command(
            _ns(sink_group="sink_asma", add_server="reporting"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_duplicate_server_returns_error(
        self,
        mock_load: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = (
            {"sink_analytics": standalone_sink_group},
            standalone_sink_group,
            "sink_analytics",
            Path("/tmp/sink-groups.yaml"),
        )

        result = handle_add_server_command(
            _ns(sink_group="sink_analytics", add_server="default"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group.print_env_update_summary")
    @patch("cdc_generator.cli.sink_group.append_env_vars_to_dotenv")
    @patch("cdc_generator.cli.sink_group.save_sink_groups")
    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_add_server_success(
        self,
        mock_load: Mock,
        mock_save: Mock,
        mock_append_env: Mock,
        mock_env_summary: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = (
            {"sink_analytics": standalone_sink_group},
            standalone_sink_group,
            "sink_analytics",
            Path("/tmp/sink-groups.yaml"),
        )
        mock_append_env.return_value = 4

        result = handle_add_server_command(
            _ns(
                sink_group="sink_analytics",
                add_server="reporting",
                host="reporting.local",
                port="5432",
                user="postgres",
                password="secret",
            ),
        )

        assert result == 0
        assert mock_save.called
        assert mock_append_env.called
        assert mock_env_summary.called


class TestHandleRemoveServerCommand:
    """Tests for ``handle_remove_server_command``."""

    def test_requires_sink_group_and_server_name(self) -> None:
        result = handle_remove_server_command(
            _ns(sink_group=None, remove_server=None),
        )
        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_server_not_found_returns_error(
        self,
        mock_load: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = (
            {"sink_analytics": standalone_sink_group},
            standalone_sink_group,
            "sink_analytics",
            Path("/tmp/sink-groups.yaml"),
        )

        result = handle_remove_server_command(
            _ns(sink_group="sink_analytics", remove_server="ghost"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_inherited_group_cannot_remove_server(
        self,
        mock_load: Mock,
        inherited_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = (
            {"sink_asma": inherited_sink_group},
            inherited_sink_group,
            "sink_asma",
            Path("/tmp/sink-groups.yaml"),
        )

        result = handle_remove_server_command(
            _ns(sink_group="sink_asma", remove_server="default"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_remove_server_in_use_fails(
        self,
        mock_load: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        standalone_sink_group["servers"]["reporting"] = {
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "secret",
        }
        standalone_sink_group["sources"] = {
            "directory": {
                "nonprod": {
                    "server": "reporting",
                    "database": "directory_db",
                },
            },
        }

        mock_load.return_value = (
            {"sink_analytics": standalone_sink_group},
            standalone_sink_group,
            "sink_analytics",
            Path("/tmp/sink-groups.yaml"),
        )

        result = handle_remove_server_command(
            _ns(sink_group="sink_analytics", remove_server="reporting"),
        )

        assert result == 1

    @patch("cdc_generator.cli.sink_group.print_env_removal_summary")
    @patch("cdc_generator.cli.sink_group.remove_env_vars_from_dotenv")
    @patch("cdc_generator.cli.sink_group.save_sink_groups")
    @patch("cdc_generator.cli.sink_group._load_sink_group_for_server_op")
    def test_remove_server_success(
        self,
        mock_load: Mock,
        mock_save: Mock,
        mock_remove_env: Mock,
        mock_env_summary: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        standalone_sink_group["servers"]["reporting"] = {
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "secret",
        }
        mock_load.return_value = (
            {"sink_analytics": standalone_sink_group},
            standalone_sink_group,
            "sink_analytics",
            Path("/tmp/sink-groups.yaml"),
        )
        mock_remove_env.return_value = 4

        result = handle_remove_server_command(
            _ns(sink_group="sink_analytics", remove_server="reporting"),
        )

        assert result == 0
        assert mock_save.called
        assert mock_remove_env.called
        assert mock_env_summary.called


class TestHandleRemoveSinkGroupCommand:
    """Tests for ``handle_remove_sink_group_command``."""

    def test_remove_requires_name(self) -> None:
        result = handle_remove_sink_group_command(_ns(remove=None))
        assert result == 1

    @patch("cdc_generator.cli.sink_group.load_sink_groups")
    def test_remove_inherited_group_fails(
        self, mock_load: Mock, inherited_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = {"sink_asma": inherited_sink_group}

        result = handle_remove_sink_group_command(_ns(remove="sink_asma"))

        assert result == 1

    @patch("cdc_generator.cli.sink_group.save_sink_groups")
    @patch("cdc_generator.cli.sink_group.load_sink_groups")
    def test_remove_standalone_group_success(
        self,
        mock_load: Mock,
        mock_save: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_load.return_value = {"sink_analytics": standalone_sink_group}

        result = handle_remove_sink_group_command(_ns(remove="sink_analytics"))

        assert result == 0
        assert mock_save.called


class TestValidateSingleSinkGroup:
    """Tests for ``_validate_single_sink_group`` helper."""

    @patch("cdc_generator.cli.sink_group._check_readiness_and_warnings")
    @patch("cdc_generator.cli.sink_group._check_structure_and_resolution")
    def test_invalid_structure_skips_warnings(
        self,
        mock_structure: Mock,
        mock_warnings: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_structure.return_value = False

        valid, warned = _validate_single_sink_group(
            "sink_analytics",
            standalone_sink_group,
            {"sink_analytics": standalone_sink_group},
            {"asma": {"servers": {}, "sources": {}}},
        )

        assert valid is False
        assert warned is False
        assert not mock_warnings.called

    @patch("cdc_generator.cli.sink_group._check_readiness_and_warnings")
    @patch("cdc_generator.cli.sink_group._check_structure_and_resolution")
    def test_valid_structure_runs_warnings(
        self,
        mock_structure: Mock,
        mock_warnings: Mock,
        standalone_sink_group: dict[str, Any],
    ) -> None:
        mock_structure.return_value = True
        mock_warnings.return_value = True

        valid, warned = _validate_single_sink_group(
            "sink_analytics",
            standalone_sink_group,
            {"sink_analytics": standalone_sink_group},
            {"asma": {"servers": {}, "sources": {}}},
        )

        assert valid is True
        assert warned is True
        assert mock_warnings.called
