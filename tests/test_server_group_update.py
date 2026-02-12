"""Tests for manage-source-groups --update command handler.

This module tests the handle_update function which performs database inspection
and updates the server group configuration. Tests focus on error paths with
mocked database dependencies.
"""

from argparse import Namespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cdc_generator.validators.manage_server_group.db_inspector import (
    MissingEnvironmentVariableError,
    PostgresConnectionError,
)
from cdc_generator.validators.manage_server_group.handlers_update import (
    handle_update,
)


def _ns(**kwargs: Any) -> Namespace:
    """Create Namespace with defaults for update command."""
    defaults = {
        'update': None,
        'all': False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


@pytest.fixture
def mock_server_group() -> dict[str, Any]:
    """Server group config with multiple servers."""
    return {
        'name': 'testgroup',
        'type': 'postgres',
        'pattern': 'db-shared',
        'database_exclude_patterns': ['test_*'],
        'schema_exclude_patterns': ['temp_*'],
        'servers': {
            'default': {
                'host': 'localhost',
                'port': 5432,
                'user': 'test',
                'password': 'secret',
            },
            'secondary': {
                'host': 'backup.local',
                'port': 5433,
                'user': 'test',
                'password': 'secret',
            }
        },
        'sources': {}
    }


@pytest.fixture
def mock_databases() -> list[dict[str, Any]]:
    """Mock database list from inspection."""
    return [
        {'name': 'db1', 'server_name': 'default'},
        {'name': 'db2', 'server_name': 'default'},
    ]


class TestHandleUpdateFileErrors:
    """Test handle_update with config file errors."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    def test_config_file_not_found(self, mock_load: MagicMock) -> None:
        """Test error when source-groups.yaml is missing."""
        mock_load.side_effect = FileNotFoundError()

        result = handle_update(_ns())

        assert result == 1
        mock_load.assert_called_once()

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    def test_no_server_group_found(
        self,
        mock_get: MagicMock,
        mock_load: MagicMock
    ) -> None:
        """Test error when no server group exists in config."""
        mock_load.return_value = {}
        mock_get.return_value = None

        result = handle_update(_ns())

        assert result == 1
        mock_load.assert_called_once()
        mock_get.assert_called_once()


class TestHandleUpdateValidation:
    """Test handle_update validation errors."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    def test_missing_type_field(
        self,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock
    ) -> None:
        """Test error when server group has no 'type' field."""
        config = {'name': 'testgroup', 'servers': {'default': {}}}
        mock_load.return_value = {'testgroup': config}
        mock_get.return_value = config

        result = handle_update(_ns())

        assert result == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    def test_no_servers_configured(
        self,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock
    ) -> None:
        """Test error when server group has no servers section."""
        config = {'name': 'testgroup', 'type': 'postgres'}
        mock_load.return_value = {'testgroup': config}
        mock_get.return_value = config

        result = handle_update(_ns())

        assert result == 1


class TestHandleUpdateServerSelection:
    """Test handle_update server selection logic."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    def test_server_not_found(
        self,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test error when specified server doesn't exist."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group

        result = handle_update(_ns(update='nonexistent'))

        assert result == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    @patch('cdc_generator.validators.manage_server_group.handlers_update._apply_updates')
    def test_update_default_server_only(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]]
    ) -> None:
        """Test updating only default server when no --all flag."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        # Should call list_postgres_databases only once (default server)
        assert mock_list_pg.call_count == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    @patch('cdc_generator.validators.manage_server_group.handlers_update._apply_updates')
    def test_update_all_servers(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]]
    ) -> None:
        """Test updating all servers with --all flag."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns(all=True))

        assert result == 0
        # Should call list_postgres_databases twice (all servers)
        assert mock_list_pg.call_count == 2


class TestHandleUpdateDatabaseInspection:
    """Test handle_update database inspection logic."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_mssql_databases')
    @patch('cdc_generator.validators.manage_server_group.handlers_update._apply_updates')
    def test_mssql_database_inspection(
        self,
        mock_apply: MagicMock,
        mock_list_mssql: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_databases: list[dict[str, Any]]
    ) -> None:
        """Test MSSQL database inspection."""
        config = {
            'name': 'testgroup',
            'type': 'mssql',
            'servers': {'default': {}},
            'sources': {}
        }
        mock_load.return_value = {'testgroup': config}
        mock_get.return_value = config
        mock_list_mssql.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        mock_list_mssql.assert_called_once()

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    def test_database_scan_failure(
        self,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test error when database scan fails (returns None)."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        # _inspect_server_databases returns None on scan failure
        mock_list_pg.side_effect = Exception("Connection failed")

        result = handle_update(_ns())

        assert result == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    def test_no_databases_found(
        self,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test success with warning when no databases found."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = []

        result = handle_update(_ns())

        # Returns 0 (success) but with warning message
        assert result == 0


class TestHandleUpdateExceptionHandling:
    """Test handle_update exception handling."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    def test_missing_environment_variable_error(
        self,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of missing environment variable."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.side_effect = MissingEnvironmentVariableError("POSTGRES_HOST")

        result = handle_update(_ns())

        assert result == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    def test_postgres_connection_error(
        self,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of PostgreSQL connection error."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        error = PostgresConnectionError("Connection refused", "localhost", 5432)
        mock_list_pg.side_effect = error

        result = handle_update(_ns())

        assert result == 1

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    def test_generic_exception(
        self,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of generic exceptions."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.side_effect = ValueError("Unexpected error")

        result = handle_update(_ns())

        assert result == 1


class TestHandleUpdateSuccess:
    """Test successful handle_update scenarios."""

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    @patch('cdc_generator.validators.manage_server_group.handlers_update._apply_updates')
    def test_successful_update(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]]
    ) -> None:
        """Test successful database update."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        mock_apply.assert_called_once()

    @patch('cdc_generator.validators.manage_server_group.handlers_update.load_server_groups')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure')
    @patch('cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases')
    @patch('cdc_generator.validators.manage_server_group.handlers_update._apply_updates')
    def test_apply_updates_failure(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]]
    ) -> None:
        """Test when _apply_updates fails."""
        mock_load.return_value = {'testgroup': mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = False

        result = handle_update(_ns())

        assert result == 1
