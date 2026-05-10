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
    extract_identifiers,
    get_postgres_connection,
    list_mssql_databases,
)
from cdc_generator.validators.manage_server_group.handlers_update import (
    _merge_with_existing_sources,
    _apply_updates,
    handle_update,
)


def _ns(**kwargs: Any) -> Namespace:
    """Create Namespace with defaults for update command."""
    defaults = {
        "update": None,
        "all": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


@pytest.fixture
def mock_server_group() -> dict[str, Any]:
    """Server group config with multiple servers."""
    return {
        "name": "testgroup",
        "type": "postgres",
        "pattern": "db-shared",
        "database_exclude_patterns": ["test_*"],
        "schema_exclude_patterns": ["temp_*"],
        "servers": {
            "default": {
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "secret",
            },
            "secondary": {
                "host": "backup.local",
                "port": 5433,
                "user": "test",
                "password": "secret",
            },
        },
        "sources": {},
    }


@pytest.fixture
def mock_databases() -> list[dict[str, Any]]:
    """Mock database list from inspection."""
    return [
        {"name": "db1", "server_name": "default"},
        {"name": "db2", "server_name": "default"},
    ]


class TestHandleUpdateFileErrors:
    """Test handle_update with config file errors."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    def test_config_file_not_found(self, mock_load: MagicMock) -> None:
        """Test error when source-groups.yaml is missing."""
        mock_load.side_effect = FileNotFoundError()

        result = handle_update(_ns())

        assert result == 1
        mock_load.assert_called_once()

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    def test_no_server_group_found(self, mock_get: MagicMock, mock_load: MagicMock) -> None:
        """Test error when no server group exists in config."""
        mock_load.return_value = {}
        mock_get.return_value = None

        result = handle_update(_ns())

        assert result == 1
        mock_load.assert_called_once()
        mock_get.assert_called_once()


class TestHandleUpdateValidation:
    """Test handle_update validation errors."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    def test_missing_type_field(self, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock) -> None:
        """Test error when server group has no 'type' field."""
        config = {"name": "testgroup", "servers": {"default": {}}}
        mock_load.return_value = {"testgroup": config}
        mock_get.return_value = config

        result = handle_update(_ns())

        assert result == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    def test_no_servers_configured(self, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock) -> None:
        """Test error when server group has no servers section."""
        config = {"name": "testgroup", "type": "postgres"}
        mock_load.return_value = {"testgroup": config}
        mock_get.return_value = config

        result = handle_update(_ns())

        assert result == 1


class TestHandleUpdateServerSelection:
    """Test handle_update server selection logic."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    def test_server_not_found(self, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]) -> None:
        """Test error when specified server doesn't exist."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group

        result = handle_update(_ns(update="nonexistent"))

        assert result == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    @patch("cdc_generator.validators.manage_server_group.handlers_update._apply_updates")
    def test_update_default_server_only(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]],
    ) -> None:
        """Test updating only default server when no --all flag."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        # Should call list_postgres_databases only once (default server)
        assert mock_list_pg.call_count == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    @patch("cdc_generator.validators.manage_server_group.handlers_update._apply_updates")
    def test_update_all_servers(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]],
    ) -> None:
        """Test updating all servers with --all flag."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns(all=True))

        assert result == 0
        # Should call list_postgres_databases twice (all servers)
        assert mock_list_pg.call_count == 2


class TestHandleUpdateDatabaseInspection:
    """Test handle_update database inspection logic."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_mssql_databases")
    @patch("cdc_generator.validators.manage_server_group.handlers_update._apply_updates")
    def test_mssql_database_inspection(
        self,
        mock_apply: MagicMock,
        mock_list_mssql: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_databases: list[dict[str, Any]],
    ) -> None:
        """Test MSSQL database inspection."""
        config = {"name": "testgroup", "type": "mssql", "servers": {"default": {}}, "sources": {}}
        mock_load.return_value = {"testgroup": config}
        mock_get.return_value = config
        mock_list_mssql.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        mock_list_mssql.assert_called_once()

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    def test_database_scan_failure(
        self, mock_list_pg: MagicMock, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]
    ) -> None:
        """Test error when database scan fails (returns None)."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        # _inspect_server_databases returns None on scan failure
        mock_list_pg.side_effect = Exception("Connection failed")

        result = handle_update(_ns())

        assert result == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    def test_no_databases_found(
        self, mock_list_pg: MagicMock, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]
    ) -> None:
        """Test success with warning when no databases found."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = []

        result = handle_update(_ns())

        # Returns 0 (success) but with warning message
        assert result == 0


class TestHandleUpdateExceptionHandling:
    """Test handle_update exception handling."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    def test_missing_environment_variable_error(
        self, mock_list_pg: MagicMock, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of missing environment variable."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.side_effect = MissingEnvironmentVariableError("POSTGRES_HOST")

        result = handle_update(_ns())

        assert result == 1


class TestDbPerTenantSourceNameOverrides:
    """Tests for db-per-tenant source_name_map behavior."""

    def test_extract_identifiers_prefers_source_name_map(self) -> None:
        """A database override should win before extraction_pattern is evaluated."""
        server_group = {
            "name": "fdw",
            "pattern": "db-per-tenant",
            "extraction_pattern": r"^AdOpus(?P<customer>.+)$",
            "source_name_map": {"AdOpusTest": "avansas"},
            "servers": {"default": {}},
            "sources": {},
        }

        result = extract_identifiers("AdOpusTest", server_group, "default")

        assert result["customer"] == "avansas"
        assert result["service"] == "fdw"

    def test_merge_preserves_route_metadata_when_source_name_changes(self) -> None:
        """Renaming a source via source_name_map should not drop route metadata."""
        server_group = {
            "pattern": "db-per-tenant",
            "sources": {
                "Test": {
                    "schemas": ["dbo"],
                    "default": {
                        "server": "default",
                        "database": "AdOpusTest",
                        "table_count": 101,
                        "target_sink_env": "prod",
                    },
                },
            },
        }
        scanned_databases = [
            {
                "name": "AdOpusTest",
                "service": "avansas",
                "customer": "avansas",
                "environment": "default",
                "server": "default",
                "schemas": ["dbo"],
                "table_count": 101,
            },
        ]

        merged = _merge_with_existing_sources(server_group, scanned_databases, {"default"})

        assert merged[0]["target_sink_env"] == "prod"


class TestPostgresConnectionFallback:
    """Tests for local docker-compose PostgreSQL hostname fallback."""

    def test_retries_local_compose_postgres_via_localhost(
        self,
        tmp_path: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Retries via localhost and published POSTGRES_PORT for compose hostnames."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "source-groups.yaml").write_text("testgroup: {}\n")
        (tmp_path / "docker-compose.yml").write_text("services:\n  postgres:\n    image: postgres:17\n")
        (tmp_path / ".env").write_text("POSTGRES_PORT=55432\n")

        class FakeOperationalError(Exception):
            pass

        connect_calls: list[dict[str, Any]] = []
        expected_connection = object()

        def fake_connect(**kwargs: Any) -> object:
            connect_calls.append(kwargs)
            if len(connect_calls) == 1:
                raise FakeOperationalError('could not translate host name "postgres" to address: nodename nor servname provided, or not known')
            return expected_connection

        fake_pg = MagicMock()
        fake_pg.connect.side_effect = fake_connect
        fake_pg.OperationalError = FakeOperationalError

        monkeypatch.setattr(
            "cdc_generator.validators.manage_server_group.db_inspector.has_psycopg2",
            True,
        )
        monkeypatch.setattr(
            "cdc_generator.validators.manage_server_group.db_inspector.ensure_psycopg2",
            lambda: fake_pg,
        )

        connection = get_postgres_connection(
            {
                "host": "postgres",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
            },
            "directory_dev",
        )

        assert connection is expected_connection
        assert len(connect_calls) == 2
        assert connect_calls[0]["host"] == "postgres"
        assert connect_calls[0]["port"] == 5432
        assert connect_calls[1]["host"] == "localhost"
        assert connect_calls[1]["port"] == 55432


class TestMssqlInspectionConnectionDatabase:
    """Tests for MSSQL inspection connection database selection."""

    def test_uses_database_ref_for_initial_mssql_connection(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """db-per-tenant inspection should connect via database_ref when present."""
        connection_calls: list[tuple[dict[str, Any], str]] = []

        class FakeCursor:
            def __init__(self) -> None:
                self._fetchall_calls = 0

            def execute(self, query: str, args: object | None = None) -> None:
                _ = query
                _ = args

            def fetchall(self) -> list[tuple[object, ...]]:
                self._fetchall_calls += 1
                if self._fetchall_calls == 1:
                    return [("AdOpusTest",)]
                return [("dbo", "Actor")]

        class FakeConnection:
            def cursor(self, *, as_dict: bool = False) -> FakeCursor:
                _ = as_dict
                return FakeCursor()

            def close(self) -> None:
                return None

        def fake_get_mssql_connection(server_config: dict[str, Any], database: str = "") -> FakeConnection:
            connection_calls.append((server_config, database))
            return FakeConnection()

        monkeypatch.setattr(
            "cdc_generator.validators.manage_server_group.db_inspector.get_mssql_connection",
            fake_get_mssql_connection,
        )

        databases = list_mssql_databases(
            {"host": "localhost", "port": 1433, "user": "sa", "password": "secret"},
            {
                "name": "fdw",
                "pattern": "db-per-tenant",
                "type": "mssql",
                "database_ref": "AdOpusTest",
                "validation_env": "default",
                "sources": {
                    "avansas": {
                        "schemas": ["dbo"],
                        "default": {
                            "server": "default",
                            "database": "AdOpusTest",
                        },
                    },
                },
            },
        )

        assert connection_calls == [({"host": "localhost", "port": 1433, "user": "sa", "password": "secret"}, "AdOpusTest")]
        assert len(databases) == 1

    def test_falls_back_to_master_when_database_ref_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """MSSQL inspection should use master when no database_ref is configured."""
        connection_calls: list[tuple[dict[str, Any], str]] = []

        class FakeCursor:
            def __init__(self) -> None:
                self._fetchall_calls = 0

            def execute(self, query: str, args: object | None = None) -> None:
                _ = query
                _ = args

            def fetchall(self) -> list[tuple[object, ...]]:
                self._fetchall_calls += 1
                if self._fetchall_calls == 1:
                    return [("CustomerDb",)]
                return [("dbo", "Actor")]

        class FakeConnection:
            def cursor(self, *, as_dict: bool = False) -> FakeCursor:
                _ = as_dict
                return FakeCursor()

            def close(self) -> None:
                return None

        def fake_get_mssql_connection(server_config: dict[str, Any], database: str = "") -> FakeConnection:
            connection_calls.append((server_config, database))
            return FakeConnection()

        monkeypatch.setattr(
            "cdc_generator.validators.manage_server_group.db_inspector.get_mssql_connection",
            fake_get_mssql_connection,
        )

        databases = list_mssql_databases(
            {"host": "localhost", "port": 1433, "user": "sa", "password": "secret"},
            {
                "name": "fdw",
                "pattern": "db-per-tenant",
                "type": "mssql",
                "sources": {},
            },
        )

        assert connection_calls == [({"host": "localhost", "port": 1433, "user": "sa", "password": "secret"}, "master")]
        assert len(databases) == 1

    def test_falls_back_to_master_when_database_ref_belongs_to_other_server(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Inspection should not reuse a reference DB that belongs to another server."""
        connection_calls: list[tuple[dict[str, Any], str]] = []

        class FakeCursor:
            def __init__(self) -> None:
                self._fetchall_calls = 0

            def execute(self, query: str, args: object | None = None) -> None:
                _ = query
                _ = args

            def fetchall(self) -> list[tuple[object, ...]]:
                self._fetchall_calls += 1
                if self._fetchall_calls == 1:
                    return [("CustomerDb",)]
                return [("dbo", "Actor")]

        class FakeConnection:
            def cursor(self, *, as_dict: bool = False) -> FakeCursor:
                _ = as_dict
                return FakeCursor()

            def close(self) -> None:
                return None

        def fake_get_mssql_connection(server_config: dict[str, Any], database: str = "") -> FakeConnection:
            connection_calls.append((server_config, database))
            return FakeConnection()

        monkeypatch.setattr(
            "cdc_generator.validators.manage_server_group.db_inspector.get_mssql_connection",
            fake_get_mssql_connection,
        )

        databases = list_mssql_databases(
            {"host": "localhost", "port": 1433, "user": "sa", "password": "secret"},
            {
                "name": "fdw",
                "pattern": "db-per-tenant",
                "type": "mssql",
                "database_ref": "AdOpusTest",
                "validation_env": "default",
                "sources": {
                    "avansas": {
                        "schemas": ["dbo"],
                        "default": {
                            "server": "nonprod",
                            "database": "AdOpusTest",
                        },
                    },
                },
            },
            server_name="prod",
        )

        assert connection_calls == [({"host": "localhost", "port": 1433, "user": "sa", "password": "secret"}, "master")]
        assert len(databases) == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    def test_postgres_connection_error(
        self, mock_list_pg: MagicMock, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of PostgreSQL connection error."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        error = PostgresConnectionError("Connection refused", "localhost", 5432)
        mock_list_pg.side_effect = error

        result = handle_update(_ns())

        assert result == 1

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    def test_generic_exception(
        self, mock_list_pg: MagicMock, mock_ensure: MagicMock, mock_get: MagicMock, mock_load: MagicMock, mock_server_group: dict[str, Any]
    ) -> None:
        """Test handling of generic exceptions."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.side_effect = ValueError("Unexpected error")

        result = handle_update(_ns())

        assert result == 1


class TestHandleUpdateSuccess:
    """Test successful handle_update scenarios."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    @patch("cdc_generator.validators.manage_server_group.handlers_update._apply_updates")
    def test_successful_update(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]],
    ) -> None:
        """Test successful database update."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = True

        result = handle_update(_ns())

        assert result == 0
        mock_apply.assert_called_once()

    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.ensure_project_structure")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.list_postgres_databases")
    @patch("cdc_generator.validators.manage_server_group.handlers_update._apply_updates")
    def test_apply_updates_failure(
        self,
        mock_apply: MagicMock,
        mock_list_pg: MagicMock,
        mock_ensure: MagicMock,
        mock_get: MagicMock,
        mock_load: MagicMock,
        mock_server_group: dict[str, Any],
        mock_databases: list[dict[str, Any]],
    ) -> None:
        """Test when _apply_updates fails."""
        mock_load.return_value = {"testgroup": mock_server_group}
        mock_get.return_value = mock_server_group
        mock_list_pg.return_value = mock_databases
        mock_apply.return_value = False

        result = handle_update(_ns())

        assert result == 1


class TestApplyUpdatesAutocompleteCache:
    """Tests for autocomplete cache generation side effects in _apply_updates."""

    @patch("cdc_generator.validators.manage_server_group.handlers_update.update_server_group_yaml")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.load_server_groups")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.get_single_server_group")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.update_envs_list")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.write_server_group_yaml")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.update_vscode_schema")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.generate_service_autocomplete_definitions")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.update_completions")
    @patch("cdc_generator.validators.manage_server_group.handlers_update.regenerate_all_validation_schemas")
    def test_apply_updates_generates_autocomplete_cache(  # noqa: PLR0913
        self,
        mock_regen: MagicMock,
        mock_update_completions: MagicMock,
        mock_generate_autocomplete: MagicMock,
        mock_vscode: MagicMock,
        mock_write_yaml: MagicMock,
        mock_update_envs: MagicMock,
        mock_get_single: MagicMock,
        mock_load_groups: MagicMock,
        mock_update_yaml: MagicMock,
    ) -> None:
        """_apply_updates should trigger service autocomplete cache generation."""
        mock_update_yaml.return_value = True
        mock_load_groups.return_value = {"testgroup": {"name": "testgroup"}}
        mock_get_single.return_value = {"name": "testgroup"}
        mock_generate_autocomplete.return_value = True

        server_group = {
            "name": "testgroup",
            "type": "postgres",
            "table_include_patterns": ["^core_"],
            "table_exclude_patterns": ["tmp"],
            "schema_exclude_patterns": ["logs"],
            "servers": {
                "default": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "test",
                    "password": "secret",
                },
            },
        }
        scanned_databases = [
            {
                "name": "directory_dev",
                "server": "default",
                "service": "directory",
                "environment": "nonprod",
                "customer": "",
                "schemas": ["public"],
                "table_count": 10,
            },
        ]

        result = _apply_updates(
            "testgroup",
            scanned_databases,
            server_group,
            scanned_databases,
            ["^core_"],
        )

        assert result is True
        mock_generate_autocomplete.assert_called_once_with(
            server_group,
            scanned_databases,
            ["^core_"],
            ["tmp"],
            ["logs"],
        )
