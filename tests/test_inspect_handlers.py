"""Unit tests for inspect handler error paths.

Tests the validation / error branches of handle_inspect and
handle_inspect_sink that don't require a real database connection.
"""

import argparse
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
from cdc_generator.validators.manage_service.db_inspector_common import (
    get_service_db_config,
)

# project_dir fixture is provided by tests/conftest.py


@pytest.fixture()
def service_yaml(project_dir: Path) -> Path:
    """Service YAML with tables and a sink for inspect tests."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables: {}\n"
    )
    return sf


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

    def test_resolves_db_per_tenant_by_group_key(
        self, project_dir: Path,
    ) -> None:
        """db-per-tenant service may resolve via server-group key, not source key."""
        (project_dir / "source-groups.yaml").write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  type: mssql\n"
            "  validation_env: default\n"
            "  sources:\n"
            "    Test:\n"
            "      schemas:\n"
            "        - dbo\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusTest\n"
        )

        db_type, sg, _ = _resolve_inspect_db_type("adopus")
        assert db_type == "mssql"
        assert sg == "adopus"


class TestServiceDbConfigDbPerTenant:
    """Regression tests for db-per-tenant service DB config resolution."""

    def test_resolves_validation_database_from_customer_source_entry(
        self, project_dir: Path,
    ) -> None:
        """Service-level config should match source entry by validation_database."""
        (project_dir / "source-groups.yaml").write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  type: mssql\n"
            "  validation_env: default\n"
            "  servers:\n"
            "    default:\n"
            "      host: localhost\n"
            "      port: 1433\n"
            "      user: sa\n"
            "      password: secret\n"
            "  sources:\n"
            "    Test:\n"
            "      schemas:\n"
            "        - dbo\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusTest\n"
        )
        (project_dir / "services" / "adopus.yaml").write_text(
            "adopus:\n"
            "  source:\n"
            "    validation_database: AdOpusTest\n"
            "    tables:\n"
            "      dbo.Actor: {}\n"
        )

        config = get_service_db_config("adopus")

        assert config is not None
        assert config["env_config"]["database_name"] == "AdOpusTest"
        mssql_cfg = config["env_config"]["mssql"]
        assert mssql_cfg["host"] == "localhost"
        assert mssql_cfg["port"] == 1433


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
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 1 when neither --all nor --schema provided."""
        args = _ns(inspect_sink="sink_asma.chat")
        result = handle_inspect_sink(args)
        assert result == 1

    def test_invalid_sink_key_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 1 when sink key not found."""
        args = _ns(
            inspect_sink="sink_asma.nonexistent",
            all=True,
        )
        result = handle_inspect_sink(args)
        assert result == 1

    def test_all_sinks_mode_requires_all_flag(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--inspect-sink without key requires --all to inspect all sinks."""
        args = _ns(inspect_sink="__all_sinks__", all=False)
        result = handle_inspect_sink(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Happy paths
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleInspectHappyPath:
    """Success-path tests for handle_inspect."""

    def test_inspect_all_success(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 and filters tables to allowed schemas for --all."""
        args = _ns(inspect=True, all=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "queries",
                "COLUMN_COUNT": 5,
            },
            {
                "TABLE_SCHEMA": "private",
                "TABLE_NAME": "internal_logs",
                "COLUMN_COUNT": 2,
            },
        ]

        with patch(
            "cdc_generator.cli.service_handlers_inspect.inspect_postgres_schema",
            return_value=tables,
        ) as inspect_mock:
            result = handle_inspect(args)

        assert result == 0
        inspect_mock.assert_called_once_with("proxy", "nonprod")

    def test_inspect_save_calls_schema_saver(
        self, project_dir: Path,
    ) -> None:
        """--save calls save_detailed_schema with filtered tables."""
        args = _ns(inspect=True, all=True, save=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "queries",
                "COLUMN_COUNT": 5,
            },
            {
                "TABLE_SCHEMA": "private",
                "TABLE_NAME": "internal_logs",
                "COLUMN_COUNT": 2,
            },
        ]

        with patch(
            "cdc_generator.cli.service_handlers_inspect.inspect_postgres_schema",
            return_value=tables,
        ), patch(
            "cdc_generator.cli.service_handlers_inspect.save_detailed_schema",
            return_value=True,
        ) as save_mock:
            result = handle_inspect(args)

        assert result == 0
        save_mock.assert_called_once()
        save_args = save_mock.call_args.args
        assert save_args[0] == "proxy"
        assert save_args[1] == "nonprod"
        assert save_args[2] == ""
        saved_tables = save_args[3]
        assert isinstance(saved_tables, list)
        assert len(saved_tables) == 1
        assert saved_tables[0]["TABLE_SCHEMA"] == "public"
        assert save_args[4] == "postgres"


class TestHandleInspectSinkHappyPath:
    """Success-path tests for handle_inspect_sink."""

    def test_inspect_sink_all_success(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 0 and filters sink tables to allowed schemas."""
        args = _ns(inspect_sink="sink_asma.chat", all=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "users",
                "COLUMN_COUNT": 5,
            },
            {
                "TABLE_SCHEMA": "private",
                "TABLE_NAME": "internal_logs",
                "COLUMN_COUNT": 2,
            },
        ]

        with patch(
            "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
            return_value=["sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=(tables, "postgres", ["public"]),
        ) as inspect_mock:
            result = handle_inspect_sink(args)

        assert result == 0
        inspect_mock.assert_called_once_with(
            "proxy", "sink_asma.chat", "nonprod",
        )

    def test_inspect_sink_save_calls_schema_saver(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--save on --inspect-sink calls save_sink_schema."""
        args = _ns(inspect_sink="sink_asma.chat", all=True, save=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "users",
                "COLUMN_COUNT": 5,
            },
            {
                "TABLE_SCHEMA": "private",
                "TABLE_NAME": "internal_logs",
                "COLUMN_COUNT": 2,
            },
        ]

        with patch(
            "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
            return_value=["sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=(tables, "postgres", ["public"]),
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.save_sink_schema",
            return_value=True,
        ) as save_mock:
            result = handle_inspect_sink(args)

        assert result == 0
        save_mock.assert_called_once()
        save_args = save_mock.call_args.args
        assert save_args[0] == "chat"
        assert save_args[1] == "sink_asma.chat"
        assert save_args[2] == "proxy"
        assert save_args[3] == "nonprod"
        saved_tables = save_args[4]
        assert isinstance(saved_tables, list)
        assert len(saved_tables) == 1
        assert saved_tables[0]["TABLE_SCHEMA"] == "public"

    def test_inspect_all_sinks_success(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """--inspect-sink --all inspects each configured sink."""
        args = _ns(inspect_sink="__all_sinks__", all=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "users",
                "COLUMN_COUNT": 5,
            },
        ]

        with patch(
            "cdc_generator.cli.service_handlers_inspect_sink._get_available_sinks",
            return_value=["sink_asma.activities", "sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=(tables, "postgres", ["public"]),
        ) as inspect_mock:
            result = handle_inspect_sink(args)

        assert result == 0
        assert inspect_mock.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════
# handle_inspect — additional coverage
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleInspectUnsupportedDb:
    """Tests for unsupported db_type and edge cases."""

    def test_unsupported_db_type_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 for unsupported database type."""
        # Override source-groups with an unsupported type
        (project_dir / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  type: oracle\n"
            "  sources:\n"
            "    proxy:\n"
            "      schemas:\n"
            "        - public\n"
        )
        args = _ns(inspect=True, all=True)
        result = handle_inspect(args)
        assert result == 1

    def test_mssql_inspect_path(
        self, project_dir: Path,
    ) -> None:
        """MSSQL inspect dispatches to inspect_mssql_schema."""
        (project_dir / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-per-tenant\n"
            "  type: mssql\n"
            "  database_ref: proxy\n"
            "  sources:\n"
            "    proxy:\n"
            "      schemas:\n"
            "        - dbo\n"
        )
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "dbo",
                "TABLE_NAME": "Actor",
                "COLUMN_COUNT": 10,
            },
        ]
        args = _ns(inspect=True, all=True)
        with patch(
            "cdc_generator.cli.service_handlers_inspect.inspect_mssql_schema",
            return_value=tables,
        ) as mssql_mock:
            result = handle_inspect(args)
        assert result == 0
        mssql_mock.assert_called_once_with("proxy", "nonprod")

    def test_no_tables_after_filter_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when all tables are filtered out by schema."""
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "internal",
                "TABLE_NAME": "logs",
                "COLUMN_COUNT": 3,
            },
        ]
        args = _ns(inspect=True, all=True)
        with patch(
            "cdc_generator.cli.service_handlers_inspect.inspect_postgres_schema",
            return_value=tables,
        ):
            result = handle_inspect(args)
        assert result == 1

    def test_db_per_tenant_database_ref_as_database_name(
        self, project_dir: Path,
    ) -> None:
        """db-per-tenant inspect resolves schemas when database_ref is DB name."""
        (project_dir / "source-groups.yaml").write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  type: mssql\n"
            "  validation_env: default\n"
            "  database_ref: AdOpusTest\n"
            "  sources:\n"
            "    Test:\n"
            "      schemas:\n"
            "        - dbo\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusTest\n"
        )
        (project_dir / "services" / "adopus.yaml").write_text(
            "adopus:\n"
            "  source:\n"
            "    validation_database: AdOpusTest\n"
            "    tables:\n"
            "      dbo.Actor: {}\n"
        )

        args = _ns(service="adopus", inspect=True, all=True)
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "dbo",
                "TABLE_NAME": "Actor",
                "COLUMN_COUNT": 10,
            },
        ]

        with patch(
            "cdc_generator.cli.service_handlers_inspect.inspect_mssql_schema",
            return_value=tables,
        ) as mssql_mock:
            result = handle_inspect(args)

        assert result == 0
        mssql_mock.assert_called_once_with("adopus", "nonprod")


class TestHandleInspectSinkSchemaNotAllowed:
    """Test for --schema not in allowed schemas for inspect-sink."""

    def test_disallowed_schema_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 1 when requested schema not in allowed list."""
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "public",
                "TABLE_NAME": "users",
                "COLUMN_COUNT": 5,
            },
        ]
        args = _ns(
            inspect_sink="sink_asma.chat",
            schema="private",
        )
        with patch(
            "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
            return_value=["sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=(tables, "postgres", ["public"]),
        ):
            result = handle_inspect_sink(args)
        assert result == 1

    def test_no_tables_after_filter_returns_1(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 1 when all sink tables are filtered out by schema."""
        tables: list[dict[str, object]] = [
            {
                "TABLE_SCHEMA": "internal",
                "TABLE_NAME": "logs",
                "COLUMN_COUNT": 3,
            },
        ]
        args = _ns(inspect_sink="sink_asma.chat", all=True)
        with patch(
            "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
            return_value=["sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=(tables, "postgres", ["public"]),
        ):
            result = handle_inspect_sink(args)
        assert result == 1

    def test_inspect_sink_returns_1_when_connection_fails(
        self, project_dir: Path, service_yaml: Path,
    ) -> None:
        """Returns 1 when inspect_sink_schema returns None."""
        args = _ns(inspect_sink="sink_asma.chat", all=True)
        with patch(
            "cdc_generator.validators.manage_service.db_inspector_common.get_available_sinks",
            return_value=["sink_asma.chat"],
        ), patch(
            "cdc_generator.cli.service_handlers_inspect_sink.inspect_sink_schema",
            return_value=None,
        ):
            result = handle_inspect_sink(args)
        assert result == 1
