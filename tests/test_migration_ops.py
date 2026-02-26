"""Unit tests for migration_ops.py.

Covers:
- CdcOpsResult dataclass defaults
- _get_source_tables (table extraction from service config)
- enable_cdc_tables (dry-run, connection failure, missing config)
- clean_cdc_data (dry-run, connection failure, table filter)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cdc_generator.core.migration_ops import (
    CdcOpsResult,
    _get_source_tables,
    clean_cdc_data,
    enable_cdc_tables,
)

# ---------------------------------------------------------------------------
# CdcOpsResult defaults
# ---------------------------------------------------------------------------


class TestCdcOpsResult:
    """Verify CdcOpsResult dataclass defaults."""

    def test_defaults(self) -> None:
        r = CdcOpsResult()
        assert r.enabled_count == 0
        assert r.already_enabled == 0
        assert r.cleaned_count == 0
        assert r.errors == []
        assert r.tables == []


# ---------------------------------------------------------------------------
# _get_source_tables
# ---------------------------------------------------------------------------


class TestGetSourceTables:
    """Test source table extraction from service config."""

    def test_basic_extraction(self) -> None:
        """Extracts (schema, table) tuples from dotted keys."""
        config: dict[str, object] = {
            "source": {
                "tables": {
                    "dbo.Actor": {},
                    "dbo.Role": {},
                    "hr.Employee": {},
                },
            },
        }
        result = _get_source_tables(config)
        assert ("dbo", "Actor") in result
        assert ("dbo", "Role") in result
        assert ("hr", "Employee") in result
        assert len(result) == 3

    def test_single_part_key_defaults_to_dbo(self) -> None:
        """Table key without dot defaults schema to 'dbo'."""
        config: dict[str, object] = {
            "source": {"tables": {"SimpleTable": {}}},
        }
        result = _get_source_tables(config)
        assert result == [("dbo", "SimpleTable")]

    def test_sorted_output(self) -> None:
        """Results are sorted."""
        config: dict[str, object] = {
            "source": {
                "tables": {
                    "dbo.Z": {},
                    "dbo.A": {},
                },
            },
        }
        result = _get_source_tables(config)
        assert result == [("dbo", "A"), ("dbo", "Z")]

    def test_no_source(self) -> None:
        """Returns empty when 'source' is missing."""
        assert _get_source_tables({}) == []

    def test_no_tables(self) -> None:
        """Returns empty when 'tables' is missing."""
        config: dict[str, object] = {"source": {}}
        assert _get_source_tables(config) == []

    def test_invalid_source_type(self) -> None:
        """Returns empty when source is not a dict."""
        config: dict[str, object] = {"source": "invalid"}
        assert _get_source_tables(config) == []


# ---------------------------------------------------------------------------
# enable_cdc_tables
# ---------------------------------------------------------------------------


class TestEnableCdcTables:
    """Test CDC enable operations."""

    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_dry_run(self, mock_config: MagicMock) -> None:
        """Dry run lists tables without connecting to MSSQL."""
        mock_config.return_value = {
            "source": {
                "tables": {
                    "dbo.Actor": {},
                    "dbo.Role": {},
                },
            },
        }
        result = enable_cdc_tables("test", dry_run=True)
        assert result.errors == []
        assert result.enabled_count == 0

    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_missing_config(self, mock_config: MagicMock) -> None:
        """Returns error when service config not found."""
        mock_config.side_effect = FileNotFoundError("not found")
        result = enable_cdc_tables("missing")
        assert len(result.errors) == 1

    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_no_source_tables(self, mock_config: MagicMock) -> None:
        """Returns error when no source tables found."""
        mock_config.return_value = {"source": {"tables": {}}}
        result = enable_cdc_tables("test")
        assert len(result.errors) == 1
        assert "No source tables" in result.errors[0]

    @patch("cdc_generator.core.migration_ops.get_mssql_connection")
    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_connection_failure(
        self,
        mock_config: MagicMock,
        mock_mssql: MagicMock,
    ) -> None:
        """Returns error when MSSQL connection fails."""
        mock_config.return_value = {
            "source": {"tables": {"dbo.Actor": {}}},
        }
        mock_mssql.side_effect = ValueError("conn failed")
        result = enable_cdc_tables("test", env="nonprod")
        assert len(result.errors) == 1
        assert "connection failed" in result.errors[0].lower()

    @patch("cdc_generator.core.migration_ops.get_mssql_connection")
    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_already_enabled(
        self,
        mock_config: MagicMock,
        mock_mssql: MagicMock,
    ) -> None:
        """Tables already CDC-enabled are counted but not re-enabled."""
        mock_config.return_value = {
            "source": {"tables": {"dbo.Actor": {}}},
        }
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)  # Already enabled
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mssql.return_value = (mock_conn, None)

        result = enable_cdc_tables("test", env="nonprod")
        assert result.already_enabled == 1
        assert result.enabled_count == 0

    @patch("cdc_generator.core.migration_ops.get_mssql_connection")
    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_table_filter(
        self,
        mock_config: MagicMock,
        mock_mssql: MagicMock,
    ) -> None:
        """Table filter limits which tables are processed."""
        mock_config.return_value = {
            "source": {
                "tables": {
                    "dbo.Actor": {},
                    "dbo.Role": {},
                },
            },
        }
        result = enable_cdc_tables("test", dry_run=True, table_filter="Actor")
        # Only Actor should be listed (dry run doesn't connect)
        assert result.errors == []


# ---------------------------------------------------------------------------
# clean_cdc_data
# ---------------------------------------------------------------------------


class TestCleanCdcData:
    """Test CDC cleanup operations."""

    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_dry_run(self, mock_config: MagicMock) -> None:
        """Dry run lists tables without connecting."""
        mock_config.return_value = {
            "source": {"tables": {"dbo.Actor": {}}},
        }
        result = clean_cdc_data("test", dry_run=True, days=30)
        assert result.errors == []
        assert result.cleaned_count == 0

    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_missing_config(self, mock_config: MagicMock) -> None:
        """Returns error when service config not found."""
        mock_config.side_effect = FileNotFoundError("not found")
        result = clean_cdc_data("missing")
        assert len(result.errors) == 1

    @patch("cdc_generator.core.migration_ops.get_mssql_connection")
    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_connection_failure(
        self,
        mock_config: MagicMock,
        mock_mssql: MagicMock,
    ) -> None:
        """Returns error when MSSQL connection fails."""
        mock_config.return_value = {
            "source": {"tables": {"dbo.Actor": {}}},
        }
        mock_mssql.side_effect = ValueError("conn failed")
        result = clean_cdc_data("test", env="nonprod")
        assert len(result.errors) == 1

    @patch("cdc_generator.core.migration_ops.get_mssql_connection")
    @patch("cdc_generator.core.migration_ops.load_service_config")
    def test_successful_clean(
        self,
        mock_config: MagicMock,
        mock_mssql: MagicMock,
    ) -> None:
        """Tables are cleaned successfully."""
        mock_config.return_value = {
            "source": {"tables": {"dbo.Actor": {}, "dbo.Role": {}}},
        }
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_mssql.return_value = (mock_conn, None)

        result = clean_cdc_data("test", env="nonprod", days=30)
        assert result.cleaned_count == 2
        assert result.errors == []
        mock_conn.close.assert_called_once()
