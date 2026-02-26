"""Unit tests for migration_diff.py.

Covers:
- ChangeKind enum values
- SchemaChange / DiffResult / ParsedColumn dataclasses
- _parse_create_table (various DDL formats, multi-word types, PK extraction)
- _compare_columns (add/remove/type change detection)
- _build_expected_tables (table filter, target_exists exclusion)
- diff_migrations (integration with mocked service config)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from cdc_generator.core.migration_diff import (
    ChangeKind,
    DiffResult,
    ParsedColumn,
    SchemaChange,
    _build_expected_tables,
    _compare_columns,
    _parse_create_table,
    diff_migrations,
)

# ---------------------------------------------------------------------------
# DataClasses & enum
# ---------------------------------------------------------------------------


class TestDataStructures:
    """Verify dataclass defaults and enum values."""

    def test_change_kind_values(self) -> None:
        assert ChangeKind.TABLE_ADDED.value == "table_added"
        assert ChangeKind.COLUMN_TYPE_CHANGED.value == "column_type_changed"

    def test_schema_change_str(self) -> None:
        c = SchemaChange(
            kind=ChangeKind.COLUMN_TYPE_CHANGED,
            sink_name="s1",
            table_name="T",
            column_name="col",
            old_value="INTEGER",
            new_value="BIGINT",
            severity="breaking",
        )
        s = str(c)
        assert "COLUMN_TYPE_CHANGED" in s or "column_type_changed" in s
        assert "col" in s

    def test_schema_change_str_no_column(self) -> None:
        c = SchemaChange(
            kind=ChangeKind.TABLE_ADDED,
            sink_name="s1",
            table_name="NewTable",
        )
        assert "NewTable" in str(c)

    def test_diff_result_properties(self) -> None:
        r = DiffResult(changes=[
            SchemaChange(kind=ChangeKind.TABLE_ADDED, sink_name="s", table_name="T1"),
            SchemaChange(kind=ChangeKind.TABLE_REMOVED, sink_name="s", table_name="T2"),
            SchemaChange(
                kind=ChangeKind.COLUMN_ADDED, sink_name="s",
                table_name="T3", column_name="c",
            ),
        ])
        assert r.has_changes is True
        assert len(r.added_tables) == 1
        assert len(r.removed_tables) == 1
        assert len(r.column_changes) == 1

    def test_diff_result_empty(self) -> None:
        r = DiffResult()
        assert r.has_changes is False
        assert r.added_tables == []
        assert r.tables_compared == 0

    def test_parsed_column_defaults(self) -> None:
        c = ParsedColumn(name="x", type="TEXT")
        assert c.nullable is True
        assert c.primary_key is False


# ---------------------------------------------------------------------------
# _parse_create_table
# ---------------------------------------------------------------------------


class TestParseCreateTable:
    """Test DDL parsing from migration SQL files."""

    def test_basic_create_table(self) -> None:
        """Parses a simple CREATE TABLE with columns and PK."""
        sql = (
            'CREATE TABLE IF NOT EXISTS "myschema"."Actor" (\n'
            '    "actno" INTEGER NOT NULL,\n'
            '    "name" VARCHAR(255),\n'
            '    PRIMARY KEY ("actno")\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        assert len(cols) == 2
        assert cols[0].name == "actno"
        assert cols[0].type == "INTEGER"
        assert cols[0].primary_key is True
        assert cols[0].nullable is False
        assert cols[1].name == "name"
        assert cols[1].type == "VARCHAR(255)"
        assert cols[1].primary_key is False

    def test_multi_word_type(self) -> None:
        """Handles multi-word types like double precision (lowercase from TypeMapper)."""
        sql = (
            'CREATE TABLE IF NOT EXISTS "s"."T" (\n'
            '    "val" double precision,\n'
            '    "id" INTEGER NOT NULL,\n'
            '    PRIMARY KEY ("id")\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        val_col = next(c for c in cols if c.name == "val")
        assert val_col.type == "DOUBLE PRECISION"

    def test_character_varying(self) -> None:
        """Handles character varying(n) type (lowercase from TypeMapper)."""
        sql = (
            'CREATE TABLE IF NOT EXISTS "s"."T" (\n'
            '    "name" character varying(100) NOT NULL\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        assert cols[0].type == "CHARACTER VARYING(100)"
        assert cols[0].nullable is False

    def test_composite_primary_key(self) -> None:
        """Extracts all columns from composite PK."""
        sql = (
            'CREATE TABLE IF NOT EXISTS "s"."T" (\n'
            '    "a" INTEGER NOT NULL,\n'
            '    "b" INTEGER NOT NULL,\n'
            '    "c" TEXT,\n'
            '    PRIMARY KEY ("a", "b")\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        pk_cols = [c for c in cols if c.primary_key]
        assert len(pk_cols) == 2
        assert {c.name for c in pk_cols} == {"a", "b"}

    def test_no_create_table(self) -> None:
        """Returns None for non-DDL SQL."""
        assert _parse_create_table("SELECT 1;") is None

    def test_with_default_value(self) -> None:
        """Parses columns that have DEFAULT clauses."""
        sql = (
            'CREATE TABLE IF NOT EXISTS "s"."T" (\n'
            '    "ts" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n'
            '    "id" INTEGER NOT NULL,\n'
            '    PRIMARY KEY ("id")\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        ts_col = next(c for c in cols if c.name == "ts")
        assert ts_col.nullable is False

    def test_with_header_block(self) -> None:
        """Parses DDL that includes header comments and checksum."""
        sql = (
            "-- ============================================================================\n"
            "-- DO NOT EDIT — AUTO-GENERATED\n"
            "-- ============================================================================\n"
            "-- Checksum: sha256:abc123\n"
            "\n"
            'CREATE TABLE IF NOT EXISTS "s"."T" (\n'
            '    "id" INTEGER NOT NULL,\n'
            '    PRIMARY KEY ("id")\n'
            ");\n"
        )
        cols = _parse_create_table(sql)
        assert cols is not None
        assert len(cols) == 1


# ---------------------------------------------------------------------------
# _compare_columns
# ---------------------------------------------------------------------------


class TestCompareColumns:
    """Test column comparison logic."""

    def test_no_changes(self) -> None:
        """Identical column lists produce no changes."""
        cols = [
            ParsedColumn(name="id", type="INTEGER", primary_key=True),
            ParsedColumn(name="name", type="VARCHAR(255)"),
        ]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=cols, generated=cols,
        )
        assert changes == []

    def test_column_added(self) -> None:
        """Detects new column in expected but not generated."""
        expected = [
            ParsedColumn(name="id", type="INTEGER"),
            ParsedColumn(name="new_col", type="TEXT"),
        ]
        generated = [
            ParsedColumn(name="id", type="INTEGER"),
        ]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=expected, generated=generated,
        )
        added = [c for c in changes if c.kind == ChangeKind.COLUMN_ADDED]
        assert len(added) == 1
        assert added[0].column_name == "new_col"

    def test_column_removed(self) -> None:
        """Detects column in generated but removed from expected."""
        expected = [
            ParsedColumn(name="id", type="INTEGER"),
        ]
        generated = [
            ParsedColumn(name="id", type="INTEGER"),
            ParsedColumn(name="old_col", type="TEXT"),
        ]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=expected, generated=generated,
        )
        removed = [c for c in changes if c.kind == ChangeKind.COLUMN_REMOVED]
        assert len(removed) == 1
        assert removed[0].column_name == "old_col"
        assert removed[0].severity == "warning"

    def test_type_changed(self) -> None:
        """Detects column type change."""
        expected = [
            ParsedColumn(name="val", type="BIGINT"),
        ]
        generated = [
            ParsedColumn(name="val", type="INTEGER"),
        ]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=expected, generated=generated,
        )
        type_changes = [c for c in changes if c.kind == ChangeKind.COLUMN_TYPE_CHANGED]
        assert len(type_changes) == 1
        assert type_changes[0].old_value == "INTEGER"
        assert type_changes[0].new_value == "BIGINT"
        assert type_changes[0].severity == "breaking"

    def test_type_comparison_case_insensitive(self) -> None:
        """Type comparison is case-insensitive (both uppercased)."""
        expected = [ParsedColumn(name="x", type="integer")]
        generated = [ParsedColumn(name="x", type="INTEGER")]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=expected, generated=generated,
        )
        assert changes == []

    def test_multiple_changes(self) -> None:
        """Detects multiple changes in a single table."""
        expected = [
            ParsedColumn(name="id", type="INTEGER"),
            ParsedColumn(name="new_col", type="TEXT"),
        ]
        generated = [
            ParsedColumn(name="id", type="BIGINT"),
            ParsedColumn(name="old_col", type="TEXT"),
        ]
        changes = _compare_columns(
            sink_name="s", table_name="T",
            expected=expected, generated=generated,
        )
        assert len(changes) == 3  # type change + add + remove


# ---------------------------------------------------------------------------
# _build_expected_tables
# ---------------------------------------------------------------------------


class TestBuildExpectedTables:
    """Test expected table mapping construction."""

    def test_basic(self) -> None:
        tables: dict[str, dict[str, Any]] = {
            "adopus.Actor": {"from": "dbo.Actor"},
            "adopus.Role": {"from": "dbo.Role"},
        }
        result = _build_expected_tables(tables, table_filter=None)
        assert "Actor" in result
        assert "Role" in result
        assert result["Actor"] == ("dbo.Actor", "adopus.Actor")

    def test_excludes_target_exists(self) -> None:
        tables: dict[str, dict[str, Any]] = {
            "adopus.Actor": {"from": "dbo.Actor"},
            "adopus.Existing": {"from": "dbo.Existing", "target_exists": True},
        }
        result = _build_expected_tables(tables, table_filter=None)
        assert "Actor" in result
        assert "Existing" not in result

    def test_table_filter(self) -> None:
        tables: dict[str, dict[str, Any]] = {
            "adopus.Actor": {"from": "dbo.Actor"},
            "adopus.Role": {"from": "dbo.Role"},
        }
        result = _build_expected_tables(tables, table_filter="Actor")
        assert "Actor" in result
        assert "Role" not in result

    def test_filter_case_insensitive(self) -> None:
        tables: dict[str, dict[str, Any]] = {
            "x.Actor": {"from": "dbo.Actor"},
        }
        result = _build_expected_tables(tables, table_filter="actor")
        assert "Actor" in result


# ---------------------------------------------------------------------------
# diff_migrations — integration
# ---------------------------------------------------------------------------


class TestDiffMigrations:
    """Integration tests for full diff flow."""

    @patch("cdc_generator.core.migration_diff.get_project_root")
    @patch("cdc_generator.core.migration_diff.load_service_config")
    @patch("cdc_generator.core.migration_diff.load_table_definitions")
    def test_no_migrations_dir(
        self,
        mock_table_defs: MagicMock,
        mock_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """All tables reported as added when no migrations exist."""
        mock_root.return_value = tmp_path
        mock_config.return_value = {
            "sinks": {
                "s.d": {
                    "tables": {
                        "schema.T1": {"from": "dbo.T1"},
                    },
                },
            },
        }
        mock_table_defs.return_value = {}

        result = diff_migrations(
            "test", migrations_dir=tmp_path / "migrations",
        )
        added = result.added_tables
        assert len(added) == 1
        assert added[0].table_name == "T1"

    @patch("cdc_generator.core.migration_diff.get_project_root")
    @patch("cdc_generator.core.migration_diff.load_service_config")
    def test_missing_config_returns_error(
        self,
        mock_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when service config not found."""
        mock_root.return_value = tmp_path
        mock_config.side_effect = FileNotFoundError("not found")

        result = diff_migrations("missing")
        assert len(result.errors) >= 1

    @patch("cdc_generator.core.migration_diff.get_project_root")
    @patch("cdc_generator.core.migration_diff.load_service_config")
    @patch("cdc_generator.core.migration_diff.load_table_definitions")
    @patch("cdc_generator.core.migration_diff.get_sinks")
    def test_no_sinks_returns_error(
        self,
        mock_sinks: MagicMock,
        mock_table_defs: MagicMock,
        mock_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when no sinks found."""
        mock_root.return_value = tmp_path
        mock_config.return_value = {}
        mock_table_defs.return_value = {}
        mock_sinks.return_value = {}

        result = diff_migrations("test")
        assert any("No sinks" in e for e in result.errors)

    @patch("cdc_generator.core.migration_diff.get_project_root")
    @patch("cdc_generator.core.migration_diff.load_service_config")
    @patch("cdc_generator.core.migration_diff.load_table_definitions")
    def test_detects_removed_table(
        self,
        mock_table_defs: MagicMock,
        mock_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Detects table present in SQL but removed from config."""
        mock_root.return_value = tmp_path
        mock_config.return_value = {
            "sinks": {
                "s.d": {
                    "tables": {
                        # Only KeepTable in config, OldTable was removed
                        "schema.KeepTable": {"from": "dbo.KeepTable"},
                    },
                },
            },
        }
        mock_table_defs.return_value = {}

        # Create migration SQL files for both tables
        tables_dir = tmp_path / "migrations" / "s.d" / "01-tables"
        tables_dir.mkdir(parents=True)
        for tbl in ("OldTable", "KeepTable"):
            (tables_dir / f"{tbl}.sql").write_text(
                f'CREATE TABLE IF NOT EXISTS "s"."{tbl}" (\n'
                f'    "id" INTEGER NOT NULL,\n'
                f'    PRIMARY KEY ("id")\n'
                ");\n",
            )
        # Need manifest for sink discovery
        (tmp_path / "migrations" / "s.d" / "manifest.yaml").write_text(
            "generated_at: now\n",
        )

        result = diff_migrations(
            "test", migrations_dir=tmp_path / "migrations",
        )
        removed = result.removed_tables
        assert len(removed) == 1
        assert removed[0].table_name == "OldTable"
