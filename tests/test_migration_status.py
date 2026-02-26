"""Unit tests for migration_status.py.

Covers:
- FileStatus enum values
- MigrationFileStatus / StatusResult dataclasses
- StatusResult properties (pending_count, applied_count, modified_count)
- check_migration_status_offline (file listing, checksums, sink filter)
- check_migration_status (with mocked PG connection)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from cdc_generator.core.migration_status import (
    FileStatus,
    MigrationFileStatus,
    StatusResult,
    check_migration_status,
    check_migration_status_offline,
)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class TestDataStructures:
    """Verify dataclass defaults and enum values."""

    def test_file_status_values(self) -> None:
        assert FileStatus.PENDING.value == "pending"
        assert FileStatus.APPLIED.value == "applied"
        assert FileStatus.MODIFIED.value == "modified"

    def test_migration_file_status_defaults(self) -> None:
        f = MigrationFileStatus(file_name="f.sql", status=FileStatus.PENDING)
        assert f.applied_at is None
        assert f.checksum == ""
        assert f.applied_checksum is None

    def test_status_result_defaults(self) -> None:
        r = StatusResult()
        assert r.files == []
        assert r.errors == []
        assert r.connected is False

    def test_status_result_counts(self) -> None:
        r = StatusResult(files=[
            MigrationFileStatus(file_name="a", status=FileStatus.PENDING),
            MigrationFileStatus(file_name="b", status=FileStatus.APPLIED),
            MigrationFileStatus(file_name="c", status=FileStatus.APPLIED),
            MigrationFileStatus(file_name="d", status=FileStatus.MODIFIED),
        ])
        assert r.pending_count == 1
        assert r.applied_count == 2
        assert r.modified_count == 1


# ---------------------------------------------------------------------------
# Helper: create sink directory with migration files
# ---------------------------------------------------------------------------


def _create_sink_structure(tmp_path: Path, sink_name: str = "sink_test.db") -> Path:
    """Create a minimal sink directory with SQL files and manifest."""
    sink_dir = tmp_path / sink_name
    infra = sink_dir / "00-infrastructure"
    infra.mkdir(parents=True)
    (infra / "01-create-schemas.sql").write_text("CREATE SCHEMA test;\n")

    tables = sink_dir / "01-tables"
    tables.mkdir()
    (tables / "Actor.sql").write_text(
        'CREATE TABLE IF NOT EXISTS "s"."Actor" ("id" INTEGER);\n',
    )

    (sink_dir / "manifest.yaml").write_text(
        "sink_target:\n  databases:\n    dev: test_db\n",
    )
    return tmp_path


# ---------------------------------------------------------------------------
# check_migration_status_offline
# ---------------------------------------------------------------------------


class TestCheckMigrationStatusOffline:
    """Test offline status checking (no DB connection)."""

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_all_pending(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """All files are marked PENDING in offline mode."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")

        result = check_migration_status_offline(
            migrations_dir=migrations_dir,
        )
        assert all(f.status == FileStatus.PENDING for f in result.files)
        assert len(result.files) == 2  # 1 infra + 1 table

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_files_have_checksums(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Each file entry has a computed checksum."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")

        result = check_migration_status_offline(
            migrations_dir=migrations_dir,
        )
        for f in result.files:
            assert f.checksum != ""
            assert len(f.checksum) == 64  # SHA256 hex

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_sink_filter(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Sink filter limits results to the specified sink."""
        mock_root.return_value = tmp_path
        base = tmp_path / "migrations"
        _create_sink_structure(base, "sink_a")
        _create_sink_structure(base, "sink_b")

        result = check_migration_status_offline(
            migrations_dir=base,
            sink_filter="sink_a",
        )
        file_names = [f.file_name for f in result.files]
        assert all("sink_a" in n for n in file_names)

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_no_migrations_dir(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when migrations directory doesn't exist."""
        mock_root.return_value = tmp_path
        result = check_migration_status_offline(
            migrations_dir=tmp_path / "nonexistent",
        )
        assert len(result.errors) == 1
        assert "not found" in result.errors[0].lower()

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_file_names_include_sink_prefix(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """File names are prefixed with sink directory name."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")

        result = check_migration_status_offline(
            migrations_dir=migrations_dir,
        )
        for f in result.files:
            assert f.file_name.startswith("sink_test.db/")


# ---------------------------------------------------------------------------
# check_migration_status (online with mocked PG)
# ---------------------------------------------------------------------------


class TestCheckMigrationStatus:
    """Test online status checking with mocked PostgreSQL connection."""

    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_no_migrations_dir_error(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when migrations directory is missing."""
        mock_root.return_value = tmp_path
        result = check_migration_status(
            "test",
            migrations_dir=tmp_path / "nonexistent",
        )
        assert len(result.errors) >= 1

    @patch("cdc_generator.core.migration_status.get_pg_connection")
    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_applied_files_detected(
        self,
        mock_root: MagicMock,
        mock_pg: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Files present in history table are marked APPLIED."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")

        # Read actual file content to compute checksum
        from cdc_generator.core.migration_apply import compute_content_checksum

        infra_content = (
            migrations_dir
            / "sink_test.db"
            / "00-infrastructure"
            / "01-create-schemas.sql"
        ).read_text()
        infra_checksum = compute_content_checksum(infra_content)

        # Mock PG to return matching checksum for infrastructure file
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "00-infrastructure/01-create-schemas.sql",
                infra_checksum,
                "2025-01-01",
            ),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg.return_value = mock_conn

        result = check_migration_status(
            "test",
            env="dev",
            migrations_dir=migrations_dir,
        )
        assert result.connected is True
        applied = [f for f in result.files if f.status == FileStatus.APPLIED]
        assert len(applied) >= 1

    @patch("cdc_generator.core.migration_status.get_pg_connection")
    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_modified_file_detected(
        self,
        mock_root: MagicMock,
        mock_pg: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Files with changed checksum are marked MODIFIED."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")

        # Return different checksum than actual file
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (
                "00-infrastructure/01-create-schemas.sql",
                "old_checksum_does_not_match",
                "2025-01-01",
            ),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg.return_value = mock_conn

        result = check_migration_status(
            "test",
            env="dev",
            migrations_dir=migrations_dir,
        )
        modified = [f for f in result.files if f.status == FileStatus.MODIFIED]
        assert len(modified) >= 1

    @patch("cdc_generator.core.migration_status.get_pg_connection")
    @patch("cdc_generator.core.migration_status.get_project_root")
    def test_connection_failure_falls_back(
        self,
        mock_root: MagicMock,
        mock_pg: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Falls back to offline mode when connection fails."""
        mock_root.return_value = tmp_path
        migrations_dir = _create_sink_structure(tmp_path / "migrations")
        mock_pg.side_effect = ValueError("cannot connect")

        result = check_migration_status(
            "test",
            env="dev",
            migrations_dir=migrations_dir,
        )
        # Should have errors but still have files (from offline fallback)
        assert len(result.errors) >= 1
        assert len(result.files) > 0
