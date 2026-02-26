"""Unit tests for migration_apply.py.

Covers:
- ApplyResult dataclass defaults
- extract_checksum (header parsing)
- compute_content_checksum (SHA256 excluding checksum line)
- get_ordered_files (infrastructure-first, DDL before staging)
- _categorize_file (infrastructure / table / staging)
- apply_migrations (with mocked PG connection, dry-run, error paths)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from cdc_generator.core.migration_apply import (
    ApplyResult,
    _categorize_file,
    apply_migrations,
    compute_content_checksum,
    extract_checksum,
    get_ordered_files,
)

# ---------------------------------------------------------------------------
# ApplyResult defaults
# ---------------------------------------------------------------------------


class TestApplyResult:
    """Verify ApplyResult dataclass defaults."""

    def test_defaults(self) -> None:
        r = ApplyResult()
        assert r.applied_count == 0
        assert r.skipped_count == 0
        assert r.updated_count == 0
        assert r.errors == []
        assert r.applied_files == []


# ---------------------------------------------------------------------------
# extract_checksum
# ---------------------------------------------------------------------------


class TestExtractChecksum:
    """Test checksum extraction from SQL file content."""

    def test_extracts_from_header(self) -> None:
        content = (
            "-- header\n"
            "-- Checksum: sha256:abc123def456\n"
            "\nSELECT 1;\n"
        )
        assert extract_checksum(content) == "abc123def456"

    def test_returns_none_without_checksum(self) -> None:
        content = "-- header\nSELECT 1;\n"
        assert extract_checksum(content) is None

    def test_only_checks_first_10_lines(self) -> None:
        """Checksum line beyond line 10 is not found."""
        lines = ["-- line " + str(i) for i in range(15)]
        lines.append("-- Checksum: sha256:hidden")
        content = "\n".join(lines) + "\n"
        assert extract_checksum(content) is None

    def test_line_1_checksum(self) -> None:
        """Checksum on first line is extracted."""
        content = "-- Checksum: sha256:firstline\nSELECT 1;\n"
        assert extract_checksum(content) == "firstline"


# ---------------------------------------------------------------------------
# compute_content_checksum
# ---------------------------------------------------------------------------


class TestComputeContentChecksum:
    """Test SHA256 computation with checksum-line exclusion."""

    def test_deterministic(self) -> None:
        content = "SELECT 1;\n"
        a = compute_content_checksum(content)
        b = compute_content_checksum(content)
        assert a == b
        assert len(a) == 64  # SHA256 hex

    def test_excludes_checksum_line(self) -> None:
        """Checksum line is filtered out before hashing."""
        base = "-- header\nSELECT 1;\n"
        with_checksum = "-- Checksum: sha256:whatever\n-- header\nSELECT 1;\n"
        assert compute_content_checksum(base) == compute_content_checksum(with_checksum)

    def test_different_content(self) -> None:
        assert compute_content_checksum("A") != compute_content_checksum("B")


# ---------------------------------------------------------------------------
# get_ordered_files
# ---------------------------------------------------------------------------


class TestGetOrderedFiles:
    """Test file ordering: infrastructure → DDL → staging."""

    def _create_sink_dir(self, tmp_path: Path) -> Path:
        """Create a realistic sink directory structure."""
        sink = tmp_path / "sink_test.db"

        infra = sink / "00-infrastructure"
        infra.mkdir(parents=True)
        (infra / "01-create-schemas.sql").write_text("-- schemas\n")
        (infra / "02-cdc-management.sql").write_text("-- mgmt\n")

        tables = sink / "01-tables"
        tables.mkdir(parents=True)
        (tables / "Actor.sql").write_text("-- DDL\n")
        (tables / "Actor-staging.sql").write_text("-- staging\n")
        (tables / "Role.sql").write_text("-- DDL\n")
        (tables / "Role-staging.sql").write_text("-- staging\n")

        return sink

    def test_order_infra_first(self, tmp_path: Path) -> None:
        """Infrastructure files come before table files."""
        sink = self._create_sink_dir(tmp_path)
        ordered = get_ordered_files(sink)
        names = [f.name for f in ordered]
        assert names.index("01-create-schemas.sql") < names.index("Actor.sql")
        assert names.index("02-cdc-management.sql") < names.index("Actor.sql")

    def test_order_ddl_before_staging(self, tmp_path: Path) -> None:
        """DDL files come before staging files."""
        sink = self._create_sink_dir(tmp_path)
        ordered = get_ordered_files(sink)
        names = [f.name for f in ordered]
        assert names.index("Actor.sql") < names.index("Actor-staging.sql")
        assert names.index("Role.sql") < names.index("Role-staging.sql")

    def test_all_ddl_before_all_staging(self, tmp_path: Path) -> None:
        """All DDL files are grouped before all staging files."""
        sink = self._create_sink_dir(tmp_path)
        ordered = get_ordered_files(sink)
        names = [f.name for f in ordered]
        # All DDL should be before any staging
        last_ddl_idx = max(
            i for i, n in enumerate(names)
            if not n.endswith("-staging.sql") and "tables" in str(ordered[i])
        )
        first_staging_idx = min(
            i for i, n in enumerate(names)
            if n.endswith("-staging.sql")
        )
        assert last_ddl_idx < first_staging_idx

    def test_empty_dir(self, tmp_path: Path) -> None:
        """Returns empty list for non-existent directory."""
        assert get_ordered_files(tmp_path / "nonexistent") == []

    def test_count(self, tmp_path: Path) -> None:
        """Returns all 6 files."""
        sink = self._create_sink_dir(tmp_path)
        assert len(get_ordered_files(sink)) == 6


# ---------------------------------------------------------------------------
# _categorize_file
# ---------------------------------------------------------------------------


class TestCategorizeFile:
    """Test migration file categorization."""

    def test_infrastructure(self) -> None:
        p = Path("migrations/sink/00-infrastructure/01-schemas.sql")
        assert _categorize_file(p) == "infrastructure"

    def test_staging(self) -> None:
        p = Path("migrations/sink/01-tables/Actor-staging.sql")
        assert _categorize_file(p) == "staging"

    def test_table(self) -> None:
        p = Path("migrations/sink/01-tables/Actor.sql")
        assert _categorize_file(p) == "table"


# ---------------------------------------------------------------------------
# apply_migrations
# ---------------------------------------------------------------------------


class TestApplyMigrations:
    """Test the main apply entry point."""

    @patch("cdc_generator.core.migration_apply.get_project_root")
    def test_no_migrations_dir(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when migrations directory doesn't exist."""
        mock_root.return_value = tmp_path
        result = apply_migrations(
            "test",
            migrations_dir=tmp_path / "nonexistent",
        )
        assert len(result.errors) == 1
        assert "not found" in result.errors[0].lower()

    @patch("cdc_generator.core.migration_apply.get_project_root")
    def test_dry_run_lists_files(
        self,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Dry run lists files without applying."""
        mock_root.return_value = tmp_path

        # Create migration structure with manifest
        sink_dir = tmp_path / "migrations" / "sink_test.db"
        infra = sink_dir / "00-infrastructure"
        infra.mkdir(parents=True)
        (infra / "01-create-schemas.sql").write_text("-- schemas\n")
        (sink_dir / "manifest.yaml").write_text(
            "sink_target:\n  databases:\n    dev: test_db\n",
        )

        result = apply_migrations(
            "test",
            dry_run=True,
            migrations_dir=tmp_path / "migrations",
        )
        # Dry run does not apply, no errors expected
        assert result.applied_count == 0

    @patch("cdc_generator.core.migration_apply.get_project_root")
    @patch("cdc_generator.core.migration_apply.get_pg_connection")
    def test_apply_new_file(
        self,
        mock_pg: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Applies a new migration file to PG."""
        mock_root.return_value = tmp_path

        # Mock PG connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # file not applied yet
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg.return_value = mock_conn

        # Create migration files
        sink_dir = tmp_path / "migrations" / "sink_test.db"
        infra = sink_dir / "00-infrastructure"
        infra.mkdir(parents=True)
        (infra / "01-create-schemas.sql").write_text(
            "-- Checksum: sha256:abc\nCREATE SCHEMA test;\n",
        )
        (sink_dir / "manifest.yaml").write_text(
            "sink_target:\n  databases:\n    dev: test_db\n",
        )

        result = apply_migrations(
            "test",
            env="dev",
            migrations_dir=tmp_path / "migrations",
        )
        assert result.applied_count >= 1
        assert result.errors == []
        mock_conn.close.assert_called_once()
