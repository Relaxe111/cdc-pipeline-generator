"""End-to-end regression tests for the migration system.

Tests the full generate → diff → status cycle:
1. Generate migrations from a mock project
2. Diff shows no changes (generated matches schema)
3. Offline status shows all files as PENDING
4. Regenerate matches (idempotent)
5. Modify schema YAML → diff detects column addition
6. Checksum round-trip: generate → verify → tamper → detect
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


_SERVICE_CONFIG: dict[str, object] = {
    "service": "e2e_test",
    "source": {
        "tables": {
            "dbo.Actor": {"primary_key": "actno"},
            "dbo.Role": {"primary_key": "roleid"},
        },
    },
    "sinks": {
        "sink_test.db": {
            "tables": {
                "myschema.Actor": {"from": "dbo.Actor"},
                "myschema.Role": {"from": "dbo.Role"},
            },
        },
    },
}

_TABLE_DEFS: dict[str, dict[str, Any]] = {
    "dbo.Actor": {
        "table": "Actor",
        "columns": [
            {"name": "actno", "type": "int", "nullable": False, "primary_key": True},
            {"name": "name", "type": "nvarchar", "nullable": True},
        ],
    },
    "dbo.Role": {
        "table": "Role",
        "columns": [
            {"name": "roleid", "type": "int", "nullable": False, "primary_key": True},
            {"name": "title", "type": "nvarchar", "nullable": True},
        ],
    },
}


def _setup_project(tmp_path: Path) -> None:
    """Create minimal project structure for migration generation."""
    # source-groups.yaml (pattern detection)
    (tmp_path / "source-groups.yaml").write_text(
        "test:\n  pattern: db-per-tenant\n  type: mssql\n"
        "  sources:\n    proxy:\n      schemas:\n        - dbo\n",
    )
    # sink-groups.yaml (database names)
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_test:\n  type: postgres\n  sources:\n"
        "    db:\n      dev:\n        database: test_dev\n",
    )
    # services/
    svc_dir = tmp_path / "services"
    svc_dir.mkdir()
    # Table definition YAMLs
    for schema_name in ("dbo",):
        schema_dir = tmp_path / "services" / "_schemas" / "e2e_test" / schema_name
        schema_dir.mkdir(parents=True)
        for table_key, table_def in _TABLE_DEFS.items():
            s, tname = table_key.split(".", 1)
            if s == schema_name:
                lines = [f"table: {tname}", "columns:"]
                for col in table_def["columns"]:
                    lines.append(f"  - name: {col['name']}")
                    lines.append(f"    type: {col['type']}")
                    if not col.get("nullable", True):
                        lines.append("    nullable: false")
                    if col.get("primary_key"):
                        lines.append("    primary_key: true")
                (schema_dir / f"{tname}.yaml").write_text("\n".join(lines) + "\n")


class _E2EPatchContext:
    """Manages all patches needed for E2E tests."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self._patches: list[Any] = []
        self._mocks: dict[str, MagicMock] = {}

    def start(self) -> None:
        """Start all patches."""
        schema_base = self.tmp_path / "services" / "_schemas" / "e2e_test"

        # Patch get_project_root everywhere
        for module in (
            "cdc_generator.core.migration_generator",
            "cdc_generator.core.migration_diff",
            "cdc_generator.core.migration_status",
            "cdc_generator.core.migration_apply",
        ):
            p = patch(f"{module}.get_project_root", return_value=self.tmp_path)
            self._patches.append(p)
            p.start()

        # Patch load_service_config for generator and diff
        for module in (
            "cdc_generator.core.migration_generator",
            "cdc_generator.core.migration_diff",
        ):
            p = patch(f"{module}.load_service_config", return_value=_SERVICE_CONFIG)
            self._patches.append(p)
            p.start()

        # Patch schema dirs for generator (it uses get_service_schema_read_dirs)
        p = patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        )
        self._patches.append(p)
        p.start()

        # Patch table definitions for diff (it imports load_table_definitions)
        p = patch(
            "cdc_generator.core.migration_diff.load_table_definitions",
            return_value=_TABLE_DEFS,
        )
        self._patches.append(p)
        p.start()

    def stop(self) -> None:
        """Stop all patches."""
        for p in reversed(self._patches):
            p.stop()
        self._patches.clear()


# ---------------------------------------------------------------------------
# E2E: Generate → Diff → Status cycle
# ---------------------------------------------------------------------------


class TestE2EGenerateDiffStatus:
    """Full generate → diff → status cycle."""

    def test_generate_then_diff_no_changes(self, tmp_path: Path) -> None:
        """After generation, diff reports no changes."""
        _setup_project(tmp_path)
        ctx = _E2EPatchContext(tmp_path)
        ctx.start()
        try:
            from cdc_generator.core.migration_generator import generate_migrations

            output = tmp_path / "migrations"
            gen_result = generate_migrations("e2e_test", output_dir=output)
            assert gen_result.errors == [], f"Gen errors: {gen_result.errors}"
            assert gen_result.files_written > 0

            # Now diff should find no changes
            from cdc_generator.core.migration_diff import diff_migrations

            diff_result = diff_migrations(
                "e2e_test", migrations_dir=output,
            )
            # Only tables without table defs show as added; with defs → no column changes
            column_changes = diff_result.column_changes
            assert column_changes == [], (
                f"Expected no column changes but got: {column_changes}"
            )
        finally:
            ctx.stop()

    def test_generate_then_offline_status(self, tmp_path: Path) -> None:
        """After generation, offline status shows all files as PENDING."""
        _setup_project(tmp_path)
        ctx = _E2EPatchContext(tmp_path)
        ctx.start()
        try:
            from cdc_generator.core.migration_generator import generate_migrations
            from cdc_generator.core.migration_status import (
                FileStatus,
                check_migration_status_offline,
            )

            output = tmp_path / "migrations"
            gen_result = generate_migrations("e2e_test", output_dir=output)
            assert gen_result.errors == []

            status = check_migration_status_offline(
                migrations_dir=output,
            )
            assert len(status.files) > 0
            assert all(f.status == FileStatus.PENDING for f in status.files)
        finally:
            ctx.stop()

    def test_idempotent_regeneration(self, tmp_path: Path) -> None:
        """Regeneration produces same file count."""
        _setup_project(tmp_path)
        ctx = _E2EPatchContext(tmp_path)
        ctx.start()
        try:
            from cdc_generator.core.migration_generator import generate_migrations

            output = tmp_path / "migrations"
            r1 = generate_migrations("e2e_test", output_dir=output)
            r2 = generate_migrations("e2e_test", output_dir=output)
            assert r1.files_written == r2.files_written
            assert r1.tables_processed == r2.tables_processed
        finally:
            ctx.stop()


# ---------------------------------------------------------------------------
# E2E: Checksum round-trip
# ---------------------------------------------------------------------------


class TestE2EChecksumRoundTrip:
    """Verify checksum integrity across generate/verify cycle."""

    def test_checksum_valid_after_generate(self, tmp_path: Path) -> None:
        """Generated files have valid checksums."""
        _setup_project(tmp_path)
        ctx = _E2EPatchContext(tmp_path)
        ctx.start()
        try:
            from cdc_generator.core.migration_apply import (
                compute_content_checksum,
                extract_checksum,
            )
            from cdc_generator.core.migration_generator import generate_migrations

            output = tmp_path / "migrations"
            generate_migrations("e2e_test", output_dir=output)

            # Check all SQL files in the output
            sql_files = list(output.rglob("*.sql"))
            assert len(sql_files) > 0

            for sql_file in sql_files:
                content = sql_file.read_text(encoding="utf-8")
                embedded = extract_checksum(content)
                computed = compute_content_checksum(content)
                assert embedded is not None, f"No checksum in {sql_file.name}"
                assert embedded == computed, (
                    f"Checksum mismatch in {sql_file.name}: "
                    f"embedded={embedded[:12]}... computed={computed[:12]}..."
                )
        finally:
            ctx.stop()

    def test_tampered_file_detected(self, tmp_path: Path) -> None:
        """Modifying file content makes checksum invalid."""
        _setup_project(tmp_path)
        ctx = _E2EPatchContext(tmp_path)
        ctx.start()
        try:
            from cdc_generator.core.migration_apply import (
                compute_content_checksum,
                extract_checksum,
            )
            from cdc_generator.core.migration_generator import generate_migrations

            output = tmp_path / "migrations"
            generate_migrations("e2e_test", output_dir=output)

            # Tamper with a generated file
            sql_files = list(output.rglob("*.sql"))
            target_file = sql_files[0]
            original = target_file.read_text(encoding="utf-8")
            tampered = original + "\n-- TAMPERED!\n"
            target_file.write_text(tampered, encoding="utf-8")

            # Checksum should no longer match
            embedded = extract_checksum(tampered)
            computed = compute_content_checksum(tampered)
            assert embedded is not None
            assert embedded != computed, "Tampered file should have mismatched checksum"
        finally:
            ctx.stop()


# ---------------------------------------------------------------------------
# E2E: Schema change detection
# ---------------------------------------------------------------------------


class TestE2ESchemaChangeDetection:
    """Test that schema changes are detected after regeneration."""

    def test_new_table_detected(self, tmp_path: Path) -> None:
        """Adding a table to config without generating detects TABLE_ADDED."""
        _setup_project(tmp_path)

        output = tmp_path / "migrations"

        # Generate with original config
        with (
            patch(
                "cdc_generator.core.migration_generator.get_project_root",
                return_value=tmp_path,
            ),
            patch(
                "cdc_generator.core.migration_generator.load_service_config",
                return_value=_SERVICE_CONFIG,
            ),
            patch(
                "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
                return_value=[
                    tmp_path / "services" / "_schemas" / "e2e_test",
                ],
            ),
        ):
            from cdc_generator.core.migration_generator import generate_migrations

            generate_migrations("e2e_test", output_dir=output)

        # Now diff with an extra table in config
        extended_config: dict[str, object] = {
            **_SERVICE_CONFIG,
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Actor": {"from": "dbo.Actor"},
                        "myschema.Role": {"from": "dbo.Role"},
                        "myschema.NewTable": {"from": "dbo.NewTable"},
                    },
                },
            },
        }

        with (
            patch(
                "cdc_generator.core.migration_diff.get_project_root",
                return_value=tmp_path,
            ),
            patch(
                "cdc_generator.core.migration_diff.load_service_config",
                return_value=extended_config,
            ),
            patch(
                "cdc_generator.core.migration_diff.load_table_definitions",
                return_value=_TABLE_DEFS,
            ),
        ):
            from cdc_generator.core.migration_diff import ChangeKind, diff_migrations

            diff_result = diff_migrations("e2e_test", migrations_dir=output)

        added = [c for c in diff_result.changes if c.kind == ChangeKind.TABLE_ADDED]
        assert any(c.table_name == "NewTable" for c in added)

    def test_removed_table_detected(self, tmp_path: Path) -> None:
        """Removing a table from config detects TABLE_REMOVED."""
        _setup_project(tmp_path)

        output = tmp_path / "migrations"

        # Generate with 2 tables
        with (
            patch(
                "cdc_generator.core.migration_generator.get_project_root",
                return_value=tmp_path,
            ),
            patch(
                "cdc_generator.core.migration_generator.load_service_config",
                return_value=_SERVICE_CONFIG,
            ),
            patch(
                "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
                return_value=[
                    tmp_path / "services" / "_schemas" / "e2e_test",
                ],
            ),
        ):
            from cdc_generator.core.migration_generator import generate_migrations

            generate_migrations("e2e_test", output_dir=output)

        # Diff with only 1 table (Role removed)
        reduced_config: dict[str, object] = {
            "service": "e2e_test",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Actor": {"from": "dbo.Actor"},
                    },
                },
            },
        }

        with (
            patch(
                "cdc_generator.core.migration_diff.get_project_root",
                return_value=tmp_path,
            ),
            patch(
                "cdc_generator.core.migration_diff.load_service_config",
                return_value=reduced_config,
            ),
            patch(
                "cdc_generator.core.migration_diff.load_table_definitions",
                return_value=_TABLE_DEFS,
            ),
        ):
            from cdc_generator.core.migration_diff import ChangeKind, diff_migrations

            diff_result = diff_migrations("e2e_test", migrations_dir=output)

        removed = [c for c in diff_result.changes if c.kind == ChangeKind.TABLE_REMOVED]
        assert any(c.table_name == "Role" for c in removed)
