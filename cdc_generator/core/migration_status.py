"""Migration status reporter for CDC pipelines.

Connects to a target PostgreSQL database and reports which migration
files have been applied, which are pending, and whether any have been
modified since they were last applied (checksum mismatch).

Usage:
    >>> from cdc_generator.core.migration_status import check_migration_status
    >>> result = check_migration_status("adopus", env="dev")
    >>> print(f"Pending: {result.pending_count}, Applied: {result.applied_count}")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from cdc_generator.core.migration_apply import (
    compute_content_checksum,
    get_ordered_files,
    get_pg_connection,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root

if TYPE_CHECKING:
    from cdc_generator.helpers.psycopg2_stub import PgConnection


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class FileStatus(Enum):
    """Status of a migration file."""

    PENDING = "pending"
    APPLIED = "applied"
    MODIFIED = "modified"


@dataclass
class MigrationFileStatus:
    """Status of a single migration file.

    Attributes:
        file_name: Relative path within sink directory.
        status: Current status.
        applied_at: When it was last applied (if applicable).
        checksum: Current file checksum.
        applied_checksum: Checksum when applied (if applicable).
    """

    file_name: str
    status: FileStatus
    applied_at: str | None = None
    checksum: str = ""
    applied_checksum: str | None = None


@dataclass
class StatusResult:
    """Result of a migration status check.

    Attributes:
        files: Per-file status entries.
        errors: Error messages.
        connected: Whether the database connection succeeded.
    """

    files: list[MigrationFileStatus] = field(default_factory=list[MigrationFileStatus])
    errors: list[str] = field(default_factory=list[str])
    connected: bool = False

    @property
    def pending_count(self) -> int:
        """Number of files waiting to be applied."""
        return sum(1 for f in self.files if f.status == FileStatus.PENDING)

    @property
    def applied_count(self) -> int:
        """Number of files already applied."""
        return sum(1 for f in self.files if f.status == FileStatus.APPLIED)

    @property
    def modified_count(self) -> int:
        """Number of files modified since last apply."""
        return sum(1 for f in self.files if f.status == FileStatus.MODIFIED)


# ---------------------------------------------------------------------------
# Status check (with DB connection)
# ---------------------------------------------------------------------------

_FETCH_HISTORY_SQL = """
    SELECT "file_name", "checksum", "applied_at"::TEXT
    FROM "cdc_management"."migration_history"
"""


def _fetch_applied_migrations(
    conn: PgConnection,
) -> dict[str, tuple[str, str]]:
    """Fetch all previously applied migrations from the history table.

    Args:
        conn: PostgreSQL connection.

    Returns:
        Dict mapping file_name → (checksum, applied_at).
    """
    try:
        cursor = conn.cursor()
        cursor.execute(_FETCH_HISTORY_SQL)
        rows = cursor.fetchall()
        cursor.close()

        history: dict[str, tuple[str, str]] = {}
        for row in rows:
            if isinstance(row, tuple):
                file_name = str(row[0])
                checksum = str(row[1])
                applied_at = str(row[2])
            else:
                # Dict cursor
                file_name = str(row.get("file_name", ""))
                checksum = str(row.get("checksum", ""))
                applied_at = str(row.get("applied_at", ""))
            history[file_name] = (checksum, applied_at)
        return history

    except Exception:
        conn.rollback()
        return {}


# ---------------------------------------------------------------------------
# Status check (offline — no DB connection needed)
# ---------------------------------------------------------------------------


def check_migration_status_offline(
    *,
    migrations_dir: Path | None = None,
    sink_filter: str | None = None,
) -> StatusResult:
    """Check migration status without a database connection.

    Lists all migration files and marks them as 'pending' since we can't
    verify against the database. Useful for seeing what would be applied.

    Args:
        migrations_dir: Override migrations root (default: migrations/).
        sink_filter: Only check this sink target.

    Returns:
        StatusResult with all files marked as pending.
    """
    result = StatusResult()
    project_root = get_project_root()

    if migrations_dir is None:
        migrations_dir = project_root / "migrations"

    if not migrations_dir.exists():
        result.errors.append(f"Migrations directory not found: {migrations_dir}")
        return result

    sink_dirs = sorted(
        d for d in migrations_dir.iterdir()
        if d.is_dir() and (d / "manifest.yaml").exists()
    )

    for sink_dir in sink_dirs:
        sink_name = sink_dir.name
        if sink_filter and sink_filter != sink_name:
            continue

        ordered = get_ordered_files(sink_dir)
        for sql_file in ordered:
            rel = str(sql_file.relative_to(sink_dir))
            content = sql_file.read_text(encoding="utf-8")
            checksum = compute_content_checksum(content)
            result.files.append(MigrationFileStatus(
                file_name=f"{sink_name}/{rel}",
                status=FileStatus.PENDING,
                checksum=checksum,
            ))

    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def check_migration_status(
    service_name: str = "adopus",
    *,
    env: str = "dev",
    migrations_dir: Path | None = None,
    sink_filter: str | None = None,
) -> StatusResult:
    """Check which migrations have been applied to the target database.

    Connects to the target PostgreSQL database and compares the
    ``migration_history`` table against the local migration files.

    Args:
        service_name: Service name (for logging).
        env: Target environment (dev, stage, prod).
        migrations_dir: Override migrations root (default: migrations/).
        sink_filter: Only check this sink target.

    Returns:
        StatusResult with per-file status.

    Example:
        >>> result = check_migration_status("adopus", env="dev")
        >>> for f in result.files:
        ...     print(f"{f.status.value}: {f.file_name}")
    """
    result = StatusResult()
    project_root = get_project_root()

    if migrations_dir is None:
        migrations_dir = project_root / "migrations"

    if not migrations_dir.exists():
        result.errors.append(
            f"Migrations directory not found: {migrations_dir}. "
            + "Run 'cdc manage-migrations generate' first.",
        )
        print_error(result.errors[-1])
        return result

    print_header(f"Migration status for: {service_name} (env: {env})")

    sink_dirs = sorted(
        d for d in migrations_dir.iterdir()
        if d.is_dir() and (d / "manifest.yaml").exists()
    )

    for sink_dir in sink_dirs:
        sink_name = sink_dir.name
        if sink_filter and sink_filter != sink_name:
            continue

        print_info(f"Sink: {sink_name}")

        # Connect to target database
        try:
            conn = get_pg_connection(env, sink_name, migrations_dir)
            result.connected = True
        except (ValueError, Exception) as e:
            result.errors.append(f"Connection failed for {sink_name}: {e}")
            print_warning(f"  Cannot connect to {env} — showing offline status")
            # Fall back to offline mode for this sink
            offline = check_migration_status_offline(
                migrations_dir=migrations_dir,
                sink_filter=sink_name,
            )
            result.files.extend(offline.files)
            continue

        try:
            history = _fetch_applied_migrations(conn)
            ordered = get_ordered_files(sink_dir)

            for sql_file in ordered:
                rel = str(sql_file.relative_to(sink_dir))
                content = sql_file.read_text(encoding="utf-8")
                checksum = compute_content_checksum(content)

                if rel in history:
                    applied_checksum, applied_at = history[rel]
                    status = (
                        FileStatus.APPLIED if applied_checksum == checksum
                        else FileStatus.MODIFIED
                    )
                    result.files.append(MigrationFileStatus(
                        file_name=f"{sink_name}/{rel}",
                        status=status,
                        applied_at=applied_at,
                        checksum=checksum,
                        applied_checksum=applied_checksum,
                    ))
                else:
                    result.files.append(MigrationFileStatus(
                        file_name=f"{sink_name}/{rel}",
                        status=FileStatus.PENDING,
                        checksum=checksum,
                    ))

        finally:
            conn.close()

    # Print summary
    if result.files:
        pending = result.pending_count
        applied = result.applied_count
        modified = result.modified_count

        for f in result.files:
            icon = {"pending": "○", "applied": "●", "modified": "◐"}[f.status.value]
            suffix = ""
            if f.status == FileStatus.MODIFIED:
                suffix = " (checksum changed)"
            elif f.status == FileStatus.APPLIED and f.applied_at:
                suffix = f" ({f.applied_at})"
            print_info(f"  {icon} {f.file_name}{suffix}")

        print_info("")
        if pending > 0:
            print_warning(f"  {pending} pending, {applied} applied, {modified} modified")
        else:
            print_success(f"  All {applied} migrations applied")
            if modified > 0:
                print_warning(f"  {modified} file(s) modified since last apply")

    return result
