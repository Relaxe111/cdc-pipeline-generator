"""Migration apply engine for CDC pipelines.

Connects to a target PostgreSQL database and applies pending migration
SQL files in the correct order, recording each applied file in the
``cdc_management.migration_history`` table.

Usage:
    >>> from cdc_generator.core.migration_apply import apply_migrations
    >>> result = apply_migrations("adopus", env="dev")
    >>> print(f"Applied {result.applied_count} migrations")

The execution order follows the directory/file naming convention:
    00-infrastructure/01-create-schemas.sql  (first)
    00-infrastructure/02-cdc-management.sql
    01-tables/Actor.sql
    01-tables/Actor-staging.sql
    ...

Each file is only applied once — the ``migration_history`` table tracks
file name + checksum so re-runs are safe (idempotent).
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cdc_generator.helpers.helpers_sink_groups import load_sink_groups
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import load_yaml_file

if TYPE_CHECKING:
    from cdc_generator.helpers.psycopg2_stub import PgConnection


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HISTORY_CHECK_SQL = """
    SELECT "checksum" FROM "cdc_management"."migration_history"
    WHERE "file_name" = %s AND ("schema_name" = %s OR "schema_name" IS NULL)
"""

_HISTORY_INSERT_SQL = """
    INSERT INTO "cdc_management"."migration_history"
        ("file_name", "checksum", "schema_name", "category")
    VALUES (%s, %s, %s, %s)
    ON CONFLICT ("file_name", "schema_name") DO UPDATE
        SET "checksum" = EXCLUDED."checksum",
            "applied_at" = NOW()
"""


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ApplyResult:
    """Result of a migration apply operation.

    Attributes:
        applied_count: Number of files successfully applied.
        skipped_count: Number of files skipped (already applied, unchanged).
        updated_count: Number of files re-applied (checksum changed).
        errors: Error messages.
        applied_files: Names of files that were applied.
    """

    applied_count: int = 0
    skipped_count: int = 0
    updated_count: int = 0
    errors: list[str] = field(default_factory=list[str])
    applied_files: list[str] = field(default_factory=list[str])


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def get_pg_connection(
    env: str,
    sink_name: str,
    migrations_dir: Path,
) -> PgConnection:
    """Create a PostgreSQL connection for the target environment.

    Resolves the database name from the manifest's sink_target.databases
    section, then connects using environment variables for credentials.

    Args:
        env: Environment name (dev, stage, prod, etc.).
        sink_name: Sink target name (e.g., 'sink_asma.directory').
        migrations_dir: Path to migrations root.

    Returns:
        psycopg2 connection.

    Raises:
        ValueError: If connection parameters are missing.
        PostgresNotAvailableError: If psycopg2 is not installed.
    """
    from cdc_generator.helpers.psycopg2_loader import create_postgres_connection

    # Read database name from manifest
    manifest_path = migrations_dir / sink_name / "manifest.yaml"
    db_name: str | None = None
    if manifest_path.exists():
        manifest = load_yaml_file(manifest_path)
        sink_target = cast(dict[str, Any], manifest).get("sink_target", {})
        if isinstance(sink_target, dict):
            databases = cast(dict[str, Any], sink_target).get("databases", {})
            if isinstance(databases, dict):
                db_name = str(cast(dict[str, Any], databases).get(env, ""))

    if not db_name:
        msg = (
            f"No database configured for env '{env}' in sink '{sink_name}'. "
            + f"Check manifest at {manifest_path}"
        )
        raise ValueError(msg)

    # Prefer sink-group server credentials from sink-groups.yaml
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None

    sink_target = cast(dict[str, Any], manifest).get("sink_target", {}) if manifest_path.exists() else {}
    sink_group_name = ""
    service_name = ""
    if isinstance(sink_target, dict):
        sink_group_name = str(cast(dict[str, Any], sink_target).get("sink_group", ""))
        service_name = str(cast(dict[str, Any], sink_target).get("service", ""))

    if sink_group_name and service_name:
        project_root = get_project_root()
        sink_groups_path = project_root / "sink-groups.yaml"
        if sink_groups_path.exists():
            sink_groups = load_sink_groups(sink_groups_path)
            sink_group = sink_groups.get(sink_group_name)
            if isinstance(sink_group, dict):
                sources = sink_group.get("sources", {})
                source_cfg = (
                    cast(dict[str, Any], sources).get(service_name, {})
                    if isinstance(sources, dict)
                    else {}
                )
                env_cfg = (
                    cast(dict[str, Any], source_cfg).get(env, {})
                    if isinstance(source_cfg, dict)
                    else {}
                )
                server_name = str(cast(dict[str, Any], env_cfg).get("server", ""))
                servers = sink_group.get("servers", {})
                server_cfg = (
                    cast(dict[str, Any], servers).get(server_name, {})
                    if isinstance(servers, dict)
                    else {}
                )

                if isinstance(server_cfg, dict) and server_cfg:
                    missing_env_vars: list[str] = []

                    def _resolve_value(raw: object) -> str:
                        value = str(raw or "").strip()
                        if value.startswith("${") and value.endswith("}"):
                            env_var = value[2:-1]
                            resolved = os.getenv(env_var, "").strip()
                            if not resolved:
                                missing_env_vars.append(env_var)
                            return resolved
                        return value

                    resolved_host = _resolve_value(server_cfg.get("host"))
                    resolved_port = _resolve_value(server_cfg.get("port"))
                    resolved_user = _resolve_value(server_cfg.get("user"))
                    resolved_password = _resolve_value(server_cfg.get("password"))

                    if missing_env_vars:
                        missing_joined = ", ".join(sorted(set(missing_env_vars)))
                        raise ValueError(
                            "Missing sink-group credential environment variables: "
                            + missing_joined,
                        )

                    if resolved_host and resolved_port and resolved_user:
                        host = resolved_host
                        port = int(resolved_port)
                        user = resolved_user
                        password = resolved_password

    # Fallback to generic PG_* credentials when sink-group credentials
    # are unavailable in current workspace configuration.
    if host is None or port is None or user is None or password is None:
        env_upper = env.upper()
        host = os.getenv(f"PG_{env_upper}_HOST", os.getenv("PG_HOST", "localhost"))
        port = int(os.getenv(f"PG_{env_upper}_PORT", os.getenv("PG_PORT", "5432")))
        user = os.getenv(f"PG_{env_upper}_USER", os.getenv("PG_USER", "postgres"))
        password = os.getenv(
            f"PG_{env_upper}_PASSWORD",
            os.getenv("PG_PASSWORD", ""),
        )

    return create_postgres_connection(
        host=host,
        port=port,
        dbname=db_name,
        user=user,
        password=password,
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------


def extract_checksum(content: str) -> str | None:
    """Extract the SHA256 checksum from a migration file header.

    Args:
        content: SQL file content.

    Returns:
        Hex digest string, or None if no checksum found.
    """
    for line in content.splitlines()[:10]:
        if line.startswith("-- Checksum: sha256:"):
            return line.split("sha256:", 1)[1].strip()
    return None


def compute_content_checksum(content: str) -> str:
    """Compute SHA256 of SQL body excluding checksum and header block.

    Args:
        content: SQL file content (may include checksum line).

    Returns:
        Hex digest.
    """
    lines = content.splitlines(keepends=True)
    filtered = [
        line for line in lines
        if not line.startswith("-- Checksum: sha256:")
    ]

    # Ignore auto-generated header (includes volatile Generated timestamp)
    # so only SQL body changes trigger migration updates.
    normalized = "".join(filtered)
    header_end = "-- " + "=" * 76
    first_sep = normalized.find(header_end)
    if first_sep != -1:
        second_sep = normalized.find(header_end, first_sep + len(header_end))
        if second_sep != -1:
            line_end = normalized.find("\n", second_sep)
            if line_end != -1:
                normalized = normalized[line_end + 1:]

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# File ordering
# ---------------------------------------------------------------------------


def get_ordered_files(sink_dir: Path) -> list[Path]:
    """Get migration files in execution order.

    Order:
        1. 00-infrastructure/01-create-schemas.sql
        2. 00-infrastructure/02-cdc-management.sql
        3. 01-tables/{Table}.sql (DDL first)
        4. 01-tables/{Table}-staging.sql (staging after DDL)

    Args:
        sink_dir: Sink-specific migration directory.

    Returns:
        Ordered list of SQL file paths.
    """
    files: list[Path] = []

    # Infrastructure files (sorted by filename)
    infra_dir = sink_dir / "00-infrastructure"
    if infra_dir.exists():
        for f in sorted(infra_dir.glob("*.sql")):
            files.append(f)

    # Table files — DDL first, then staging
    tables_dir = sink_dir / "01-tables"
    if tables_dir.exists():
        ddl_files: list[Path] = []
        staging_files: list[Path] = []
        for f in sorted(tables_dir.glob("*.sql")):
            if f.stem.endswith("-staging"):
                staging_files.append(f)
            else:
                ddl_files.append(f)
        files.extend(ddl_files)
        files.extend(staging_files)

    return files


def _categorize_file(file_path: Path) -> str:
    """Determine the migration category from the file path.

    Args:
        file_path: Path to the SQL file.

    Returns:
        Category string: 'infrastructure', 'table', or 'staging'.
    """
    if "00-infrastructure" in str(file_path):
        return "infrastructure"
    if file_path.stem.endswith("-staging"):
        return "staging"
    return "table"


# ---------------------------------------------------------------------------
# Apply logic
# ---------------------------------------------------------------------------


def _check_already_applied(
    conn: PgConnection,
    file_name: str,
    checksum: str,
) -> str:
    """Check if a migration file has already been applied.

    Args:
        conn: PostgreSQL connection.
        file_name: Migration file name.
        checksum: Expected checksum.

    Returns:
        'skip' if same checksum already applied,
        'update' if applied with different checksum,
        'new' if never applied.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(_HISTORY_CHECK_SQL, (file_name, None))
        row = cursor.fetchone()
        cursor.close()

        if row is None:
            return "new"

        existing_checksum = row[0] if isinstance(row, tuple) else row.get("checksum", "")
        if str(existing_checksum) == checksum:
            return "skip"
        return "update"
    except Exception:
        # Table might not exist yet (first run)
        conn.rollback()
        return "new"


def _record_applied(
    conn: PgConnection,
    file_name: str,
    checksum: str,
    category: str,
) -> None:
    """Record a successful migration in the history table.

    Args:
        conn: PostgreSQL connection.
        file_name: Migration file name.
        checksum: File checksum.
        category: Migration category.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(_HISTORY_INSERT_SQL, (file_name, checksum, None, category))
        conn.commit()
        cursor.close()
    except Exception:
        # History table may not exist yet — that's OK for infrastructure files
        conn.rollback()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _apply_sink(
    sink_dir: Path,
    env: str,
    migrations_dir: Path,
    dry_run: bool,
    result: ApplyResult,
) -> None:
    """Apply migrations for a single sink target.

    Mutates *result* in place with apply/skip/error counts.

    Args:
        sink_dir: Path to the sink's migration directory.
        env: Target environment.
        migrations_dir: Root migrations directory.
        dry_run: If True, list pending files without applying.
        result: ApplyResult to accumulate counts into.
    """
    sink_name = sink_dir.name
    print_info(f"Sink: {sink_name}")

    ordered_files = get_ordered_files(sink_dir)
    if not ordered_files:
        print_warning(f"  No SQL files found in {sink_dir}")
        return

    if dry_run:
        print_info(f"  [DRY RUN] {len(ordered_files)} files would be applied:")
        for f in ordered_files:
            rel = f.relative_to(sink_dir)
            print_info(f"    {rel}")
        return

    try:
        conn = get_pg_connection(env, sink_name, migrations_dir)
    except (ValueError, Exception) as e:
        result.errors.append(f"Connection failed for {sink_name}: {e}")
        print_error(result.errors[-1])
        return

    try:
        for sql_file in ordered_files:
            rel_name = str(sql_file.relative_to(sink_dir))
            content = sql_file.read_text(encoding="utf-8")
            checksum = compute_content_checksum(content)
            category = _categorize_file(sql_file)

            status = _check_already_applied(conn, rel_name, checksum)
            if status == "skip":
                result.skipped_count += 1
                continue

            try:
                cursor = conn.cursor()
                cursor.execute(content)
                conn.commit()
                cursor.close()

                _record_applied(conn, rel_name, checksum, category)

                if status == "update":
                    result.updated_count += 1
                    print_info(f"  ↻ Updated: {rel_name}")
                else:
                    result.applied_count += 1
                    print_info(f"  ✓ Applied: {rel_name}")

                result.applied_files.append(rel_name)

            except Exception as e:
                conn.rollback()
                result.errors.append(f"Failed to apply {rel_name}: {e}")
                print_error(result.errors[-1])
                break  # Stop on first error for this sink
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def apply_migrations(
    service_name: str = "adopus",
    *,
    env: str = "dev",
    dry_run: bool = False,
    migrations_dir: Path | None = None,
    sink_filter: str | None = None,
) -> ApplyResult:
    """Apply pending migrations to a target PostgreSQL database.

    Connects to the target database for each sink target found in the
    migrations directory, applies files in order, and records each
    applied file in the ``migration_history`` table.

    Args:
        service_name: Service name (for logging).
        env: Target environment (dev, stage, prod, etc.).
        dry_run: If True, list pending files without applying.
        migrations_dir: Override migrations root (default: migrations/).
        sink_filter: Only apply for this sink target.

    Returns:
        ApplyResult with counts and any errors.

    Example:
        >>> result = apply_migrations("adopus", env="dev")
        >>> print(f"Applied {result.applied_count} files")
    """
    result = ApplyResult()
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

    print_header(f"Applying migrations for: {service_name} (env: {env})")

    # Discover sink targets
    sink_dirs = sorted(
        d for d in migrations_dir.iterdir()
        if d.is_dir() and (d / "manifest.yaml").exists()
    )

    for sink_dir in sink_dirs:
        if sink_filter and sink_filter != sink_dir.name:
            continue
        _apply_sink(sink_dir, env, migrations_dir, dry_run, result)

    # Summary
    _print_apply_summary(result, dry_run=dry_run)

    return result


def _print_apply_summary(result: ApplyResult, *, dry_run: bool) -> None:
    """Print a human-readable summary of the apply result.

    Args:
        result: ApplyResult with accumulated counts.
        dry_run: Whether this was a dry run.
    """
    if not dry_run:
        total = result.applied_count + result.updated_count
        if total > 0:
            print_success(
                f"Applied {result.applied_count} new, {result.updated_count} updated, "
                + f"{result.skipped_count} skipped",
            )
        elif result.skipped_count > 0:
            print_success(
                f"All {result.skipped_count} migrations already applied — nothing to do",
            )

    if result.errors:
        for err in result.errors:
            print_error(err)
