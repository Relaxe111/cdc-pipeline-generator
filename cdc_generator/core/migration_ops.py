"""MSSQL CDC operations for source database management.

Provides functions to enable/disable CDC tracking on MSSQL source tables
and clean old CDC change tracking data.

Usage:
    >>> from cdc_generator.core.migration_ops import enable_cdc_tables
    >>> result = enable_cdc_tables("adopus", env="nonprod")

    >>> from cdc_generator.core.migration_ops import clean_cdc_data
    >>> result = clean_cdc_data("adopus", env="nonprod", days=30)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.helpers_mssql import get_mssql_connection
from cdc_generator.helpers.service_config import load_service_config

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class CdcOpsResult:
    """Result of a CDC operation on MSSQL.

    Attributes:
        enabled_count: Tables successfully CDC-enabled.
        already_enabled: Tables already had CDC enabled.
        cleaned_count: Tables whose CDC data was cleaned.
        errors: Error messages.
        tables: Tables that were processed.
    """

    enabled_count: int = 0
    already_enabled: int = 0
    cleaned_count: int = 0
    errors: list[str] = field(default_factory=list[str])
    tables: list[str] = field(default_factory=list[str])


# ---------------------------------------------------------------------------
# Source table extraction
# ---------------------------------------------------------------------------


def _get_source_tables(
    service_config: dict[str, object],
) -> list[tuple[str, str]]:
    """Extract source table references from the service config.

    Args:
        service_config: Full service config.

    Returns:
        List of (schema, table) tuples.
    """
    source_raw = service_config.get("source")
    if not isinstance(source_raw, dict):
        return []
    source = cast(dict[str, Any], source_raw)
    tables_raw = source.get("tables", {})
    if not isinstance(tables_raw, dict):
        return []

    result: list[tuple[str, str]] = []
    for table_key in cast(dict[str, Any], tables_raw):
        parts = str(table_key).split(".", 1)
        schema = parts[0] if len(parts) > 1 else "dbo"
        table = parts[-1]
        result.append((schema, table))

    return sorted(result)


# ---------------------------------------------------------------------------
# Enable CDC
# ---------------------------------------------------------------------------

_CHECK_CDC_SQL = """
    SELECT COUNT(*) FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = ? AND t.name = ? AND t.is_tracked_by_cdc = 1
"""

_ENABLE_CDC_SQL = """
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'{schema}',
        @source_name = N'{table}',
        @role_name = NULL,
        @supports_net_changes = 1
"""


def enable_cdc_tables(
    service_name: str = "adopus",
    *,
    env: str = "nonprod",
    table_filter: str | None = None,
    dry_run: bool = False,
) -> CdcOpsResult:
    """Enable CDC tracking on MSSQL source tables.

    For each source table in the service config, checks if CDC is already
    enabled and enables it if not.

    Args:
        service_name: Service name.
        env: MSSQL environment (local, nonprod, prod).
        table_filter: Optional table name filter.
        dry_run: If True, only list what would be enabled.

    Returns:
        CdcOpsResult with counts.

    Example:
        >>> result = enable_cdc_tables("adopus", env="nonprod")
        >>> print(f"Enabled {result.enabled_count} tables")
    """
    result = CdcOpsResult()

    print_header(f"Enable CDC for service: {service_name} (env: {env})")

    # Load service config
    try:
        service_config = load_service_config(service_name)
    except FileNotFoundError as e:
        result.errors.append(str(e))
        print_error(str(e))
        return result

    source_tables = _get_source_tables(service_config)
    if not source_tables:
        result.errors.append("No source tables found in service config")
        print_error(result.errors[-1])
        return result

    # Apply filter
    if table_filter:
        filter_lower = table_filter.casefold()
        source_tables = [
            (s, t) for s, t in source_tables
            if filter_lower in t.casefold()
        ]

    if dry_run:
        print_info(f"[DRY RUN] Would enable CDC on {len(source_tables)} tables:")
        for schema, table in source_tables:
            print_info(f"  [{schema}].[{table}]")
        return result

    # Connect to MSSQL
    try:
        conn, _ = get_mssql_connection(env)
    except (ValueError, Exception) as e:
        result.errors.append(f"MSSQL connection failed: {e}")
        print_error(result.errors[-1])
        return result

    try:
        cursor = conn.cursor()
        for schema, table in source_tables:
            result.tables.append(f"[{schema}].[{table}]")

            # Check if already enabled
            cursor.execute(_CHECK_CDC_SQL, (schema, table))
            row = cursor.fetchone()
            if row and row[0] > 0:
                result.already_enabled += 1
                print_info(f"  ● [{schema}].[{table}] — already enabled")
                continue

            # Enable CDC
            try:
                enable_sql = _ENABLE_CDC_SQL.format(schema=schema, table=table)
                cursor.execute(enable_sql)
                conn.commit()
                result.enabled_count += 1
                print_success(f"  ✓ [{schema}].[{table}] — CDC enabled")
            except Exception as e:
                result.errors.append(f"Failed to enable CDC on [{schema}].[{table}]: {e}")
                print_error(result.errors[-1])
                conn.rollback()

    finally:
        conn.close()

    print_info(
        f"\nSummary: {result.enabled_count} enabled, "
        + f"{result.already_enabled} already active, "
        + f"{len(result.errors)} errors",
    )

    return result


# ---------------------------------------------------------------------------
# Clean CDC data
# ---------------------------------------------------------------------------

_CLEAN_CDC_SQL = """
    DECLARE @threshold BINARY(10)
    SET @threshold = sys.fn_cdc_get_min_lsn('{schema}_{table}')

    IF @threshold IS NOT NULL
    BEGIN
        DECLARE @new_low BINARY(10)
        SET @new_low = sys.fn_cdc_map_time_to_lsn(
            'largest less than or equal',
            DATEADD(DAY, -{days}, GETDATE())
        )
        IF @new_low IS NOT NULL AND @new_low > @threshold
        BEGIN
            EXEC sys.sp_cdc_cleanup_change_table
                @capture_instance = N'{schema}_{table}',
                @low_water_mark = @new_low,
                @threshold = 5000
        END
    END
"""


def clean_cdc_data(
    service_name: str = "adopus",
    *,
    env: str = "nonprod",
    days: int = 30,
    table_filter: str | None = None,
    dry_run: bool = False,
) -> CdcOpsResult:
    """Clean old CDC change tracking data from MSSQL.

    Purges CDC entries older than the specified number of days for each
    source table. This frees up space used by the change tracking system.

    Args:
        service_name: Service name.
        env: MSSQL environment.
        days: Purge entries older than this many days.
        table_filter: Optional table name filter.
        dry_run: If True, only list what would be cleaned.

    Returns:
        CdcOpsResult with counts.

    Example:
        >>> result = clean_cdc_data("adopus", env="nonprod", days=30)
        >>> print(f"Cleaned {result.cleaned_count} tables")
    """
    result = CdcOpsResult()

    print_header(f"Clean CDC data for service: {service_name} (env: {env}, days: {days})")

    # Load service config
    try:
        service_config = load_service_config(service_name)
    except FileNotFoundError as e:
        result.errors.append(str(e))
        print_error(str(e))
        return result

    source_tables = _get_source_tables(service_config)
    if not source_tables:
        result.errors.append("No source tables found in service config")
        print_error(result.errors[-1])
        return result

    if table_filter:
        filter_lower = table_filter.casefold()
        source_tables = [
            (s, t) for s, t in source_tables
            if filter_lower in t.casefold()
        ]

    if dry_run:
        print_info(
            f"[DRY RUN] Would clean CDC data older than {days} days "
            + f"for {len(source_tables)} tables:",
        )
        for schema, table in source_tables:
            print_info(f"  [{schema}].[{table}]")
        return result

    # Connect to MSSQL
    try:
        conn, _ = get_mssql_connection(env)
    except (ValueError, Exception) as e:
        result.errors.append(f"MSSQL connection failed: {e}")
        print_error(result.errors[-1])
        return result

    try:
        cursor = conn.cursor()
        for schema, table in source_tables:
            result.tables.append(f"[{schema}].[{table}]")

            try:
                clean_sql = _CLEAN_CDC_SQL.format(
                    schema=schema, table=table, days=days,
                )
                cursor.execute(clean_sql)
                conn.commit()
                result.cleaned_count += 1
                print_info(f"  ✓ [{schema}].[{table}] — cleaned entries > {days} days")
            except Exception as e:
                result.errors.append(
                    f"Failed to clean CDC data for [{schema}].[{table}]: {e}",
                )
                print_error(result.errors[-1])
                conn.rollback()

    finally:
        conn.close()

    print_info(
        f"\nSummary: {result.cleaned_count} cleaned, {len(result.errors)} errors",
    )

    return result
