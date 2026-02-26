"""Schema evolution diff engine for CDC migrations.

Compares current service-schema YAML definitions against the generated
migration SQL files to detect structural changes that require new
ALTER TABLE statements or full table regeneration.

Usage:
    >>> from cdc_generator.core.migration_diff import diff_migrations
    >>> result = diff_migrations("adopus")
    >>> for change in result.changes:
    ...     print(change)

Detects:
    - New tables (in service config but no migration file)
    - Removed tables (migration file exists but table removed from config)
    - New columns (column in schema YAML but not in generated DDL)
    - Removed columns (column in DDL but not in schema YAML)
    - Type changes (column type differs between YAML and DDL)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from cdc_generator.core.migration_generator import (
    build_full_column_list,
    get_sinks,
    load_table_definitions,
)
from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import (
    get_project_root,
    load_service_config,
)
from cdc_generator.helpers.type_mapper import TypeMapper

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class ChangeKind(Enum):
    """Type of schema change detected."""

    TABLE_ADDED = "table_added"
    TABLE_REMOVED = "table_removed"
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    PRIMARY_KEY_CHANGED = "primary_key_changed"


@dataclass
class SchemaChange:
    """A single detected schema change.

    Attributes:
        kind: Type of change.
        sink_name: Sink target name.
        table_name: Affected table.
        column_name: Affected column (if applicable).
        old_value: Previous value (type, PK list, etc.).
        new_value: New value.
        severity: 'info', 'warning', or 'breaking'.
    """

    kind: ChangeKind
    sink_name: str
    table_name: str
    column_name: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    severity: str = "info"

    def __str__(self) -> str:
        """Human-readable description."""
        parts = [f"[{self.severity.upper()}] {self.kind.value}: {self.table_name}"]
        if self.column_name:
            parts.append(f".{self.column_name}")
        if self.old_value and self.new_value:
            parts.append(f" ({self.old_value} → {self.new_value})")
        elif self.new_value:
            parts.append(f" ({self.new_value})")
        return "".join(parts)


@dataclass
class DiffContext:
    """Shared context for diff operations.

    Attributes:
        table_defs: Table definitions from service-schema YAML.
        type_mapper: Optional MSSQL→PG type converter.
        service_config: Full service config (for ignore_columns, etc.).
    """

    table_defs: dict[str, dict[str, Any]]
    type_mapper: TypeMapper | None
    service_config: dict[str, object]


@dataclass
class DiffResult:
    """Result of a migration diff operation.

    Attributes:
        changes: All detected schema changes.
        errors: Error messages encountered.
        tables_compared: Number of tables compared.
    """

    changes: list[SchemaChange] = field(default_factory=list[SchemaChange])
    errors: list[str] = field(default_factory=list[str])
    tables_compared: int = 0

    @property
    def has_changes(self) -> bool:
        """Whether any changes were detected."""
        return len(self.changes) > 0

    @property
    def added_tables(self) -> list[SchemaChange]:
        """Changes that are new tables."""
        return [c for c in self.changes if c.kind == ChangeKind.TABLE_ADDED]

    @property
    def removed_tables(self) -> list[SchemaChange]:
        """Changes that are removed tables."""
        return [c for c in self.changes if c.kind == ChangeKind.TABLE_REMOVED]

    @property
    def column_changes(self) -> list[SchemaChange]:
        """Changes that are column-level (add/remove/type)."""
        return [
            c for c in self.changes
            if c.kind in (
                ChangeKind.COLUMN_ADDED,
                ChangeKind.COLUMN_REMOVED,
                ChangeKind.COLUMN_TYPE_CHANGED,
            )
        ]


# ---------------------------------------------------------------------------
# DDL parsing — extract columns from existing .sql files
# ---------------------------------------------------------------------------

# Handles single-word types (varchar, integer) and multi-word (double precision)
_COL_PATTERN = re.compile(
    r'^\s+"(?P<name>[^"]+)"\s+'
    + r'(?P<type>(?:double\s+precision|character\s+varying|[a-zA-Z][\w]*)(?:\([^)]*\))?)'
    + r'(?:\s+(?P<rest>[^,]*))?',
    re.MULTILINE,
)

# Regex for PRIMARY KEY constraint
_PK_PATTERN = re.compile(
    r'PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)',
    re.IGNORECASE,
)


@dataclass
class ParsedColumn:
    """Column parsed from an existing CREATE TABLE statement.

    Attributes:
        name: Column name.
        type: PostgreSQL type string.
        nullable: Whether NULL is allowed.
        primary_key: Whether part of PK.
    """

    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False


def _parse_create_table(sql_content: str) -> list[ParsedColumn] | None:
    """Parse column definitions from a CREATE TABLE SQL file.

    Extracts column names, types, and primary keys from the DDL.
    Skips CDC metadata columns and staging columns.

    Args:
        sql_content: Contents of a {Table}.sql file.

    Returns:
        List of ParsedColumn, or None if no CREATE TABLE found.
    """
    # Find the CREATE TABLE block
    create_match = re.search(
        r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]+\(\s*\n(.*?)\n\)',
        sql_content,
        re.DOTALL | re.IGNORECASE,
    )
    if not create_match:
        return None

    block = create_match.group(1)

    # Extract primary key columns
    pk_match = _PK_PATTERN.search(block)
    pk_names: set[str] = set()
    if pk_match:
        pk_raw = pk_match.group("cols")
        pk_names = {
            name.strip().strip('"')
            for name in pk_raw.split(",")
        }

    # Extract columns
    columns: list[ParsedColumn] = []
    for match in _COL_PATTERN.finditer(block):
        name = match.group("name")
        col_type = match.group("type").upper()
        rest = match.group("rest") or ""

        # Skip PRIMARY KEY constraint line (safety check)
        if name.upper().startswith("PRIMARY"):
            continue

        nullable = "NOT NULL" not in rest.upper()
        columns.append(ParsedColumn(
            name=name,
            type=col_type,
            nullable=nullable,
            primary_key=name in pk_names,
        ))

    return columns if columns else None


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------


def _compare_columns(
    *,
    sink_name: str,
    table_name: str,
    expected: list[ParsedColumn],
    generated: list[ParsedColumn],
) -> list[SchemaChange]:
    """Compare expected (from schema YAML) vs generated (from SQL) columns.

    Args:
        sink_name: Sink target name.
        table_name: Table being compared.
        expected: Columns from current schema YAML.
        generated: Columns parsed from existing SQL.

    Returns:
        List of schema changes detected.
    """
    changes: list[SchemaChange] = []
    gen_by_name = {c.name: c for c in generated}
    exp_by_name = {c.name: c for c in expected}

    # New columns (in expected but not generated)
    for name, col in exp_by_name.items():
        if name not in gen_by_name:
            changes.append(SchemaChange(
                kind=ChangeKind.COLUMN_ADDED,
                sink_name=sink_name,
                table_name=table_name,
                column_name=name,
                new_value=col.type,
                severity="info",
            ))

    # Removed columns (in generated but not expected)
    for name, col in gen_by_name.items():
        if name not in exp_by_name:
            changes.append(SchemaChange(
                kind=ChangeKind.COLUMN_REMOVED,
                sink_name=sink_name,
                table_name=table_name,
                column_name=name,
                old_value=col.type,
                severity="warning",
            ))

    # Type changes
    for name, exp_col in exp_by_name.items():
        gen_col = gen_by_name.get(name)
        if gen_col is not None:
            exp_type = exp_col.type.upper()
            gen_type = gen_col.type.upper()
            if exp_type != gen_type:
                changes.append(SchemaChange(
                    kind=ChangeKind.COLUMN_TYPE_CHANGED,
                    sink_name=sink_name,
                    table_name=table_name,
                    column_name=name,
                    old_value=gen_type,
                    new_value=exp_type,
                    severity="breaking",
                ))

    return changes


# ---------------------------------------------------------------------------
# Per-sink diff helper
# ---------------------------------------------------------------------------


def _diff_sink(
    sink_name: str,
    sink_tables: dict[str, dict[str, Any]],
    sink_dir: Path,
    table_filter: str | None,
    result: DiffResult,
    ctx: DiffContext,
) -> None:
    """Compare a single sink target against its generated SQL files.

    Detects new/removed tables and column-level changes for existing tables.
    Mutates *result* in place.

    Args:
        sink_name: Sink target name.
        sink_tables: Table configs for this sink.
        sink_dir: Path to generated migration dir for this sink.
        table_filter: Optional table name filter.
        result: DiffResult to accumulate changes into.
        ctx: Shared diff context (table defs, type mapper, service config).
    """
    if not sink_dir.exists():
        # Entire sink is new — all tables are new
        for table_key in sorted(sink_tables):
            if table_filter and table_filter.casefold() not in table_key.casefold():
                continue
            table_name = table_key.split(".", 1)[-1]
            result.changes.append(SchemaChange(
                kind=ChangeKind.TABLE_ADDED,
                sink_name=sink_name,
                table_name=table_name,
                severity="info",
            ))
        return

    tables_dir = sink_dir / "01-tables"

    # Build set of expected tables from config
    expected_tables = _build_expected_tables(sink_tables, table_filter)

    # Check for existing SQL files (generated tables)
    generated_tables: set[str] = set()
    if tables_dir.exists():
        for sql_file in tables_dir.glob("*.sql"):
            name = sql_file.stem
            if not name.endswith("-staging"):
                generated_tables.add(name)

    # Detect new tables
    for table_name in sorted(expected_tables):
        if table_name not in generated_tables:
            result.changes.append(SchemaChange(
                kind=ChangeKind.TABLE_ADDED,
                sink_name=sink_name,
                table_name=table_name,
                severity="info",
            ))

    # Detect removed tables (warn only, never auto-drop)
    for table_name in sorted(generated_tables):
        if table_name not in expected_tables:
            result.changes.append(SchemaChange(
                kind=ChangeKind.TABLE_REMOVED,
                sink_name=sink_name,
                table_name=table_name,
                severity="warning",
            ))

    # Compare columns for existing tables
    _diff_columns(
        sink_name=sink_name,
        sink_tables=sink_tables,
        expected_tables=expected_tables,
        tables_dir=tables_dir,
        result=result,
        ctx=ctx,
    )


def _build_expected_tables(
    sink_tables: dict[str, dict[str, Any]],
    table_filter: str | None,
) -> dict[str, tuple[str, str]]:
    """Build map of expected table_name → (from_ref, sink_key) from sink config.

    Args:
        sink_tables: Table configs for a sink.
        table_filter: Optional table name filter.

    Returns:
        Dict mapping table_name → (source_reference, sink_key).
    """
    expected: dict[str, tuple[str, str]] = {}
    for table_key, table_cfg in sink_tables.items():
        if bool(table_cfg.get("target_exists", False)):
            continue
        table_name = table_key.split(".", 1)[-1]
        from_ref = str(table_cfg.get("from", ""))
        if table_filter and table_filter.casefold() not in table_name.casefold():
            continue
        expected[table_name] = (from_ref, table_key)
    return expected


def _diff_columns(
    *,
    sink_name: str,
    sink_tables: dict[str, dict[str, Any]],
    expected_tables: dict[str, tuple[str, str]],
    tables_dir: Path,
    result: DiffResult,
    ctx: DiffContext,
) -> None:
    """Compare columns for tables that exist in both config and SQL.

    Uses the full column pipeline (including ignore_columns, column
    templates, and CDC metadata) so the expected columns exactly match
    what the generator would produce.

    Args:
        sink_name: Sink target name.
        sink_tables: Full sink table configs for looking up column_templates.
        expected_tables: table_name → (from_ref, sink_key) mapping.
        tables_dir: Directory containing generated SQL files.
        result: DiffResult to accumulate changes into.
        ctx: Shared diff context (table defs, type mapper, service config).
    """
    for table_name, (from_ref, sink_key) in sorted(expected_tables.items()):
        sql_file = tables_dir / f"{table_name}.sql"
        if not sql_file.exists():
            continue  # Already reported as TABLE_ADDED

        # Parse generated DDL
        sql_content = sql_file.read_text(encoding="utf-8")
        generated_cols = _parse_create_table(sql_content)
        if generated_cols is None:
            result.errors.append(
                f"Could not parse CREATE TABLE from {sql_file.name}",
            )
            continue

        # Build expected columns using the full generation pipeline
        table_def = ctx.table_defs.get(from_ref)
        if table_def is None:
            continue

        sink_cfg = sink_tables.get(sink_key, {})
        expected_cols_raw, _ = build_full_column_list(
            table_def,
            sink_cfg=sink_cfg,
            service_config=ctx.service_config,
            source_key=from_ref,
            type_mapper=ctx.type_mapper,
        )
        expected_cols = [
            ParsedColumn(
                name=c.name,
                type=c.type.upper(),
                nullable=c.nullable,
                primary_key=c.primary_key,
            )
            for c in expected_cols_raw
        ]

        changes = _compare_columns(
            sink_name=sink_name,
            table_name=table_name,
            expected=expected_cols,
            generated=generated_cols,
        )
        result.changes.extend(changes)
        result.tables_compared += 1


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def diff_migrations(
    service_name: str = "adopus",
    *,
    migrations_dir: Path | None = None,
    table_filter: str | None = None,
) -> DiffResult:
    """Compare service-schema definitions against generated migrations.

    Reads the current service-schema YAML files and compares them to
    the SQL DDL in the migrations/ output directory to detect changes.

    Args:
        service_name: Service name.
        migrations_dir: Override migrations root (default: migrations/).
        table_filter: Optional table name filter.

    Returns:
        DiffResult with all detected changes.

    Example:
        >>> result = diff_migrations("adopus")
        >>> print(f"{len(result.changes)} changes detected")
    """
    result = DiffResult()
    project_root = get_project_root()

    if migrations_dir is None:
        migrations_dir = project_root / "migrations"

    print_header(f"Comparing schema definitions for service: {service_name}")

    # 1. Load service config
    try:
        service_config = load_service_config(service_name)
    except FileNotFoundError as e:
        result.errors.append(str(e))
        print_error(str(e))
        return result

    # 2. Load current table definitions
    table_defs = load_table_definitions(service_name, project_root)

    # 3. Initialize type mapper
    try:
        type_mapper: TypeMapper | None = TypeMapper("mssql", "pgsql")
    except FileNotFoundError:
        type_mapper = None

    # 4. Get sinks from service config
    sinks = get_sinks(service_config)
    if not sinks:
        result.errors.append("No sinks found in service config")
        print_error(result.errors[-1])
        return result

    # 5. Build shared context
    ctx = DiffContext(
        table_defs=table_defs,
        type_mapper=type_mapper,
        service_config=service_config,
    )

    # 6. Compare per sink
    for sink_name, sink_tables in sorted(sinks.items()):
        sink_dir = migrations_dir / sink_name
        _diff_sink(
            sink_name=sink_name,
            sink_tables=sink_tables,
            sink_dir=sink_dir,
            table_filter=table_filter,
            result=result,
            ctx=ctx,
        )

    # Print summary
    _print_diff_summary(result)

    return result


def _print_diff_summary(result: DiffResult) -> None:
    """Print a human-readable summary of the diff result.

    Args:
        result: DiffResult with accumulated changes.
    """
    if result.has_changes:
        print_warning(f"Found {len(result.changes)} change(s):")
        for change in result.changes:
            severity_fn = (
                print_error if change.severity == "breaking"
                else print_warning if change.severity == "warning"
                else print_info
            )
            severity_fn(f"  {change}")
    else:
        print_success("No schema changes detected — migrations are up to date")
