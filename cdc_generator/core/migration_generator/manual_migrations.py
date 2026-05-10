"""Manual migration hint parsing and destructive-change detection helpers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

from .data_structures import (
    ExistingColumnDef,
    GenerationResult,
    ManualMigrationHints,
    MigrationColumn,
    RuntimeMode,
)

_DDL_COL_PATTERN = re.compile(
    r'^\s+"(?P<name>[^"]+)"\s+' + r"(?P<type>(?:double\s+precision|character\s+varying|[a-zA-Z][\w]*)(?:\([^)]*\))?)" + r"(?:\s+(?P<rest>[^,]*))?",
    re.MULTILINE,
)

_DDL_PK_PATTERN = re.compile(
    r"PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)",
    re.IGNORECASE,
)


def _normalize_type_for_compare(type_name: str) -> str:
    """Normalize SQL type text for robust destructive change comparison."""
    normalized = " ".join(type_name.casefold().split())
    if normalized.startswith("varchar"):
        normalized = normalized.replace("varchar", "character varying", 1)
    if normalized == "timestamp":
        normalized = "timestamp without time zone"
    if normalized == "timestamptz":
        normalized = "timestamp with time zone"
    return normalized


def extract_manual_migration_hints(table_cfg: dict[str, Any]) -> ManualMigrationHints:
    """Extract optional manual migration hints from sink table config."""
    hints_raw = table_cfg.get("manual_migration_hints")
    if not isinstance(hints_raw, dict):
        return ManualMigrationHints()

    hints = cast(dict[str, Any], hints_raw)
    parsed = ManualMigrationHints()

    renames_raw = hints.get("renames")
    if isinstance(renames_raw, list):
        for item in cast(list[Any], renames_raw):
            if not isinstance(item, dict):
                continue
            from_name = str(cast(dict[str, Any], item).get("from", "")).strip()
            to_name = str(cast(dict[str, Any], item).get("to", "")).strip()
            if from_name and to_name:
                parsed.renames.append((from_name, to_name))

    type_changes_raw = hints.get("type_changes")
    if isinstance(type_changes_raw, list):
        for item in cast(list[Any], type_changes_raw):
            if not isinstance(item, dict):
                continue
            change = cast(dict[str, Any], item)
            column_name = str(change.get("column", "")).strip()
            using_expr = str(change.get("using", "")).strip()
            if column_name and using_expr:
                parsed.type_casts[column_name.casefold()] = using_expr

    set_not_null_raw = hints.get("set_not_null")
    if isinstance(set_not_null_raw, list):
        for item in cast(list[Any], set_not_null_raw):
            if not isinstance(item, dict):
                continue
            entry = cast(dict[str, Any], item)
            column_name = str(entry.get("column", "")).strip()
            pre_sql = str(entry.get("pre_sql", "")).strip()
            if column_name and pre_sql:
                parsed.pre_not_null_sql[column_name.casefold()] = pre_sql

    return parsed


def _build_hint_sql_suggestions(
    target_schema: str,
    table_name: str,
    expected_columns: list[MigrationColumn],
    hints: ManualMigrationHints,
) -> list[str]:
    if not hints.renames and not hints.type_casts and not hints.pre_not_null_sql:
        return []

    table_qualified = f'"{target_schema}"."{table_name}"'
    expected_by_name = {column.name.casefold(): column for column in expected_columns}
    hint_sql: list[str] = []

    for from_name, to_name in hints.renames:
        hint_sql.append(f'ALTER TABLE {table_qualified} RENAME COLUMN "{from_name}" TO "{to_name}";')

    for column_name, using_expr in sorted(hints.type_casts.items()):
        expected = expected_by_name.get(column_name)
        target_type = expected.type if expected is not None else "/* TODO: target type */"
        rendered_column = expected.name if expected is not None else column_name
        hint_sql.append(f'ALTER TABLE {table_qualified} ALTER COLUMN "{rendered_column}" TYPE {target_type} ' + f"USING {using_expr};")

    for column_name, pre_sql in sorted(hints.pre_not_null_sql.items()):
        expected = expected_by_name.get(column_name)
        rendered_column = expected.name if expected is not None else column_name
        hint_sql.append(pre_sql)
        hint_sql.append(f'ALTER TABLE {table_qualified} ALTER COLUMN "{rendered_column}" SET NOT NULL;')

    return hint_sql


def _parse_existing_table_signature(
    sql_content: str,
) -> tuple[dict[str, ExistingColumnDef], set[str]] | None:
    create_match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]+\(\s*\n(.*?)\n\)",
        sql_content,
        re.DOTALL | re.IGNORECASE,
    )
    if create_match is None:
        return None

    block = create_match.group(1)
    pk_match = _DDL_PK_PATTERN.search(block)
    pk_names: set[str] = set()
    if pk_match is not None:
        pk_raw = pk_match.group("cols")
        for raw_name in pk_raw.split(","):
            pk_names.add(raw_name.strip().strip('"').casefold())

    parsed_columns: dict[str, ExistingColumnDef] = {}
    for match in _DDL_COL_PATTERN.finditer(block):
        name = match.group("name")
        rest = match.group("rest") or ""
        parsed_columns[name.casefold()] = ExistingColumnDef(
            name=name,
            type=match.group("type"),
            nullable="NOT NULL" not in rest.upper(),
        )

    if not parsed_columns:
        return None

    return parsed_columns, pk_names


def detect_destructive_changes(
    target_schema: str,
    table_name: str,
    existing_sql_path: Path,
    expected_columns: list[MigrationColumn],
    expected_primary_keys: list[str],
    hints: ManualMigrationHints,
) -> tuple[list[str], list[str]]:
    """Detect destructive or semantic changes vs previous generated table DDL."""
    if not existing_sql_path.exists():
        return [], []

    parsed = _parse_existing_table_signature(existing_sql_path.read_text(encoding="utf-8"))
    if parsed is None:
        return [], []

    existing_columns, existing_pk = parsed
    expected_by_name = {column.name.casefold(): column for column in expected_columns}
    expected_pk = {name.casefold() for name in expected_primary_keys}

    destructive_messages: list[str] = []
    suggested_sql: list[str] = []
    table_qualified = f'"{target_schema}"."{table_name}"'
    removed_columns: list[str] = []
    added_columns: list[str] = []

    for col_name, existing_col in existing_columns.items():
        expected_col = expected_by_name.get(col_name)
        if expected_col is None:
            removed_columns.append(existing_col.name)
            destructive_messages.append(f"COLUMN_REMOVED: {existing_col.name}")
            suggested_sql.append(f'ALTER TABLE {table_qualified} DROP COLUMN IF EXISTS "{existing_col.name}";')
            continue

        existing_type = _normalize_type_for_compare(existing_col.type)
        expected_type = _normalize_type_for_compare(expected_col.type)
        if existing_type != expected_type:
            destructive_messages.append("COLUMN_TYPE_CHANGED: " + f"{existing_col.name} ({existing_col.type} -> {expected_col.type})")
            suggested_sql.append(
                f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" '
                + f"TYPE {expected_col.type} USING /* TODO: safe cast expression */;"
            )

        if existing_col.nullable != expected_col.nullable:
            destructive_messages.append("COLUMN_NULLABILITY_CHANGED: " + f"{existing_col.name} ({existing_col.nullable} -> {expected_col.nullable})")
            if expected_col.nullable:
                suggested_sql.append(f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" DROP NOT NULL;')
            else:
                suggested_sql.append(f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" SET NOT NULL;')

    for col_name, expected_col in expected_by_name.items():
        if col_name not in existing_columns:
            added_columns.append(expected_col.name)

    if existing_pk != expected_pk:
        old_pk = ", ".join(sorted(existing_pk)) or "<none>"
        new_pk = ", ".join(sorted(expected_pk)) or "<none>"
        destructive_messages.append(f"PRIMARY_KEY_CHANGED: ({old_pk} -> {new_pk})")
        expected_pk_sql = ", ".join(f'"{name}"' for name in sorted(expected_pk))
        suggested_sql.append(f'ALTER TABLE {table_qualified} DROP CONSTRAINT IF EXISTS "{table_name}_pkey";')
        if expected_pk_sql:
            suggested_sql.append(f"ALTER TABLE {table_qualified} ADD PRIMARY KEY ({expected_pk_sql});")

    if len(removed_columns) == 1 and len(added_columns) == 1:
        destructive_messages.append("POSSIBLE_RENAME_HINT: " + f"{removed_columns[0]} -> {added_columns[0]} (verify manually)")
        suggested_sql.append(
            "-- Possible rename candidate:\n" + f'-- ALTER TABLE {table_qualified} RENAME COLUMN "{removed_columns[0]}" TO "{added_columns[0]}";'
        )

    hint_sql = _build_hint_sql_suggestions(
        target_schema,
        table_name,
        expected_columns,
        hints,
    )

    if hint_sql:
        prefixed_hint_sql = [
            "-- Hint-based SQL from services/<service>.yaml manual_migration_hints",
            *hint_sql,
            "",
            "-- Auto-detected fallback suggestions",
        ]
        merged = prefixed_hint_sql + suggested_sql
    else:
        merged = suggested_sql

    return destructive_messages, merged


def _manual_required_file_path(output_dir: Path, table_name: str) -> Path:
    return output_dir / "02-manual" / table_name / "MANUAL_REQUIRED.sql"


def write_manual_required_file(
    *,
    output_dir: Path,
    sink_name: str,
    table_name: str,
    reasons: list[str],
    suggested_sql: list[str],
    result: GenerationResult,
) -> None:
    """Create a table-scoped manual migration requirement file if missing."""
    if not reasons:
        return

    manual_path = _manual_required_file_path(output_dir, table_name)
    if manual_path.exists():
        result.warnings.append("Manual migration already exists for " + f"{sink_name}.{table_name}: {manual_path.relative_to(output_dir)}")
        return

    manual_path.parent.mkdir(parents=True, exist_ok=True)
    reasons_block = "\n".join(f"-- - {reason}" for reason in reasons)

    content = (
        "-- ============================================================================\n"
        "-- MANUAL MIGRATION REQUIRED\n"
        "-- Auto-created by: cdc manage-migrations generate\n"
        + f"-- Sink: {sink_name}\n"
        + f"-- Table: {table_name}\n"
        + "-- ============================================================================\n"
        + "-- Detected destructive/semantic changes:\n"
        + reasons_block
        + "\n\n"
        + "-- 1) Replace placeholders below with your real migration SQL\n"
        + "-- 2) Execute this manually in the target PostgreSQL database\n"
        + "-- 3) Keep this file as an audit artifact in git\n"
        + "-- 4) Then run: cdc manage-migrations generate && cdc manage-migrations apply\n\n"
        + "BEGIN;\n\n"
        + "-- Suggested SQL (review and adjust before executing):\n"
        + ("\n".join(suggested_sql) if suggested_sql else "-- TODO: write manual ALTER/UPDATE statements here")
        + "\n\nCOMMIT;\n"
    )

    manual_path.write_text(content, encoding="utf-8")
    result.files_written += 1
    result.warnings.append("Manual migration required for " + f"{sink_name}.{table_name}: {manual_path.relative_to(output_dir)}")


def remove_manual_required_file(output_dir: Path, table_name: str) -> None:
    """Remove stale manual-required artifacts for tables that no longer need them."""
    manual_dir = output_dir / "02-manual" / table_name
    manual_path = manual_dir / "MANUAL_REQUIRED.sql"
    if not manual_path.exists():
        return

    manual_path.unlink()
    if manual_dir.exists() and not any(manual_dir.iterdir()):
        manual_dir.rmdir()

    manual_root = output_dir / "02-manual"
    if manual_root.exists() and not any(manual_root.iterdir()):
        manual_root.rmdir()


def detect_removed_tables_for_manual_files(
    *,
    output_dir: Path,
    sink_name: str,
    sink_tables: dict[str, dict[str, Any]],
    result: GenerationResult,
    runtime_mode: RuntimeMode = "brokered",
) -> None:
    """Create manual-required files for tables removed from sink config."""
    tables_dir = output_dir / "01-tables"
    if not tables_dir.exists():
        return

    expected_table_names: set[str] = set()
    for sink_key, sink_cfg in sink_tables.items():
        if runtime_mode != "native" and bool(sink_cfg.get("target_exists", False)):
            continue
        table_name = sink_key.split(".", 1)[-1]
        expected_table_names.add(table_name.casefold())
        remove_manual_required_file(output_dir, table_name)

    existing_table_names: set[str] = set()
    existing_name_by_casefold: dict[str, str] = {}
    for sql_file in tables_dir.glob("*.sql"):
        if sql_file.stem.endswith("-staging"):
            continue
        key = sql_file.stem.casefold()
        existing_table_names.add(key)
        existing_name_by_casefold[key] = sql_file.stem

    for removed_key in sorted(existing_table_names - expected_table_names):
        removed_name = existing_name_by_casefold[removed_key]
        write_manual_required_file(
            output_dir=output_dir,
            sink_name=sink_name,
            table_name=removed_name,
            reasons=[f"TABLE_REMOVED: {removed_name} no longer present in sink config"],
            suggested_sql=[
                "-- Table removed from service config. Choose one:",
                f'-- DROP TABLE IF EXISTS "{removed_name}";',
                "-- or keep table and stop syncing it intentionally.",
            ],
            result=result,
        )
