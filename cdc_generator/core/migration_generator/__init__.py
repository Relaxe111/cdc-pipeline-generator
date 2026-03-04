"""Migration SQL generator for CDC pipelines.

Generates PostgreSQL migration files from service config and table definitions.
Organises output per sink target from the service config:
    migrations/
    └── {sink_name}/                 (e.g., sink_asma.directory/)
        ├── 00-infrastructure/
        │   ├── 01-create-schemas.sql
        │   └── 02-cdc-management.sql
        ├── 01-tables/
        │   ├── Actor.sql
        │   ├── Actor-staging.sql
        │   └── ...per table
        └── manifest.yaml

Uses Jinja2 templates from cdc_generator/templates/migrations/.

Example:
    >>> from cdc_generator.core.migration_generator import generate_migrations
    >>> result = generate_migrations("adopus")
    >>> print(result.files_written)
    12
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from jinja2 import Environment, FileSystemLoader

from cdc_generator.core.column_template_operations import (
    resolve_column_templates,
    resolve_transforms,
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
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.type_mapper import TypeMapper
from cdc_generator.helpers.yaml_loader import load_yaml_file

from .data_structures import (
    ExistingColumnDef,
    GenerationResult,
    ManualMigrationHints,
    MigrationColumn,
    RenderContext,
    ServiceData,
    SinkTarget,
    TableMigration,
)
from .file_writers import (
    _compute_checksum,
    _inject_checksum,
    _normalize_sql_for_compare,
    _write_manifest,
    _write_migration_file,
)
from .helpers import (
    CDC_METADATA_COLUMNS,
    DO_NOT_EDIT_HEADER,
    TEMPLATES_DIR,
    _add_cdc_metadata_columns,
    _add_column_template_columns,
    _create_jinja_env,
    _dedupe_names_case_insensitive,
    _extract_manual_migration_hints,
    _load_source_groups,
    _normalize_type_for_compare,
    build_columns_from_table_def,
    load_table_definitions,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DB_USER = "postgres"

# Max table names to show in 'no schema definition' warning
_MAX_WARNING_TABLES = 10

_DDL_COL_PATTERN = re.compile(
    r'^\s+"(?P<name>[^"]+)"\s+'
    + r'(?P<type>(?:double\s+precision|character\s+varying|[a-zA-Z][\w]*)(?:\([^)]*\))?)'
    + r'(?:\s+(?P<rest>[^,]*))?',
    re.MULTILINE,
)

_DDL_PK_PATTERN = re.compile(
    r'PRIMARY\s+KEY\s*\((?P<cols>[^)]+)\)',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Table definition loading
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Column building
# ---------------------------------------------------------------------------


def build_columns_from_table_def(
    table_def: dict[str, Any],
    ignore_columns: list[str] | None = None,
    type_mapper: TypeMapper | None = None,
) -> tuple[list[MigrationColumn], list[str]]:
    """Build column list from a service-schema YAML.

    Supports both formats:
    - services/_schemas format: ``columns[].name``, ``columns[].type`` (MSSQL types)
    - generated/table-definitions format: ``fields[].postgres``, ``fields[].type`` (PG types)

    When reading MSSQL types from _schemas, uses TypeMapper to convert to PG types.

    Args:
        table_def: Parsed table definition dict.
        ignore_columns: Column names to exclude.
        type_mapper: Optional type mapper for MSSQL→PG conversion.

    Returns:
        Tuple of (columns, primary_keys).
    """
    ignore_set = {c.casefold() for c in (ignore_columns or [])}

    # Detect format: _schemas uses 'columns', table-definitions uses 'fields'
    columns_raw = table_def.get("columns", table_def.get("fields", []))
    if not isinstance(columns_raw, list):
        return [], []

    # Detect which format we're reading:
    # _schemas format has 'columns' key and MSSQL types
    is_schemas_format = "columns" in table_def

    columns: list[MigrationColumn] = []
    primary_keys: list[str] = []
    seen_column_names: set[str] = set()

    for field_entry in cast(list[object], columns_raw):
        if not isinstance(field_entry, dict):
            continue
        f = cast(dict[str, Any], field_entry)

        # Column name: _schemas uses 'name', table-definitions may use 'postgres'
        pg_name = str(f.get("name", f.get("postgres", "")))
        if not pg_name:
            continue
        if pg_name.casefold() in ignore_set:
            continue
        column_key = pg_name.casefold()
        if column_key in seen_column_names:
            continue
        seen_column_names.add(column_key)

        # Type: _schemas has MSSQL types that need mapping, table-defs have PG types
        raw_type = str(f.get("type", "TEXT"))
        pg_type = (
            type_mapper.map_type(raw_type)
            if is_schemas_format and type_mapper is not None
            else raw_type
        )

        col = MigrationColumn(
            name=pg_name,
            type=pg_type,
            nullable=bool(f.get("nullable", True)),
            primary_key=bool(f.get("primary_key", False)),
        )
        columns.append(col)
        if col.primary_key:
            primary_keys.append(pg_name)

    return columns, _dedupe_names_case_insensitive(primary_keys)


def _add_column_template_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add columns from column_templates to the column list.

    Args:
        columns: Existing column list.
        table_cfg: Sink table config with column_templates.

    Returns:
        Extended column list with template columns appended.
    """
    resolved = resolve_column_templates(table_cfg)
    existing_names = {c.name for c in columns}

    for r in resolved:
        if r.name in existing_names:
            continue
        col = MigrationColumn(
            name=r.name,
            type=r.template.column_type.upper(),
            nullable=not r.template.not_null,
            default=r.template.default,
        )
        columns.append(col)

    return columns


def _add_transform_output_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add columns produced by transforms without altering configured names.

    For migration DDL, transform outputs are represented as nullable TEXT
    columns unless already present from source/table templates.

    Args:
        columns: Existing column list.
        table_cfg: Sink table config with transforms.

    Returns:
        Extended column list with transform output columns appended.
    """
    from cdc_generator.validators.bloblang_parser import extract_root_assignments

    existing_names = {c.name for c in columns}

    transforms_raw = table_cfg.get("transforms")
    if isinstance(transforms_raw, list):
        for item in cast(list[object], transforms_raw):
            if not isinstance(item, dict):
                continue
            entry = cast(dict[str, object], item)
            expected_output = entry.get("expected_output_column")
            if isinstance(expected_output, str) and expected_output and expected_output not in existing_names:
                columns.append(MigrationColumn(name=expected_output, type="TEXT"))
                existing_names.add(expected_output)

    for transform in resolve_transforms(table_cfg):
        output_columns = sorted(extract_root_assignments(transform.bloblang))
        for output_name in output_columns:
            if output_name in existing_names:
                continue
            columns.append(MigrationColumn(name=output_name, type="TEXT"))
            existing_names.add(output_name)

    return columns


def _add_cdc_metadata_columns(
    columns: list[MigrationColumn],
) -> list[MigrationColumn]:
    """Append standard CDC metadata columns.

    Args:
        columns: Existing column list.

    Returns:
        Extended column list with CDC metadata appended.
    """
    existing_names = {c.name for c in columns}
    for meta in CDC_METADATA_COLUMNS:
        name = str(meta["name"])
        if name in existing_names:
            continue
        columns.append(MigrationColumn(
            name=name,
            type=str(meta["type"]),
            nullable=bool(meta.get("nullable", True)),
            default=str(meta["default"]) if meta.get("default") else None,
        ))
    return columns


def build_full_column_list(
    table_def: dict[str, Any],
    sink_cfg: dict[str, object],
    service_config: dict[str, object],
    source_key: str,
    type_mapper: TypeMapper | None = None,
) -> tuple[list[MigrationColumn], list[str]]:
    """Build the complete column list using the full generation pipeline.

    Runs the same steps as migration generation:
    1. Build columns from table definition YAML (with ignore_columns).
    2. Add column template columns (e.g., _customer_id for db-per-tenant).
    3. Add CDC metadata columns (__sync_timestamp, etc.).

    This is used by the schema-diff engine to produce the same expected
    columns as the generator, ensuring apples-to-apples comparison.

    Args:
        table_def: Parsed table definition dict.
        sink_cfg: Sink table config (for column_templates).
        service_config: Full service config (for ignore_columns).
        source_key: Source table key like 'dbo.Actor'.
        type_mapper: Optional type mapper for MSSQL→PG conversion.

    Returns:
        Tuple of (columns, primary_keys).
    """
    source_cfg = _get_source_table_config(service_config, source_key)
    ignore_raw = source_cfg.get("ignore_columns")
    ignore_cols = (
        [str(c) for c in cast(list[object], ignore_raw)]
        if isinstance(ignore_raw, list) else None
    )

    columns, primary_keys = build_columns_from_table_def(
        table_def, ignore_cols, type_mapper,
    )

    # Add column template columns (e.g., _customer_id for db-per-tenant)
    columns = _add_column_template_columns(columns, sink_cfg)

    # Add transform output columns using exact configured/assigned names
    columns = _add_transform_output_columns(columns, sink_cfg)

    # Add CDC metadata columns (__sync_timestamp, __source, etc.)
    columns = _add_cdc_metadata_columns(columns)

    return columns, primary_keys


# ---------------------------------------------------------------------------
# SQL rendering
# ---------------------------------------------------------------------------


def _render_template(
    jinja_env: Environment,
    template_name: str,
    context: dict[str, Any],
) -> str:
    """Render a Jinja2 template with the given context.

    Args:
        jinja_env: Jinja2 environment.
        template_name: Template filename.
        context: Template variables.

    Returns:
        Rendered SQL string.
    """
    template = jinja_env.get_template(template_name)
    return template.render(context)


def _build_column_names_sql(columns: list[MigrationColumn]) -> str:
    """Build a comma-separated quoted column name list for INSERT/SELECT.

    Args:
        columns: Column list.

    Returns:
        SQL fragment like '"col1", "col2", "col3"'.
    """
    return ", ".join(f'"{c.name}"' for c in columns)


def _dedupe_names_case_insensitive(names: list[str]) -> list[str]:
    """Return unique names preserving order with case-insensitive matching.

    Args:
        names: Input names.

    Returns:
        Deduplicated names preserving first occurrence.
    """
    seen: set[str] = set()
    deduped: list[str] = []
    for name in names:
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


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


def _extract_manual_migration_hints(table_cfg: dict[str, Any]) -> ManualMigrationHints:
    """Extract optional manual migration hints from sink table config.

    Supported shape under sinks.<sink>.tables.<table>.manual_migration_hints:
      renames:
        - from: old_col
          to: new_col
      type_changes:
        - column: score
          using: "NULLIF(trim(\"score\"), '')::integer"
      set_not_null:
        - column: country
          pre_sql: "UPDATE ..."
    """
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
    """Build extra SQL suggestions from explicit service.yaml migration hints."""
    if (
        not hints.renames
        and not hints.type_casts
        and not hints.pre_not_null_sql
    ):
        return []

    table_qualified = f'"{target_schema}"."{table_name}"'
    expected_by_name = {column.name.casefold(): column for column in expected_columns}

    hint_sql: list[str] = []

    for from_name, to_name in hints.renames:
        hint_sql.append(
            f'ALTER TABLE {table_qualified} RENAME COLUMN "{from_name}" TO "{to_name}";'
        )

    for column_name, using_expr in sorted(hints.type_casts.items()):
        expected = expected_by_name.get(column_name)
        target_type = expected.type if expected is not None else "/* TODO: target type */"
        rendered_column = expected.name if expected is not None else column_name
        hint_sql.append(
            f'ALTER TABLE {table_qualified} ALTER COLUMN "{rendered_column}" TYPE {target_type} '
            + f"USING {using_expr};"
        )

    for column_name, pre_sql in sorted(hints.pre_not_null_sql.items()):
        expected = expected_by_name.get(column_name)
        rendered_column = expected.name if expected is not None else column_name
        hint_sql.append(pre_sql)
        hint_sql.append(
            f'ALTER TABLE {table_qualified} ALTER COLUMN "{rendered_column}" SET NOT NULL;'
        )

    return hint_sql


def _parse_existing_table_signature(
    sql_content: str,
) -> tuple[dict[str, ExistingColumnDef], set[str]] | None:
    """Parse existing generated CREATE TABLE SQL into columns + PK set."""
    create_match = re.search(
        r'CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]+\(\s*\n(.*?)\n\)',
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


def _detect_destructive_changes(
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
            destructive_messages.append(
                f"COLUMN_REMOVED: {existing_col.name}"
            )
            suggested_sql.append(
                f'ALTER TABLE {table_qualified} DROP COLUMN IF EXISTS "{existing_col.name}";'
            )
            continue

        existing_type = _normalize_type_for_compare(existing_col.type)
        expected_type = _normalize_type_for_compare(expected_col.type)
        if existing_type != expected_type:
            destructive_messages.append(
                "COLUMN_TYPE_CHANGED: "
                + f"{existing_col.name} ({existing_col.type} -> {expected_col.type})"
            )
            suggested_sql.append(
                f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" '
                + f"TYPE {expected_col.type} USING /* TODO: safe cast expression */;"
            )

        if existing_col.nullable != expected_col.nullable:
            destructive_messages.append(
                "COLUMN_NULLABILITY_CHANGED: "
                + f"{existing_col.name} ({existing_col.nullable} -> {expected_col.nullable})"
            )
            if expected_col.nullable:
                suggested_sql.append(
                    f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" DROP NOT NULL;'
                )
            else:
                suggested_sql.append(
                    f'ALTER TABLE {table_qualified} ALTER COLUMN "{existing_col.name}" SET NOT NULL;'
                )

    for col_name, expected_col in expected_by_name.items():
        if col_name not in existing_columns:
            added_columns.append(expected_col.name)

    if existing_pk != expected_pk:
        old_pk = ", ".join(sorted(existing_pk)) or "<none>"
        new_pk = ", ".join(sorted(expected_pk)) or "<none>"
        destructive_messages.append(f"PRIMARY_KEY_CHANGED: ({old_pk} -> {new_pk})")
        expected_pk_sql = ", ".join(f'"{name}"' for name in sorted(expected_pk))
        suggested_sql.append(
            f'ALTER TABLE {table_qualified} DROP CONSTRAINT IF EXISTS "{table_name}_pkey";'
        )
        if expected_pk_sql:
            suggested_sql.append(
                f'ALTER TABLE {table_qualified} ADD PRIMARY KEY ({expected_pk_sql});'
            )

    if len(removed_columns) == 1 and len(added_columns) == 1:
        destructive_messages.append(
            "POSSIBLE_RENAME_HINT: "
            + f"{removed_columns[0]} -> {added_columns[0]} (verify manually)"
        )
        suggested_sql.append(
            f'-- Possible rename candidate:\n'
            + f'-- ALTER TABLE {table_qualified} RENAME COLUMN "{removed_columns[0]}" TO "{added_columns[0]}";'
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
    """Return table-scoped manual migration guidance file path."""
    return output_dir / "02-manual" / table_name / "MANUAL_REQUIRED.sql"


def _write_manual_required_file(
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
        result.warnings.append(
            "Manual migration already exists for "
            + f"{sink_name}.{table_name}: {manual_path.relative_to(output_dir)}"
        )
        return

    manual_path.parent.mkdir(parents=True, exist_ok=True)
    reasons_block = "\n".join(f"-- - {reason}" for reason in reasons)

    content = (
        "-- ============================================================================\n"
        "-- MANUAL MIGRATION REQUIRED\n"
        "-- Auto-created by: cdc manage-migrations generate\n"
        + f"-- Sink: {sink_name}\n"
        + f"-- Table: {table_name}\n"
        "-- ============================================================================\n"
        "-- Detected destructive/semantic changes:\n"
        + reasons_block + "\n\n"
        "-- 1) Replace placeholders below with your real migration SQL\n"
        "-- 2) Execute this manually in the target PostgreSQL database\n"
        "-- 3) Keep this file as an audit artifact in git\n"
        "-- 4) Then run: cdc manage-migrations generate && cdc manage-migrations apply\n\n"
        "BEGIN;\n"
        "\n"
        "-- Suggested SQL (review and adjust before executing):\n"
        + ("\n".join(suggested_sql) if suggested_sql else "-- TODO: write manual ALTER/UPDATE statements here")
        + "\n"
        "\n"
        "COMMIT;\n"
    )

    manual_path.write_text(content, encoding="utf-8")
    result.files_written += 1
    result.warnings.append(
        "Manual migration required for "
        + f"{sink_name}.{table_name}: {manual_path.relative_to(output_dir)}"
    )


def _detect_removed_tables_for_manual_files(
    *,
    output_dir: Path,
    sink_name: str,
    sink_tables: dict[str, dict[str, Any]],
    result: GenerationResult,
) -> None:
    """Create manual-required files for tables removed from sink config."""
    tables_dir = output_dir / "01-tables"
    if not tables_dir.exists():
        return

    expected_table_names: set[str] = set()
    for sink_key, sink_cfg in sink_tables.items():
        if bool(sink_cfg.get("target_exists", False)):
            continue
        expected_table_names.add(sink_key.split(".", 1)[-1].casefold())

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
        _write_manual_required_file(
            output_dir=output_dir,
            sink_name=sink_name,
            table_name=removed_name,
            reasons=[
                f"TABLE_REMOVED: {removed_name} no longer present in sink config",
            ],
            suggested_sql=[
                "-- Table removed from service config. Choose one:",
                f"-- DROP TABLE IF EXISTS \"{removed_name}\";",
                "-- or keep table and stop syncing it intentionally.",
            ],
            result=result,
        )


def _build_pk_names_sql(primary_keys: list[str]) -> str:
    """Build a comma-separated quoted PK column name list.

    Args:
        primary_keys: PK column names.

    Returns:
        SQL fragment like '"pk1", "pk2"'.
    """
    unique_primary_keys = _dedupe_names_case_insensitive(primary_keys)
    return ", ".join(f'"{pk}"' for pk in unique_primary_keys)


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Service config parsing
# ---------------------------------------------------------------------------
# Service config parsing
# ---------------------------------------------------------------------------


def _derive_target_schemas(
    sink_tables: dict[str, dict[str, Any]],
) -> list[str]:
    """Derive unique target schemas from sink table keys.

    Each sink table key has the format 'schema.table' (e.g., 'adopus.Actor').
    Extracts the schema part and returns unique schemas, excluding 'public'
    which always exists in PostgreSQL.

    Args:
        sink_tables: Sink table configurations keyed by 'schema.table'.

    Returns:
        Sorted list of unique target schema names (excluding 'public').
    """
    schemas: set[str] = set()
    for sink_key in sink_tables:
        parts = sink_key.split(".", 1)
        if len(parts) > 1:
            schema = parts[0]
            if schema != "public":
                schemas.add(schema)
    return sorted(schemas)


def _get_source_table_config(
    service_config: dict[str, object],
    source_key: str,
) -> dict[str, Any]:
    """Get source table config (e.g., ignore_columns) for a given source key.

    Args:
        service_config: Full service config.
        source_key: Source table key like 'dbo.Actor'.

    Returns:
        Source table config dict.
    """
    source_raw = service_config.get("source")
    if not isinstance(source_raw, dict):
        return {}
    source = cast(dict[str, Any], source_raw)
    tables_raw = source.get("tables", {})
    if not isinstance(tables_raw, dict):
        return {}
    tables = cast(dict[str, Any], tables_raw)
    entry = tables.get(source_key)
    if isinstance(entry, dict):
        return cast(dict[str, Any], entry)
    return {}


def get_sinks(
    service_config: dict[str, object],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Extract sink table configs organized per sink target.

    Returns a dict mapping sink_name → {table_key → table_config}.
    Each sink gets its own entry so migrations can be generated per target.

    Args:
        service_config: Full service config.

    Returns:
        Dict of sink_name → dict of table configurations.
    """
    sinks_raw = service_config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return {}
    sinks = cast(dict[str, Any], sinks_raw)

    result: dict[str, dict[str, dict[str, Any]]] = {}
    for sink_name, sink_cfg_raw in sinks.items():
        if not isinstance(sink_cfg_raw, dict):
            continue
        sink_cfg = cast(dict[str, Any], sink_cfg_raw)
        tables_raw = sink_cfg.get("tables", {})
        if not isinstance(tables_raw, dict):
            continue
        tables: dict[str, dict[str, Any]] = {}
        for table_key, table_cfg_raw in cast(dict[str, Any], tables_raw).items():
            if isinstance(table_cfg_raw, dict):
                tables[str(table_key)] = cast(dict[str, Any], table_cfg_raw)
        if tables:
            result[str(sink_name)] = tables
    return result


def resolve_sink_target(sink_name: str, project_root: Path) -> SinkTarget:
    """Resolve a sink name to a SinkTarget with per-env database names.

    Parses the sink key format '{sink_group}.{service}' and looks up the
    actual database names per environment from sink-groups.yaml.

    Args:
        sink_name: Sink key from service config (e.g., 'sink_asma.directory').
        project_root: Implementation project root.

    Returns:
        SinkTarget with parsed names and resolved database info.
    """
    parts = sink_name.split(".", 1)
    sink_group = parts[0]
    sink_service = parts[1] if len(parts) > 1 else ""

    # Look up per-env database names from sink-groups.yaml
    databases: dict[str, str] = {}
    sg_path = project_root / "sink-groups.yaml"
    if sg_path.exists():
        raw = load_yaml_file(sg_path)
        group_cfg = cast(dict[str, Any], raw).get(sink_group, {})
        if isinstance(group_cfg, dict):
            sources = cast(dict[str, Any], group_cfg).get("sources", {})
            if isinstance(sources, dict):
                service_cfg = cast(dict[str, Any], sources).get(sink_service, {})
                if isinstance(service_cfg, dict):
                    for env_key, env_val in cast(dict[str, Any], service_cfg).items():
                        if isinstance(env_val, dict) and "database" in env_val:
                            databases[str(env_key)] = str(
                                cast(dict[str, Any], env_val)["database"],
                            )

    return SinkTarget(
        sink_name=sink_name,
        sink_group=sink_group,
        sink_service=sink_service,
        databases=databases,
    )


def _resolve_pattern(project_root: Path) -> str:
    """Determine the architecture pattern from source-groups.yaml.

    Args:
        project_root: Implementation project root.

    Returns:
        Pattern string (db-per-tenant or db-shared).
    """
    source_groups = _load_source_groups(project_root)
    for _group_name, group_cfg_raw in source_groups.items():
        if not isinstance(group_cfg_raw, dict):
            continue
        group_cfg = cast(dict[str, Any], group_cfg_raw)
        p = str(group_cfg.get("pattern", "")).strip().lower()
        if p:
            return p
    return "db-per-tenant"


def _validate_db_shared_customer_id(
    sinks: dict[str, dict[str, dict[str, Any]]],
    result: GenerationResult,
) -> None:
    """Warn if any db-shared table is missing customer_id in column_templates.

    For the db-shared pattern, every table should have a ``customer_id``
    column to maintain multi-tenant isolation.

    Args:
        sinks: Per-sink table configs.
        result: GenerationResult for tracking warnings.
    """
    for _sink_name, tables in sinks.items():
        for table_key, table_cfg in tables.items():
            if bool(table_cfg.get("target_exists", False)):
                continue
            if bool(table_cfg.get("target_exists", False)):
                continue
            templates = table_cfg.get("column_templates", [])
            has_customer_id = False
            if isinstance(templates, list):
                for tmpl in cast(list[object], templates):
                    if isinstance(tmpl, dict):
                        t = cast(dict[str, Any], tmpl)
                        if str(t.get("name", "")).casefold() == "customer_id":
                            has_customer_id = True
                            break
                    elif isinstance(tmpl, str) and tmpl.casefold() == "customer_id":
                        has_customer_id = True
                        break
            if not has_customer_id:
                result.warnings.append(
                    f"db-shared: {table_key} has no customer_id column_template "
                    + "— multi-tenant isolation may be incomplete",
                )


# ---------------------------------------------------------------------------
# Table processing
# ---------------------------------------------------------------------------


def _process_table(
    sink_key: str,
    sink_cfg: dict[str, Any],
    service_config: dict[str, object],
    table_defs: dict[str, dict[str, Any]],
    result: GenerationResult,
    type_mapper: TypeMapper | None = None,
) -> TableMigration | None:
    """Process a single sink table into a TableMigration.

    Args:
        sink_key: Sink table key (e.g., 'adopus.Actor').
        sink_cfg: Sink table config from service YAML.
        service_config: Full service config.
        table_defs: Loaded table definitions.
        result: GenerationResult for tracking warnings.
        type_mapper: Optional MSSQL→PG type converter.

    Returns:
        TableMigration if successful, None if skipped.
    """
    # Parse source reference
    from_ref = sink_cfg.get("from", "")
    if not isinstance(from_ref, str) or not from_ref:
        result.warnings.append(f"Table {sink_key}: missing 'from' reference, skipped")
        return None

    # Split source reference (e.g., 'dbo.Actor' → schema=dbo, table=Actor)
    from_parts = from_ref.split(".", 1)
    source_schema = from_parts[0] if len(from_parts) > 1 else "dbo"
    source_table = from_parts[-1]

    # Determine sink table name and target schema from the key
    # Sink key format: 'schema.table' (e.g., 'adopus.Actor')
    key_parts = sink_key.split(".", 1)
    target_schema = key_parts[0] if len(key_parts) > 1 else "public"
    table_name = key_parts[-1]

    target_exists = bool(sink_cfg.get("target_exists", False))
    replicate_structure = bool(sink_cfg.get("replicate_structure", False))

    # Skip target_exists tables — they already exist in PostgreSQL
    if target_exists and not replicate_structure:
        return None

    # Build columns from service-schema YAML
    # Lookup key is 'schema.table' (e.g., 'dbo.Actor')
    table_def = table_defs.get(from_ref)
    if table_def is None:
        # Also try just the table name (legacy table-definitions format)
        table_def = table_defs.get(source_table)
    if table_def is None:
        available = sorted(table_defs.keys())[:_MAX_WARNING_TABLES]
        result.warnings.append(
            f"Table {sink_key}: no schema definition for '{from_ref}' "
            + f"(have {len(table_defs)}: {', '.join(available)}{'...' if len(table_defs) > _MAX_WARNING_TABLES else ''})",
        )
        return None

    # Get source config for ignore_columns
    source_cfg = _get_source_table_config(service_config, from_ref)
    ignore_raw = source_cfg.get("ignore_columns")
    ignore_cols = (
        [str(c) for c in cast(list[object], ignore_raw)]
        if isinstance(ignore_raw, list) else None
    )

    columns, primary_keys = build_columns_from_table_def(
        table_def, ignore_cols, type_mapper,
    )

    # Add column template columns
    columns = _add_column_template_columns(columns, cast(dict[str, object], sink_cfg))

    # Add CDC metadata columns
    columns = _add_cdc_metadata_columns(columns)

    if not columns:
        result.warnings.append(f"Table {sink_key}: no columns resolved, skipped")
        return None

    return TableMigration(
        table_name=table_name,
        target_schema=target_schema,
        source_schema=source_schema,
        columns=columns,
        primary_keys=primary_keys,
        replicate_structure=replicate_structure,
        target_exists=target_exists,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def generate_migrations(
    service_name: str = "adopus",
    *,
    table_filter: str | None = None,
    dry_run: bool = False,
    output_dir: Path | None = None,
) -> GenerationResult:
    """Generate PostgreSQL migration files for a CDC service.

    Reads service config, table definitions, and source-groups to produce
    a complete set of idempotent migration SQL files.

    Args:
        service_name: Service name to generate migrations for.
        table_filter: Optional table name filter (only generate for this table).
        dry_run: If True, print what would be generated without writing files.
        output_dir: Override output directory (default: migrations/).

    Returns:
        GenerationResult with file counts and any errors/warnings.

    Example:
        >>> result = generate_migrations("adopus")
        >>> print(f"Generated {result.files_written} files for {result.tables_processed} tables")
    """
    result = GenerationResult()
    project_root = get_project_root()
    generated_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    db_user = os.environ.get("CDC_DB_USER", DEFAULT_DB_USER)

    # Resolve output directory
    if output_dir is None:
        output_dir = project_root / "migrations"
    result.output_dir = output_dir

    print_header(f"Generating migrations for service: {service_name}")

    # 1. Load service config
    try:
        service_config = load_service_config(service_name)
    except FileNotFoundError as e:
        result.errors.append(str(e))
        print_error(str(e))
        return result

    # 2. Load table definitions from services/_schemas/
    table_defs = load_table_definitions(service_name, project_root)
    if not table_defs:
        result.warnings.append(
            f"No table definitions found in services/_schemas/{service_name}/. "
            + "Run 'cdc manage-services config --inspect' first to generate them.",
        )

    # 3. Initialize type mapper (MSSQL → PostgreSQL)
    try:
        type_mapper: TypeMapper | None = TypeMapper("mssql", "pgsql")
    except FileNotFoundError:
        type_mapper = None
        result.warnings.append(
            "No MSSQL→PG type mapping file found. Column types will not be converted.",
        )
        print_warning(result.warnings[-1])

    # 4. Get sinks organized per sink target
    sinks = get_sinks(service_config)
    if not sinks:
        result.errors.append("No sink tables found in service config")
        print_error(result.errors[-1])
        return result

    # 5. Resolve pattern and create shared service data
    pattern = _resolve_pattern(project_root)
    jinja_env = _create_jinja_env()
    svc_data = ServiceData(
        service_config=service_config,
        table_defs=table_defs,
        type_mapper=type_mapper,
    )

    # 5b. db-shared validation: warn if customer_id missing from tables
    if pattern == "db-shared":
        _validate_db_shared_customer_id(sinks, result)

    # 6. Process each sink target separately
    for sink_name, sink_tables_iter in sorted(sinks.items()):
        tables_for_sink = sink_tables_iter

        # Apply table filter
        if table_filter:
            filter_lower = table_filter.casefold()
            tables_for_sink = {
                k: v for k, v in tables_for_sink.items()
                if filter_lower in k.casefold()
            }
            if not tables_for_sink:
                continue

        # Resolve sink target (database names from sink-groups.yaml)
        sink_target = resolve_sink_target(sink_name, project_root)
        result.sink_targets.append(sink_target)
        schemas = _derive_target_schemas(tables_for_sink)
        result.schemas = sorted(set(result.schemas) | set(schemas))

        ctx = RenderContext(
            jinja_env=jinja_env,
            output_dir=output_dir / sink_name,
            generated_at=generated_at,
            db_user=db_user,
            sink_target=sink_target,
        )

        _generate_for_sink(
            ctx=ctx,
            sink_tables=tables_for_sink,
            schemas=schemas,
            pattern=pattern,
            svc_data=svc_data,
            result=result,
            dry_run=dry_run,
        )

    if dry_run:
        return result

    # Summary
    sink_count = len(result.sink_targets)
    print_success(
        f"Generated {result.files_written} files for {result.tables_processed} tables"
        + f" ({len(result.schemas)} schemas, {sink_count} sink{'s' if sink_count != 1 else ''})",
    )
    if result.warnings:
        for w in result.warnings:
            print_warning(w)
    if result.errors:
        for e in result.errors:
            print_error(e)

    return result


# ---------------------------------------------------------------------------
# Per-sink generation
# ---------------------------------------------------------------------------


def _generate_for_sink(
    *,
    ctx: RenderContext,
    sink_tables: dict[str, dict[str, Any]],
    schemas: list[str],
    pattern: str,
    svc_data: ServiceData,
    result: GenerationResult,
    dry_run: bool,
) -> None:
    """Generate all migration files for a single sink target.

    Args:
        ctx: Shared render context (includes output_dir and sink_target).
        sink_tables: Tables for this sink.
        schemas: Target schemas to create.
        pattern: Architecture pattern.
        svc_data: Loaded service data (config, table defs, type mapper).
        result: GenerationResult to update.
        dry_run: If True, only print what would be generated.
    """
    if dry_run:
        db_list = ", ".join(
            f"{e}={d}" for e, d in sorted(ctx.sink_target.databases.items())
        )
        print_info(f"[DRY RUN] Sink: {ctx.sink_target.sink_name}")
        print_info(f"  Output: {ctx.output_dir}")
        print_info(f"  Databases: {db_list or '(none resolved)'}")
        print_info(f"  Pattern: {pattern}")
        print_info(f"  Schemas: {len(schemas)}")
        print_info(f"  Tables: {len(sink_tables)}")
        for table_key in sorted(sink_tables):
            print_info(f"    - {table_key}")
        return

    # Generate infrastructure files
    _generate_infrastructure(ctx, schemas, pattern, result)

    _detect_removed_tables_for_manual_files(
        output_dir=ctx.output_dir,
        sink_name=ctx.sink_target.sink_name,
        sink_tables=sink_tables,
        result=result,
    )

    # Generate per-table files
    tables_generated: list[str] = []
    for sink_key in sorted(sink_tables):
        sink_cfg = sink_tables[sink_key]
        migration = _process_table(
            sink_key, sink_cfg, svc_data.service_config,
            svc_data.table_defs, result, svc_data.type_mapper,
        )
        if migration is None:
            continue

        _generate_table_files(ctx, migration, sink_cfg, result)
        tables_generated.append(migration.table_name)
        result.tables_processed += 1

    # Write per-sink manifest
    manifest_written = _write_manifest(
        ctx.output_dir, tables_generated, schemas, ctx.generated_at, ctx.sink_target,
    )
    _ = manifest_written
    result.files_written += 1


# ---------------------------------------------------------------------------
# Infrastructure generation
# ---------------------------------------------------------------------------


def _generate_infrastructure(
    ctx: RenderContext,
    schemas: list[str],
    pattern: str,
    result: GenerationResult,
) -> None:
    """Generate infrastructure migration files (schemas, cdc-management).

    Args:
        ctx: Shared render context.
        schemas: Customer schema names.
        pattern: Architecture pattern.
        result: GenerationResult to update.
    """
    infra_dir = ctx.output_dir / "00-infrastructure"

    # 1. Create schemas
    schema_sql = _render_template(ctx.jinja_env, "create-schemas.sql.j2", {
        "generated_at": ctx.generated_at,
        "pattern": pattern,
        "schemas": schemas,
        "sink_target": ctx.sink_target,
    })
    _write_migration_file(infra_dir / "01-create-schemas.sql", schema_sql, result)

    # 2. CDC management infrastructure
    mgmt_sql = _render_template(ctx.jinja_env, "cdc-management.sql.j2", {
        "generated_at": ctx.generated_at,
        "db_user": ctx.db_user,
        "sink_target": ctx.sink_target,
        "pattern": pattern,
    })
    _write_migration_file(infra_dir / "02-cdc-management.sql", mgmt_sql, result)


def _build_column_defs_sql(columns: list[MigrationColumn]) -> list[str]:
    """Build formatted column definitions for CREATE TABLE.

    Args:
        columns: Column list.

    Returns:
        List of column definition lines with 4-space indentation.
    """
    lines: list[str] = []
    for col in columns:
        parts = [f'    "{col.name}"', col.type]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        lines.append(" ".join(parts))
    return lines


def _build_create_table_sql(
    target_schema: str,
    table_name: str,
    columns: list[MigrationColumn],
    primary_keys: list[str],
    source_schema: str,
    generated_at: str,
) -> str:
    """Build a complete CREATE TABLE DDL statement.

    Args:
        target_schema: PostgreSQL target schema (e.g., 'adopus').
        table_name: Table name.
        columns: All columns.
        primary_keys: Primary key column names.
        source_schema: MSSQL source schema.
        generated_at: Timestamp string.

    Returns:
        Complete CREATE TABLE SQL string.
    """
    col_lines = _build_column_defs_sql(columns)
    unique_primary_keys = _dedupe_names_case_insensitive(primary_keys)
    if unique_primary_keys:
        pk_expr = ", ".join(f'"{pk}"' for pk in unique_primary_keys)
        col_lines.append(f"    PRIMARY KEY ({pk_expr})")

    columns_sql = ",\n".join(col_lines)
    qualified = f'"{ target_schema }"."{ table_name }"'

    header = (
        f"-- ============================================================================\n"
        f"-- DO NOT EDIT — AUTO-GENERATED by: cdc manage-migrations generate\n"
        f"-- Generated: {generated_at}\n"
        f"-- Source: MSSQL [{source_schema}].[{table_name}]\n"
        f"-- Target: {qualified}\n"
        f"-- ============================================================================\n"
    )

    evolution_lines: list[str] = []
    for col in columns:
        col_parts = [
            f'ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS "{col.name}" {col.type}',
        ]
        if col.default:
            col_parts.append(f"DEFAULT {col.default}")
        evolution_lines.append(" ".join(col_parts) + ";")
    evolution_sql = "\n".join(evolution_lines)

    return (
        header + "\n"
        + f'CREATE TABLE IF NOT EXISTS {qualified} (\n'
        + columns_sql + "\n"
        + ");\n\n"
        + "-- Schema evolution (additive only): add missing columns to existing tables.\n"
        + "-- Drift guard runs in 'cdc manage-migrations apply' (Python pre-check), not in SQL body.\n"
        + "-- Manual migration is required for remove/rename/type-change or nullability/default transitions.\n"
        + evolution_sql + "\n\n"
        + "-- Index on sync timestamp for efficient querying\n"
        + f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_sync_ts"\n'
        + f'    ON {qualified} ("__sync_timestamp");\n'
    )


# ---------------------------------------------------------------------------
# Per-table generation
# ---------------------------------------------------------------------------


def _generate_table_files(
    ctx: RenderContext,
    migration: TableMigration,
    sink_table_cfg: dict[str, Any],
    result: GenerationResult,
) -> None:
    """Generate table DDL and staging/merge files for a single table.

    Args:
        ctx: Shared render context.
        migration: Parsed table migration info.
        result: GenerationResult to update.
    """
    tables_dir = ctx.output_dir / "01-tables"

    existing_table_sql = tables_dir / f"{migration.table_name}.sql"
    hints = _extract_manual_migration_hints(sink_table_cfg)
    destructive_reasons, suggested_sql = _detect_destructive_changes(
        migration.target_schema,
        migration.table_name,
        existing_table_sql,
        migration.columns,
        migration.primary_keys,
        hints,
    )
    _write_manual_required_file(
        output_dir=ctx.output_dir,
        sink_name=ctx.sink_target.sink_name,
        table_name=migration.table_name,
        reasons=destructive_reasons,
        suggested_sql=suggested_sql,
        result=result,
    )

    # 1. Table DDL (built in Python for clean formatting)
    table_sql = _build_create_table_sql(
        migration.target_schema,
        migration.table_name,
        migration.columns,
        migration.primary_keys,
        migration.source_schema,
        ctx.generated_at,
    )
    _write_migration_file(tables_dir / f"{migration.table_name}.sql", table_sql, result)

    # 2. Staging + merge (only if table has primary keys)
    if migration.primary_keys:
        unique_primary_keys = _dedupe_names_case_insensitive(migration.primary_keys)
        pk_set = {pk.casefold() for pk in unique_primary_keys}
        non_pk_cols = [c for c in migration.columns if c.name.casefold() not in pk_set]
        all_column_names = _build_column_names_sql(migration.columns)
        pk_column_names = _build_pk_names_sql(unique_primary_keys)

        # Build the UPDATE SET clause with proper formatting
        update_lines: list[str] = []
        for i, col in enumerate(non_pk_cols):
            comma = "," if i < len(non_pk_cols) - 1 else ""
            update_lines.append(f'            "{col.name}" = EXCLUDED."{col.name}"{comma}')
        update_set_sql = "\n".join(update_lines)

        staging_context = {
            "generated_at": ctx.generated_at,
            "table_name": migration.table_name,
            "target_schema": f'"{ migration.target_schema }"',
            "target_schema_raw": migration.target_schema,
            "all_column_names": all_column_names,
            "pk_column_names": pk_column_names,
            "update_set_sql": update_set_sql,
            "db_user": ctx.db_user,
        }
        staging_sql = _render_template(ctx.jinja_env, "staging.sql.j2", staging_context)
        _write_migration_file(
            tables_dir / f"{migration.table_name}-staging.sql",
            staging_sql,
            result,
        )
    else:
        result.warnings.append(
            f"Table {migration.table_name}: no primary key, staging/merge skipped",
        )
