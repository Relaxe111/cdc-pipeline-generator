"""Runtime implementation for migration generator package API.

This module contains executable logic that used to live in
``migration_generator.__init__`` so that the package initializer can remain
import/export-only.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from jinja2 import Environment, FileSystemLoader

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.topology_runtime import (
    resolve_runtime_engine,
    resolve_runtime_mode,
    resolve_topology,
    resolve_topology_kind,
    supported_topologies_for_source_type,
    topology_supported_for_source_type,
)
from cdc_generator.helpers.type_mapper import TypeMapper

from .columns import (
    add_cdc_metadata_columns as _add_cdc_metadata_columns,
)
from .columns import (
    build_columns_from_table_def as _build_columns_from_table_def,
)
from .data_structures import (
    GenerationResult,
    MigrationColumn,
    RenderContext,
    RuntimeMode,
    ServiceData,
)
from .native_cdc_policy import build_native_cdc_policy_seeds
from .file_writers import write_manifest
from .manual_migrations import (
    detect_removed_tables_for_manual_files as _detect_removed_tables_for_manual_files,
)
from .rendering import (
    generate_infrastructure as _generate_infrastructure,
)
from .rendering import (
    generate_table_files as _generate_table_files,
)
from .service_parsing import (
    derive_target_schemas as _derive_target_schemas,
)
from .service_parsing import (
    get_sinks,
    resolve_sink_target,
)
from .service_parsing import (
    get_source_table_config as _get_source_table_config,
)
from .service_parsing import (
    resolve_pattern as _resolve_pattern,
)
from .service_parsing import (
    resolve_source_group_config as _resolve_source_group_config,
)
from .service_parsing import (
    resolve_source_type as _resolve_source_type,
)
from .service_parsing import (
    validate_db_shared_customer_id as _validate_db_shared_customer_id,
)
from .table_processing import process_table as _process_table

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates" / "migrations"
DEFAULT_DB_USER = "postgres"


def _sql_literal(value: object | None) -> str:
    """Render a Python value as a SQL literal for Jinja templates."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    text_value = str(value).replace("'", "''")
    return "'" + text_value + "'"


def _package_api() -> ModuleType:
    """Return package module so runtime honors package-level monkeypatching."""
    import cdc_generator.core.migration_generator as package_api

    return package_api


def _create_jinja_env() -> Environment:
    """Create Jinja2 environment with migration template filters."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["format_pk"] = lambda name: f'"{name}"'
    env.filters["sql_literal"] = _sql_literal
    return env


def build_columns_from_table_def(
    table_def: dict[str, Any],
    ignore_columns: list[str] | None = None,
    type_mapper: TypeMapper | None = None,
) -> tuple[list[MigrationColumn], list[str]]:
    """Compatibility wrapper for base column extraction."""
    return _build_columns_from_table_def(table_def, ignore_columns, type_mapper)


def _add_column_template_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add columns from column templates using package-level resolver."""
    package_api = _package_api()
    resolved = package_api.resolve_column_templates(table_cfg)
    existing_names = {column.name for column in columns}

    for entry in resolved:
        if entry.name in existing_names:
            continue
        columns.append(
            MigrationColumn(
                name=entry.name,
                type=entry.template.column_type.upper(),
                nullable=not entry.template.not_null,
                default=entry.template.default,
            ),
        )

    return columns


def _add_transform_output_columns(
    columns: list[MigrationColumn],
    table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Add transform output columns while preserving exact configured names."""
    from cdc_generator.validators.bloblang_parser import extract_root_assignments

    package_api = _package_api()
    existing_names = {column.name for column in columns}

    transforms_raw = table_cfg.get("transforms")
    if isinstance(transforms_raw, list):
        for item in cast(list[object], transforms_raw):
            if not isinstance(item, dict):
                continue
            item_dict = cast(dict[str, object], item)
            expected_output = item_dict.get("expected_output_column")
            if (
                isinstance(expected_output, str)
                and expected_output
                and expected_output not in existing_names
            ):
                columns.append(MigrationColumn(name=expected_output, type="TEXT"))
                existing_names.add(expected_output)

    for transform in package_api.resolve_transforms(table_cfg):
        output_columns = sorted(extract_root_assignments(transform.bloblang))
        for output_name in output_columns:
            if output_name in existing_names:
                continue
            columns.append(MigrationColumn(name=output_name, type="TEXT"))
            existing_names.add(output_name)

    return columns


def build_full_column_list(
    table_def: dict[str, Any],
    sink_cfg: dict[str, object],
    service_config: dict[str, object],
    source_key: str,
    type_mapper: TypeMapper | None = None,
    runtime_mode: RuntimeMode = "brokered",
) -> tuple[list[MigrationColumn], list[str]]:
    """Build the full migration column pipeline (compatibility entrypoint)."""
    source_cfg = _get_source_table_config(service_config, source_key)
    ignore_raw = source_cfg.get("ignore_columns")
    ignore_cols = (
        [str(column) for column in cast(list[object], ignore_raw)]
        if isinstance(ignore_raw, list) else None
    )

    columns, primary_keys = build_columns_from_table_def(table_def, ignore_cols, type_mapper)
    columns = _add_column_template_columns(columns, sink_cfg)
    columns = _add_transform_output_columns(columns, sink_cfg)
    if runtime_mode == "native":
        from .columns import add_native_cdc_metadata_columns

        columns = add_native_cdc_metadata_columns(columns)
    else:
        columns = _add_cdc_metadata_columns(columns)
    return columns, primary_keys


def load_table_definitions(
    service_name: str,
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    """Load table definitions from services/_schemas/{service}/{schema}/{table}.yaml."""
    package_api = _package_api()
    schema_dirs = package_api.get_service_schema_read_dirs(service_name, project_root)

    tables: dict[str, dict[str, Any]] = {}
    for schema_dir in schema_dirs:
        if not schema_dir.exists():
            continue
        for sub_dir in sorted(schema_dir.iterdir()):
            if not sub_dir.is_dir():
                continue
            schema_name = sub_dir.name
            for yaml_file in sorted(sub_dir.glob("*.yaml")):
                raw_dict = package_api.load_yaml_file(yaml_file)
                if not isinstance(raw_dict, dict):
                    continue
                raw_dict_typed = cast(dict[str, object], raw_dict)
                table_name = raw_dict_typed.get("table")
                if not isinstance(table_name, str):
                    table_name = yaml_file.stem
                key = f"{schema_name}.{table_name}"
                if key not in tables:
                    tables[key] = cast(dict[str, Any], raw_dict_typed)
    return tables


def generate_migrations(
    service_name: str = "adopus",
    *,
    table_filter: str | None = None,
    dry_run: bool = False,
    output_dir: Path | None = None,
    runtime_mode: RuntimeMode | None = None,
    topology: str | None = None,
) -> GenerationResult:
    """Generate PostgreSQL migration files for a CDC service."""
    package_api = _package_api()
    result = GenerationResult()
    project_root = package_api.get_project_root()
    generated_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    db_user = os.environ.get("CDC_DB_USER", DEFAULT_DB_USER)

    resolved_output_dir = output_dir if output_dir is not None else project_root / "migrations"
    result.output_dir = resolved_output_dir

    try:
        service_config = package_api.load_service_config(service_name)
    except FileNotFoundError as e:
        result.errors.append(str(e))
        print_error(str(e))
        return result

    table_defs = load_table_definitions(service_name, project_root)
    if not table_defs:
        result.warnings.append(
            f"No table definitions found in services/_schemas/{service_name}/. "
            + "Run 'cdc manage-services config --inspect' first to generate them.",
        )

    try:
        type_mapper: TypeMapper | None = TypeMapper("mssql", "pgsql")
    except FileNotFoundError:
        type_mapper = None
        result.warnings.append(
            "No MSSQL→PG type mapping file found. Column types will not be converted.",
        )
        print_warning(result.warnings[-1])

    sinks = get_sinks(service_config)
    if not sinks:
        result.errors.append("No sink tables found in service config")
        print_error(result.errors[-1])
        return result

    pattern = _resolve_pattern(project_root)
    source_type = _resolve_source_type(project_root)
    source_group_config = _resolve_source_group_config(project_root)
    effective_topology = cast(
        str | None,
        topology
        or resolve_topology(
            {},
            source_group=source_group_config,
            runtime_mode=runtime_mode,
            source_type=source_type,
        ),
    )

    if effective_topology is not None and not topology_supported_for_source_type(
        cast(Any, effective_topology),
        source_type,
    ):
        supported_topologies = ", ".join(supported_topologies_for_source_type(source_type))
        result.errors.append(
            f"Topology '{effective_topology}' is not supported for source type '{source_type}'. "
            + f"Supported values: {supported_topologies}",
        )
        print_error(result.errors[-1])
        return result

    if runtime_mode is not None and effective_topology is not None:
        derived_runtime_mode = resolve_runtime_mode(
            {},
            topology=cast(Any, effective_topology),
            source_type=source_type,
        )
        if runtime_mode != derived_runtime_mode:
            result.errors.append(
                f"Runtime '{runtime_mode}' conflicts with topology '{effective_topology}'. "
                + f"Use runtime '{derived_runtime_mode}' or omit runtime so it is derived automatically.",
            )
            print_error(result.errors[-1])
            return result

    effective_runtime_mode = resolve_runtime_mode(
        {},
        topology=cast(Any, effective_topology),
        source_group=source_group_config,
        runtime_mode=runtime_mode,
        source_type=source_type,
    )
    if effective_topology is None:
        effective_topology = cast(
            str,
            resolve_topology(
                {},
                runtime_mode=effective_runtime_mode,
                source_type=source_type,
            )
            or "redpanda",
        )

    print_header(
        f"Generating migrations for service: {service_name}"
        + f" (topology: {effective_topology}, runtime: {effective_runtime_mode})",
    )

    jinja_env = _create_jinja_env()
    svc_data = ServiceData(
        service_config=service_config,
        table_defs=table_defs,
        type_mapper=type_mapper,
    )

    if effective_runtime_mode == "native":
        if pattern != "db-per-tenant":
            result.errors.append(
                "Native runtime generation currently supports only db-per-tenant services",
            )
            print_error(result.errors[-1])
            return result
        if source_type != "mssql":
            result.errors.append(
                "Native runtime generation currently supports only MSSQL source groups",
            )
            print_error(result.errors[-1])
            return result

    if pattern == "db-shared":
        _validate_db_shared_customer_id(sinks, result)

    for sink_name, sink_tables_iter in sorted(sinks.items()):
        tables_for_sink = sink_tables_iter

        if table_filter:
            filter_lower = table_filter.casefold()
            tables_for_sink = {
                key: value for key, value in tables_for_sink.items()
                if filter_lower in key.casefold()
            }
            if not tables_for_sink:
                continue

        sink_target = resolve_sink_target(sink_name, project_root)
        result.sink_targets.append(sink_target)
        schemas = _derive_target_schemas(tables_for_sink)
        result.schemas = sorted(set(result.schemas) | set(schemas))

        ctx = RenderContext(
            jinja_env=jinja_env,
            output_dir=resolved_output_dir / sink_name,
            generated_at=generated_at,
            db_user=db_user,
            sink_target=sink_target,
            runtime_mode=effective_runtime_mode,
            native_cdc_policy_seeds=(
                build_native_cdc_policy_seeds(tables_for_sink, service_config, result)
                if effective_runtime_mode == "native" else []
            ),
        )

        _generate_for_sink(
            ctx=ctx,
            sink_tables=tables_for_sink,
            schemas=schemas,
            pattern=pattern,
            source_type=source_type,
            svc_data=svc_data,
            result=result,
            dry_run=dry_run,
        )

    if dry_run:
        return result

    sink_count = len(result.sink_targets)
    print_success(
        f"Generated {result.files_written} files for {result.tables_processed} tables"
        + f" ({len(result.schemas)} schemas, {sink_count} sink{'s' if sink_count != 1 else ''})",
    )
    if result.warnings:
        for warning in result.warnings:
            print_warning(warning)
    if result.errors:
        for error in result.errors:
            print_error(error)

    return result


def _generate_for_sink(
    *,
    ctx: RenderContext,
    sink_tables: dict[str, dict[str, Any]],
    schemas: list[str],
    pattern: str,
    source_type: str,
    svc_data: ServiceData,
    result: GenerationResult,
    dry_run: bool,
) -> None:
    """Generate all migration files for a single sink target."""
    topology = resolve_topology(
        {},
        runtime_mode=ctx.runtime_mode,
        source_type=source_type,
    )
    topology_kind = resolve_topology_kind(
        {},
        runtime_mode=ctx.runtime_mode,
        source_type=source_type,
    )
    runtime_engine = resolve_runtime_engine(
        {},
        topology_kind=topology_kind,
        runtime_mode=ctx.runtime_mode,
    )

    if dry_run:
        db_list = ", ".join(
            f"{env_name}={db_name}"
            for env_name, db_name in sorted(ctx.sink_target.databases.items())
        )
        print_info(f"[DRY RUN] Sink: {ctx.sink_target.sink_name}")
        print_info(f"  Output: {ctx.output_dir}")
        print_info(f"  Databases: {db_list or '(none resolved)'}")
        print_info(f"  Pattern: {pattern}")
        print_info(f"  Topology: {topology or 'unknown'}")
        print_info(f"  Runtime: {ctx.runtime_mode}")
        print_info(f"  Topology Kind: {topology_kind}")
        print_info(f"  Runtime Engine: {runtime_engine}")
        print_info(f"  Schemas: {len(schemas)}")
        print_info(f"  Tables: {len(sink_tables)}")
        for table_key in sorted(sink_tables):
            print_info(f"    - {table_key}")
        return

    _generate_infrastructure(ctx, schemas, pattern, result)

    _detect_removed_tables_for_manual_files(
        output_dir=ctx.output_dir,
        sink_name=ctx.sink_target.sink_name,
        sink_tables=sink_tables,
        result=result,
    )

    tables_generated: list[str] = []
    source_table_name_counts = _count_source_table_names(sink_tables)
    for sink_key in sorted(sink_tables):
        sink_cfg = sink_tables[sink_key]
        from_ref = sink_cfg.get("from")
        source_table_name = ""
        if isinstance(from_ref, str) and from_ref:
            source_table_name = from_ref.split(".", 1)[-1]
        migration = _process_table(
            sink_key,
            sink_cfg,
            svc_data.service_config,
            svc_data.table_defs,
            result,
            svc_data.type_mapper,
            runtime_mode=ctx.runtime_mode,
            duplicate_source_table_name_count=source_table_name_counts.get(
                source_table_name.casefold(),
                1,
            ),
        )
        if migration is None:
            continue

        _generate_table_files(ctx, migration, sink_cfg, result)
        tables_generated.append(migration.table_name)
        result.tables_processed += 1

    _ = write_manifest(
        ctx.output_dir,
        tables_generated,
        schemas,
        ctx.generated_at,
        ctx.sink_target,
        runtime_mode=ctx.runtime_mode,
        include_native_runtime=ctx.runtime_mode == "native",
        source_type=source_type,
        topology_kind=topology_kind,
        runtime_engine=runtime_engine,
    )
    result.files_written += 1


def _count_source_table_names(
    sink_tables: dict[str, dict[str, Any]],
) -> dict[str, int]:
    """Count source table-name occurrences to keep FDW naming deterministic."""
    counts: dict[str, int] = {}
    for sink_cfg in sink_tables.values():
        from_ref = sink_cfg.get("from")
        if not isinstance(from_ref, str) or not from_ref:
            continue
        source_table_name = from_ref.split(".", 1)[-1].casefold()
        counts[source_table_name] = counts.get(source_table_name, 0) + 1
    return counts
