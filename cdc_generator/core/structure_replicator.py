"""Replicate source table structure to sink with type mapping.

Reads source schema definitions from service-schemas/{service}/{schema}/{table}.yaml
and generates CREATE TABLE DDL for the sink database, applying type conversion
via adapter mapping files.

Used when a sink table has `replicate_structure: true` in the service config.

Example service config:
    sinks:
      sink_asma.proxy:
        tables:
          public.customer_user:
            target_exists: false
            replicate_structure: true
            source_engine: pgsql
            sink_engine: pgsql

Example usage:
    >>> from cdc_generator.core.structure_replicator import replicate_table_structure
    >>> ddl = replicate_table_structure(
    ...     service="directory",
    ...     source_schema="public",
    ...     table_name="customer_user",
    ...     source_engine="pgsql",
    ...     sink_engine="pgsql",
    ... )
    >>> print(ddl)
    CREATE TABLE IF NOT EXISTS "public"."customer_user" (
        "customer_id" uuid NOT NULL,
        ...
        PRIMARY KEY ("customer_id", "user_id")
    );
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.type_mapper import TypeMapper
from cdc_generator.helpers.yaml_loader import load_yaml_file


@dataclass
class CustomTableReference:
    """Parsed custom table reference from custom-tables/*.yaml.

    Attributes:
        source_service: Service name of source table.
        source_schema: Schema name of source table.
        source_table: Table name of source table.
        sink_schema: Target schema in sink.
        sink_table: Target table name in sink.
        extra_columns: Additional columns not in source (if any).
        transforms: Transform rules to apply (if any).
        column_templates: Column templates to apply (if any).
    """

    source_service: str
    source_schema: str
    source_table: str
    sink_schema: str
    sink_table: str
    extra_columns: list[dict[str, Any]] | None = None
    transforms: list[dict[str, Any]] | None = None
    column_templates: list[dict[str, Any]] | None = None


@dataclass
class ReplicationConfig:
    """Configuration for replicating a source table to a sink.

    Attributes:
        service: Service name (e.g., 'directory').
        source_schema: Source schema name (e.g., 'public').
        table_name: Table name (e.g., 'customer_user').
        source_engine: Source database engine (e.g., 'pgsql', 'mssql').
        sink_engine: Target database engine (e.g., 'pgsql').
        target_schema: Override schema name in sink (defaults to source_schema).
        include_columns: Only include these columns (None = all columns).
        schemas_dir: Override path to service-schemas/ directory.
    """

    service: str
    source_schema: str
    table_name: str
    source_engine: str
    sink_engine: str
    target_schema: str | None = None
    include_columns: list[str] | None = None
    schemas_dir: Path | None = None


def _find_schema_dir() -> Path | None:
    """Find the service-schemas directory by searching upward from CWD.

    Looks for service-schemas/ in current directory and parent directories.

    Returns:
        Path to service-schemas directory, or None if not found.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        candidate = parent / "service-schemas"
        if candidate.is_dir():
            return candidate
    return None


def load_source_schema(
    service: str,
    source_schema: str,
    table_name: str,
    schemas_dir: Path | None = None,
) -> dict[str, Any] | None:
    """Load a source table schema definition from service-schemas/.

    Args:
        service: Service name (e.g., 'directory').
        source_schema: Source schema name (e.g., 'public', 'logs').
        table_name: Table name (e.g., 'customer_user').
        schemas_dir: Override path to service-schemas/ directory.

    Returns:
        Parsed schema dict with columns, primary_key, etc.
        None if schema file not found.

    Example:
        >>> schema = load_source_schema("directory", "public", "customer_user")
        >>> schema["columns"][0]["name"]
        'customer_id'
    """
    if schemas_dir is None:
        schemas_dir = _find_schema_dir()

    if schemas_dir is None:
        return None

    schema_file = schemas_dir / service / source_schema / f"{table_name}.yaml"
    if not schema_file.exists():
        return None

    raw = load_yaml_file(schema_file)
    return cast(dict[str, Any], raw) if raw else None


def load_custom_table_reference(
    target_service: str,
    table_key: str,
    schemas_dir: Path | None = None,
) -> CustomTableReference | None:
    """Load a custom table reference from service-schemas/{target}/custom-tables/.

    Args:
        target_service: Target service name (e.g., 'chat', 'activities').
        table_key: Table key in format 'schema.table' (e.g., 'activities.customer_user').
        schemas_dir: Override path to service-schemas/ directory.

    Returns:
        Parsed CustomTableReference with source info and extra columns/transforms.
        None if reference file not found.

    Example:
        >>> ref = load_custom_table_reference("chat", "activities.customer_user")
        >>> ref.source_service
        'directory'
        >>> ref.source_table
        'customer_user'
    """
    if schemas_dir is None:
        schemas_dir = _find_schema_dir()

    if schemas_dir is None:
        return None

    ref_file = (
        schemas_dir / target_service / "custom-tables" / f"{table_key.replace('/', '_')}.yaml"
    )
    if not ref_file.exists():
        return None

    raw = load_yaml_file(ref_file)
    if not raw:
        return None

    # Parse source_reference (required)
    source_ref = raw.get("source_reference")
    if not isinstance(source_ref, dict):
        return None

    source_service = source_ref.get("service")
    source_schema = source_ref.get("schema")
    source_table = source_ref.get("table")

    if not all([source_service, source_schema, source_table]):
        return None

    # Parse sink_target (required)
    sink_target = raw.get("sink_target")
    if not isinstance(sink_target, dict):
        return None

    sink_schema = sink_target.get("schema")
    sink_table = sink_target.get("table")

    if not all([sink_schema, sink_table]):
        return None

    # Parse optional fields - prefer column_templates over deprecated extra_columns
    column_templates = raw.get("column_templates")
    extra_columns = raw.get("extra_columns")  # Backwards compatibility
    transforms = raw.get("transforms")

    # Use column_templates if present, otherwise fall back to extra_columns
    final_column_templates = column_templates if column_templates is not None else extra_columns

    return CustomTableReference(
        source_service=str(source_service),
        source_schema=str(source_schema),
        source_table=str(source_table),
        sink_schema=str(sink_schema),
        sink_table=str(sink_table),
        extra_columns=(
            cast(list[dict[str, Any]], final_column_templates)
            if isinstance(final_column_templates, list)
            else None
        ),
        transforms=(
            cast(list[dict[str, Any]], transforms)
            if isinstance(transforms, list)
            else None
        ),
        column_templates=(
            cast(list[dict[str, Any]], final_column_templates)
            if isinstance(final_column_templates, list)
            else None
        ),
    )


def replicate_table_structure(config: ReplicationConfig) -> str | None:
    """Generate CREATE TABLE DDL by replicating source schema with type mapping.

    Reads the source table definition from service-schemas/ and converts
    column types using the appropriate adapter mapping file.

    Args:
        config: Replication configuration with service, table, and engine details.

    Returns:
        CREATE TABLE DDL string, or None if source schema not found.

    Example:
        >>> cfg = ReplicationConfig(
        ...     service="directory",
        ...     source_schema="public",
        ...     table_name="customer_user",
        ...     source_engine="pgsql",
        ...     sink_engine="pgsql",
        ... )
        >>> ddl = replicate_table_structure(cfg)
        >>> print(ddl)
        CREATE TABLE IF NOT EXISTS "public"."customer_user" (
            "customer_id" uuid NOT NULL,
            "user_id" uuid NOT NULL,
            PRIMARY KEY ("customer_id", "user_id")
        );
    """
    schema_data = load_source_schema(
        config.service, config.source_schema, config.table_name, config.schemas_dir,
    )
    if schema_data is None:
        return None

    raw_columns = schema_data.get("columns", [])
    if not isinstance(raw_columns, list):
        return None

    # Cast to typed list for downstream processing
    columns = _to_column_dicts(cast(list[object], raw_columns))

    # Filter columns if include_columns specified
    columns = _filter_columns(columns, config.include_columns, schema_data)

    # Apply type mapping
    mapper = TypeMapper(config.source_engine, config.sink_engine)
    mapped_columns = mapper.map_columns(columns)

    # Build DDL
    sink_schema = config.target_schema or config.source_schema
    return _build_create_table_ddl(
        sink_schema, config.table_name, mapped_columns, schema_data,
    )


def _filter_columns(
    columns: list[dict[str, Any]],
    include_columns: list[str] | None,
    schema_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """Filter columns to only include specified ones (plus primary keys)."""
    if not include_columns:
        return columns

    include_set = set(include_columns)

    # Always include primary key columns
    pk_raw = schema_data.get("primary_key", [])
    if isinstance(pk_raw, list):
        for pk_entry in cast(list[object], pk_raw):
            if isinstance(pk_entry, str):
                include_set.add(pk_entry)
    elif isinstance(pk_raw, str):
        include_set.add(pk_raw)

    return [
        col for col in columns
        if str(col.get("name", "")) in include_set
    ]


def _to_column_dicts(raw_columns: list[object]) -> list[dict[str, Any]]:
    """Convert raw YAML column entries to typed dicts."""
    result: list[dict[str, Any]] = []
    for col in raw_columns:
        if isinstance(col, dict):
            result.append(cast(dict[str, Any], col))
    return result


def _build_create_table_ddl(
    schema: str,
    table_name: str,
    columns: list[dict[str, str | bool]],
    schema_data: dict[str, Any],
) -> str:
    """Build CREATE TABLE DDL from mapped column definitions.

    Args:
        schema: Target schema name.
        table_name: Target table name.
        columns: List of mapped column dicts with name, type, nullable, primary_key.
        schema_data: Original schema data (for primary_key constraint).

    Returns:
        Complete CREATE TABLE DDL string.
    """
    lines: list[str] = []

    for col in columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "text")
        nullable = col.get("nullable", True)

        if not isinstance(col_name, str) or not isinstance(col_type, str):
            continue

        parts = [f'    "{col_name}"', str(col_type)]

        if nullable is False:
            parts.append("NOT NULL")

        lines.append(" ".join(parts))

    # Add primary key constraint
    pk_raw = schema_data.get("primary_key", [])
    pk_list: list[str] = []

    if isinstance(pk_raw, list):
        for pk_entry in cast(list[object], pk_raw):
            if isinstance(pk_entry, str):
                pk_list.append(f'"{ pk_entry}"')
    elif isinstance(pk_raw, str):
        pk_list.append(f'"{ pk_raw}"')

    if pk_list:
        pk_expr = ", ".join(pk_list)
        lines.append(f"    PRIMARY KEY ({pk_expr})")

    columns_sql = ",\n".join(lines)

    return (
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (\n'
        + f"{columns_sql}\n"
        + ");"
    )


def get_replication_summary(config: ReplicationConfig) -> dict[str, Any] | None:
    """Get a summary of what replication would produce (without generating DDL).

    Useful for CLI preview/confirmation before generating.

    Args:
        config: Replication configuration with service, table, and engine details.

    Returns:
        Summary dict with column_count, primary_key, type_changes, etc.
        None if source schema not found.
    """
    schema_data = load_source_schema(
        config.service, config.source_schema, config.table_name, config.schemas_dir,
    )
    if schema_data is None:
        return None

    raw_columns = schema_data.get("columns", [])
    if not isinstance(raw_columns, list):
        return None

    mapper = TypeMapper(config.source_engine, config.sink_engine)
    column_dicts = _to_column_dicts(cast(list[object], raw_columns))

    type_changes: list[dict[str, str]] = []
    for col_dict in column_dicts:
        name = str(col_dict.get("name", ""))
        source_type = str(col_dict.get("type", ""))
        sink_type = mapper.map_type(source_type)
        if source_type != sink_type:
            type_changes.append({
                "column": name,
                "source_type": source_type,
                "sink_type": sink_type,
            })

    pk_columns = schema_data.get("primary_key", [])

    return {
        "service": config.service,
        "source_schema": config.source_schema,
        "table_name": config.table_name,
        "source_engine": config.source_engine,
        "sink_engine": config.sink_engine,
        "column_count": len(column_dicts),
        "primary_key": pk_columns,
        "type_changes": type_changes,
        "adapter": f"{config.source_engine}-to-{config.sink_engine}",
    }


def replicate_from_custom_reference(
    target_service: str,
    table_key: str,
    source_engine: str,
    sink_engine: str,
    schemas_dir: Path | None = None,
) -> str | None:
    """Generate CREATE TABLE DDL from a custom table reference.

    Loads the minimal reference file from custom-tables/, deduces base structure
    from the source table, applies type mapping, and adds any extra_columns.

    Args:
        target_service: Target service name (e.g., 'chat').
        table_key: Table key in 'schema.table' format (e.g., 'activities.customer_user').
        source_engine: Source database engine (e.g., 'pgsql').
        sink_engine: Target database engine (e.g., 'pgsql').
        schemas_dir: Override path to service-schemas/ directory.

    Returns:
        CREATE TABLE DDL string, or None if reference/source not found.

    Example:
        >>> ddl = replicate_from_custom_reference(
        ...     target_service="chat",
        ...     table_key="activities.customer_user",
        ...     source_engine="pgsql",
        ...     sink_engine="pgsql",
        ... )
        >>> print(ddl)
        CREATE TABLE IF NOT EXISTS "activities"."customer_user" (
            -- base columns from source directory.public.customer_user
            "customer_id" uuid NOT NULL,
            "user_id" uuid NOT NULL,
            -- extra columns from custom-tables reference
            "user_class" text NOT NULL,
            PRIMARY KEY ("customer_id", "user_id")
        );
    """
    # Load custom table reference
    ref = load_custom_table_reference(target_service, table_key, schemas_dir)
    if ref is None:
        return None

    # Load source schema using the reference
    schema_data = load_source_schema(
        ref.source_service,
        ref.source_schema,
        ref.source_table,
        schemas_dir,
    )
    if schema_data is None:
        return None

    # Get base columns from source
    raw_columns = schema_data.get("columns", [])
    if not isinstance(raw_columns, list):
        return None

    columns = _to_column_dicts(cast(list[object], raw_columns))

    # Apply type mapping
    mapper = TypeMapper(source_engine, sink_engine)
    mapped_columns = mapper.map_columns(columns)

    # Add extra_columns if defined in reference
    if ref.extra_columns:
        for extra_col in ref.extra_columns:
            # Extra columns already in target type, no mapping needed
            mapped_columns.append(cast(dict[str, str | bool], extra_col))

    # Build DDL
    return _build_create_table_ddl(
        ref.sink_schema,
        ref.sink_table,
        mapped_columns,
        schema_data,
    )
