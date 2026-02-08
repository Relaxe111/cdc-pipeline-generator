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
