"""Generate per-service table autocomplete definitions.

Creates files under:
  services/_schemas/_definitions/{service}-autocompletes.yaml

Each file structure:
  schema_name:
    - TableA
    - TableB
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from cdc_generator.helpers.helpers_logging import print_info, print_warning
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import yaml

from .db_inspector import get_mssql_connection, get_postgres_connection
from .filters import should_exclude_schema, should_exclude_table, should_include_table
from .types import DatabaseInfo, ServerConfig, ServerGroupConfig


def _definitions_dir() -> Path:
    return get_project_root() / "services" / "_schemas" / "_definitions"


def _load_existing_autocomplete_file(path: Path) -> dict[str, set[str]]:
    if not path.is_file():
        return {}

    with path.open() as f:
        raw_data = yaml.load(f)

    if not isinstance(raw_data, dict):
        return {}

    loaded: dict[str, set[str]] = {}
    for schema_raw, tables_raw in raw_data.items():
        if not isinstance(schema_raw, str):
            continue
        schema_name = schema_raw.strip()
        if not schema_name or not isinstance(tables_raw, list):
            continue

        table_names = {
            table_name.strip()
            for table_name in tables_raw
            if isinstance(table_name, str) and table_name.strip()
        }
        if table_names:
            loaded[schema_name] = table_names

    return loaded


def _fetch_postgres_tables_by_schema(
    server_config: ServerConfig,
    database_name: str,
    schemas: list[str],
) -> dict[str, list[str]]:
    db_conn = get_postgres_connection(server_config, database_name)
    db_cursor = db_conn.cursor()

    by_schema: dict[str, list[str]] = {}
    for schema_name in schemas:
        db_cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema_name,),
        )
        table_names = [
            str(row[0])
            for row in db_cursor.fetchall()
            if row and isinstance(row[0], str)
        ]
        if table_names:
            by_schema[schema_name] = table_names

    db_conn.close()
    return by_schema


def _fetch_mssql_tables_by_schema(
    server_config: ServerConfig,
    database_name: str,
    schemas: list[str],
) -> dict[str, list[str]]:
    conn = get_mssql_connection(server_config, database_name)
    cursor = conn.cursor()

    by_schema: dict[str, list[str]] = {}
    for schema_name in schemas:
        cursor.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
              AND TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME
            """,
            (schema_name,),
        )
        table_names = [
            str(row[0])
            for row in cursor.fetchall()
            if row and isinstance(row[0], str)
        ]
        if table_names:
            by_schema[schema_name] = table_names

    conn.close()
    return by_schema


def _fetch_tables_by_schema(
    db_type: str,
    server_config: ServerConfig,
    database_name: str,
    schemas: list[str],
) -> dict[str, list[str]]:
    if db_type == "postgres":
        return _fetch_postgres_tables_by_schema(
            server_config,
            database_name,
            schemas,
        )
    return _fetch_mssql_tables_by_schema(
        server_config,
        database_name,
        schemas,
    )


def generate_service_autocomplete_definitions(
    server_group: ServerGroupConfig,
    scanned_databases: list[DatabaseInfo],
    table_include_patterns: list[str] | None = None,
    table_exclude_patterns: list[str] | None = None,
    schema_exclude_patterns: list[str] | None = None,
) -> bool:
    """Generate/refresh per-service table autocomplete definitions from scanned DBs."""
    db_type = str(server_group.get("type") or "").strip().lower()
    if db_type not in {"postgres", "mssql"}:
        return False

    servers_raw = server_group.get("servers", {})
    if not isinstance(servers_raw, dict):
        return False
    servers = cast(dict[str, ServerConfig], servers_raw)

    aggregated: dict[str, dict[str, set[str]]] = {}
    regenerated_schemas_by_service: dict[str, set[str]] = {}

    for database in scanned_databases:
        service_name = str(database.get("service") or "").strip()
        database_name = str(database.get("name") or "").strip()
        server_name = str(database.get("server") or "default").strip() or "default"

        schemas_raw = database.get("schemas", [])
        schemas = [
            str(schema_name).strip()
            for schema_name in schemas_raw
            if isinstance(schema_name, str) and str(schema_name).strip()
        ]

        if not service_name or not database_name or not schemas:
            continue

        server_config = servers.get(server_name)
        if not server_config:
            continue

        try:
            tables_by_schema = _fetch_tables_by_schema(
                db_type,
                server_config,
                database_name,
                schemas,
            )
        except Exception as exc:
            print_warning(
                "Autocomplete definition scan failed for "
                + f"{service_name}/{database_name}: {exc}"
            )
            continue

        aggregated.setdefault(service_name, {})
        regenerated_schemas_by_service.setdefault(service_name, set()).update(schemas)

        for schema_name in schemas:
            aggregated[service_name].setdefault(schema_name, set())

        for schema_name, table_names in tables_by_schema.items():
            filtered_table_names = [
                table_name
                for table_name in table_names
                if (
                    should_include_table(table_name, table_include_patterns)
                    and not should_exclude_table(table_name, table_exclude_patterns)
                )
            ]
            aggregated[service_name].setdefault(schema_name, set())
            aggregated[service_name][schema_name].update(filtered_table_names)

    if not aggregated:
        return False

    defs_dir = _definitions_dir()
    defs_dir.mkdir(parents=True, exist_ok=True)

    for service_name, schemas_data in aggregated.items():
        target_file = defs_dir / f"{service_name}-autocompletes.yaml"
        merged = _load_existing_autocomplete_file(target_file)

        # Prune schemas excluded by current config, even if they are stale from
        # old runs and were not part of this specific regeneration batch.
        for schema_name in list(merged):
            if should_exclude_schema(schema_name, schema_exclude_patterns):
                merged.pop(schema_name, None)

        regenerated_schemas = regenerated_schemas_by_service.get(service_name, set())
        for schema_name in regenerated_schemas:
            table_names = schemas_data.get(schema_name, set())
            if table_names:
                merged[schema_name] = set(table_names)
            else:
                merged.pop(schema_name, None)

        payload = {
            schema_name: sorted(table_names)
            for schema_name, table_names in sorted(merged.items())
            if table_names
        }

        with target_file.open("w") as f:
            yaml.dump(payload, f)

        print_info(
            "✓ Updated autocomplete definitions: "
            + f"services/_schemas/_definitions/{service_name}-autocompletes.yaml"
        )

    return True
