"""Table operations for CDC service configuration."""

import traceback
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file

from .config import save_service_config


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items = cast(list[object], value)
    return [item for item in items if isinstance(item, str)]


def _ensure_source_tables(config: dict[str, object]) -> dict[str, object]:
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        source_raw = {}
        config["source"] = source_raw

    source_dict = cast(dict[str, object], source_raw)
    tables_raw = source_dict.get("tables")
    if not isinstance(tables_raw, dict):
        tables_raw = {}
        source_dict["tables"] = tables_raw

    return cast(dict[str, object], tables_raw)


def _get_source_tables_if_present(config: dict[str, object]) -> dict[str, object] | None:
    source_raw = config.get("source")
    if not isinstance(source_raw, dict):
        return None
    source_dict = cast(dict[str, object], source_raw)
    tables_raw = source_dict.get("tables")
    if not isinstance(tables_raw, dict):
        return None
    return cast(dict[str, object], tables_raw)


def _sort_source_tables_in_config(config: dict[str, object]) -> None:
    """Sort source.tables keys alphabetically in-place."""
    source_raw = config.get('source')
    if not isinstance(source_raw, dict):
        return

    source_dict = cast(dict[str, object], source_raw)
    tables_raw = source_dict.get('tables')
    if not isinstance(tables_raw, dict):
        return

    table_items = cast(dict[str, object], tables_raw).items()
    sorted_tables = dict(
        sorted(
            table_items,
            key=lambda entry: entry[0].casefold(),
        )
    )
    source_dict['tables'] = sorted_tables


def get_primary_key_from_schema(service: str, schema: str, table: str) -> str | None:
    """Auto-detect primary key from table schema file."""
    try:
        project_root = get_project_root()
        table_schema: dict[str, object] | None = None
        for service_dir in get_service_schema_read_dirs(service, project_root):
            schema_file = service_dir / schema / f'{table}.yaml'
            if not schema_file.exists():
                continue
            table_schema = cast(dict[str, object], load_yaml_file(schema_file))
            break

        if table_schema is None:
            return None

        # Look for primary_key field
        primary_key = table_schema.get('primary_key')
        if isinstance(primary_key, str) and primary_key:
            return primary_key

        # Fallback: look for columns with primary_key: true
        columns_raw = table_schema.get('columns', [])
        if not isinstance(columns_raw, list):
            return None

        pk_columns: list[str] = []
        for column_raw in cast(list[object], columns_raw):
            if not isinstance(column_raw, dict):
                continue
            column = cast(dict[str, object], column_raw)
            if not column.get('primary_key'):
                continue
            name_raw = column.get('name')
            if isinstance(name_raw, str):
                pk_columns.append(name_raw)

        if pk_columns:
            return pk_columns[0]

        return None

    except Exception as e:
        print_warning(f"Could not read schema file for {schema}.{table}: {e}")
        return None


def add_table_to_service(service: str, schema: str, table: str, primary_key: str | None = None,
                        ignore_columns: list[str] | None = None, track_columns: list[str] | None = None) -> bool:
    """Add a table to the service configuration or update columns if table exists."""
    try:
        config = load_service_config(service)
        tables = _ensure_source_tables(config)

        # Create schema-qualified table name
        qualified_name = f"{schema}.{table}"

        # Check if table already exists
        table_exists = qualified_name in tables

        if table_exists:
            # If columns are specified, update the existing table
            if ignore_columns or track_columns:
                table_raw = tables.get(qualified_name, {})
                table_def = cast(dict[str, object], table_raw) if isinstance(table_raw, dict) else {}
                if ignore_columns:
                    # Merge with existing ignore_columns
                    existing_ignore = _as_str_list(table_def.get('ignore_columns', []))
                    table_def['ignore_columns'] = sorted(set(existing_ignore + ignore_columns))
                if track_columns:
                    # Merge with existing include_columns
                    existing_include = _as_str_list(table_def.get('include_columns', []))
                    table_def['include_columns'] = sorted(set(existing_include + track_columns))

                tables[qualified_name] = table_def
                _sort_source_tables_in_config(config)

                if save_service_config(service, config):
                    print_success(f"Updated columns for table {qualified_name} in {service}.yaml")
                    return True
                return False
            # No columns specified and table exists - just a warning
            print_warning(f"Table {qualified_name} already exists in service config (use --track-columns or --ignore-columns to update)")
            return False

        # Add new table - start with empty dict (primary key available in service-schemas)
        table_def = {}

        if primary_key:
            table_def['primary_key'] = primary_key

        # Only add columns if explicitly specified
        if ignore_columns:
            table_def['ignore_columns'] = ignore_columns
        if track_columns:
            table_def['include_columns'] = track_columns

        tables[qualified_name] = table_def
        _sort_source_tables_in_config(config)

        # Save config
        if save_service_config(service, config):
            print_success(f"Added table {qualified_name} to {service}.yaml")
            return True
        return False

    except Exception as e:
        print_error(f"Failed to add table: {e}")
        traceback.print_exc()
        return False


def remove_table_from_service(service: str, schema: str, table: str) -> bool:
    """Remove a table from the service configuration."""
    try:
        config = load_service_config(service)
        tables = _get_source_tables_if_present(config)

        # Ensure source.tables exists
        if tables is None:
            print_warning(f"No tables configured in service {service}")
            return False

        # Create schema-qualified table name
        qualified_name = f"{schema}.{table}"

        # Check if table exists
        if qualified_name not in tables:
            print_warning(f"Table {qualified_name} not found in service config")
            return False

        # Remove table
        del tables[qualified_name]

        # Save config
        if save_service_config(service, config):
            print_success(f"Removed table {qualified_name} from {service}.yaml")
            return True
        return False

    except Exception as e:
        print_error(f"Failed to remove table: {e}")
        return False
