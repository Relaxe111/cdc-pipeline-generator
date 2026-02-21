"""Table operations for CDC service configuration."""


import yaml

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root, load_service_config

from .config import save_service_config


def _sort_source_tables_in_config(config: dict[str, object]) -> None:
    """Sort source.tables keys alphabetically in-place."""
    source_raw = config.get('source')
    if not isinstance(source_raw, dict):
        return

    tables_raw = source_raw.get('tables')
    if not isinstance(tables_raw, dict):
        return

    sorted_tables = dict(sorted(tables_raw.items(), key=lambda entry: str(entry[0]).casefold()))
    source_raw['tables'] = sorted_tables


def get_primary_key_from_schema(service: str, schema: str, table: str) -> str | None:
    """Auto-detect primary key from table schema file."""
    try:
        schema_file = get_project_root() / 'service-schemas' / service / schema / f'{table}.yaml'

        if not schema_file.exists():
            return None

        with open(schema_file) as f:
            table_schema = yaml.safe_load(f)

        # Look for primary_key field
        primary_key = table_schema.get('primary_key')
        if primary_key:
            return primary_key

        # Fallback: look for columns with primary_key: true
        columns = table_schema.get('columns', [])
        pk_columns = [col['name'] for col in columns if col.get('primary_key')]

        if len(pk_columns) == 1:
            return pk_columns[0]
        if len(pk_columns) > 1:
            return pk_columns  # Composite key

        return None

    except Exception as e:
        print_warning(f"Could not read schema file for {schema}.{table}: {e}")
        return None


def add_table_to_service(service: str, schema: str, table: str, primary_key: str | None = None,
                        ignore_columns: list[str] | None = None, track_columns: list[str] | None = None) -> bool:
    """Add a table to the service configuration or update columns if table exists."""
    try:
        config = load_service_config(service)

        # Ensure source.tables exists
        if 'source' not in config:
            config['source'] = {}
        if 'tables' not in config['source']:
            config['source']['tables'] = {}

        # Create schema-qualified table name
        qualified_name = f"{schema}.{table}"

        # Check if table already exists
        table_exists = qualified_name in config['source']['tables']

        if table_exists:
            # If columns are specified, update the existing table
            if ignore_columns or track_columns:
                table_def = config['source']['tables'][qualified_name]
                if ignore_columns:
                    # Merge with existing ignore_columns
                    existing_ignore = table_def.get('ignore_columns', [])
                    table_def['ignore_columns'] = sorted(set(existing_ignore + ignore_columns))
                if track_columns:
                    # Merge with existing include_columns
                    existing_include = table_def.get('include_columns', [])
                    table_def['include_columns'] = sorted(set(existing_include + track_columns))

                config['source']['tables'][qualified_name] = table_def
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

        # Only add columns if explicitly specified
        if ignore_columns:
            table_def['ignore_columns'] = ignore_columns
        if track_columns:
            table_def['include_columns'] = track_columns

        config['source']['tables'][qualified_name] = table_def
        _sort_source_tables_in_config(config)

        # Save config
        if save_service_config(service, config):
            print_success(f"Added table {qualified_name} to {service}.yaml")
            return True
        return False

    except Exception as e:
        print_error(f"Failed to add table: {e}")
        import traceback
        traceback.print_exc()
        return False


def remove_table_from_service(service: str, schema: str, table: str) -> bool:
    """Remove a table from the service configuration."""
    try:
        config = load_service_config(service)

        # Ensure source.tables exists
        if 'source' not in config or 'tables' not in config['source']:
            print_warning(f"No tables configured in service {service}")
            return False

        # Create schema-qualified table name
        qualified_name = f"{schema}.{table}"

        # Check if table exists
        if qualified_name not in config['source']['tables']:
            print_warning(f"Table {qualified_name} not found in service config")
            return False

        # Remove table
        del config['source']['tables'][qualified_name]

        # Save config
        if save_service_config(service, config):
            print_success(f"Removed table {qualified_name} from {service}.yaml")
            return True
        return False

    except Exception as e:
        print_error(f"Failed to remove table: {e}")
        return False
