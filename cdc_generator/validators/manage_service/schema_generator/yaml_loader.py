"""YAML-based schema loading from service-schemas/ directory."""

from pathlib import Path
from typing import Any

import yaml  # type: ignore

from cdc_generator.helpers.helpers_logging import print_error

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def load_schemas_from_yaml(service: str, schema_filter: str | None = None) -> dict[str, Any]:
    """Load table schemas from service-schemas/{service}/ YAML files.
    
    Args:
        service: Service name (e.g., 'adopus', 'directory')
        schema_filter: Optional schema name to filter by
    
    Returns:
        Dict[schema_name, Dict[table_name, table_metadata]]
        where table_metadata = {'columns': [...], 'primary_key': ...}
    """
    service_schemas_dir = PROJECT_ROOT / 'service-schemas' / service

    if not service_schemas_dir.exists():
        return {}

    schemas_data: dict[str, Any] = {}

    # Iterate through schema directories
    for schema_dir in service_schemas_dir.iterdir():
        if not schema_dir.is_dir():
            continue

        schema_name = schema_dir.name

        # Apply schema filter if provided
        if schema_filter and schema_name != schema_filter:
            continue

        schemas_data[schema_name] = {}

        # Load all table YAML files in this schema
        for table_file in schema_dir.glob('*.yaml'):
            try:
                with open(table_file) as f:
                    table_data = yaml.safe_load(f)

                table_name = table_data.get('table')
                if not table_name:
                    continue

                columns = table_data.get('columns', [])

                # Extract primary key(s) from columns
                primary_keys = [col['name'] for col in columns if col.get('primary_key')]
                primary_key = primary_keys[0] if len(primary_keys) == 1 else primary_keys if primary_keys else None

                # Store in schema data structure (matches database query format)
                schemas_data[schema_name][table_name] = {
                    'columns': columns,
                    'primary_key': primary_key
                }
            except Exception as e:
                print_error(f"Failed to load {table_file}: {e}")

    return schemas_data


def load_server_groups_config() -> dict[str, Any]:
    """Load server_group.yaml configuration.
    
    Returns:
        Dict with server group configurations
    """
    server_groups_file = PROJECT_ROOT / 'server_group.yaml'
    if not server_groups_file.exists():
        return {}

    with open(server_groups_file) as f:
        return yaml.safe_load(f)


def extract_database_names_by_group(server_groups_data: dict[str, Any]) -> dict[str, set[str]]:
    """Extract all database names grouped by server group.
    
    Args:
        server_groups_data: Parsed server_group.yaml data
    
    Returns:
        Dict[group_name, Set[database_names]]
    """
    all_database_names_by_group: dict[str, set[str]] = {}
    server_group_dict = server_groups_data.get('server_group', {})

    for group_name, sg in server_group_dict.items():
        db_names: set[str] = set()
        databases = sg.get('databases', [])
        if databases:  # Check if databases is not None
            for db in databases:
                # Handle both string and dict formats
                if isinstance(db, str):
                    db_names.add(db)
                elif isinstance(db, dict):
                    db_name = db.get('name')  # type: ignore[misc]
                    if db_name:
                        db_names.add(db_name)  # type: ignore[arg-type]
        all_database_names_by_group[group_name] = db_names

    return all_database_names_by_group
