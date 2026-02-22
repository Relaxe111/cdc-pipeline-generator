"""YAML-based schema loading from service schema directories."""

from typing import Any, cast

from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file


def load_schemas_from_yaml(service: str, schema_filter: str | None = None) -> dict[str, Any]:
    """Load table schemas from preferred/legacy service schema paths.

    Args:
        service: Service name (e.g., 'adopus', 'directory')
        schema_filter: Optional schema name to filter by

    Returns:
        Dict[schema_name, Dict[table_name, table_metadata]]
        where table_metadata = {'columns': [...], 'primary_key': ...}
    """
    schemas_data: dict[str, Any] = {}

    project_root = get_project_root()
    for service_schemas_dir in get_service_schema_read_dirs(service, project_root):
        if not service_schemas_dir.exists() or not service_schemas_dir.is_dir():
            continue

        # Iterate through schema directories
        for schema_dir in service_schemas_dir.iterdir():
            if not schema_dir.is_dir():
                continue

            schema_name = schema_dir.name

            # Apply schema filter if provided
            if schema_filter and schema_name != schema_filter:
                continue

            if schema_name not in schemas_data:
                schemas_data[schema_name] = {}

            # Load all table YAML files in this schema
            for table_file in schema_dir.glob('*.yaml'):
                try:
                    loaded = load_yaml_file(table_file)
                    table_data = loaded

                    table_name = table_data.get('table')
                    if not isinstance(table_name, str) or not table_name:
                        continue

                    columns_raw = table_data.get('columns', [])
                    if not isinstance(columns_raw, list):
                        columns_raw = []

                    # Extract primary key(s) from columns
                    primary_keys = [
                        col.get('name')
                        for col in columns_raw
                        if isinstance(col, dict)
                        and col.get('primary_key')
                        and isinstance(col.get('name'), str)
                    ]
                    primary_key = (
                        primary_keys[0]
                        if len(primary_keys) == 1
                        else primary_keys
                        if primary_keys
                        else None
                    )

                    # Store in schema data structure (matches database query format)
                    schemas_data[schema_name][table_name] = {
                        'columns': columns_raw,
                        'primary_key': primary_key,
                    }
                except Exception as e:
                    print_error(f"Failed to load {table_file}: {e}")

    return schemas_data


def load_server_groups_config() -> dict[str, Any]:
    """Load source-groups.yaml configuration.

    Returns:
        Dict with server group configurations
    """
    project_root = get_project_root()
    server_groups_file = project_root / 'source-groups.yaml'
    if not server_groups_file.exists():
        return {}
    return load_yaml_file(server_groups_file)


def extract_database_names_by_group(server_groups_data: dict[str, Any]) -> dict[str, set[str]]:
    """Extract all database names grouped by server group.

    Args:
        server_groups_data: Parsed source-groups.yaml data

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
                    db_dict = cast(dict[str, object], db)
                    db_name = db_dict.get('name')
                    if isinstance(db_name, str) and db_name:
                        db_names.add(db_name)
        all_database_names_by_group[group_name] = db_names

    return all_database_names_by_group
