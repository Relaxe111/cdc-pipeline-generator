"""Table and column autocompletion functions."""

from cdc_generator.helpers.autocompletions.utils import find_directory_upward
from cdc_generator.helpers.yaml_loader import load_yaml_file


def list_tables_for_service(service_name: str) -> list[str]:
    """List available tables for a specific service from service-schemas directory.

    Format: schema.TableName

    Args:
        service_name: Name of the service.

    Returns:
        List of tables in format "schema.TableName".

    Example:
        >>> list_tables_for_service('chat')
        ['public.users', 'public.rooms', 'logs.activity']
    """
    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []

    service_dir = schemas_dir / service_name
    if not service_dir.is_dir():
        return []

    tables: list[str] = []
    for schema_dir in service_dir.iterdir():
        if schema_dir.is_dir():
            schema_name = schema_dir.name
            for table_file in schema_dir.glob('*.yaml'):
                if table_file.is_file():
                    table_name = table_file.stem
                    tables.append(f"{schema_name}.{table_name}")

    return sorted(tables)


def list_columns_for_table(service_name: str, schema: str, table: str) -> list[str]:
    """List columns for a specific table from service-schemas directory.

    Format: schema.table.column

    Args:
        service_name: Name of the service.
        schema: Schema name.
        table: Table name.

    Returns:
        List of columns in format "schema.table.column".

    Expected YAML structure:
        columns:
          - name: col1
          - name: col2

    Example:
        >>> list_columns_for_table('chat', 'public', 'users')
        ['public.users.id', 'public.users.username', 'public.users.email']
    """
    schemas_dir = find_directory_upward('service-schemas')
    if not schemas_dir:
        return []

    table_file = schemas_dir / service_name / schema / f'{table}.yaml'
    if not table_file.is_file():
        return []

    try:
        table_schema = load_yaml_file(table_file)
        if not table_schema:
            return []

        columns = table_schema.get('columns', [])
        if not isinstance(columns, list):
            return []

        # Return in format schema.table.column
        return sorted(
            f"{schema}.{table}.{col['name']}"
            for col in columns
            if isinstance(col, dict) and col.get('name')
        )

    except Exception:
        return []


def list_source_tables_for_service(service_name: str) -> list[str]:
    """List source tables from a service's source.tables configuration.

    Args:
        service_name: Name of the service.

    Returns:
        List of table keys in format 'schema.table'.

    Expected YAML structure (services/{service}.yaml):
        service_name:
          source:
            tables:
              schema.table1: {...}
              schema.table2: {...}

    Example:
        >>> list_source_tables_for_service('chat')
        ['public.users', 'public.rooms']
    """
    services_dir = find_directory_upward('services')
    if not services_dir:
        return []

    service_file = services_dir / f'{service_name}.yaml'
    if not service_file.is_file():
        return []

    try:
        data = load_yaml_file(service_file)
        if not data:
            return []

        # Support new format (service name as root key)
        config = data.get(service_name, data) if service_name in data else data
        if not isinstance(config, dict):
            return []

        source = config.get('source', {})
        tables = source.get('tables', {}) if isinstance(source, dict) else {}
        return sorted(str(k) for k in tables) if isinstance(tables, dict) else []

    except Exception:
        return []
