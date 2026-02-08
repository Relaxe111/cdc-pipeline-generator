"""YAML structure building for different server group patterns."""

from typing import Any


def build_db_shared_structure(
    databases: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Build sources structure for db-shared pattern (environment-aware).

    Args:
        databases: List of database info dictionaries

    Returns:
        Dictionary mapping source name -> {schemas, environments}
    """
    source_data: dict[str, dict[str, Any]] = {}

    for db in databases:
        service = db.get('service', db['name'])
        env = db.get('environment', '')
        server_name = db.get('server', 'default')

        # Initialize source if new
        if service not in source_data:
            source_data[service] = {
                'schemas': set(),
                'environments': {}
            }

        # Collect all schemas at source level
        for schema in db['schemas']:
            source_data[service]['schemas'].add(schema)

        # Store database for this environment
        if env:
            if env not in source_data[service]['environments']:
                source_data[service]['environments'][env] = {
                    'server': server_name,
                    'database': db['name'],
                    'table_count': db.get('table_count', 0)
                }
            else:
                # Multiple databases for same source+env
                existing = source_data[service]['environments'][env]
                existing_db = existing['database']

                if isinstance(existing_db, str):
                    existing['database'] = [existing_db, db['name']]
                else:
                    existing_db.append(db['name'])

                existing['table_count'] += db.get('table_count', 0)

    return source_data


def build_db_per_tenant_structure(
    databases: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Build sources structure for db-per-tenant pattern.

    Args:
        databases: List of database info dictionaries

    Returns:
        Dictionary mapping source name -> {schemas, default}
    """
    source_data: dict[str, dict[str, Any]] = {}

    for db in databases:
        customer = db.get('customer', db['name'])
        server_name = db.get('server', 'default')

        if customer not in source_data:
            source_data[customer] = {
                'schemas': set(db.get('schemas', [])),
                'default': {
                    'server': server_name,
                    'database': db['name'],
                    'table_count': db.get('table_count', 0)
                }
            }
        else:
            # Merge schemas
            for schema in db.get('schemas', []):
                source_data[customer]['schemas'].add(schema)

    return source_data


def convert_to_yaml_structure(
    source_data: dict[str, dict[str, Any]],
    pattern: str
) -> dict[str, Any]:
    """
    Convert source data to final YAML structure.

    Args:
        source_data: Processed source data from build functions
        pattern: Server group pattern (db-shared or db-per-tenant)

    Returns:
        Dictionary ready for YAML serialization
    """
    sources: dict[str, Any] = {}

    for source_name, data in sorted(source_data.items()):
        sources[source_name] = {
            'schemas': sorted(data['schemas'])
        }

        if pattern == 'db-shared':
            # Add each environment as direct key
            for env, env_data in sorted(data['environments'].items()):
                sources[source_name][env] = env_data
        else:
            # db-per-tenant: single 'default' environment
            sources[source_name]['default'] = data['default']

    return sources
