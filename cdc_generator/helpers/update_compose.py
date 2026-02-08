#!/usr/bin/env python3
"""
Update docker-compose.yml with database services based on server group configuration.
"""

from pathlib import Path
from typing import Any

import yaml


def get_mssql_service(env_prefix: str = "MSSQL") -> dict[str, Any]:
    """Generate MSSQL service configuration."""
    return {
        'image': 'mcr.microsoft.com/mssql/server:2022-latest',
        'environment': {
            'ACCEPT_EULA': 'Y',
            'SA_PASSWORD': f'${{{env_prefix}_PASSWORD}}',
            'MSSQL_PID': 'Developer'
        },
        'ports': [f'${{{env_prefix}_PORT:-1433}}:1433'],
        'volumes': ['mssql-data:/var/opt/mssql'],
        'healthcheck': {
            'test': ['CMD-SHELL', '/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1"'],
            'interval': '10s',
            'timeout': '3s',
            'retries': 5
        }
    }


def get_postgres_service(service_name: str = "postgres", env_prefix: str = "POSTGRES") -> dict[str, Any]:
    """Generate PostgreSQL service configuration."""
    return {
        'image': 'postgres:15-alpine',
        'environment': {
            'POSTGRES_USER': f'${{{env_prefix}_USER:-postgres}}',
            'POSTGRES_PASSWORD': f'${{{env_prefix}_PASSWORD}}',
            'POSTGRES_DB': f'${{{env_prefix}_DB:-cdc_target}}'
        },
        'ports': [f'${{{env_prefix}_PORT:-5432}}:5432'],
        'volumes': [f'{service_name}-data:/var/lib/postgresql/data'],
        'healthcheck': {
            'test': ['CMD-SHELL', 'pg_isready -U ${POSTGRES_USER:-postgres}'],
            'interval': '10s',
            'timeout': '3s',
            'retries': 5
        }
    }


def update_docker_compose(
    source_type: str,
    project_root: Path,
    source_service_name: str | None = None,
    target_service_name: str | None = None
) -> bool:
    """
    Update docker-compose.yml with database services.

    Args:
        source_type: Database type ('mssql' or 'postgresql')
        project_root: Path to project root
        source_service_name: Optional custom name for source database service
        target_service_name: Optional custom name for target database service

    Returns:
        bool: True if successful, False otherwise
    """
    compose_file = project_root / 'docker-compose.yml'

    if not compose_file.exists():
        print(f"⚠️  docker-compose.yml not found at {compose_file}")
        return False

    try:
        # Load existing docker-compose.yml
        with open(compose_file) as f:
            compose_config = yaml.safe_load(f) or {}

        # Ensure services section exists
        if 'services' not in compose_config:
            compose_config['services'] = {}

        services = compose_config['services']

        # Determine service names
        source_name = source_service_name or ('mssql' if source_type == 'mssql' else 'postgres-source')
        target_name = target_service_name or 'postgres-target'

        # Add source database service if not exists
        if source_name not in services:
            if source_type == 'mssql':
                services[source_name] = get_mssql_service()
                print(f"✅ Added '{source_name}' service to docker-compose.yml")
            elif source_type == 'postgresql':
                services[source_name] = get_postgres_service(source_name, 'POSTGRES_SOURCE')
                print(f"✅ Added '{source_name}' service to docker-compose.yml")
        else:
            print(f"ℹ️  '{source_name}' service already exists, skipping")

        # Add target PostgreSQL service if not exists (CDC always targets PostgreSQL)
        if target_name not in services:
            services[target_name] = get_postgres_service(target_name, 'POSTGRES_TARGET')
            print(f"✅ Added '{target_name}' service to docker-compose.yml")
        else:
            print(f"ℹ️  '{target_name}' service already exists, skipping")

        # Ensure volumes section exists
        if 'volumes' not in compose_config:
            compose_config['volumes'] = {}

        volumes = compose_config['volumes']

        # Add volume definitions if not exist
        if source_type == 'mssql' and 'mssql-data' not in volumes:
            volumes['mssql-data'] = None
        elif source_type == 'postgresql':
            volume_name = f'{source_name}-data'
            if volume_name not in volumes:
                volumes[volume_name] = None

        target_volume = f'{target_name}-data'
        if target_volume not in volumes:
            volumes[target_volume] = None

        # Update dev service dependencies if it exists
        if 'dev' in services:
            dev_service = services['dev']
            if 'depends_on' not in dev_service:
                dev_service['depends_on'] = []

            # Add database services as dependencies
            if source_name not in dev_service['depends_on']:
                dev_service['depends_on'].append(source_name)
            if target_name not in dev_service['depends_on']:
                dev_service['depends_on'].append(target_name)

        # Write updated docker-compose.yml
        with open(compose_file, 'w') as f:
            yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False, indent=2)

        print("✅ Updated docker-compose.yml with database services")
        return True

    except Exception as e:
        print(f"❌ Error updating docker-compose.yml: {e}")
        return False


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python update_compose.py <source_type>")
        sys.exit(1)

    source_type = sys.argv[1]
    project_root = Path.cwd()

    success = update_docker_compose(source_type, project_root)
    sys.exit(0 if success else 1)
