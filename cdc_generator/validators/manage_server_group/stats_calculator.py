"""Statistics calculation for server groups and databases."""

from collections import defaultdict
from typing import Any

from .types import ServerGroupConfig


def calculate_database_stats(
    databases: list[dict[str, Any]]
) -> tuple[int, int, int]:
    """
    Calculate total database statistics.

    Args:
        databases: List of database info dictionaries

    Returns:
        Tuple of (total_dbs, total_tables, avg_tables)
    """
    total_dbs = len(databases)
    total_tables = sum(db['table_count'] for db in databases)
    avg_tables = int(total_tables / total_dbs) if total_dbs > 0 else 0

    return total_dbs, total_tables, avg_tables


def calculate_environment_stats(
    databases: list[dict[str, Any]]
) -> dict[str, dict[str, int]]:
    """
    Calculate per-environment statistics.

    Args:
        databases: List of database info dictionaries

    Returns:
        Dictionary mapping environment -> {dbs, tables}
    """
    env_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {'dbs': 0, 'tables': 0}
    )

    for db in databases:
        env = db.get('environment', '')

        if env:
            env_stats[env]['dbs'] += 1
            env_stats[env]['tables'] += db.get('table_count', 0)

    return dict(env_stats)


def build_environment_stats_line(
    env_stats: dict[str, dict[str, int]]
) -> str:
    """
    Build formatted environment statistics line.

    Args:
        env_stats: Environment statistics dictionary

    Returns:
        Formatted stats string (e.g., "dev: 3 dbs, 45 tables | prod: 3 dbs, 43 tables")
    """
    parts: list[str] = []

    for env in sorted(env_stats.keys()):
        stats = env_stats[env]
        parts.append(f"{env}: {stats['dbs']} dbs, {stats['tables']} tables")

    return " | ".join(parts) if parts else ""


def extract_service_environments(
    databases: list[dict[str, Any]],
    pattern: str
) -> dict[str, set[str]]:
    """
    Extract service-to-environments mapping.

    Args:
        databases: List of database info dictionaries
        pattern: Server group pattern (db-shared or db-per-tenant)

    Returns:
        Dictionary mapping service name -> set of environments
    """
    service_envs: dict[str, set[str]] = defaultdict(set)

    if pattern != 'db-shared':
        return dict(service_envs)

    for db in databases:
        env = db.get('environment', '')
        if env:
            service = db.get('service', 'unknown')
            service_envs[service].add(env)

    return dict(service_envs)


def determine_service_info(
    pattern: str,
    server_group_name: str,
    server_group: ServerGroupConfig,
    service_groups: dict[str, dict[str, Any]] | None = None,
    service_envs: dict[str, set[str]] | None = None
) -> tuple[int, str]:
    """
    Determine number of services and service list.

    Args:
        pattern: Server group pattern (db-shared or db-per-tenant)
        server_group_name: Name of server group
        server_group: Server group configuration
        service_groups: Optional service groups (from db-shared grouping)
        service_envs: Optional service environments mapping

    Returns:
        Tuple of (num_services, service_list)
    """
    # Try service_groups first (most accurate for db-shared)
    if pattern == 'db-shared' and service_groups:
        num_services = len(service_groups)
        service_list = ", ".join(sorted(service_groups.keys()))
        return num_services, service_list

    # Try existing sources/services in config
    if pattern == 'db-shared' and (
        'sources' in server_group or 'services' in server_group
    ):
        existing = server_group.get(
            'sources',
            server_group.get('services', {})
        )
        num_services = len(existing)
        service_list = ", ".join(sorted(existing.keys()))
        return num_services, service_list

    # Try service_envs
    if service_envs:
        num_services = len(service_envs)
        service_list = ", ".join(sorted(service_envs.keys()))
        return num_services, service_list

    # Default to single service
    return 1, server_group_name
