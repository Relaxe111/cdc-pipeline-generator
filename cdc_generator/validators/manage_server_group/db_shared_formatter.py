"""Database formatting for db-shared pattern (multi-environment services)."""

from collections import defaultdict
from typing import Any

from cdc_generator.helpers.helpers_logging import Colors


def group_databases_by_service(databases: list[dict[str, Any]]) -> tuple[
    dict[str, dict[str, Any]],
    list[str]
]:
    """
    Group databases by service name for db-shared pattern.

    Args:
        databases: List of database info dictionaries

    Returns:
        Tuple of (service_groups, sorted_environments)
    """
    service_groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {'databases': {}}
    )
    all_environments: set[str] = set()

    for db in databases:
        inferred_service = db.get('service', 'unknown')
        env = db.get('environment', '')

        # Track service
        if inferred_service and inferred_service not in service_groups:
            service_groups[inferred_service] = {'databases': {}}

        # Store database info by environment
        if env:
            all_environments.add(env)
            service_groups[inferred_service]['databases'][env] = {
                'name': db['name'],
                'table_count': db.get('table_count', 0),
                'schemas': db.get('schemas', [])
            }

    sorted_envs = sorted(all_environments)
    return dict(service_groups), sorted_envs


def check_environment_warnings(
    env: str,
    db_info: dict[str, Any],
    dev_schemas: set[str],
    dev_table_count: int
) -> tuple[bool, list[str]]:
    """
    Check for warnings in environment database compared to dev.

    Args:
        env: Environment name
        db_info: Database information dictionary
        dev_schemas: Set of schemas in dev environment
        dev_table_count: Number of tables in dev

    Returns:
        Tuple of (has_warnings, warning_parts)
    """
    has_warnings = False
    warning_parts: list[str] = []

    # Skip dev environment
    if env == 'dev' or not dev_schemas:
        return has_warnings, warning_parts

    db_schemas: set[str] = set(db_info.get('schemas', []))
    table_count = db_info.get('table_count', 0)

    # Check schema mismatches
    if db_schemas != dev_schemas:
        missing = dev_schemas - db_schemas
        extra = db_schemas - dev_schemas

        if missing:
            warning_parts.append(
                f"missing schemas: {', '.join(sorted(missing))}"
            )
        if extra:
            warning_parts.append(
                f"extra schemas: {', '.join(sorted(extra))}"
            )
        has_warnings = True

    # Check table count differences
    if dev_table_count > 0 and table_count != dev_table_count:
        warning_parts.append(
            f"table count differs from dev ({dev_table_count} tables)"
        )
        has_warnings = True

    return has_warnings, warning_parts


def format_service_header_comments(
    service_groups: dict[str, dict[str, Any]],
    sorted_environments: list[str]
) -> list[str]:
    """
    Generate header comment lines for db-shared services.

    Displays service-by-service breakdown with environment status.

    Args:
        service_groups: Dictionary of service -> databases
        sorted_environments: List of sorted environment names

    Returns:
        List of formatted comment lines
    """
    lines: list[str] = []

    for service in sorted(service_groups.keys()):
        # Service header
        print(f"\n  {Colors.BLUE}{service}{Colors.RESET}")
        lines.append(f" ? Service: {service}")

        # Get dev reference data
        dev_db = service_groups[service]['databases'].get('dev')
        dev_schemas: set[str] = (
            set(dev_db['schemas']) if dev_db else set()
        )
        dev_table_count = dev_db['table_count'] if dev_db else 0

        # Process each environment
        for env in sorted_environments:
            db_info = service_groups[service]['databases'].get(env)

            if not db_info:
                # Missing database
                msg = f"{env}: {Colors.RED}⚠ missing database{Colors.RESET}"
                print(f"    {msg}")
                lines.append(f" !  {env}: ⚠ missing database")

            elif db_info['table_count'] == 0:
                # Empty database
                db_name = db_info['name']
                msg = (
                    f"{env}: {Colors.RED}⚠ {db_name} "
                    f"(empty - no tables){Colors.RESET}"
                )
                print(f"    {msg}")
                lines.append(f" !  {env}: ⚠ {db_name} (empty - no tables)")

            else:
                # Database with tables - check for warnings
                db_name = db_info['name']
                table_count = db_info['table_count']

                has_warnings, warning_parts = check_environment_warnings(
                    env,
                    db_info,
                    dev_schemas,
                    dev_table_count
                )

                if has_warnings:
                    warning_msg = "; ".join(warning_parts)
                    console_msg = (
                        f"{env}: {Colors.YELLOW}⚠ {db_name}{Colors.RESET} "
                        f"({table_count} tables, "
                        f"{Colors.YELLOW}{warning_msg}{Colors.RESET})"
                    )
                    print(f"    {console_msg}")
                    lines.append(
                        f" TODO: {env}: ⚠ {db_name} " +
                        f"({table_count} tables, {warning_msg})"
                    )
                else:
                    console_msg = (
                        f"{env}: {Colors.GREEN}{db_name}{Colors.RESET} "
                        f"({table_count} tables)"
                    )
                    print(f"    {console_msg}")
                    lines.append(f" *  {env}: {db_name} ({table_count} tables)")

        # Blank line between services
        lines.append("")

    return lines
