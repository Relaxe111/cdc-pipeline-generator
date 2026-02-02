#!/usr/bin/env python3
"""
Scaffold a new CDC Pipeline project with server group configuration.

This command creates a complete project structure with server_group.yaml,
Docker Compose configuration, pipeline templates, and all necessary directories.

Usage:
    # Scaffold a db-per-tenant project (e.g., AdOpus)
    cdc scaffold adopus \\
        --pattern db-per-tenant \\
        --source-type mssql \\
        --extraction-pattern "^adopus_(?P<customer>[^_]+)$"

    # Scaffold a db-shared project (e.g., ASMA)
    cdc scaffold asma \\
        --pattern db-shared \\
        --source-type postgres \\
        --extraction-pattern "^asma_(?P<service>[^_]+)_(?P<env>(test|stage|prod))(_(?P<suffix>.+))?$" \\
        --environment-aware

    # Simple fallback matching (no regex)
    cdc scaffold myproject \\
        --pattern db-shared \\
        --source-type postgres \\
        --extraction-pattern "" \\
        --environment-aware

This command replaces 'cdc manage-server-group --create' and provides
a cleaner interface for project initialization.
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List

# When executed directly, ensure the project root is on sys.path
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
)


def _default_connection_placeholders(source_type: str) -> Dict[str, str]:
    """Return default environment variable placeholders for the source database."""
    prefix = 'POSTGRES_SOURCE' if source_type == 'postgres' else 'MSSQL_SOURCE'
    return {
        'host': f"${{{prefix}_HOST}}",
        'port': f"${{{prefix}_PORT}}",
        'user': f"${{{prefix}_USER}}",
        'password': f"${{{prefix}_PASSWORD}}",
    }


def main() -> int:
    """Main entry point for scaffold command."""
    parser = argparse.ArgumentParser(
        prog="cdc scaffold",
        description="Scaffold a new CDC Pipeline project with server group configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # AdOpus (db-per-tenant with MSSQL)
  cdc scaffold adopus \\
      --pattern db-per-tenant \\
      --source-type mssql \\
      --extraction-pattern "^adopus_(?P<customer>[^_]+)$"

  # ASMA (db-shared with PostgreSQL)
  cdc scaffold asma \\
      --pattern db-shared \\
      --source-type postgres \\
      --extraction-pattern "^asma_(?P<service>[^_]+)_(?P<env>(test|stage|prod))(_(?P<suffix>.+))?$" \\
      --environment-aware

  # Simple project (fallback matching)
  cdc scaffold myproject \\
      --pattern db-shared \\
      --source-type postgres \\
      --extraction-pattern "" \\
      --environment-aware

Pattern Groups:
  db-per-tenant:
    - customer: Customer identifier (required if using regex)
  
  db-shared:
    - service: Service name (required if using regex)
    - env: Environment (test/stage/prod) (required if using regex)
    - suffix: Optional suffix (optional)

Connection Defaults:
  Credentials default to environment variables like:
    - PostgreSQL: ${POSTGRES_SOURCE_HOST}, ${POSTGRES_SOURCE_PORT}, etc.
    - MSSQL: ${MSSQL_SOURCE_HOST}, ${MSSQL_SOURCE_PORT}, etc.
        """,
    )

    # Positional argument
    parser.add_argument(
        "name",
        help="Name of the server group (e.g., 'adopus', 'asma', 'myproject')",
    )

    # Required arguments
    parser.add_argument(
        "--pattern",
        choices=["db-per-tenant", "db-shared"],
        required=True,
        help="Server group pattern",
    )
    
    parser.add_argument(
        "--source-type",
        choices=["postgres", "mssql"],
        required=True,
        help="Source database type",
    )
    
    parser.add_argument(
        "--extraction-pattern",
        required=True,
        help="Regex pattern with named groups to extract identifiers from database names. "
             "For db-per-tenant: use 'customer' group. For db-shared: use 'service', 'env', 'suffix' groups. "
             "Use empty string '' to disable regex and use simple fallback matching.",
    )

    # Optional arguments
    parser.add_argument(
        "--environment-aware",
        action="store_true",
        help="Enable environment-aware grouping (required for db-shared pattern)",
    )
    
    parser.add_argument(
        "--host",
        help="Database host (default: ${POSTGRES_SOURCE_HOST} or ${MSSQL_SOURCE_HOST})",
    )
    
    parser.add_argument(
        "--port",
        help="Database port (default: ${POSTGRES_SOURCE_PORT} or ${MSSQL_SOURCE_PORT})",
    )
    
    parser.add_argument(
        "--user",
        help="Database user (default: ${POSTGRES_SOURCE_USER} or ${MSSQL_SOURCE_USER})",
    )
    
    parser.add_argument(
        "--password",
        help="Database password (default: ${POSTGRES_SOURCE_PASSWORD} or ${MSSQL_SOURCE_PASSWORD})",
    )

    args = parser.parse_args()

    # Validation
    missing: List[str] = []
    
    # db-shared specific validation
    if args.pattern == "db-shared":
        if not args.environment_aware:
            missing.append("--environment-aware")
    
    if missing:
        print_error("Missing required options:")
        for flag in missing:
            print_info(f"  • {flag}")
        print_info("\nNotes:")
        if args.pattern == "db-shared":
            print_info("  --environment-aware: Required for db-shared pattern")
        return 1
    
    # Set default connection placeholders
    placeholders = _default_connection_placeholders(args.source_type)
    args.host = args.host or placeholders['host']
    args.port = args.port or placeholders['port']
    args.user = args.user or placeholders['user']
    args.password = args.password or placeholders['password']
    
    # Map to handler expected names for backwards compatibility
    args.add_group = args.name
    args.mode = args.pattern
    
    # Import handler
    from cdc_generator.validators.manage_server_group import handle_add_group
    
    return handle_add_group(args)


if __name__ == "__main__":
    sys.exit(main())
