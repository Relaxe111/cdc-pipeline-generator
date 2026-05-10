#!/usr/bin/env python3
"""
Scaffold a new CDC Pipeline project with server group configuration.

This command creates a complete project structure with source-groups.yaml,
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

This command replaces 'cdc manage-source-groups --create' and provides
a cleaner interface for project initialization.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# When executed directly, ensure the project root is on sys.path
if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
)
from cdc_generator.helpers.topology_runtime import (
    supported_topologies_for_source_type,
    topology_supported_for_source_type,
    topology_uses_broker,
)


def _default_connection_placeholders(source_type: str, server_name: str = "default") -> dict[str, str]:
    """Return default environment variable placeholders for the source database.

    Args:
        source_type: 'postgres' or 'mssql'
        server_name: Server name for postfix (default has no postfix)
    """
    prefix = "POSTGRES_SOURCE" if source_type == "postgres" else "MSSQL_SOURCE"
    postfix = "" if server_name == "default" else f"_{server_name.upper()}"
    return {
        "host": f"${{{prefix}_HOST{postfix}}}",
        "port": f"${{{prefix}_PORT{postfix}}}",
        "user": f"${{{prefix}_USER{postfix}}}",
        "password": f"${{{prefix}_PASSWORD{postfix}}}",
    }


def _broker_bootstrap_placeholder(
    broker_topology: str,
    server_name: str = "default",
) -> str:
    """Return broker bootstrap servers placeholder.

    Args:
        broker_topology: Broker topology value: 'shared' or 'per-server'
        server_name: Server name for postfix (only used if per-server)
    """
    if broker_topology == "shared":
        return "${KAFKA_BOOTSTRAP_SERVERS}"
    postfix = f"_{server_name.upper()}"
    return f"${{KAFKA_BOOTSTRAP_SERVERS{postfix}}}"


class ScaffoldArgumentParser(argparse.ArgumentParser):
    """Custom argument parser with better error messages."""

    # ANSI color codes
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    def error(self, message: str) -> None:
        """Override error method to provide detailed missing argument information."""
        # Parse which arguments are missing from the error message
        if "required:" in message or "arguments are required:" in message:
            print(f"\n{self.RED}❌ Missing required arguments for 'cdc scaffold':{self.RESET}\n")

            # Check what's missing by examining sys.argv
            # sys.argv has already been modified by commands.py: [script_name, --flag1, val1, --flag2, ...]
            # The positional 'name' argument would be a non-flag argument
            provided_args = sys.argv[1:]  # Everything after script name

            missing_items: list[str] = []

            # Check positional argument (name)
            has_name = False
            for arg in provided_args:
                if not arg.startswith("-") and not arg.startswith("--"):
                    # Also check it's not a flag value by looking at previous arg
                    idx = provided_args.index(arg)
                    if idx == 0 or not provided_args[idx - 1].startswith("-"):
                        has_name = True
                        break

            if not has_name:
                examples = f"{self.GREEN}'adopus'{self.RESET}, {self.GREEN}'asma'{self.RESET}, {self.GREEN}'myproject'{self.RESET}"
                missing_items.append(
                    f"  {self.CYAN}📝 name{self.RESET} (positional argument)\n"
                    f"      {self.BOLD}Project/server group name{self.RESET} (e.g., {examples})\n"
                    f"      {self.YELLOW}⚠️  This must come FIRST, before any --flags{self.RESET}"
                )

            # Check required flags
            if "--pattern" not in provided_args:
                missing_items.append(
                    f"  {self.CYAN}🎯 --pattern{self.RESET} {{db-per-tenant,db-shared}}\n"
                    + f"      {self.YELLOW}db-per-tenant:{self.RESET} One database per customer (e.g., adopus_customer1, adopus_customer2)\n"
                    + f"      {self.YELLOW}db-shared:{self.RESET} Multiple services in shared databases (e.g., asma_service_env)"
                )

            if "--source-type" not in provided_args:
                missing_items.append(
                    f"  {self.CYAN}🗄️  --source-type{self.RESET} {{postgres,mssql}}\n"
                    + f"      Type of {self.BOLD}source database{self.RESET} to extract data from"
                )

            if "--extraction-pattern" not in provided_args:
                tenant_ex = '"^adopus_(?P<customer>[^_]+)$"'
                shared_ex = '"^asma_(?P<service>[^_]+)_(?P<env>(test|stage|prod))$"'
                missing_items.append(
                    f"  {self.CYAN}🔍 --extraction-pattern{self.RESET}"
                    f' {self.MAGENTA}"REGEX_PATTERN"{self.RESET}\n'
                    f"      Regex pattern to extract identifiers from database names\n"
                    f"      {self.BLUE}db-per-tenant example:{self.RESET}"
                    f" {self.GREEN}{tenant_ex}{self.RESET}\n"
                    f"      {self.BLUE}db-shared example:{self.RESET}"
                    f" {self.GREEN}{shared_ex}{self.RESET}\n"
                    f'      Use empty string {self.GREEN}""{self.RESET}'
                    f" for simple fallback matching (no regex)"
                )

            if missing_items:
                print("\n".join(missing_items))
                print(f"\n{self.YELLOW}💡 Quick start examples:{self.RESET}")
                adopus_cmd = (
                    f"{self.GREEN}cdc scaffold adopus"
                    f" --pattern db-per-tenant"
                    f" --source-type mssql"
                    f' --extraction-pattern "^adopus_(?P<customer>[^_]+)$"'
                    f"{self.RESET}"
                )
                print(f"  {adopus_cmd}")
                asma_cmd = (
                    f"{self.GREEN}cdc scaffold asma"
                    f" --pattern db-shared"
                    f" --source-type postgres"
                    f' --extraction-pattern ""'
                    f" --environment-aware{self.RESET}"
                )
                print(f"  {asma_cmd}")

            sys.exit(2)

        # Default behavior for other errors
        super().error(message)


def main() -> int:
    """Main entry point for scaffold command."""
    parser = ScaffoldArgumentParser(
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
        nargs="?",  # Optional when using --update
        help="Name of the server group (e.g., 'adopus', 'asma', 'myproject')",
    )

    # Update mode (no other args required)
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing project scaffold with latest structure and configurations. "
        + "Adds new directories, merges .vscode/settings.json, updates .gitignore. "
        + "Does not modify source-groups.yaml content or services.",
    )

    # Required arguments (for new scaffold)
    parser.add_argument(
        "--pattern",
        choices=["db-per-tenant", "db-shared"],
        required=False,  # Not required when using --update
        help="Server group pattern",
    )

    parser.add_argument(
        "--source-type",
        choices=["postgres", "mssql"],
        required=False,  # Not required when using --update
        help="Source database type",
    )

    parser.add_argument(
        "--extraction-pattern",
        required=False,  # Not required when using --update
        help="Regex pattern with named groups to extract identifiers from database names. "
        + "For db-per-tenant: use 'customer' group. For db-shared: use 'service', 'env', 'suffix' groups. "
        + "Use empty string '' to disable regex and use simple fallback matching.",
    )

    # Optional arguments
    parser.add_argument(
        "--environment-aware",
        action="store_true",
        help="Enable environment-aware grouping (required for db-shared pattern)",
    )

    parser.add_argument(
        "--topology",
        choices=["redpanda", "fdw", "pg_native"],
        default="redpanda",
        help=("User-facing topology. MSSQL supports redpanda|fdw and PostgreSQL supports redpanda|pg_native. Default: redpanda"),
    )

    parser.add_argument(
        "--broker-topology",
        dest="broker_topology",
        choices=["shared", "per-server"],
        default=None,
        help="Broker distribution for redpanda topology only: 'shared' or 'per-server'",
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

    parser.add_argument(
        "--cdc-cli",
        action="store_true",
        dest="cdc_cli",
        help="After scaffolding, pip-install cdc-pipeline-generator[full] "
        + "so the CDC CLI has all runtime dependencies (pymssql, psycopg2, etc.).",
    )

    parser.add_argument(
        "--cli-development",
        action="store_true",
        dest="cli_development",
        help="Implies --cdc-cli plus dev extras (pytest, ruff, mypy, black).",
    )

    args = parser.parse_args()

    # Handle --update mode (no other args required)
    if args.update:
        from cdc_generator.helpers.service_config import get_project_root
        from cdc_generator.validators.manage_server_group.scaffolding import update_scaffold

        project_root = get_project_root()
        return 0 if update_scaffold(project_root) else 1

    # For new scaffold, require all arguments
    missing: list[str] = []

    if not args.name:
        missing.append("name (positional argument)")
    if not args.pattern:
        missing.append("--pattern")
    if not args.source_type:
        missing.append("--source-type")
    if args.extraction_pattern is None:
        missing.append("--extraction-pattern")

    # db-shared specific validation
    if args.pattern == "db-shared" and not args.environment_aware:
        missing.append("--environment-aware")

    if missing:
        print_error("Missing required options:")
        for flag in missing:
            print_info(f"  • {flag}")
        print_info("\nNotes:")
        print_info("  Use 'cdc scaffold --update' to update existing project structure")
        if args.pattern == "db-shared":
            print_info("  --environment-aware: Required for db-shared pattern")
        return 1

    if not topology_supported_for_source_type(args.topology, args.source_type):
        supported = ", ".join(supported_topologies_for_source_type(args.source_type))
        print_error(f"Topology '{args.topology}' is not supported for source type '{args.source_type}'.")
        print_info(f"  Supported topologies: {supported}")
        return 1

    if args.broker_topology and not topology_uses_broker(args.topology):
        print_error("--broker-topology is only valid when --topology redpanda")
        return 1

    # Set default connection placeholders for 'default' server
    placeholders = _default_connection_placeholders(args.source_type, server_name="default")
    args.host = args.host or placeholders["host"]
    args.port = args.port or placeholders["port"]
    args.user = args.user or placeholders["user"]
    args.password = args.password or placeholders["password"]

    # Set broker bootstrap servers based on topology
    if topology_uses_broker(args.topology):
        args.broker_topology = args.broker_topology or "shared"
        args.kafka_bootstrap_servers = _broker_bootstrap_placeholder(args.broker_topology, server_name="default")
    else:
        args.kafka_bootstrap_servers = None

    # Map to handler expected names for backwards compatibility
    args.add_group = args.name
    args.mode = args.pattern

    # Import handler
    from cdc_generator.validators.manage_server_group import handle_add_group

    result = handle_add_group(args)
    if result != 0:
        return result

    # --- Post-scaffold: install CDC CLI dependencies ---
    return _install_cdc_cli_deps(args)


def _install_cdc_cli_deps(args: argparse.Namespace) -> int:
    """Install cdc-pipeline-generator extras when --cdc-cli / --cli-development are set."""
    if args.cli_development:
        args.cdc_cli = True  # --cli-development implies --cdc-cli

    if not args.cdc_cli:
        return 0

    extras = "full,dev" if args.cli_development else "full"
    print_info(f"\n📦 Installing cdc-pipeline-generator[{extras}] ...")
    pip_cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        f"cdc-pipeline-generator[{extras}]",
    ]
    try:
        pip_result = subprocess.run(pip_cmd, check=False)
    except Exception as exc:
        print_error(f"Failed to run pip: {exc}")
        return 1

    if pip_result.returncode != 0:
        print_error(f"pip install cdc-pipeline-generator[{extras}] failed (exit code {pip_result.returncode}).")
        print_info(f"You can retry manually:\n  pip install cdc-pipeline-generator[{extras}]")
        return 1

    print_success(f"Installed cdc-pipeline-generator[{extras}] successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
