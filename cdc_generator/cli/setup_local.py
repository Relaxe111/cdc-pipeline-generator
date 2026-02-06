#!/usr/bin/env python3
"""
Setup local development environment with on-demand database services.

This command manages Docker Compose services for local development,
allowing you to start PostgreSQL (sink) and/or MSSQL (source) databases
only when needed.

Usage:
    # Start PostgreSQL (target/sink database)
    cdc setup-local --enable-local-sink

    # Start MSSQL (source database)
    cdc setup-local --enable-local-source

    # Start both databases
    cdc setup-local --enable-local-sink --enable-local-source

    # Start all infrastructure (postgres, mssql, redpanda, console, adminer)
    cdc setup-local --full

The dev container always starts by default with `docker compose up -d`.
This command is for starting additional infrastructure services.
"""

import argparse
import subprocess
import sys
from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)


def get_project_root() -> Path:
    """Find project root by looking for docker-compose.yml."""
    current = Path.cwd()
    while current != current.parent:
        if (current / "docker-compose.yml").exists():
            return current
        current = current.parent
    raise FileNotFoundError(
        "Could not find docker-compose.yml. "
        "Run this command from within a CDC pipeline project directory."
    )


def run_docker_compose(args: list[str]) -> int:
    """Run docker compose command with error handling."""
    try:
        cmd = ["docker", "compose"] + args
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except FileNotFoundError:
        print_error("docker command not found. Please install Docker.")
        return 1
    except Exception as e:
        print_error(f"Failed to run docker compose: {e}")
        return 1


def main() -> int:
    """Main entry point for setup-local command."""
    parser = argparse.ArgumentParser(
        prog="cdc setup-local",
        description="Set up local development environment with on-demand services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  cdc setup-local --enable-local-sink
      Start PostgreSQL (target database) for sink pipelines

  cdc setup-local --enable-local-source
      Start MSSQL (source database) for testing source pipelines

  cdc setup-local --enable-local-sink --enable-local-source
      Start both PostgreSQL and MSSQL

  cdc setup-local --full
      Start all infrastructure (postgres, mssql, redpanda, console, adminer)

Services:
  - Dev container: Always started with 'docker compose up -d'
  - PostgreSQL (sink): Started with --enable-local-sink
  - MSSQL (source): Started with --enable-local-source
  - Redpanda + Console: Started with --enable-streaming
  - Everything: Started with --full
        """,
    )

    parser.add_argument(
        "--enable-local-sink",
        action="store_true",
        help="Start PostgreSQL (target/sink database) and Adminer",
    )

    parser.add_argument(
        "--enable-local-source",
        action="store_true",
        help="Start MSSQL (source database) for local testing",
    )

    parser.add_argument(
        "--enable-streaming",
        action="store_true",
        help="Start Redpanda and Console (streaming infrastructure)",
    )

    parser.add_argument(
        "--full",
        action="store_true",
        help="Start all infrastructure services (postgres, mssql, redpanda, console, adminer)",
    )

    parser.add_argument(
        "--down",
        action="store_true",
        help="Stop all services (keeps dev container running)",
    )

    args = parser.parse_args()

    # Check if we're in a project directory
    try:
        project_root = get_project_root()
    except FileNotFoundError as e:
        print_error(str(e))
        return 1

    print_header("CDC Local Environment Setup")
    print_info(f"Project: {project_root.name}")
    print_info(f"Location: {project_root}\n")

    # Handle --down flag
    if args.down:
        print_info("Stopping all infrastructure services (dev container keeps running)...")
        profiles_to_stop = ["local-sink", "local-source", "streaming", "full"]
        for profile in profiles_to_stop:
            run_docker_compose(["--profile", profile, "down"])
        print_success("✓ Infrastructure services stopped")
        print_info("Dev container is still running. Enter with: docker compose exec dev fish")
        return 0

    # Determine which profiles to start
    profiles: list[str] = []

    if args.full:
        profiles.append("full")
        print_info("Starting ALL infrastructure services...")
    else:
        if args.enable_local_sink:
            profiles.append("local-sink")
            print_info("Starting PostgreSQL (sink) + Adminer...")

        if args.enable_local_source:
            profiles.append("local-source")
            print_info("Starting MSSQL (source)...")

        if args.enable_streaming:
            profiles.append("streaming")
            print_info("Starting Redpanda + Console...")

    if not profiles:
        print_warning("No services specified. Use --help to see available options.")
        print_info("\nQuick examples:")
        print_info("  cdc setup-local --enable-local-sink     # Start PostgreSQL")
        print_info("  cdc setup-local --enable-local-source   # Start MSSQL")
        print_info("  cdc setup-local --full                  # Start everything")
        return 0

    # Start services with the appropriate profiles
    compose_args: list[str] = []
    for profile in profiles:
        compose_args.extend(["--profile", profile])
    compose_args.extend(["up", "-d"])

    print_info("\nStarting services...")
    result = run_docker_compose(compose_args)

    if result == 0:
        print_success("\n✓ Services started successfully!\n")

        # Show what was started
        if args.full:
            print_info("Started services:")
            print_info("  • PostgreSQL (sink) - localhost:5432")
            print_info("  • MSSQL (source) - localhost:1433")
            print_info("  • Redpanda - localhost:19092")
            print_info("  • Redpanda Console - http://localhost:8080")
            print_info("  • Adminer - http://localhost:8090")
        else:
            if args.enable_local_sink:
                print_info("  • PostgreSQL (sink) - localhost:5432")
                print_info("  • Adminer - http://localhost:8090")
            if args.enable_local_source:
                print_info("  • MSSQL (source) - localhost:1433")
            if args.enable_streaming:
                print_info("  • Redpanda - localhost:19092")
                print_info("  • Redpanda Console - http://localhost:8080")

        print_info("\nNext steps:")
        print_info("  • Enter dev container: docker compose exec dev fish")
        print_info("  • Update server group: cdc manage-source-groups --update")
        print_info("  • Check status: docker compose ps")
    else:
        print_error("Failed to start services. Check docker compose logs.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
