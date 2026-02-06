#!/usr/bin/env python3
"""
Initialize a new CDC pipeline project with scaffolding.

Usage:
    cdc init --name my-project --type adopus
    cdc init --name my-project --type asma --git-repo https://bitbucket.org/...
"""

import argparse
from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
)


def create_project_structure(project_name: str, project_type: str, target_dir: Path) -> bool:
    """Create the basic project structure for a CDC pipeline implementation.
    
    Args:
        project_name: Name of the project (e.g., "adopus-cdc-pipeline")
        project_type: Type of implementation ("adopus" = db-per-tenant, "asma" = db-shared)
        target_dir: Target directory to create the project in
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory structure
        directories = [
            "1-cdc-management",
            "services",
            "2-customers",  # For backward compatibility
            "pipeline-templates",
            "generated/pipelines",
            "generated/schemas",
            "generated/table-definitions",
            "generated/pg-migrations",
            "service-schemas",
            "kubernetes/base",
            "kubernetes/overlays/nonprod",
            "kubernetes/overlays/prod",
            "argocd",
            "docs",
            "scripts",
        ]

        for dir_path in directories:
            (target_dir / dir_path).mkdir(parents=True, exist_ok=True)
            print_info(f"Created: {dir_path}/")

        # Create server_group.yaml template
        pattern = "db-per-tenant" if project_type == "adopus" else "db-shared"

        # For db-per-tenant, service field is implicit (group name)
        # For db-shared, service is specified per database
        server_groups_template = f"""# Server Groups Configuration
# This file defines database server groups and their associated services

server_group:
  {project_name}:
    pattern: {pattern}
    
    # Source database (MSSQL)
    server:
      type: mssql
      host: ${{SOURCE_DB_HOST}}
      port: ${{SOURCE_DB_PORT}}
      database: ${{SOURCE_DB_NAME}}
      user: ${{SOURCE_DB_USER}}
      password: ${{SOURCE_DB_PASSWORD}}
    
    # Target database (PostgreSQL)
    replica:
      type: postgresql
      host: ${{REPLICA_DB_HOST}}
      port: ${{REPLICA_DB_PORT}}
      database: ${{REPLICA_DB_NAME}}
      user: ${{REPLICA_DB_USER}}
      password: ${{REPLICA_DB_PASSWORD}}
    
    # Allowed schemas for CDC
    allowed_schemas:
      - dbo
      - public
"""

        (target_dir / "server_group.yaml").write_text(server_groups_template)
        print_success("Created: server_group.yaml")

        # Create docker-compose.yml template
        docker_compose_template = """version: '3.8'

services:
  dev:
    build:
      context: .
      dockerfile: Dockerfile.dev
    volumes:
      - .:/workspace
    working_dir: /workspace
    environment:
      - PYTHONPATH=/workspace
    env_file:
      - .env
    command: fish
    stdin_open: true
    tty: true
"""

        (target_dir / "docker-compose.yml").write_text(docker_compose_template)
        print_success("Created: docker-compose.yml")

        # Create Dockerfile.dev
        dockerfile_template = """FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    git \\
    curl \\
    fish \\
    && rm -rf /var/lib/apt/lists/*

# Install cdc-pipeline-generator
RUN pip install --no-cache-dir cdc-pipeline-generator

# Set Fish as default shell
SHELL ["/usr/bin/fish", "-c"]
CMD ["/usr/bin/fish"]
"""

        (target_dir / "Dockerfile.dev").write_text(dockerfile_template)
        print_success("Created: Dockerfile.dev")

        # Create .env.example
        env_example = """# Source Database (MSSQL)
SOURCE_DB_HOST=mssql-server
SOURCE_DB_PORT=1433
SOURCE_DB_NAME=YourDatabase
SOURCE_DB_USER=sa
SOURCE_DB_PASSWORD=YourPassword123!

# Replica Database (PostgreSQL)
REPLICA_DB_HOST=postgres-replica
REPLICA_DB_PORT=5432
REPLICA_DB_NAME=replica
REPLICA_DB_USER=postgres
REPLICA_DB_PASSWORD=postgres
"""

        (target_dir / ".env.example").write_text(env_example)
        print_success("Created: .env.example")

        # Create README.md
        readme_template = f"""# {project_name}

CDC Pipeline implementation using cdc-pipeline-generator.

## Quick Start

1. Copy environment file:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

2. Start development container:
   ```bash
   docker compose up -d
   docker compose exec dev fish
   ```

3. Create a service:
   ```bash
   cdc manage-service --service {project_name} --create-service
   ```

4. Add tables and generate pipelines:
   ```bash
   cdc manage-service --service {project_name} --add-source-table dbo.YourTable
   cdc generate
   ```

## Project Type

**Type**: {pattern}

## Documentation

See `_docs/` for detailed documentation.
"""

        (target_dir / "README.md").write_text(readme_template)
        print_success("Created: README.md")

        # Create .gitignore
        gitignore = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
ENV/
*.egg-info/

# Environment
.env
*.local

# Generated files
generated/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
"""

        (target_dir / ".gitignore").write_text(gitignore)
        print_success("Created: .gitignore")

        return True

    except Exception as e:
        print_error(f"Failed to create project structure: {e}")
        return False


def main(args: argparse.Namespace | None = None) -> int:
    """Main entry point for cdc init command."""
    parser = argparse.ArgumentParser(
        description="Initialize a new CDC pipeline project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize adopus-style project (db-per-tenant)
  cdc init --name adopus-cdc --type adopus

  # Initialize asma-style project (db-shared)
  cdc init --name asma-cdc --type asma

  # Initialize in specific directory
  cdc init --name my-project --type adopus --target-dir /path/to/project
        """
    )

    parser.add_argument(
        "--name",
        required=True,
        help="Project name (e.g., adopus-cdc-pipeline)"
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=["adopus", "asma"],
        help="Implementation type: adopus (db-per-tenant) or asma (db-shared)"
    )

    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path.cwd(),
        help="Target directory (default: current directory)"
    )

    parser.add_argument(
        "--git-init",
        action="store_true",
        help="Initialize git repository after scaffolding"
    )

    args = parser.parse_args()

    target_dir = args.target_dir.resolve()

    print_header(f"Initializing CDC Pipeline Project: {args.name}")
    print_info(f"Type: {args.type}")
    print_info(f"Location: {target_dir}")

    # Check if directory is empty
    if target_dir.exists() and any(target_dir.iterdir()):
        print_error(f"Directory {target_dir} is not empty. Please use an empty directory.")
        return 1

    # Create project structure
    if not create_project_structure(args.name, args.type, target_dir):
        return 1

    # Initialize git if requested
    if args.git_init:
        import subprocess
        try:
            subprocess.run(["git", "init"], cwd=target_dir, check=True)
            print_success("Initialized git repository")
        except Exception as e:
            print_error(f"Failed to initialize git: {e}")

    print_success(f"\n✓ Project '{args.name}' initialized successfully!")
    print_info("\nNext steps:")
    print_info(f"  1. cd {target_dir}")
    print_info("  2. cp .env.example .env && edit .env")
    print_info("  3. docker compose up -d")
    print_info("  4. docker compose exec dev fish")
    print_info(f"  5. cdc manage-service --service {args.name} --create-service")

    return 0


if __name__ == "__main__":
    exit(main())
