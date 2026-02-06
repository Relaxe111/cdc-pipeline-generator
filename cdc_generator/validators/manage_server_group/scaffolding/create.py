"""Create new project scaffolds."""

import json
import shutil
from pathlib import Path

from .templates import (
    get_docker_compose_template,
    get_env_example_template,
    get_gitignore_template,
    get_readme_template,
    get_sink_pipeline_template,
    get_source_pipeline_template,
)
from .vscode_settings import create_vscode_settings


def scaffold_project_structure(
    server_group_name: str,
    pattern: str,
    source_type: str,
    project_root: Path,
    kafka_topology: str = "shared",
    servers: "dict[str, dict[str, str]] | None" = None,
) -> None:
    """Create complete directory structure and template files for new implementation.
    
    Creates:
    - Directory structure (services, generated, pipeline-templates, etc.)
    - docker-compose.yml with full CDC infrastructure
    - .env.example with environment variable templates
    - README.md with quick start guide
    - .gitignore with appropriate patterns
    - .vscode/settings.json with editor configuration
    - Pipeline templates for source and sink
    
    Args:
        server_group_name: Name of the server group (e.g., 'adopus', 'asma')
        pattern: 'db-per-tenant' or 'db-shared'
        source_type: 'mssql' or 'postgres'
        project_root: Root directory of the implementation
        kafka_topology: 'shared' or 'per-server' (default: 'shared')
        servers: Dict of server configurations for multi-server support
    """
    # Create directory structure
    directories = [
        "services",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        "generated/pg-migrations",
        "_docs",
        ".vscode",
        "service-schemas",
    ]

    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")

    # Create .gitkeep files in generated directories
    for gen_dir in ["pipelines", "schemas", "pg-migrations"]:
        gitkeep = project_root / "generated" / gen_dir / ".gitkeep"
        gitkeep.touch()

    # Create template files
    files_to_create = {
        "docker-compose.yml": get_docker_compose_template(server_group_name, pattern),
        ".env.example": get_env_example_template(
            server_group_name, pattern, source_type,
            kafka_topology=kafka_topology, servers=servers
        ),
        "README.md": get_readme_template(server_group_name, pattern),
        ".gitignore": get_gitignore_template(),
    }

    # docker-compose.yml should always be created/overwritten with full CDC infrastructure
    # (init template only has basic postgres, server-group scaffold has full Redpanda setup)
    _create_docker_compose(project_root, files_to_create["docker-compose.yml"])

    # Create other files (skip if they exist)
    for filename, content in files_to_create.items():
        if filename == "docker-compose.yml":
            continue  # Already handled above

        file_path = project_root / filename
        if not file_path.exists():  # Don't overwrite existing files
            file_path.write_text(content)
            print(f"✓ Created file: {filename}")
        else:
            print(f"⊘ Skipped (exists): {filename}")

    # Create .vscode/settings.json
    _create_vscode_settings_file(project_root)

    # Create pipeline templates with real, tested templates
    _create_pipeline_templates(project_root)

    _print_next_steps(server_group_name)


def _create_docker_compose(project_root: Path, content: str) -> None:
    """Create docker-compose.yml, backing up existing if present.
    
    Args:
        project_root: Root directory of the implementation
        content: Docker compose file content
    """
    docker_compose_path = project_root / "docker-compose.yml"

    if docker_compose_path.exists():
        # Backup existing file
        backup_path = project_root / "docker-compose.yml.bak"
        shutil.copy2(docker_compose_path, backup_path)
        print("✓ Backed up existing docker-compose.yml to docker-compose.yml.bak")

    docker_compose_path.write_text(content)
    print("✓ Created file: docker-compose.yml (with Redpanda CDC infrastructure)")


def _create_vscode_settings_file(project_root: Path) -> None:
    """Create .vscode/settings.json if not exists.
    
    Args:
        project_root: Root directory of the implementation
    """
    vscode_settings = project_root / ".vscode" / "settings.json"

    if not vscode_settings.exists():
        vscode_settings.write_text(json.dumps(create_vscode_settings(), indent=2))
        print("✓ Created file: .vscode/settings.json")


def _create_pipeline_templates(project_root: Path) -> None:
    """Create pipeline template files.
    
    Args:
        project_root: Root directory of the implementation
    """
    source_template = project_root / "pipeline-templates" / "source-pipeline.yaml"
    if not source_template.exists():
        source_template.write_text(get_source_pipeline_template())
        print("✓ Created file: pipeline-templates/source-pipeline.yaml")

    sink_template = project_root / "pipeline-templates" / "sink-pipeline.yaml"
    if not sink_template.exists():
        sink_template.write_text(get_sink_pipeline_template())
        print("✓ Created file: pipeline-templates/sink-pipeline.yaml")


def _print_next_steps(server_group_name: str) -> None:
    """Print next steps after scaffolding.
    
    Args:
        server_group_name: Name of the server group
    """
    print(f"\n✅ Project scaffolding complete for '{server_group_name}'!")
    print("\n📋 Next steps:")
    print("   1. cp .env.example .env")
    print("   2. Edit .env with your database credentials")
    print("   3. docker compose up -d")
    print("   4. cdc manage-source-groups --update")
