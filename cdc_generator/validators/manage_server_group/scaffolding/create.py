"""Create new project scaffolds."""

import json
import shutil
from pathlib import Path
from typing import cast

import yaml

from .templates import (
    get_cdc_cli_doc_template,
    get_cdc_cli_flow_doc_template,
    get_destructive_changes_doc_template,
    get_docker_compose_template,
    get_env_example_template,
    get_env_variables_doc_template,
    get_gitignore_template,
    get_migrations_architecture_doc_template,
    get_project_structure_doc_template,
    get_readme_template,
    get_sink_pipeline_template,
    get_source_pipeline_template,
)
from .vscode_settings import create_vscode_settings


def _get_template_source_dirs(generator_root: Path) -> list[Path]:
    """Return candidate template roots (new first, then legacy)."""
    candidates = [
        generator_root / "templates" / "init" / "services" / "_schemas",
        generator_root / "templates" / "init" / "service-schemas",
    ]
    return [candidate for candidate in candidates if candidate.exists()]


def _resolve_template_file(generator_root: Path, relative_path: Path) -> Path | None:
    """Resolve a template file across supported template root locations."""
    for source_dir in _get_template_source_dirs(generator_root):
        candidate = source_dir / relative_path
        if candidate.exists():
            return candidate
    return None


def _copy_vscode_schema_files(project_root: Path) -> None:
    """Copy static YAML validation schemas into implementation .vscode/schemas/."""
    import cdc_generator

    package_root = Path(cdc_generator.__file__).resolve().parent
    repo_root = package_root.parent
    source_dir = repo_root / ".vscode" / "schemas"
    target_dir = project_root / ".vscode" / "schemas"

    if not source_dir.exists() or not source_dir.is_dir():
        print("⚠ Warning: .vscode/schemas not found in generator")
        return

    target_dir.mkdir(parents=True, exist_ok=True)

    for schema_file in sorted(source_dir.glob("*.json")):
        target_file = target_dir / schema_file.name
        if target_file.exists():
            print(f"⊘ Skipped (exists): .vscode/schemas/{schema_file.name}")
            continue
        shutil.copy2(schema_file, target_file)
        print(f"✓ Copied VS Code schema: .vscode/schemas/{schema_file.name}")


def _resolve_source_group_name(project_root: Path) -> str:
    """Resolve source group name from source-groups.yaml or fallback."""
    source_groups_path = project_root / "source-groups.yaml"
    if not source_groups_path.exists():
        return "source-group"

    try:
        raw_data: object = yaml.safe_load(source_groups_path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return "source-group"

    if not isinstance(raw_data, dict):
        return "source-group"

    for key in cast(dict[str, object], raw_data):
        if key.startswith("_"):
            continue
        return key

    return "source-group"


def _copy_template_library_files(
    project_root: Path,
    source_group_name: str | None = None,
) -> None:
    """Copy template library files from generator to implementation.

    Copies column-templates.yaml, transform-rules.yaml, and bloblang examples
    from the generator's templates/init/service-schemas/ directory to the
    implementation's services/_schemas/ and services/_bloblang/.

    Args:
        project_root: Root directory of the implementation
    """
    # Find generator root (cdc-pipeline-generator package location)
    import cdc_generator

    generator_root = Path(cdc_generator.__file__).parent
    # Files to copy with examples and inline comments
    template_files = [
        "column-templates.yaml",
        "transform-rules.yaml",
    ]

    for filename in template_files:
        target_file = project_root / "services" / "_schemas" / filename

        if target_file.exists():
            print(f"⊘ Skipped (exists): services/_schemas/{filename}")
            continue

        source_file = _resolve_template_file(generator_root, Path(filename))

        if source_file is not None:
            shutil.copy2(source_file, target_file)
            print(f"✓ Copied template library: services/_schemas/{filename}")
        else:
            print(f"⚠ Warning: Template not found in generator: {filename}")

    # Copy bloblang directory (examples and README)
    bloblang_target = project_root / "services" / "_bloblang"
    bloblang_source = _resolve_template_file(generator_root, Path("_bloblang"))

    if bloblang_source is None:
        print("⚠ Warning: Bloblang templates not found in generator")
    else:
        # Merge entire bloblang directory recursively.
        # This ensures examples are copied even when scaffold pre-created dirs.
        shutil.copytree(bloblang_source, bloblang_target, dirs_exist_ok=True)
        print("✓ Copied Bloblang examples: services/_bloblang/")

    # Copy DB type mapping definitions
    map_files = [
        "map-mssql-pgsql.yaml",
    ]

    for filename in map_files:
        target_file = (
            project_root
            / "services"
            / "_schemas"
            / "_definitions"
            / filename
        )

        if target_file.exists():
            print(
                "⊘ Skipped (exists): "
                + f"services/_schemas/_definitions/{filename}"
            )
            continue

        source_file = _resolve_template_file(
            generator_root,
            Path("_definitions") / filename,
        )

        if source_file is not None:
            shutil.copy2(source_file, target_file)
            print(
                "✓ Copied type mapping definition: "
                + f"services/_schemas/_definitions/{filename}"
            )
        else:
            print(
                "⚠ Warning: Type mapping template not found in generator: "
                + filename
            )

    resolved_source_group_name = source_group_name or _resolve_source_group_name(
        project_root,
    )
    override_template_name = "source-type-overrides.yaml"
    override_target_name = (
        f"source-{resolved_source_group_name}-type-overrides.yaml"
    )
    override_target_file = (
        project_root
        / "services"
        / "_schemas"
        / "_definitions"
        / override_target_name
    )

    if override_target_file.exists():
        print(
            "⊘ Skipped (exists): "
            + f"services/_schemas/_definitions/{override_target_name}"
        )
    else:
        source_file = _resolve_template_file(
            generator_root,
            Path("_definitions") / override_template_name,
        )
        if source_file is not None:
            shutil.copy2(source_file, override_target_file)
            print(
                "✓ Copied source type overrides definition: "
                + f"services/_schemas/_definitions/{override_target_name}"
            )
        else:
            print(
                "⚠ Warning: Source type overrides template not found in generator: "
                + override_template_name
            )


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
        "services/_schemas",
        "services/_schemas/_definitions",
        "services/_schemas/adapters",
        "services/_bloblang",
        "services/_bloblang/examples",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        "generated/pg-migrations",
        "_docs",
        ".vscode",
    ]

    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")

    # Create .gitkeep files in generated directories
    for gen_dir in ["pipelines", "schemas", "pg-migrations"]:
        gitkeep = project_root / "generated" / gen_dir / ".gitkeep"
        gitkeep.touch()

    # Copy template library files from generator to implementation
    _copy_template_library_files(project_root, server_group_name)

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

    docs_to_create = {
        "_docs/PROJECT_STRUCTURE.md": get_project_structure_doc_template(
            server_group_name, pattern,
        ),
        "_docs/ENV_VARIABLES.md": get_env_variables_doc_template(
            server_group_name, source_type,
        ),
        "_docs/CDC_CLI.md": get_cdc_cli_doc_template(server_group_name),
        "_docs/CDC_CLI_FLOW.md": get_cdc_cli_flow_doc_template(server_group_name),
        "_docs/architecture/MIGRATIONS.md": get_migrations_architecture_doc_template(),
        "_docs/architecture/DESTRUCTIVE_CHANGES.md": get_destructive_changes_doc_template(),
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

    # Create documentation files under _docs (skip if they exist)
    for filename, content in docs_to_create.items():
        file_path = project_root / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if not file_path.exists():
            file_path.write_text(content)
            print(f"✓ Created file: {filename}")
        else:
            print(f"⊘ Skipped (exists): {filename}")

    # Create .vscode/settings.json
    _create_vscode_settings_file(project_root)

    # Copy static .vscode/schemas/*.json validation files
    _copy_vscode_schema_files(project_root)

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
