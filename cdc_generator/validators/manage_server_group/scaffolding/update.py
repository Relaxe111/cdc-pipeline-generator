"""Update existing project scaffolds."""

import json
import shutil
from pathlib import Path
from typing import cast

import yaml

from cdc_generator.helpers.helpers_logging import print_info, print_success, print_warning

from .templates import (
    get_cdc_cli_doc_template,
    get_cdc_cli_flow_doc_template,
    get_destructive_changes_doc_template,
    get_docker_compose_template,
    get_env_variables_doc_template,
    get_migrations_architecture_doc_template,
    get_project_structure_doc_template,
    get_readme_template,
)
from .vscode_settings import (
    create_vscode_settings,
    get_generated_subdirs,
    get_gitignore_patterns,
    get_scaffold_directories,
)

_CONFLICT_PREVIEW_LIMIT = 5


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
        print_warning("⚠️  .vscode/schemas not found in generator")
        return

    target_dir.mkdir(parents=True, exist_ok=True)

    for schema_file in sorted(source_dir.glob("*.json")):
        target_file = target_dir / schema_file.name
        if target_file.exists():
            print_info(f"⊘ Skipped (exists): .vscode/schemas/{schema_file.name}")
            continue
        shutil.copy2(schema_file, target_file)
        print_success(f"✓ Copied VS Code schema: .vscode/schemas/{schema_file.name}")


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
            print_info(f"⊘ Skipped (exists): services/_schemas/{filename}")
            continue

        source_file = _resolve_template_file(generator_root, Path(filename))

        if source_file is not None:
            shutil.copy2(source_file, target_file)
            print_success(f"✓ Copied template library: services/_schemas/{filename}")
        else:
            print_warning(f"⚠️  Template not found in generator: {filename}")

    # Copy bloblang directory (examples and README)
    bloblang_target = project_root / "services" / "_bloblang"
    bloblang_source = _resolve_template_file(generator_root, Path("_bloblang"))

    if bloblang_source is None:
        print_warning("⚠️  Bloblang templates not found in generator")
    else:
        # Merge entire bloblang directory recursively.
        # This ensures examples are copied even when scaffold pre-created dirs.
        shutil.copytree(bloblang_source, bloblang_target, dirs_exist_ok=True)
        print_success("✓ Copied Bloblang examples: services/_bloblang/")

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
            print_info(
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
            print_success(
                "✓ Copied type mapping definition: "
                + f"services/_schemas/_definitions/{filename}"
            )
        else:
            print_warning(
                "⚠️  Type mapping template not found in generator: "
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
        print_info(
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
            print_success(
                "✓ Copied source type overrides definition: "
                + f"services/_schemas/_definitions/{override_target_name}"
            )
        else:
            print_warning(
                "⚠️  Source type overrides template not found in generator: "
                + override_template_name
            )


def _merge_legacy_schema_tree(project_root: Path) -> None:
    """Merge legacy ``service-schemas/`` into ``services/_schemas/``.

    Keeps existing destination files as source of truth and only moves missing
    files/directories from legacy layout. Leaves legacy directory in place if
    unresolved conflicts remain.
    """
    legacy_root = project_root / "service-schemas"
    target_root = project_root / "services" / "_schemas"

    if not legacy_root.exists() or not legacy_root.is_dir():
        return

    target_root.mkdir(parents=True, exist_ok=True)

    conflicts: list[Path] = []

    def _merge_dir(src: Path, dst: Path) -> None:
        dst.mkdir(parents=True, exist_ok=True)
        for child in src.iterdir():
            dst_child = dst / child.name
            if child.is_dir():
                if dst_child.exists() and not dst_child.is_dir():
                    conflicts.append(child)
                    continue
                _merge_dir(child, dst_child)
                if not any(child.iterdir()):
                    child.rmdir()
                continue

            if dst_child.exists():
                conflicts.append(child)
                continue
            shutil.move(str(child), str(dst_child))

    _merge_dir(legacy_root, target_root)

    # Remove legacy root only when fully migrated.
    if not any(legacy_root.iterdir()):
        legacy_root.rmdir()
        print_success("✓ Migrated legacy service-schemas/ to services/_schemas/")
        return

    print_warning(
        "⚠️  Legacy service-schemas/ still contains unresolved entries; "
        + "manual review recommended"
    )
    if conflicts:
        print_info(
            "Conflicts kept in legacy path: "
            + ", ".join(
                str(path.relative_to(project_root))
                for path in conflicts[:_CONFLICT_PREVIEW_LIMIT]
            )
            + (" ..." if len(conflicts) > _CONFLICT_PREVIEW_LIMIT else "")
        )


def update_scaffold(project_root: Path) -> bool:
    """Update existing project scaffold with latest structure and configurations.

    This function updates:
    - Directory structure (adds new directories, never removes)
    - .vscode/settings.json (merges new settings, preserves existing)
    - .gitignore (appends new patterns, preserves existing)
    - docker-compose.yml (creates if missing)

    It does NOT modify:
    - source-groups.yaml content (only ensures header exists)
    - services/*.yaml files
    - Any user data

    Args:
        project_root: Root directory of the implementation

    Returns:
        True if successful, False otherwise
    """
    print_info("🔄 Updating project scaffold...")

    # 1. Ensure all directories exist
    directories = get_scaffold_directories()

    for directory in directories:
        dir_path = project_root / directory
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print_success(f"✓ Created directory: {directory}")

    # 1b. Migrate legacy schema tree into canonical location
    _merge_legacy_schema_tree(project_root)

    # 2. Update .gitkeep files in generated directories
    for gen_dir in get_generated_subdirs():
        gitkeep = project_root / "generated" / gen_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
            print_success(f"✓ Created: generated/{gen_dir}/.gitkeep")

    # 3. Merge .vscode/settings.json (add new keys, preserve existing)
    _update_vscode_settings(project_root)

    # 3b. Ensure static VS Code schema files exist
    _copy_vscode_schema_files(project_root)

    # 4. Update .gitignore (append new patterns if missing)
    _update_gitignore(project_root)

    # 5. Copy template library files if missing
    server_group_name, _pattern, _source_type = _infer_scaffold_metadata(project_root)
    _copy_template_library_files(project_root, server_group_name)

    # 6. Check if source-groups.yaml exists
    server_group_path = project_root / "source-groups.yaml"

    if server_group_path.exists():
        print_success("✓ Verified source-groups.yaml exists")
    else:
        print_warning("⚠️  source-groups.yaml not found - run 'cdc scaffold <name>' first")

    # 7. Ensure scaffold markdown docs exist (README at root, docs under _docs)
    _ensure_docker_compose_file(project_root)

    # 8. Ensure scaffold markdown docs exist (README at root, docs under _docs)
    _ensure_scaffold_markdown_files(project_root)

    print_success("\n✅ Scaffold update complete!")
    return True


def _infer_scaffold_metadata(project_root: Path) -> tuple[str, str, str]:
    """Infer server group metadata from source-groups.yaml.

    Returns:
        Tuple of (server_group_name, pattern, source_type)
    """
    source_groups_path = project_root / "source-groups.yaml"
    default_metadata = (project_root.name, "db-per-tenant", "mssql")

    if not source_groups_path.exists():
        return default_metadata

    try:
        raw_data: object = yaml.safe_load(source_groups_path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return default_metadata

    if not isinstance(raw_data, dict):
        return default_metadata

    data = cast(dict[str, object], raw_data)

    server_group_name = ""
    group_data: dict[str, object] | None = None
    for key, value in data.items():
        if key == "_metadata":
            continue
        if isinstance(value, dict):
            server_group_name = key
            group_data = cast(dict[str, object], value)
            break

    if not server_group_name or group_data is None:
        return default_metadata

    pattern_value = group_data.get("pattern")
    source_type_value = group_data.get("type")
    pattern = str(pattern_value) if pattern_value is not None else "db-per-tenant"
    source_type = str(source_type_value) if source_type_value is not None else "mssql"
    return server_group_name, pattern, source_type


def _ensure_scaffold_markdown_files(project_root: Path) -> None:
    """Create missing scaffold markdown files without overwriting existing."""
    server_group_name, pattern, source_type = _infer_scaffold_metadata(project_root)

    markdown_files = {
        "README.md": get_readme_template(server_group_name, pattern),
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

    for relative_path, content in markdown_files.items():
        target_path = project_root / relative_path
        if target_path.exists():
            print_info(f"⊘ Skipped (exists): {relative_path}")
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(content, encoding="utf-8")
        print_success(f"✓ Created file: {relative_path}")


def _ensure_docker_compose_file(project_root: Path) -> None:
    """Create docker-compose.yml if missing during scaffold update."""
    docker_compose_path = project_root / "docker-compose.yml"
    if docker_compose_path.exists():
        print_info("⊘ Skipped (exists): docker-compose.yml")
        return

    server_group_name, pattern, _source_type = _infer_scaffold_metadata(project_root)
    content = get_docker_compose_template(server_group_name, pattern)
    docker_compose_path.write_text(content, encoding="utf-8")
    print_success("✓ Created file: docker-compose.yml")


def _update_vscode_settings(project_root: Path) -> None:
    """Update .vscode/settings.json by merging new settings with existing.

    Args:
        project_root: Root directory of the implementation
    """
    vscode_settings_path = project_root / ".vscode" / "settings.json"
    new_settings = create_vscode_settings()

    if vscode_settings_path.exists():
        try:
            existing_settings = json.loads(vscode_settings_path.read_text())
            updated = False

            # Merge each top-level key
            for key, value in new_settings.items():
                if key not in existing_settings:
                    existing_settings[key] = value
                    updated = True
                    print_success(f"✓ Added setting: {key}")
                elif isinstance(value, dict) and isinstance(existing_settings[key], dict):
                    # Merge nested dicts (add missing keys only)
                    value_dict = cast(dict[str, object], value)
                    for subkey, subvalue in value_dict.items():
                        if subkey not in existing_settings[key]:
                            existing_settings[key][subkey] = subvalue
                            updated = True
                            print_success(f"✓ Added setting: {key}.{subkey}")

            if updated:
                vscode_settings_path.write_text(json.dumps(existing_settings, indent=2))
                print_success("✓ Updated .vscode/settings.json")
            else:
                print_info("⊘ .vscode/settings.json already up to date")

        except json.JSONDecodeError:
            print_warning("⚠️  Could not parse .vscode/settings.json, skipping")
    else:
        vscode_settings_path.write_text(json.dumps(new_settings, indent=2))
        print_success("✓ Created .vscode/settings.json")


def _update_gitignore(project_root: Path) -> None:
    """Update .gitignore by appending new patterns.

    Args:
        project_root: Root directory of the implementation
    """
    gitignore_path = project_root / ".gitignore"
    new_patterns = get_gitignore_patterns()

    if gitignore_path.exists():
        existing_content = gitignore_path.read_text()
        existing_patterns = {
            line.strip()
            for line in existing_content.splitlines()
            if line.strip() and not line.startswith('#')
        }

        new_to_add = [p for p in new_patterns if p not in existing_patterns]
        if new_to_add:
            with gitignore_path.open('a') as f:
                f.write("\n# Added by cdc scaffold --update\n")
                for pattern in new_to_add:
                    f.write(f"{pattern}\n")
            print_success(f"✓ Added {len(new_to_add)} patterns to .gitignore")
        else:
            print_info("⊘ .gitignore already up to date")
    else:
        gitignore_path.write_text("\n".join(new_patterns) + "\n")
        print_success("✓ Created .gitignore")
