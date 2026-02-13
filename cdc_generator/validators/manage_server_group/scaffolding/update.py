"""Update existing project scaffolds."""

import json
import shutil
from pathlib import Path
from typing import cast

from cdc_generator.helpers.helpers_logging import print_info, print_success, print_warning

from .vscode_settings import (
    create_vscode_settings,
    get_generated_subdirs,
    get_gitignore_patterns,
    get_scaffold_directories,
)


def _copy_template_library_files(project_root: Path) -> None:
    """Copy template library files from generator to implementation.

    Copies column-templates.yaml, transform-rules.yaml, and bloblang examples
    from the generator's templates/init/service-schemas/ directory to the
    implementation's services/_schemas/.

    Args:
        project_root: Root directory of the implementation
    """
    # Find generator root (cdc-pipeline-generator package location)
    import cdc_generator

    generator_root = Path(cdc_generator.__file__).parent
    template_source_dir = generator_root / "templates" / "init" / "service-schemas"

    # Files to copy with examples and inline comments
    template_files = [
        "column-templates.yaml",
        "transform-rules.yaml",
    ]

    for filename in template_files:
        source_file = template_source_dir / filename
        target_file = project_root / "services" / "_schemas" / filename

        if source_file.exists():
            if target_file.exists():
                print_info(f"⊘ Skipped (exists): service-schemas/{filename}")
            else:
                shutil.copy2(source_file, target_file)
                print_success(f"✓ Copied template library: services/_schemas/{filename}")
        else:
            print_warning(f"⚠️  Template not found in generator: {filename}")

    # Copy bloblang directory (examples and README)
    bloblang_source = template_source_dir / "_bloblang"
    bloblang_target = project_root / "services" / "_schemas" / "_bloblang"

    if bloblang_source.exists():
        # Copy entire bloblang directory recursively if it doesn't exist
        if bloblang_target.exists():
            print_info("⊘ Skipped (exists): services/_schemas/_bloblang/")
        else:
            shutil.copytree(bloblang_source, bloblang_target)
            print_success("✓ Copied Bloblang examples: services/_schemas/_bloblang/")
    else:
        print_warning("⚠️  Bloblang templates not found in generator")


def update_scaffold(project_root: Path) -> bool:
    """Update existing project scaffold with latest structure and configurations.

    This function updates:
    - Directory structure (adds new directories, never removes)
    - .vscode/settings.json (merges new settings, preserves existing)
    - .gitignore (appends new patterns, preserves existing)
    - docker-compose.yml (only if user confirms, creates backup)

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

    # 2. Update .gitkeep files in generated directories
    for gen_dir in get_generated_subdirs():
        gitkeep = project_root / "generated" / gen_dir / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
            print_success(f"✓ Created: generated/{gen_dir}/.gitkeep")

    # 3. Merge .vscode/settings.json (add new keys, preserve existing)
    _update_vscode_settings(project_root)

    # 4. Update .gitignore (append new patterns if missing)
    _update_gitignore(project_root)

    # 5. Copy template library files if missing
    _copy_template_library_files(project_root)

    # 6. Check if source-groups.yaml exists
    server_group_path = project_root / "source-groups.yaml"

    if server_group_path.exists():
        print_success("✓ Verified source-groups.yaml exists")
    else:
        print_warning("⚠️  source-groups.yaml not found - run 'cdc scaffold <name>' first")

    print_success("\n✅ Scaffold update complete!")
    return True


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
        existing_patterns = set(
            line.strip()
            for line in existing_content.splitlines()
            if line.strip() and not line.startswith('#')
        )

        new_to_add = [p for p in new_patterns if p not in existing_patterns]
        if new_to_add:
            with open(gitignore_path, 'a') as f:
                f.write("\n# Added by cdc scaffold --update\n")
                for pattern in new_to_add:
                    f.write(f"{pattern}\n")
            print_success(f"✓ Added {len(new_to_add)} patterns to .gitignore")
        else:
            print_info("⊘ .gitignore already up to date")
    else:
        gitignore_path.write_text("\n".join(new_patterns) + "\n")
        print_success("✓ Created .gitignore")
