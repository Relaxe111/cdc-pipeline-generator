"""Project scaffolding package for CDC pipeline implementations.

This package provides functionality for creating and updating project scaffolds
for new CDC pipeline implementations.

Public API:
    scaffold_project_structure: Create new project with full directory structure
    update_scaffold: Update existing project with latest scaffold changes

Example:
    from cdc_generator.validators.manage_server_group.scaffolding import (
        scaffold_project_structure,
        update_scaffold,
    )
    
    # Create new project
    scaffold_project_structure("my_project", "db-per-tenant", "mssql", Path("."))
    
    # Update existing project
    update_scaffold(Path("."))
"""

from .create import scaffold_project_structure
from .update import update_scaffold

# Template functions (for use by create.py and external callers if needed)
from .templates import (
    get_docker_compose_template,
    get_env_example_template,
    get_readme_template,
    get_gitignore_template,
    get_source_pipeline_template,
    get_sink_pipeline_template,
)

# Settings functions
from .vscode_settings import (
    create_vscode_settings,
    get_gitignore_patterns,
    get_scaffold_directories,
    get_generated_subdirs,
)

# Type definitions
from .types import (
    FilePatterns,
    FilesAssociations,
    VSCodeSettings,
    DirectoryList,
    PatternList,
)

__all__ = [
    # Main public API
    "scaffold_project_structure",
    "update_scaffold",
    # Template functions
    "get_docker_compose_template",
    "get_env_example_template",
    "get_readme_template",
    "get_gitignore_template",
    "get_source_pipeline_template",
    "get_sink_pipeline_template",
    # Settings functions
    "create_vscode_settings",
    "get_gitignore_patterns",
    "get_scaffold_directories",
    "get_generated_subdirs",
    # Type definitions
    "FilePatterns",
    "FilesAssociations",
    "VSCodeSettings",
    "DirectoryList",
    "PatternList",
]
