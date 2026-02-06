"""VS Code settings creation for project scaffolding."""


# Type definitions for VS Code settings structure
FilePatterns = dict[str, bool]
FilesAssociations = dict[str, str]


class VSCodeSettingsDict:
    """Type structure for VS Code settings.json.
    
    Note: Using class-level annotations for documentation.
    Actual return is Dict since VS Code settings have dynamic keys.
    """
    files_associations: FilesAssociations
    yaml_schemas: dict[str, str]
    files_exclude: FilePatterns
    files_readonly_include: FilePatterns
    search_exclude: FilePatterns


def create_vscode_settings() -> dict[str, object]:
    """Create .vscode/settings.json with useful defaults.
    
    Returns:
        Dictionary with VS Code settings ready for JSON serialization.
        Keys are VS Code setting identifiers, values are setting values.
        
    Settings included:
        - files.associations: Map file patterns to language modes
        - yaml.schemas: JSON schema validation for YAML files
        - files.exclude: Hide generated/cache files from explorer
        - files.readonlyInclude: Mark config files as read-only
        - search.exclude: Exclude generated content from search
    """
    return {
        "files.associations": {
            "*.yaml": "yaml",
            "docker-compose*.yml": "dockercompose"
        },
        "yaml.schemas": {
            ".vscode/service-schema.json": "services/*.yaml"
        },
        "files.exclude": {
            "**/__pycache__": True,
            "**/.pytest_cache": True,
            "**/*.pyc": True,
            ".lsn_cache": True
        },
        "files.readonlyInclude": {
            "server_group.yaml": True,
            "services/**/*.yaml": True,
            "generated/**/*.yaml": True
        },
        "search.exclude": {
            "**/generated": True,
            "**/.venv": True
        }
    }


def get_gitignore_patterns() -> list[str]:
    """Get list of patterns for .gitignore.
    
    Returns:
        List of gitignore patterns to include
    """
    return [
        ".env",
        ".venv",
        "__pycache__/",
        "*.pyc",
        ".pytest_cache/",
        ".lsn_cache/",
        "generated/pipelines/*",
        "generated/schemas/*",
        "generated/table-definitions/*",
        "!generated/**/.gitkeep",
        ".DS_Store",
        "*.swp",
        "*.swo",
        "*~",
    ]


def get_scaffold_directories() -> list[str]:
    """Get list of directories to create in scaffold.
    
    Returns:
        List of directory paths relative to project root
    """
    return [
        "services",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        "generated/pg-migrations",
        "generated/table-definitions",
        "_docs",
        ".vscode",
        "service-schemas",
    ]


def get_generated_subdirs() -> list[str]:
    """Get list of generated subdirectories that need .gitkeep.
    
    Returns:
        List of subdirectory names under generated/
    """
    return [
        "pipelines",
        "schemas",
        "pg-migrations",
        "table-definitions",
    ]
