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
            ".vscode/schemas/source-groups.schema.json": "source-groups.yaml",
            ".vscode/schemas/sink-groups.schema.json": "sink-groups.yaml",
            ".vscode/schemas/service.schema.json": "services/*.yaml",
            ".vscode/schemas/migration-manifest.schema.json": "migrations/*/manifest.yaml",
            ".vscode/schemas/autocomplete-definitions.schema.json": "services/_schemas/_definitions/*-autocompletes.yaml",
            ".vscode/schemas/map-types.schema.json": "services/_schemas/_definitions/map-*.yaml",
            ".vscode/schemas/source-type-overrides.schema.json": "services/_schemas/_definitions/source-*-type-overrides.yaml",
            ".vscode/schemas/column-templates.schema.json": "services/_schemas/column-templates.yaml",
            ".vscode/schemas/transform-rules.schema.json": "services/_schemas/transform-rules.yaml"
        },
        "files.exclude": {
            "**/__pycache__": True,
            "**/.pytest_cache": True,
            "**/*.pyc": True,
            ".lsn_cache": True
        },
        "files.readonlyInclude": {
            "source-groups.yaml": True,
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
        "services/_schemas",
        "services/_schemas/_definitions",
        "services/_schemas/adapters",
        "services/_bloblang",
        "services/_bloblang/examples",
        "pipeline-templates",
        "generated/pipelines",
        "generated/schemas",
        "generated/pg-migrations",
        "generated/table-definitions",
        "_docs",
        ".vscode",
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
