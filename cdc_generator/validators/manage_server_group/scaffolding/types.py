"""Type definitions for scaffolding module.

This module contains type hints and documentation for scaffolding data structures.
Using Dict with explicit value types since VS Code settings have dynamic keys.
"""


# Type aliases for clarity
FilePatterns = dict[str, bool]
"""Pattern dict for files.exclude, files.readonlyInclude, etc.
Keys are glob patterns, values are True to include/exclude."""

FilesAssociations = dict[str, str]
"""VS Code file associations.
Keys are glob patterns, values are language identifiers."""

VSCodeSettings = dict[str, object]
"""VS Code settings.json structure.
Keys are setting identifiers (e.g., 'files.exclude'), values vary by setting."""

DirectoryList = list[str]
"""List of directory paths relative to project root."""

PatternList = list[str]
"""List of gitignore or file patterns."""

# Template return types are str, no special types needed
