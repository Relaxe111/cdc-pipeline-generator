"""Type definitions for scaffolding module.

This module contains type hints and documentation for scaffolding data structures.
Using Dict with explicit value types since VS Code settings have dynamic keys.
"""

from typing import Dict, List

# Type aliases for clarity
FilePatterns = Dict[str, bool]
"""Pattern dict for files.exclude, files.readonlyInclude, etc.
Keys are glob patterns, values are True to include/exclude."""

FilesAssociations = Dict[str, str]
"""VS Code file associations.
Keys are glob patterns, values are language identifiers."""

VSCodeSettings = Dict[str, object]
"""VS Code settings.json structure.
Keys are setting identifiers (e.g., 'files.exclude'), values vary by setting."""

DirectoryList = List[str]
"""List of directory paths relative to project root."""

PatternList = List[str]
"""List of gitignore or file patterns."""

# Template return types are str, no special types needed
