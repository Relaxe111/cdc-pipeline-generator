"""Centralized metadata comment management for server_group.yaml.

This module ensures that file header comments are ALWAYS present and preserved
across all server_group.yaml write operations. This prevents the recurring issue
of metadata comments disappearing.

CRITICAL: ANY function that writes to server_group.yaml MUST use these utilities.
"""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import List, Optional
from datetime import datetime, timezone


def get_file_header_comments() -> List[str]:
    """Get the standard file header comments that MUST appear at the top of server_group.yaml.
    
    Returns:
        List of comment lines (including '#' prefix) that form the file header.
    
    Usage:
        Always call this when creating a new server_group.yaml or when no preserved
        comments exist. These comments provide context and guidance to users.
    """
    return [
        "# ============================================================================",
        "# AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY",
        "# Use 'cdc manage-server-group' commands to modify this file",
        "# ============================================================================",
        "# ",
        "# This file contains the server group configuration for CDC pipelines.",
        "# Changes made directly to this file may be overwritten by CLI commands.",
        "# ",
        "# Common commands:",
        "#   - cdc manage-server-group --update              # Refresh database/schema info",
        "#   - cdc manage-server-group --info                # Show configuration details",
        "#   - cdc manage-server-group --add-to-ignore-list  # Add database exclude patterns",
        "#   - cdc manage-server-group --add-to-schema-excludes  # Add schema exclude patterns",
        "# ",
        "# For detailed documentation, see:",
        "#   - CDC_CLI.md in the implementation repository",
        "#   - cdc-pipeline-generator/_docs/ for generator documentation",
        "# ============================================================================",
    ]


def get_update_timestamp_comment() -> str:
    """Get a formatted timestamp comment for when the file was last updated.
    
    Returns:
        Comment line with current UTC timestamp.
    
    Usage:
        Call this when updating database/schema information to track when the
        configuration was last synchronized with the source database.
    """
    return f"# ? Updated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"


def ensure_file_header_exists(preserved_comments: List[str]) -> List[str]:
    """Ensure file header comments exist in the preserved comments list.
    
    This is the CRITICAL function that prevents metadata comments from disappearing.
    
    Args:
        preserved_comments: List of comments that were preserved from the file
        
    Returns:
        List of comments with file header guaranteed to be present
    
    Logic:
        1. If preserved_comments is empty → add full file header
        2. If preserved_comments exists but missing key markers → prepend file header
        3. If preserved_comments has header → return as-is
    
    Key markers to check:
        - "AUTO-GENERATED FILE"
        - "Use 'cdc manage-server-group' commands"
        - At least one separator line ("==========")
    """
    # Check if we have essential header markers
    has_auto_generated = any("AUTO-GENERATED FILE" in c for c in preserved_comments)
    has_command_hint = any("cdc manage-server-group" in c for c in preserved_comments)
    has_separator = any("========" in c for c in preserved_comments)
    
    # If we have all markers, the header exists
    if has_auto_generated and has_command_hint and has_separator:
        return preserved_comments
    
    # Header is missing or incomplete - add/prepend it
    header = get_file_header_comments()
    
    if not preserved_comments:
        # No preserved comments at all
        return header
    
    # Some comments exist but header is missing - prepend header
    # Add a blank line between header and existing comments
    return header + [""] + preserved_comments


def validate_output_has_metadata(output_lines: List[str]) -> None:
    """Validate that output has required metadata comments before writing to file.
    
    This is a SAFETY CHECK to prevent accidentally writing files without metadata.
    
    Args:
        output_lines: Lines that will be written to server_group.yaml
        
    Raises:
        ValueError: If required metadata comments are missing
    
    Required elements:
        - File header with "AUTO-GENERATED FILE"
        - At least one separator line
        - A top-level server group key (e.g., "adopus:", "asma:")
    
    Usage:
        Call this immediately before writing output_lines to server_group.yaml
        in ANY function that modifies the file.
    """
    if not output_lines:
        raise ValueError(
            "Cannot write empty server_group.yaml file.\n"
            "  💡 Ensure you have at least one server group configured."
        )
    
    # Check for file header
    has_header = any("AUTO-GENERATED FILE" in line for line in output_lines[:20])
    if not has_header:
        raise ValueError(
            "Missing file header in server_group.yaml output.\n"
            "  💡 Call ensure_file_header_exists() before building output."
        )
    
    # Check for a top-level server group key (line ending with ":" at column 0, not a comment)
    has_server_group_key = any(
        line.endswith(':') and not line.startswith('#') and not line.startswith(' ')
        for line in output_lines
    )
    if not has_server_group_key:
        raise ValueError(
            "Missing server group key in server_group.yaml output.\n"
            "  💡 Expected a top-level key like 'adopus:' or 'asma:' at the start of a line.\n"
            "  💡 Check that the server group name is valid and the YAML structure is correct."
        )
    
    # Check for at least one separator
    has_separator = any("========" in line for line in output_lines)
    if not has_separator:
        raise ValueError(
            "Missing separator lines in server_group.yaml output.\n"
            "  💡 Each server group section should have a separator comment above it."
        )


def add_metadata_stats_comments(
    total_dbs: int,
    total_tables: int,
    avg_tables: int,
    env_stats_line: str = "",
    db_list_lines: Optional[List[str]] = None,
    service_names: Optional[List[str]] = None
) -> List[str]:
    """Generate metadata statistics comments for server group.
    
    Args:
        total_dbs: Total number of databases
        total_tables: Total number of tables across all databases
        avg_tables: Average tables per database
        env_stats_line: Optional per-environment statistics
        db_list_lines: Optional database list for display
        service_names: Optional list of service names
        
    Returns:
        List of formatted comment lines with statistics
    
    Usage:
        Call this when running --update to add current statistics to the file.
    """
    stats_comments = [
        get_update_timestamp_comment(),
        f"# Total: {total_dbs} databases | {total_tables} tables | Avg: {avg_tables} tables/db"
    ]
    
    # Add services line if provided
    if service_names:
        service_list = ", ".join(sorted(service_names))
        stats_comments.append(f"# ? Services ({len(service_names)}): {service_list}")
    
    if env_stats_line:
        stats_comments.append(f"# Per Environment: {env_stats_line}")
    
    if db_list_lines:
        stats_comments.append("# Databases:")
        for line in db_list_lines:
            stats_comments.append(f"#{line}")
    
    return stats_comments
