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

from datetime import UTC, datetime
from typing import TypedDict


class ServerStats(TypedDict):
    """Statistics for a single server."""
    databases: int
    tables: int
    environments: set[str]


def get_file_header_comments() -> list[str]:
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


# Markers that identify header lines (to avoid duplicating)
_HEADER_MARKERS = [
    "============",
    "AUTO-GENERATED FILE",
    "DO NOT EDIT DIRECTLY",
    "Use 'cdc manage-server-group' commands",
    "This file contains the server group configuration",
    "Changes made directly to this file may be overwritten",
    "Common commands:",
    "cdc manage-server-group --update",
    "cdc manage-server-group --info",
    "cdc manage-server-group --add-to-ignore-list",
    "cdc manage-server-group --add-to-schema-excludes",
    "For detailed documentation, see:",
    "CDC_CLI.md in the implementation repository",
    "cdc-pipeline-generator/_docs/",
]


def is_header_line(line: str) -> bool:
    """Check if a line is part of the file header (should not be preserved as metadata).
    
    Args:
        line: A single line from the YAML file
        
    Returns:
        True if this line is part of the file header and should be skipped
        
    Example:
        >>> is_header_line("# AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY")
        True
        >>> is_header_line("# Updated at: 2026-02-05 02:19:13 UTC")
        False
    """
    return any(marker in line for marker in _HEADER_MARKERS)


def get_update_timestamp_comment() -> str:
    """Get a formatted timestamp comment for when the file was last updated.
    
    Returns:
        Comment line with current UTC timestamp.
    
    Usage:
        Call this when updating database/schema information to track when the
        configuration was last synchronized with the source database.
    """
    return f"# ? Updated at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"


def ensure_file_header_exists(preserved_comments: list[str]) -> list[str]:
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


def validate_output_has_metadata(output_lines: list[str]) -> None:
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
    db_list_lines: list[str] | None = None,
    service_names: list[str] | None = None
) -> list[str]:
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


def generate_per_server_stats(
    databases: list[dict[str, object]],
    total_dbs: int,
    total_tables: int,
    avg_tables: int,
    service_list: str,
    num_services: int,
    env_stats_line: str
) -> list[str]:
    """Generate unified statistics for all servers.
    
    Args:
        databases: List of database info dicts with 'server', 'environment', 'table_count' fields
        total_dbs: Total databases across all servers
        total_tables: Total tables across all servers
        avg_tables: Average tables per database
        service_list: Comma-separated list of service names
        num_services: Number of services
        env_stats_line: Per-environment statistics line
        
    Returns:
        List of formatted comment lines showing unified stats across all servers
        
    Example:
        >>> dbs = [
        ...     {'server': 'default', 'environment': 'auth', 'table_count': 10},
        ...     {'server': 'prod', 'environment': 'adcuris', 'table_count': 50}
        ... ]
        >>> lines = generate_per_server_stats(dbs, 2, 60, 30, 'service1', 1, 'dev: 1 dbs')
        >>> '# Server: default' in lines[1]
        True
    """
    from collections import defaultdict

    # Helper to create default stats dict
    def _create_default_stats() -> ServerStats:
        return ServerStats(databases=0, tables=0, environments=set())

    # Collect all unique environments across all servers
    all_environments: set[str] = set()
    for db in databases:
        env = db.get('environment', '')
        if env and isinstance(env, str):
            all_environments.add(env)

    # Group by server (still needed for per-server summary)
    server_stats: dict[str, ServerStats] = defaultdict(_create_default_stats)

    for db in databases:
        server = str(db.get('server', 'default'))
        server_stats[server]['databases'] += 1

        table_count = db.get('table_count', 0)
        if isinstance(table_count, int):
            server_stats[server]['tables'] += table_count

        env = db.get('environment', '')
        if env and isinstance(env, str):
            server_stats[server]['environments'].add(env)

    # Build comment lines
    comment_lines: list[str] = []

    # Single unified header with all stats
    comment_lines.append("# ============================================================================")
    comment_lines.append("# Server Group Summary")
    comment_lines.append("# ============================================================================")
    comment_lines.append(f"# Total: {total_dbs} databases | {total_tables} tables | Avg: {avg_tables} tables/db")
    comment_lines.append(f"# Services ({num_services}): {service_list}")

    if all_environments:
        env_list = ", ".join(sorted(all_environments))
        comment_lines.append(f"# Environments: {env_list}")

    if env_stats_line:
        comment_lines.append(f"# Per Environment: {env_stats_line}")

    # Add per-server breakdown (compact)
    comment_lines.append("#")
    comment_lines.append("# Per Server:")
    for server_name in sorted(server_stats.keys()):
        stats = server_stats[server_name]
        comment_lines.append(f"#   {server_name}: {stats['databases']} dbs, {stats['tables']} tables")

    comment_lines.append("#")

    return comment_lines
