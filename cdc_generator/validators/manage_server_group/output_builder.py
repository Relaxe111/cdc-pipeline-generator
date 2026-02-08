"""Output line building for YAML files with comments."""

from datetime import UTC, datetime

from .data_models import MetadataParams
from .metadata_comments import generate_per_server_stats

# Maximum line length for database list formatting
MAX_DB_LIST_LINE_LENGTH = 75


def build_database_list_lines(
    metadata: MetadataParams,
    pattern: str,
    header_comment_lines: list[str] | None = None
) -> list[str]:
    """
    Build database list comment lines for header.

    Args:
        metadata: Metadata parameters containing databases
        pattern: Server group pattern (db-shared or db-per-tenant)
        header_comment_lines: Optional detailed service/env breakdown

    Returns:
        List of formatted database list lines
    """
    # Use detailed breakdown for db-shared if available
    if pattern == 'db-shared' and header_comment_lines:
        return header_comment_lines

    # Fallback to simple list for db-per-tenant
    db_names = [db['name'] for db in metadata.databases]
    lines: list[str] = []
    current_line = ""

    for db_name in db_names:
        if current_line and len(current_line + ", " + db_name) > MAX_DB_LIST_LINE_LENGTH:
            lines.append(current_line)
            current_line = db_name
        elif current_line:
            current_line += ", " + db_name
        else:
            current_line = db_name

    if current_line:
        lines.append(current_line)

    return lines


def insert_metadata_section(
    output_lines: list[str],
    metadata: MetadataParams
) -> None:
    """
    Insert metadata section into output lines.

    Modifies output_lines in place.

    Args:
        output_lines: List of output lines to modify
        metadata: Metadata parameters for stats generation
    """
    # Add timestamp
    timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    output_lines.append(f"# Updated at: {timestamp}")

    # Add per-server breakdown with global stats
    per_server_lines = generate_per_server_stats(
        metadata.databases,
        metadata.total_dbs,
        metadata.total_tables,
        metadata.avg_tables,
        metadata.service_list,
        metadata.num_services,
        metadata.env_stats_line
    )
    output_lines.extend(per_server_lines)

    # Add database list
    if metadata.db_list_lines:
        output_lines.append("# Databases:")
        for line in metadata.db_list_lines:
            output_lines.append(f"#{line}")


def insert_metadata_into_header(
    output_lines: list[str],
    metadata: MetadataParams
) -> bool:
    """
    Insert metadata into existing header block.

    Finds header separator and inserts before it.

    Args:
        output_lines: List of output lines to modify
        metadata: Metadata parameters for stats generation

    Returns:
        True if inserted, False if header not found
    """
    for i, line in enumerate(output_lines):
        if '============' not in line or i == 0:
            continue

        # Found header separator - insert before it
        timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
        output_lines.insert(i, f"# Updated at: {timestamp}")
        idx = i + 1

        # Add per-server breakdown
        per_server_lines = generate_per_server_stats(
            metadata.databases,
            metadata.total_dbs,
            metadata.total_tables,
            metadata.avg_tables,
            metadata.service_list,
            metadata.num_services,
            metadata.env_stats_line
        )
        for server_line in per_server_lines:
            output_lines.insert(idx, server_line)
            idx += 1

        # Add database list
        if metadata.db_list_lines:
            output_lines.insert(idx, "# Databases:")
            for line_idx, db_line in enumerate(metadata.db_list_lines, start=1):
                output_lines.insert(idx + line_idx, f"#{db_line}")

        return True

    return False


def build_server_group_header(server_group_name: str) -> list[str]:
    """
    Build server group header comment block.

    Args:
        server_group_name: Name of the server group

    Returns:
        List of header comment lines
    """
    lines = [
        "# " + "=" * 76,
    ]

    # Add descriptive title based on known groups
    if server_group_name == 'adopus':
        lines.append("# AdOpus Server Group (db-per-tenant)")
    elif server_group_name == 'asma':
        lines.append("# ASMA Server Group (db-shared)")
    else:
        lines.append(f"# {server_group_name.title()} Server Group")

    lines.append("# " + "=" * 76)

    return lines
