"""Comment processing and preservation for YAML files."""

from collections.abc import Callable

from .metadata_comments import is_header_line


def parse_existing_comments(lines: list[str], server_group_line_idx: int) -> list[str]:
    """
    Parse existing comment metadata for a server group from YAML lines.

    Args:
        lines: Full file content as list of lines
        server_group_line_idx: Index of the server group line

    Returns:
        List of metadata comment lines
    """
    comments: list[str] = []

    if server_group_line_idx == -1:
        return []

    # Look backwards from the server group line to collect metadata comments
    for i in range(server_group_line_idx - 1, -1, -1):
        line = lines[i].rstrip()

        if not line.startswith('#'):
            break

        if '============' in line:
            break

        # Collect metadata comments
        if any(keyword in line for keyword in [
            'Total Databases:',
            'Databases:',
            'Last Updated:',
            'Avg Tables:'
        ]):
            comments.insert(0, line)

    return comments


def collect_preserved_comments(lines: list[str], sg_line_idx: int) -> list[str]:
    """
    Collect comments to preserve from before server group entry.

    Skips header lines that will be regenerated.

    Args:
        lines: File content as list of lines
        sg_line_idx: Index of server group entry

    Returns:
        List of preserved comment lines
    """
    preserved: list[str] = []

    if sg_line_idx < 0:
        return preserved

    for i in range(sg_line_idx):
        line = lines[i]

        # Skip header lines and empty comment lines
        if is_header_line(line):
            continue

        if line.strip() in ('#', '# '):
            continue

        # Keep comments and blank lines
        if line.strip().startswith('#') or line.strip() == '':
            preserved.append(line)

    return preserved


def filter_metadata_comments(
    comments: list[str],
    in_metadata_callback: Callable[[bool], None] | None = None
) -> list[str]:
    """
    Filter out metadata comments that will be regenerated.

    Args:
        comments: List of comment lines
        in_metadata_callback: Optional callback for when entering metadata section

    Returns:
        Filtered list without regenerated metadata
    """
    filtered: list[str] = []
    in_metadata = False
    prev_was_blank = False

    for i, comment in enumerate(comments):
        # Update timestamp marks metadata section start
        if 'Updated at:' in comment:
            in_metadata = True
            if in_metadata_callback:
                in_metadata_callback(True)
            continue

        # Skip metadata lines
        if any(keyword in comment for keyword in [
            'Total:', 'Total Databases:',
            'Per Environment:',
            'Databases:',
            'Avg Tables',
            '? Services',
            'Environments:',
            'Server:',
            '? Service:',
            '# *  ',
            '# !  ',
            '# TODO:',
        ]) or (comment.startswith('#  ') and '=' not in comment):
            in_metadata = True
            continue

        # Skip separator lines in metadata
        if '============' in comment and in_metadata:
            continue

        # Exit metadata section on non-metadata line
        if comment.strip() and not comment.strip().startswith('#'):
            in_metadata = False

        # Skip excessive blank lines
        if comment.strip() == '':
            if prev_was_blank or i == 0:
                continue
            prev_was_blank = True
        else:
            prev_was_blank = False

        filtered.append(comment)

    return filtered
