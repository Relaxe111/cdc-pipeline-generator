"""YAML I/O operations for server group configuration."""

from io import StringIO
from pathlib import Path
from typing import Any

from cdc_generator.helpers.yaml_loader import yaml

from .config import SERVER_GROUPS_FILE
from .metadata_comments import get_file_header_comments
from .types import ServerGroupConfig


def write_server_group_yaml(
    server_group_name: str,
    server_group: ServerGroupConfig | dict[str, Any],
) -> None:
    """Write server group configuration to YAML file with proper formatting.

    Args:
        server_group_name: Name of the server group
        server_group: Server group configuration dict

    Raises:
        Exception: If file write fails
    """
    output_lines: list[str] = []
    header_comments = get_file_header_comments()
    output_lines.extend(header_comments)
    output_lines.append("")

    # Add server group section header
    pattern = server_group.get('pattern', 'db-per-tenant')
    pattern_label = str(pattern)
    separator = "# " + "=" * 76
    output_lines.append(separator)
    output_lines.append(
        f"# {server_group_name.title()} Server Group ({pattern_label})"
    )
    output_lines.append(separator)
    output_lines.append(f"{server_group_name}:")

    # Don't include 'name' in YAML output - root key is the name
    sg_to_save = {k: v for k, v in server_group.items() if k != 'name'}

    # Dump to string using YAMLLoader protocol
    stream = StringIO()
    yaml.dump(sg_to_save, stream)
    sg_yaml = stream.getvalue()
    sg_lines = sg_yaml.strip().split('\n')
    for line in sg_lines:
        output_lines.append(f"  {line}")

    with Path(SERVER_GROUPS_FILE).open('w') as f:
        f.write('\n'.join(output_lines))
        f.write('\n')
