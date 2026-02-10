"""YAML file writing and comment preservation for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_info

from .comment_processor import collect_preserved_comments, filter_metadata_comments
from .config import SERVER_GROUPS_FILE, get_single_server_group
from .data_models import MetadataParams
from .db_shared_formatter import (
    format_service_header_comments,
    group_databases_by_service,
)
from .metadata_comments import ensure_file_header_exists, validate_output_has_metadata
from .output_builder import (
    build_database_list_lines,
    build_server_group_header,
    insert_metadata_into_header,
    insert_metadata_section,
)
from .stats_calculator import (
    build_environment_stats_line,
    calculate_database_stats,
    calculate_environment_stats,
    determine_service_info,
)
from .types import ServerGroupConfig
from .yaml_builder import (
    build_db_per_tenant_structure,
    build_db_shared_structure,
    convert_to_yaml_structure,
)


def _find_server_group_line(lines: list[str]) -> int:
    """
    Find the first server group entry line in YAML file.

    Args:
        lines: File content as list of lines

    Returns:
        Index of server group line, or -1 if not found
    """
    for i, line in enumerate(lines):
        stripped = line.strip()

        # Skip comments and blank lines
        if stripped.startswith('#') or stripped == '':
            continue

        # First non-comment line ending with ':' is server group entry
        if stripped.endswith(':') and not stripped.startswith('-'):
            return i

    return -1


def _process_databases_by_pattern(
    pattern: str | None,
    databases: list[dict[str, Any]]
) -> tuple[list[str], dict[str, dict[str, Any]] | None]:
    """
    Process databases based on server group pattern.

    For db-shared: Groups by service and generates detailed breakdown.
    For db-per-tenant: Returns empty lists.

    Args:
        pattern: Server group pattern (db-shared or db-per-tenant)
        databases: List of database info dictionaries

    Returns:
        Tuple of (header_comment_lines, service_groups)
    """
    if pattern != 'db-shared':
        return [], None

    print_info("Auto-generating service names from database names...")

    # Group databases by service
    service_groups, sorted_environments = group_databases_by_service(databases)

    # Generate formatted header comments
    header_comment_lines = format_service_header_comments(
        service_groups,
        sorted_environments
    )

    return header_comment_lines, service_groups


def _build_output_lines(
    preserved_comments: list[str],
    metadata: MetadataParams
) -> list[str]:
    """
    Build complete output lines with metadata and comments.

    Args:
        preserved_comments: Comments to preserve from original file
        metadata: Metadata parameters for comment generation

    Returns:
        Complete list of output lines
    """
    output_lines: list[str] = []
    timestamp_updated = False

    # Process preserved comments and inject metadata
    def mark_timestamp_updated(_value: bool) -> None:
        nonlocal timestamp_updated
        timestamp_updated = True

    # Filter and restore comments
    for comment in filter_metadata_comments(preserved_comments):
        # Inject metadata at timestamp location
        if 'Updated at:' in comment:
            mark_timestamp_updated(True)
            insert_metadata_section(output_lines, metadata)
            continue

        output_lines.append(comment)

    # If no timestamp found, try to insert into header
    if not timestamp_updated and preserved_comments:
        insert_metadata_into_header(output_lines, metadata)

    # Clean up trailing blank lines
    while output_lines and output_lines[-1].strip() == '':
        output_lines.pop()

    # Add separator before server group
    if preserved_comments:
        output_lines.append("")

    return output_lines


def _build_server_group_dict(
    server_group: ServerGroupConfig,
    databases: list[dict[str, Any]],
    pattern: str | None
) -> dict[str, Any]:
    """
    Build server group dictionary for YAML serialization.

    Args:
        server_group: Original server group config
        databases: List of database info dictionaries
        pattern: Server group pattern (db-shared or db-per-tenant)

    Returns:
        Dictionary ready for YAML serialization
    """
    # Copy and remove injected 'name' field
    sg = dict(server_group)
    sg.pop('name', None)

    # Check if environment-aware grouping is enabled
    environment_aware = sg.get('environment_aware', False)

    # Build sources structure based on pattern
    if environment_aware and pattern == 'db-shared':
        source_data = build_db_shared_structure(databases)
        sg['sources'] = convert_to_yaml_structure(source_data, 'db-shared')
    else:
        source_data = build_db_per_tenant_structure(databases)
        sg['sources'] = convert_to_yaml_structure(source_data, 'db-per-tenant')

    # Remove legacy keys
    sg.pop('services', None)
    sg.pop('databases', None)

    return sg



def update_server_group_yaml(
    server_group_name: str,
    databases: list[dict[str, Any]]
) -> bool:
    """
    Update source-groups.yaml with database/schema information.

    Args:
        server_group_name: Name of the server group to update
        databases: List of database information dictionaries

    Returns:
        True if successful, False otherwise
    """
    try:
        # Read file and parse lines
        with SERVER_GROUPS_FILE.open() as f:
            file_content = f.read()

        lines = file_content.split('\n')
        sg_line_idx = _find_server_group_line(lines)

        # Collect and filter comments
        preserved_comments = collect_preserved_comments(lines, sg_line_idx)
        preserved_comments = ensure_file_header_exists(preserved_comments)

        # Load and validate server group config
        with SERVER_GROUPS_FILE.open() as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        server_group = get_single_server_group(config)

        if not server_group:
            print_error("No server group found in configuration")
            return False

        # Verify server group name matches
        actual_name = server_group.get('name')
        if actual_name != server_group_name:
            error_msg = (
                f"Server group name mismatch: expected '{server_group_name}', "
                f"found '{actual_name}'"
            )
            print_error(error_msg)
            return False

        pattern = server_group.get('pattern')

        # Process databases based on pattern
        header_comment_lines, service_groups = _process_databases_by_pattern(
            pattern,
            databases
        )

        # Calculate statistics
        total_dbs, total_tables, avg_tables = calculate_database_stats(databases)
        env_stats = calculate_environment_stats(databases)
        env_stats_line = build_environment_stats_line(env_stats)

        # Determine service information
        num_services, service_list = determine_service_info(
            pattern or 'db-per-tenant',
            server_group_name,
            server_group,
            service_groups
        )

        # Build metadata params (need partial for db_list_lines)
        metadata_partial = MetadataParams(
            databases=databases,
            total_dbs=total_dbs,
            total_tables=total_tables,
            avg_tables=avg_tables,
            service_list=service_list,
            num_services=num_services,
            env_stats_line=env_stats_line,
            db_list_lines=[]  # Will be filled next
        )

        # Build database list for comments
        db_list_lines = build_database_list_lines(
            metadata_partial,
            pattern or 'db-per-tenant',
            header_comment_lines
        )

        # Update metadata with db_list_lines
        metadata = MetadataParams(
            databases=databases,
            total_dbs=total_dbs,
            total_tables=total_tables,
            avg_tables=avg_tables,
            service_list=service_list,
            num_services=num_services,
            env_stats_line=env_stats_line,
            db_list_lines=db_list_lines
        )

        output_lines = _build_output_lines(
            preserved_comments,
            metadata
        )

        # Add server group header and data
        output_lines.extend(build_server_group_header(server_group_name))

        # Build server group YAML structure
        sg_dict = _build_server_group_dict(
            server_group,
            databases,
            pattern
        )

        # Add server group entry
        output_lines.append(f"{server_group_name}:")

        # Serialize and indent YAML
        sg_yaml = yaml.dump(  # type: ignore[misc]
            sg_dict,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
            allow_unicode=True
        )
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            output_lines.append(f"  {line}")

        # Validate and write
        validate_output_has_metadata(output_lines)

        with SERVER_GROUPS_FILE.open('w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')

        return True

    except Exception as e:
        print_error(f"Failed to update YAML: {e}")
        import traceback
        traceback.print_exc()
        return False

