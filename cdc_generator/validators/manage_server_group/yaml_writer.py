"""YAML file writing and comment preservation for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from datetime import UTC, datetime
from typing import Any

from cdc_generator.helpers.helpers_logging import print_error, print_info

from .config import SERVER_GROUPS_FILE, get_single_server_group
from .metadata_comments import (
    ensure_file_header_exists,
    generate_per_server_stats,
    is_header_line,
    validate_output_has_metadata,
)


def parse_existing_comments(server_group_name: str) -> list[str]:
    """Parse existing comment metadata for a server group from the YAML file."""
    try:
        with open(SERVER_GROUPS_FILE) as f:
            lines = f.readlines()

        comments: list[str] = []

        # Find the line with the server group name
        server_group_line_idx = -1
        for i, line in enumerate(lines):
            if f'name: {server_group_name}' in line and '- name:' in line:
                server_group_line_idx = i
                break

        if server_group_line_idx == -1:
            return []

        # Look backwards from the server group line to collect metadata comments
        # Stop when we hit the separator line or another server group
        for i in range(server_group_line_idx - 1, -1, -1):
            line = lines[i].rstrip()

            if not line.startswith('#'):
                # Hit non-comment line, stop
                break

            if '============' in line:
                # Hit separator, stop
                break

            # Collect metadata comments (Total Databases, Databases list, Last Updated)
            if any(keyword in line for keyword in ['Total Databases:', 'Databases:', 'Last Updated:', 'Avg Tables:']):
                comments.insert(0, line)  # Insert at beginning to maintain order

        return comments
    except Exception:
        return []


def update_server_group_yaml(server_group_name: str, databases: list[dict[str, Any]]) -> bool:
    """Update server_group.yaml with database/schema information."""
    try:
        # Read the file to preserve comments
        with open(SERVER_GROUPS_FILE) as f:
            file_content = f.read()

        # Extract all comment lines before the server group entry
        # In flat format, comments appear before the server_group_name: key
        preserved_comments: list[str] = []
        lines = file_content.split('\n')

        # Find where the server group entry starts (first non-comment, non-blank line)
        sg_line_idx = -1
        for i, line in enumerate(lines):
            stripped = line.strip()
            # Skip comments and blank lines
            if stripped.startswith('#') or stripped == '':
                continue
            # First non-comment line that ends with ':' is the server group entry
            if stripped.endswith(':') and not stripped.startswith('-'):
                sg_line_idx = i
                break

        if sg_line_idx >= 0:
            # Collect comments BEFORE the server group entry
            for i in range(sg_line_idx):
                line = lines[i]
                if line.strip().startswith('#') or line.strip() == '':
                    # Skip ALL header lines (they'll be regenerated fresh)
                    if is_header_line(line):
                        continue
                    # Skip empty comment lines (just "# " or "#")
                    if line.strip() in ('#', '# '):
                        continue
                    preserved_comments.append(line)

        # CRITICAL: Ensure file header exists
        preserved_comments = ensure_file_header_exists(preserved_comments)

        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]

        server_group = get_single_server_group(config)

        if not server_group:
            print_error("No server group found in configuration")
            return False

        # Verify the server group name matches
        actual_name = server_group.get('name')
        if actual_name != server_group_name:
            print_error(f"Server group name mismatch: expected '{server_group_name}', found '{actual_name}'")
            return False

        pattern = server_group.get('pattern')

        # Initialize header comment lines and service groups
        header_comment_lines: list[str] = []
        service_groups: dict[str, dict[str, Any]] = {}

        # Auto-generate service names for db-shared type
        if pattern == 'db-shared':
            print_info("Auto-generating service names from database names...")

            # Group databases by service
            from collections import defaultdict
            service_groups = defaultdict(lambda: {'databases': {}})
            all_environments: set[str] = set()

            for db in databases:
                db_name = db['name']
                inferred_service = db.get('service', 'unknown')

                # Use environment field from database (already extracted by db_inspector)
                env = db.get('environment', '')

                # Always track the service, even if env is missing
                if inferred_service:
                    if inferred_service not in service_groups:
                        service_groups[inferred_service] = {'databases': {}}

                    if env:
                        all_environments.add(env)
                        service_groups[inferred_service]['databases'][env] = {
                            'name': db_name,
                            'table_count': db.get('table_count', 0),
                            'schemas': db.get('schemas', [])
                        }

            # Sort environments for consistent display
            sorted_environments: list[str] = sorted(all_environments)

            # Display grouped by service (console output + header comments)
            from cdc_generator.helpers.helpers_logging import Colors
            for service in sorted(service_groups.keys()):
                print(f"\n  {Colors.BLUE}{service}{Colors.RESET}")
                header_comment_lines.append(f" ? Service: {service}")

                # Get dev schemas as reference
                dev_db = service_groups[service]['databases'].get('dev')
                dev_schemas: set[str] = set(dev_db['schemas']) if dev_db else set()
                dev_table_count = dev_db['table_count'] if dev_db else 0

                # Show all environments
                for env in sorted_environments:
                    db_info = service_groups[service]['databases'].get(env)

                    if not db_info:
                        # Missing database for this environment
                        print(f"    {env}: {Colors.RED}⚠ missing database{Colors.RESET}")
                        header_comment_lines.append(f" !  {env}: ⚠ missing database")
                    elif db_info['table_count'] == 0:
                        # Database exists but is empty
                        db_name = db_info['name']
                        print(f"    {env}: {Colors.RED}⚠ {db_name} (empty - no tables){Colors.RESET}")
                        header_comment_lines.append(f" !  {env}: ⚠ {db_name} (empty - no tables)")
                    else:
                        # Database exists with tables
                        db_name = db_info['name']
                        table_count = db_info['table_count']
                        db_schemas: set[str] = set(db_info.get('schemas', []))

                        has_warnings = False
                        warning_parts: list[str] = []

                        # Check schema match with dev (if not dev and dev exists)
                        if env != 'dev' and dev_schemas:
                            if db_schemas != dev_schemas:
                                missing_schemas: set[str] = dev_schemas - db_schemas
                                extra_schemas: set[str] = db_schemas - dev_schemas
                                if missing_schemas:
                                    warning_parts.append(f"missing schemas: {', '.join(sorted(missing_schemas))}")
                                if extra_schemas:
                                    warning_parts.append(f"extra schemas: {', '.join(sorted(extra_schemas))}")
                                has_warnings = True

                            # Check table count mismatch with dev - ANY difference triggers warning
                            if dev_table_count > 0 and table_count != dev_table_count:
                                warning_parts.append(f"table count differs from dev ({dev_table_count} tables)")
                                has_warnings = True

                        if has_warnings:
                            warning_msg = "; ".join(warning_parts)
                            print(f"    {env}: {Colors.YELLOW}⚠ {db_name}{Colors.RESET} ({table_count} tables, {Colors.YELLOW}{warning_msg}{Colors.RESET})")
                            header_comment_lines.append(f" TODO: {env}: ⚠ {db_name} ({table_count} tables, {warning_msg})")
                        else:
                            print(f"    {env}: {Colors.GREEN}{db_name}{Colors.RESET} ({table_count} tables)")
                            header_comment_lines.append(f" *  {env}: {db_name} ({table_count} tables)")

                # Add blank line between services in header
                header_comment_lines.append("")

        # Calculate metadata for comments
        total_dbs = len(databases)
        total_tables = sum(db['table_count'] for db in databases)
        avg_tables = int(total_tables / total_dbs) if total_dbs > 0 else 0

        # Calculate per-environment statistics and group databases by service
        from collections import defaultdict
        env_stats: dict[str, dict[str, int]] = defaultdict(lambda: {'dbs': 0, 'tables': 0})
        service_envs: dict[str, set[str]] = defaultdict(set)

        for db in databases:
            # Use environment field from database (already extracted by db_inspector)
            env = db.get('environment', '')

            if env:
                env_stats[env]['dbs'] += 1
                env_stats[env]['tables'] += db.get('table_count', 0)

                # Group by service for db-shared pattern
                if pattern == 'db-shared':
                    service = db.get('service', 'unknown')
                    service_envs[service].add(env)

        # Build per-environment stats line
        env_stats_parts: list[str] = []
        for env in sorted(env_stats.keys()):
            stats = env_stats[env]
            env_stats_parts.append(f"{env}: {stats['dbs']} dbs, {stats['tables']} tables")
        env_stats_line = " | ".join(env_stats_parts) if env_stats_parts else ""

        # Build database list for header comments
        db_list_lines: list[str] = []
        if pattern == 'db-shared' and header_comment_lines:
            # Use the detailed service/environment breakdown
            db_list_lines = header_comment_lines
        else:
            # Fallback to simple database list for db-per-tenant
            db_names = [db['name'] for db in databases]
            current_line = ""
            for db_name in db_names:
                if current_line and len(current_line + ", " + db_name) > 75:
                    db_list_lines.append(current_line)
                    current_line = db_name
                elif current_line:
                    current_line += ", " + db_name
                else:
                    current_line = db_name
            if current_line:
                db_list_lines.append(current_line)

        # Build the complete YAML content with comments
        output_lines: list[str] = []

        # Count sources/services - use service_groups for db-shared (more accurate)
        # Fallback to existing sources/services in server_group config if service_groups is empty
        if pattern == 'db-shared' and service_groups:
            num_services = len(service_groups)
            service_list = ", ".join(sorted(service_groups.keys()))
        elif pattern == 'db-shared' and ('sources' in server_group or 'services' in server_group):
            # Use existing sources from config (fallback to services for legacy)
            existing_sources = server_group.get('sources', server_group.get('services', {}))
            num_services = len(existing_sources)
            service_list = ", ".join(sorted(existing_sources.keys()))
        elif service_envs:
            num_services = len(service_envs)
            service_list = ", ".join(sorted(service_envs.keys()))
        else:
            num_services = 1
            service_list = server_group_name

        # Restore all preserved comments (including header, exclude patterns, etc.)
        # But update the timestamp and metadata
        timestamp_updated = False
        in_metadata_section = False  # Track if we're in a metadata section to skip

        for i, comment in enumerate(preserved_comments):
            # Update timestamp - this marks the start of metadata section
            if 'Updated at:' in comment:
                output_lines.append(f"# Updated at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
                timestamp_updated = True
                in_metadata_section = True

                # Add per-server breakdown with global stats embedded
                per_server_lines = generate_per_server_stats(
                    databases,
                    total_dbs,
                    total_tables,
                    avg_tables,
                    service_list,
                    num_services,
                    env_stats_line
                )
                output_lines.extend(per_server_lines)

                # Add database list
                if db_list_lines:
                    output_lines.append("# Databases:")
                    for line in db_list_lines:
                        output_lines.append(f"#{line}")
                continue

            # Skip all metadata-related lines (will be regenerated)
            # This includes: Total, Services, Per Environment, Databases, Server sections, service lists
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
            ]) or (comment.startswith('#  ') and '=' not in comment) or comment.strip() == '#':
                in_metadata_section = True
                continue

            # Skip separator lines that are part of server sections
            if '============' in comment and in_metadata_section:
                continue

            # If we hit a non-metadata line, we're out of the metadata section
            if comment.strip() and not comment.strip().startswith('#'):
                in_metadata_section = False

            # Skip excessive blank lines before server_group: (keep only essential structure)
            if comment.strip() == '' and i > 0 and output_lines and output_lines[-1].strip() == '':
                continue

            # Keep this line (it's not metadata)
            output_lines.append(comment)

        # If no timestamp was in original comments, add it with stats
        if not timestamp_updated and preserved_comments:
            # Find the header block and add timestamp before closing
            for i, line in enumerate(output_lines):
                if '============' in line and i > 0:
                    output_lines.insert(i, f"# Updated at: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")
                    idx = i + 1

                    # Add per-server breakdown with global stats embedded
                    per_server_lines = generate_per_server_stats(
                        databases,
                        total_dbs,
                        total_tables,
                        avg_tables,
                        service_list,
                        num_services,
                        env_stats_line
                    )
                    for server_line in per_server_lines:
                        output_lines.insert(idx, server_line)
                        idx += 1

                    if db_list_lines:
                        output_lines.insert(idx, "# Databases:")
                        for line_idx, line in enumerate(db_list_lines, start=1):
                            output_lines.insert(idx + line_idx, f"#{line}")
                    break

        # Add exactly one blank line before server_group: if we have preserved comments
        # Remove any trailing blank lines from output_lines first
        while output_lines and output_lines[-1].strip() == '':
            output_lines.pop()

        if preserved_comments:
            output_lines.append("")

        # Add server group header comment
        output_lines.append("# ============================================================================")
        if server_group_name == 'adopus':
            output_lines.append("# AdOpus Server Group (db-per-tenant)")
        elif server_group_name == 'asma':
            output_lines.append("# ASMA Server Group (db-shared)")
        else:
            output_lines.append(f"# {server_group_name.title()} Server Group")
        output_lines.append("# ============================================================================")

        # Get the server group data (without the injected 'name' field)
        sg = dict(server_group)
        sg.pop('name', None)  # Remove the 'name' field we added

        # Check if environment-aware grouping is enabled
        environment_aware = sg.get('environment_aware', False)

        if environment_aware and sg.get('pattern') == 'db-shared':
            # Group databases by service and environment
            source_data: dict[str, dict[str, Any]] = {}
            for db in databases:
                service = db.get('service', db['name'])
                env = db.get('environment', '')
                server_name = db.get('server', 'default')  # Get server from db info

                if service not in source_data:
                    source_data[service] = {
                        'schemas': set(),  # Collect all unique schemas across environments
                        'environments': {}
                    }

                # Add schemas to source-level set
                for schema in db['schemas']:
                    source_data[service]['schemas'].add(schema)

                # Store database for this environment (only one database per env)
                if env:
                    if env not in source_data[service]['environments']:
                        source_data[service]['environments'][env] = {
                            'server': server_name,  # Add server reference
                            'database': db['name'],
                            'table_count': db.get('table_count', 0)
                        }
                    else:
                        # If multiple databases for same source+env, append to database name
                        existing = source_data[service]['environments'][env]['database']
                        if isinstance(existing, str):
                            source_data[service]['environments'][env]['database'] = [existing, db['name']]
                        else:
                            source_data[service]['environments'][env]['database'].append(db['name'])
                        source_data[service]['environments'][env]['table_count'] += db.get('table_count', 0)

            # Convert to final YAML structure using 'sources' key
            sg['sources'] = {}
            for source_name, data in sorted(source_data.items()):
                sg['sources'][source_name] = {
                    'schemas': sorted(data['schemas'])  # Shared schemas at source level
                }
                # Add each environment as a direct key under source
                for env, env_data in sorted(data['environments'].items()):
                    sg['sources'][source_name][env] = env_data

            # Remove old services/databases keys
            sg.pop('services', None)
            sg.pop('databases', None)
        else:
            # db-per-tenant: source name (customer) as root key with single 'default' environment
            # Collect all unique schemas across all databases
            all_schemas: set[str] = set()
            for db in databases:
                for schema in db.get('schemas', []):
                    all_schemas.add(schema)

            # Group by customer name (extracted from db name)
            source_data: dict[str, dict[str, Any]] = {}
            for db in databases:
                customer = db.get('customer', db['name'])
                server_name = db.get('server', 'default')

                if customer not in source_data:
                    source_data[customer] = {
                        'schemas': set(db.get('schemas', [])),
                        'default': {
                            'server': server_name,
                            'database': db['name'],
                            'table_count': db.get('table_count', 0)
                        }
                    }
                else:
                    # Merge schemas
                    for schema in db.get('schemas', []):
                        source_data[customer]['schemas'].add(schema)

            # Convert to final YAML structure using 'sources' key
            sg['sources'] = {}
            for source_name, data in sorted(source_data.items()):
                sg['sources'][source_name] = {
                    'schemas': sorted(data['schemas']),
                    'default': data['default']
                }

            # Remove old services/databases keys
            sg.pop('services', None)
            sg.pop('databases', None)

        # Add the server group entry (flat structure - name is root key)
        output_lines.append(f"{server_group_name}:")

        # Don't include 'name' in YAML output - root key is the name
        sg_to_save = {k: v for k, v in sg.items() if k != 'name'}

        # Dump the server group data and indent it properly
        sg_yaml = yaml.dump(sg_to_save, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
        sg_lines = sg_yaml.strip().split('\n')
        for line in sg_lines:
            # Add 2 spaces of indentation (flat structure)
            output_lines.append(f"  {line}")

        # CRITICAL: Validate before writing
        validate_output_has_metadata(output_lines)

        # Write the complete file
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')

        return True

    except Exception as e:
        print_error(f"Failed to update YAML: {e}")
        import traceback
        traceback.print_exc()
        return False
