"""YAML file writing and comment preservation for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import List, Dict, Any
from datetime import datetime, timezone

from .config import SERVER_GROUPS_FILE, get_single_server_group
from .metadata_comments import (
    ensure_file_header_exists,
    validate_output_has_metadata,
    is_header_line,
    generate_per_server_stats,
)
from cdc_generator.helpers.helpers_logging import print_error, print_info


def parse_existing_comments(server_group_name: str) -> List[str]:
    """Parse existing comment metadata for a server group from the YAML file."""
    try:
        with open(SERVER_GROUPS_FILE, 'r') as f:
            lines = f.readlines()
        
        comments: List[str] = []
        
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


def update_server_group_yaml(server_group_name: str, databases: List[Dict[str, Any]]) -> bool:
    """Update server_group.yaml with database/schema information."""
    try:
        # Read the file to preserve comments
        with open(SERVER_GROUPS_FILE, 'r') as f:
            file_content = f.read()
        
        # Extract all comment lines before the server group entry
        # In flat format, comments appear before the server_group_name: key
        preserved_comments: List[str] = []
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
        
        with open(SERVER_GROUPS_FILE, 'r') as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        server_group = get_single_server_group(config)
        
        if not server_group:
            print_error(f"No server group found in configuration")
            return False
        
        # Verify the server group name matches
        actual_name = server_group.get('name')
        if actual_name != server_group_name:
            print_error(f"Server group name mismatch: expected '{server_group_name}', found '{actual_name}'")
            return False
        
        pattern = server_group.get('pattern')
        
        # Initialize header comment lines and service groups
        header_comment_lines: list[str] = []
        service_groups: Dict[str, Dict[str, Any]] = {}
        
        # Auto-generate service names for db-shared type
        if pattern == 'db-shared':
            print_info(f"Auto-generating service names from database names...")
            
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
        env_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {'dbs': 0, 'tables': 0})
        service_envs: Dict[str, set[str]] = defaultdict(set)
        
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
        db_list_lines: List[str] = []
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
                else:
                    if current_line:
                        current_line += ", " + db_name
                    else:
                        current_line = db_name
            if current_line:
                db_list_lines.append(current_line)
        
        # Build the complete YAML content with comments
        output_lines: List[str] = []
        
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
        for i, comment in enumerate(preserved_comments):
            # Update timestamp
            if 'Updated at:' in comment:
                output_lines.append(f"# Updated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
                timestamp_updated = True
                # Add global stats right after timestamp
                output_lines.append(f"# Total: {total_dbs} databases | {total_tables} tables | Avg: {avg_tables} tables/db")
                output_lines.append(f"# ? Services ({num_services}): {service_list}")
                if env_stats_line:
                    output_lines.append(f"# Per Environment: {env_stats_line}")
                
                # Add per-server breakdown
                per_server_lines = generate_per_server_stats(databases)
                output_lines.extend(per_server_lines)
                
                # Add database list
                if db_list_lines:
                    output_lines.append(f"# Databases:")
                    for line in db_list_lines:
                        output_lines.append(f"#{line}")
            # Skip old stats lines (they'll be regenerated)
            # Note: 'Services:' without '?' prefix is legacy format to skip
            elif any(keyword in comment for keyword in ['Total:', 'Total Databases:', 'Per Environment:', 'Databases:', 'Avg Tables']) or \
                 ('Services:' in comment and '? Services' not in comment):
                continue
            # Skip old per-server sections (will be regenerated)
            elif 'Server:' in comment and '========' not in comment:
                continue
            # Skip database list continuation lines (old format without service names)
            # This includes lines starting with "#  " or "# *  " or "# !  " or "# ?  " or "# TODO:" or "# Service:" or "# ? Service:"
            # Also skip standalone "#" lines (blank comment lines from service separators)
            # IMPORTANT: Do NOT skip "# ? Services" line - that's the services count header!
            elif ((comment.startswith('#  ') or comment.startswith('# *  ') or 
                   comment.startswith('# !  ') or 
                   (comment.startswith('# ?  ') and 'Services' not in comment) or  # Preserve "# ? Services (N):" line
                   comment.startswith('# TODO:') or
                   comment.startswith('# Service:') or comment.startswith('# ? Service:') or comment.strip() == '#') and 
                  not any(keyword in comment for keyword in ['='])):
                continue
            # Skip excessive blank lines before server_group: (keep only essential structure)
            elif comment.strip() == '' and i > 0 and output_lines and output_lines[-1].strip() == '':
                continue
                continue
            else:
                output_lines.append(comment)
        
        # If no timestamp was in original comments, add it with stats
        if not timestamp_updated and preserved_comments:
            # Find the header block and add timestamp before closing
            for i, line in enumerate(output_lines):
                if '============' in line and i > 0:
                    output_lines.insert(i, f"# Updated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
                    output_lines.insert(i + 1, f"# Total: {total_dbs} databases | {total_tables} tables | Avg: {avg_tables} tables/db")
                    output_lines.insert(i + 2, f"# ? Services ({num_services}): {service_list}")
                    idx = i + 3
                    if env_stats_line:
                        output_lines.insert(idx, f"# Per Environment: {env_stats_line}")
                        idx += 1
                    
                    # Add per-server breakdown
                    per_server_lines = generate_per_server_stats(databases)
                    for server_line in per_server_lines:
                        output_lines.insert(idx, server_line)
                        idx += 1
                    
                    if db_list_lines:
                        output_lines.insert(idx, f"# Databases:")
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
            source_data: Dict[str, Dict[str, Any]] = {}
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
            if 'services' in sg:
                del sg['services']
            if 'databases' in sg:
                del sg['databases']
        else:
            # db-per-tenant: source name (customer) as root key with single 'default' environment
            # Collect all unique schemas across all databases
            all_schemas: set[str] = set()
            for db in databases:
                for schema in db.get('schemas', []):
                    all_schemas.add(schema)
            
            # Group by customer name (extracted from db name)
            source_data: Dict[str, Dict[str, Any]] = {}
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
            if 'services' in sg:
                del sg['services']
            if 'databases' in sg:
                del sg['databases']
        
        # Add the server group entry (flat structure - name is root key)
        output_lines.append(f"{server_group_name}:")
        
        # Dump the server group data and indent it properly
        sg_yaml = yaml.dump(sg, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
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
