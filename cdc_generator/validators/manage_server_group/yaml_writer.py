"""YAML file writing and comment preservation for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from typing import List, Dict, Any, Optional

from .config import SERVER_GROUPS_FILE, get_single_server_group
from .filters import infer_service_name
from cdc_generator.helpers.helpers_logging import print_error, print_info, print_success


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
        # Read the file to preserve exclude patterns comments
        with open(SERVER_GROUPS_FILE, 'r') as f:
            file_content = f.read()
        
        # Extract exclude patterns comments if they exist
        database_exclude_patterns_line: Optional[str] = None
        schema_exclude_patterns_line: Optional[str] = None
        for line in file_content.split('\n'):
            if line.startswith('# database_exclude_patterns:'):
                database_exclude_patterns_line = line
            elif line.startswith('# schema_exclude_patterns:'):
                schema_exclude_patterns_line = line
        
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
        
        # Set service name for db-per-tenant: use server group name
        if pattern == 'db-per-tenant':
            server_group['service'] = server_group_name
            print_info(f"Set service name to '{server_group_name}' (db-per-tenant)")
        
        # Auto-generate service names for db-shared type
        if pattern == 'db-shared':
            print_info(f"Auto-generating service names from database names...")
            for db in databases:
                db_name = db['name']
                inferred_service = infer_service_name(db_name)
                
                # Check if database already has a service set (not REPLACE_ME)
                existing_service: Optional[str] = None
                for existing_db in server_group.get('databases', []):
                    if existing_db.get('name') == db_name:
                        existing_service = existing_db.get('service')
                        break
                
                # Keep existing service if it's not REPLACE_ME, otherwise use inferred
                if existing_service and existing_service != 'REPLACE_ME':
                    db['service'] = existing_service
                    print_info(f"  • {db_name:<40} -> {existing_service} (preserved)")
                else:
                    db['service'] = inferred_service
                    print_success(f"  • {db_name:<40} -> {inferred_service} (inferred)")
        
        # For db-per-tenant, set service to the server group name for all databases
        if pattern == 'db-per-tenant':
            for db in databases:
                db['service'] = server_group_name
        
        # Calculate metadata for comments
        total_dbs = len(databases)
        _total_schemas = sum(len(db['schemas']) for db in databases)
        total_tables = sum(db['table_count'] for db in databases)
        _avg_tables = int(total_tables / total_dbs) if total_dbs > 0 else 0
        
        # Build database list for comments (comma-separated)
        _db_list = ', '.join(db['name'] for db in databases)
        
        # Build the complete YAML content with comments
        output_lines: List[str] = []
        
        # Add auto-generated warning at the very top
        output_lines.append("# ============================================================================")
        output_lines.append("# AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY")
        output_lines.append("# Use 'cdc manage-server-group' commands to modify this file")
        output_lines.append("# ============================================================================")
        output_lines.append("")
        
        # Preserve database_exclude_patterns comment at the top
        if database_exclude_patterns_line:
            output_lines.append(database_exclude_patterns_line)
        
        # Preserve schema_exclude_patterns comment at the top
        if schema_exclude_patterns_line:
            output_lines.append(schema_exclude_patterns_line)
        
        output_lines.append("server_group:")
        
        server_group_dict = config.get('server_group', {})
        for sg_name, sg in server_group_dict.items():
            
            # If this is the server group being updated, add the databases
            if sg_name == server_group_name:
                # Store databases as objects with name, service, schemas (remove table_count)
                sg['databases'] = [
                    {
                        'name': db['name'],
                        'service': db['service'],
                        'schemas': db['schemas']
                    }
                    for db in databases
                ]
            
            # Add comment header before each server group
            output_lines.append("# ============================================================================")
            if sg_name == 'adopus':
                output_lines.append("# AdOpus Server Group (db-per-tenant)")
            elif sg_name == 'asma':
                output_lines.append("# ASMA Server Group (db-shared)")
            output_lines.append("# ============================================================================")
            
            # Add the server group as a dict entry (key: value format)
            # Write the key first
            output_lines.append(f"  {sg_name}:")
            
            # Dump the server group data and indent it properly
            sg_yaml = yaml.dump(sg, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
            sg_lines = sg_yaml.strip().split('\n')
            for line in sg_lines:
                # Add 4 spaces of indentation (2 for server_group level + 2 for content)
                output_lines.append(f"    {line}")
        
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
