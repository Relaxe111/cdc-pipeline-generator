"""Configuration loading and management for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from pathlib import Path
from typing import List, Optional, Dict, Any, cast
import os


def get_implementation_root() -> Path:
    """Locate implementation root by searching for known markers."""
    current = Path(os.getcwd())
    for parent in [current, *current.parents]:
        server_group = parent / "server_group.yaml"
        services_dir = parent / "services"
        customers_dir = parent / "2-customers"
        if server_group.exists() or services_dir.is_dir() or customers_dir.is_dir():
            return parent
    # As a final fallback, return current directory so new files are created
    # where the command is executed, instead of defaulting to the generator root.
    return current


PROJECT_ROOT = get_implementation_root()
SERVER_GROUPS_FILE = PROJECT_ROOT / "server_group.yaml"


def load_server_groups() -> Dict[str, Any]:
    """Load server groups configuration from YAML file."""
    if not SERVER_GROUPS_FILE.exists():
        raise FileNotFoundError(f"Server groups file not found: {SERVER_GROUPS_FILE}")
    
    with open(SERVER_GROUPS_FILE) as f:
        return yaml.safe_load(f) or {}  # type: ignore[misc]


def get_single_server_group(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get the single server group from configuration.
    
    Since each implementation has only one server group, this returns the first one found.
    Adds 'name' field to the returned dict for compatibility.
    
    Args:
        config: Loaded server groups configuration
        
    Returns:
        Server group dict with 'name' field added, or None if no server group exists
    """
    server_group_dict = config.get('server_group', {})
    if not server_group_dict:
        return None
    
    # Get the first (and should be only) server group
    for name, group_data in server_group_dict.items():
        result = dict(group_data)
        result['name'] = name
        return result
    
    return None


def load_database_exclude_patterns() -> List[str]:
    """Load database exclude patterns from server_group.yaml.
    
    Supports both comment-based format (legacy) and key-based format (current).
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # First try to load from server group config (preferred)
        # YAML structure: database_exclude_patterns is a list of strings
        server_group_dict = config.get('server_group', {})
        for group_data in server_group_dict.values():
            if isinstance(group_data, dict):
                patterns = cast(Optional[List[str]], group_data.get('database_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns
        
        # Fallback to comment-based parsing (legacy)
        with open(SERVER_GROUPS_FILE) as f:
            for line in f:
                if 'database_exclude_patterns:' in line and line.strip().startswith('#'):
                    start = line.find('[')
                    end = line.find(']')
                    if start != -1 and end != -1:
                        patterns_str = line[start+1:end]
                        patterns = [p.strip().strip("'\"") for p in patterns_str.split(',')]
                        return [p for p in patterns if p]
        
        return []
    except Exception:
        return []


def load_schema_exclude_patterns() -> List[str]:
    """Load schema exclude patterns from server_group.yaml.
    
    Supports both comment-based format (legacy) and key-based format (current).
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # First try to load from server group config (preferred)
        # YAML structure: schema_exclude_patterns is a list of strings
        server_group_dict = config.get('server_group', {})
        for group_data in server_group_dict.values():
            if isinstance(group_data, dict):
                patterns = cast(Optional[List[str]], group_data.get('schema_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns
        
        # Fallback to comment-based parsing (legacy)
        with open(SERVER_GROUPS_FILE) as f:
            for line in f:
                if 'schema_exclude_patterns:' in line and line.strip().startswith('#'):
                    start = line.find('[')
                    end = line.find(']')
                    if start != -1 and end != -1:
                        patterns_str = line[start+1:end]
                        patterns = [p.strip().strip("'\"") for p in patterns_str.split(',')]
                        return [p for p in patterns if p]
        
        return []
    except Exception:
        return []


def save_database_exclude_patterns(patterns: List[str]) -> None:
    """Save database exclude patterns to server_group.yaml as YAML key (not comment)."""
    try:
        # Preserve all comments from the file
        with open(SERVER_GROUPS_FILE, 'r') as f:
            file_content = f.read()
        
        preserved_comments: List[str] = []
        lines = file_content.split('\n')
        
        # Find where server_group: line is
        sg_line_idx = -1
        for i, line in enumerate(lines):
            if line.strip() == 'server_group:':
                sg_line_idx = i
                break
        
        if sg_line_idx >= 0:
            # Collect comments BEFORE server_group:
            for i in range(sg_line_idx):
                line = lines[i]
                if line.strip().startswith('#') or line.strip() == '':
                    preserved_comments.append(line)
            
            # Also collect comments AFTER server_group: but before first actual entry
            # BUT skip server group header comments (they'll be regenerated)
            for i in range(sg_line_idx + 1, len(lines)):
                line = lines[i]
                if line.strip() and not line.strip().startswith('#'):
                    break
                # Skip server group header separators and titles (they'll be regenerated)
                if '============' in line or 'Server Group' in line:
                    continue
                if line.strip().startswith('#') or line.strip() == '':
                    preserved_comments.append(line)
        
        # Load config
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # Update the first server group with the patterns
        server_group_dict = config.get('server_group', {})
        for group_data in server_group_dict.values():
            if isinstance(group_data, dict):
                group_data['database_exclude_patterns'] = patterns
                break
        
        # Rebuild file with preserved comments
        output_lines: List[str] = []
        
        # Add preserved comments
        for comment in preserved_comments:
            output_lines.append(comment)
        
        if preserved_comments:
            output_lines.append("")
        
        output_lines.append("server_group:")
        
        # Write each server group
        server_group_dict = config.get('server_group', {})
        for sg_name, sg in server_group_dict.items():
            output_lines.append("# ============================================================================")
            if sg_name == 'adopus':
                output_lines.append("# AdOpus Server Group (db-per-tenant)")
            elif sg_name == 'asma':
                output_lines.append("# ASMA Server Group (db-shared)")
            output_lines.append("# ============================================================================")
            output_lines.append(f"  {sg_name}:")
            
            sg_yaml = yaml.dump(sg, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
            sg_lines = sg_yaml.strip().split('\n')
            for line in sg_lines:
                output_lines.append(f"    {line}")
        
        # Write back
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
    
    except Exception as e:
        raise RuntimeError(f"Failed to save database exclude patterns: {e}")


def save_schema_exclude_patterns(patterns: List[str]) -> None:
    """Save schema exclude patterns to server_group.yaml as YAML key (not comment)."""
    try:
        # Preserve all comments from the file
        with open(SERVER_GROUPS_FILE, 'r') as f:
            file_content = f.read()
        
        preserved_comments: List[str] = []
        lines = file_content.split('\n')
        
        # Find where server_group: line is
        sg_line_idx = -1
        for i, line in enumerate(lines):
            if line.strip() == 'server_group:':
                sg_line_idx = i
                break
        
        if sg_line_idx >= 0:
            # Collect comments BEFORE server_group:
            for i in range(sg_line_idx):
                line = lines[i]
                if line.strip().startswith('#') or line.strip() == '':
                    preserved_comments.append(line)
            
            # Also collect comments AFTER server_group: but before first actual entry
            # BUT skip server group header comments (they'll be regenerated)
            for i in range(sg_line_idx + 1, len(lines)):
                line = lines[i]
                if line.strip() and not line.strip().startswith('#'):
                    break
                # Skip server group header separators and titles (they'll be regenerated)
                if '============' in line or 'Server Group' in line:
                    continue
                if line.strip().startswith('#') or line.strip() == '':
                    preserved_comments.append(line)
        
        # Load config
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # Update the first server group with the patterns
        server_group_dict = config.get('server_group', {})
        for group_data in server_group_dict.values():
            if isinstance(group_data, dict):
                group_data['schema_exclude_patterns'] = patterns
                break
        
        # Rebuild file with preserved comments
        output_lines: List[str] = []
        
        # Add preserved comments
        for comment in preserved_comments:
            output_lines.append(comment)
        
        if preserved_comments:
            output_lines.append("")
        
        output_lines.append("server_group:")
        
        # Write each server group
        server_group_dict = config.get('server_group', {})
        for sg_name, sg in server_group_dict.items():
            output_lines.append("# ============================================================================")
            if sg_name == 'adopus':
                output_lines.append("# AdOpus Server Group (db-per-tenant)")
            elif sg_name == 'asma':
                output_lines.append("# ASMA Server Group (db-shared)")
            output_lines.append("# ============================================================================")
            output_lines.append(f"  {sg_name}:")
            
            sg_yaml = yaml.dump(sg, default_flow_style=False, sort_keys=False, indent=2, allow_unicode=True)  # type: ignore[misc]
            sg_lines = sg_yaml.strip().split('\n')
            for line in sg_lines:
                output_lines.append(f"    {line}")
        
        # Write back
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')
    
    except Exception as e:
        raise RuntimeError(f"Failed to save schema exclude patterns: {e}")
