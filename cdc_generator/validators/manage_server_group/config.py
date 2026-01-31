"""Configuration loading and management for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from pathlib import Path
from typing import List, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SERVER_GROUPS_FILE = PROJECT_ROOT / "server-groups.yaml"


def load_server_groups() -> Dict[str, Any]:
    """Load server groups configuration from YAML file."""
    if not SERVER_GROUPS_FILE.exists():
        raise FileNotFoundError(f"Server groups file not found: {SERVER_GROUPS_FILE}")
    
    with open(SERVER_GROUPS_FILE) as f:
        return yaml.safe_load(f) or {}  # type: ignore[misc]


def get_server_group_by_name(config: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    """Get a specific server group by name from configuration."""
    for sg in config.get('server_groups', []):
        if sg.get('name') == name:
            return sg
    return None


def load_database_exclude_patterns() -> List[str]:
    """Load database exclude patterns from server-groups.yaml metadata."""
    try:
        with open(SERVER_GROUPS_FILE) as f:
            for line in f:
                if 'database_exclude_patterns:' in line:
                    # Extract the list from the comment
                    # Format: # database_exclude_patterns: ['pattern1', 'pattern2']
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
    """Load schema exclude patterns from server-groups.yaml metadata."""
    try:
        with open(SERVER_GROUPS_FILE) as f:
            for line in f:
                if 'schema_exclude_patterns:' in line:
                    # Extract the list from the comment
                    # Format: # schema_exclude_patterns: ['pattern1', 'pattern2']
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
    """Save database exclude patterns to server-groups.yaml metadata comment."""
    try:
        with open(SERVER_GROUPS_FILE) as f:
            lines = f.readlines()
        
        # Find and update the database_exclude_patterns line
        updated = False
        for i, line in enumerate(lines):
            if 'database_exclude_patterns:' in line:
                lines[i] = f"# database_exclude_patterns: {patterns}\n"
                updated = True
                break
        
        if not updated:
            # Add the line after the first comment block
            for i, line in enumerate(lines):
                if line.strip() and not line.strip().startswith('#'):
                    lines.insert(i, f"# database_exclude_patterns: {patterns}\n")
                    break
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.writelines(lines)
    
    except Exception as e:
        raise RuntimeError(f"Failed to save database exclude patterns: {e}")


def save_schema_exclude_patterns(patterns: List[str]) -> None:
    """Save schema exclude patterns to server-groups.yaml metadata comment."""
    try:
        with open(SERVER_GROUPS_FILE) as f:
            lines = f.readlines()
        
        # Find and update the schema_exclude_patterns line
        updated = False
        for i, line in enumerate(lines):
            if 'schema_exclude_patterns:' in line:
                lines[i] = f"# schema_exclude_patterns: {patterns}\n"
                updated = True
                break
        
        if not updated:
            # Add the line after database_exclude_patterns or at the start
            for i, line in enumerate(lines):
                if 'database_exclude_patterns:' in line:
                    lines.insert(i + 1, f"# schema_exclude_patterns: {patterns}\n")
                    updated = True
                    break
            
            if not updated:
                # Add at the start of the file
                for i, line in enumerate(lines):
                    if line.strip() and not line.strip().startswith('#'):
                        lines.insert(i, f"# schema_exclude_patterns: {patterns}\n")
                        break
        
        with open(SERVER_GROUPS_FILE, 'w') as f:
            f.writelines(lines)
    
    except Exception as e:
        raise RuntimeError(f"Failed to save schema exclude patterns: {e}")
