"""Configuration loading and management for server groups."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from pathlib import Path
from typing import List, Optional, Dict, Any, cast
import os

from .types import ServerGroupConfig, ServerGroupFile


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


def load_server_groups() -> ServerGroupFile:
    """Load server groups configuration from YAML file.
    
    Returns:
        ServerGroupFile: Dict mapping server group names to their configs
    """
    if not SERVER_GROUPS_FILE.exists():
        raise FileNotFoundError(f"Server groups file not found: {SERVER_GROUPS_FILE}")
    
    with open(SERVER_GROUPS_FILE) as f:
        return cast(ServerGroupFile, yaml.safe_load(f) or {})  # type: ignore[misc]


def get_single_server_group(config: ServerGroupFile) -> Optional[ServerGroupConfig]:
    """Get the single server group from configuration.
    
    Format: server_group_name as root key (e.g., asma1: {...})
    
    Since each implementation has only one server group, this returns the first one found.
    Adds 'name' field to the returned dict for compatibility.
    
    Args:
        config: Loaded server groups configuration (ServerGroupFile)
        
    Returns:
        ServerGroupConfig with 'name' field injected, or None if no server group exists
    """
    # Flat format: name as root key with 'pattern' field
    # Look for any top-level key that has a 'pattern' field (server group marker)
    for name, group_data in config.items():
        if 'pattern' in group_data:
            # Create a mutable copy with the name injected
            result = dict(group_data)
            result['name'] = name
            return cast(ServerGroupConfig, result)
    
    return None


def load_database_exclude_patterns() -> List[str]:
    """Load database exclude patterns from server_group.yaml.
    
    Format: server_group_name as root key with database_exclude_patterns field.
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # Flat format: server_group_name as root key with 'pattern' field
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(Optional[List[str]], group_data.get('database_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns
        
        return []
    except Exception:
        return []


def load_schema_exclude_patterns() -> List[str]:
    """Load schema exclude patterns from server_group.yaml.
    
    Format: server_group_name as root key with schema_exclude_patterns field.
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        # Flat format: server_group_name as root key with 'pattern' field
        for group_data in config.values():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                patterns = cast(Optional[List[str]], group_data.get('schema_exclude_patterns'))  # type: ignore[misc]
                if patterns:
                    return patterns
        
        return []
    except Exception:
        return []


def save_database_exclude_patterns(patterns: List[str]) -> None:
    """Save database exclude patterns to server_group.yaml.
    
    Directly updates the YAML file, preserving the flat format structure.
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config: Dict[str, Any] = yaml.safe_load(f) or {}  # type: ignore[misc]
        
        # Find and update the server group
        for _group_name, group_data in config.items():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['database_exclude_patterns'] = patterns
                
                # Write back the config
                with open(SERVER_GROUPS_FILE, 'w') as f:
                    yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)  # type: ignore[misc]
                return
        
        raise RuntimeError("No server group found to update")
    except Exception as e:
        raise RuntimeError(f"Failed to save database exclude patterns: {e}")


def save_schema_exclude_patterns(patterns: List[str]) -> None:
    """Save schema exclude patterns to server_group.yaml.
    
    Directly updates the YAML file, preserving the flat format structure.
    """
    try:
        with open(SERVER_GROUPS_FILE) as f:
            config: Dict[str, Any] = yaml.safe_load(f) or {}  # type: ignore[misc]
        
        # Find and update the server group
        for _group_name, group_data in config.items():
            if isinstance(group_data, dict) and 'pattern' in group_data:
                group_data['schema_exclude_patterns'] = patterns
                
                # Write back the config
                with open(SERVER_GROUPS_FILE, 'w') as f:
                    yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)  # type: ignore[misc]
                return
        
        raise RuntimeError("No server group found to update")
    except Exception as e:
        raise RuntimeError(f"Failed to save schema exclude patterns: {e}")
