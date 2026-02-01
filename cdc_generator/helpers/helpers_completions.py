"""Helper functions for shell completions (Fish, Bash, etc.)."""

try:
    import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

from pathlib import Path
from typing import List
import sys
import os


def find_server_group_file() -> Path | None:
    """Find server_group.yaml by searching up from current directory."""
    current = Path(os.getcwd())
    for parent in [current, *current.parents[:3]]:  # Search up to 3 levels
        server_group = parent / "server_group.yaml"
        if server_group.exists():
            return server_group
    return None


def list_databases_from_server_group() -> List[str]:
    """Extract database names from server_group.yaml for completions.
    
    Returns:
        List of database names found in the server group configuration
    """
    server_group_file = find_server_group_file()
    if not server_group_file:
        return []
    
    try:
        with open(server_group_file) as f:
            config = yaml.safe_load(f)  # type: ignore[misc]
        
        if not config:
            return []
        
        databases: List[str] = []
        server_group = config.get('server_group', {})
        
        # Iterate through all server groups (should be only one in implementations)
        for group_data in server_group.values():
            if isinstance(group_data, dict):
                db_list = group_data.get('databases', [])
                if isinstance(db_list, list):
                    for db in db_list:
                        if isinstance(db, dict):
                            db_name = db.get('name')
                            if db_name:
                                databases.append(str(db_name))
        
        return sorted(set(databases))
    
    except Exception:
        # Silently fail for completions - don't break the shell
        return []


def main() -> None:
    """CLI entry point for shell completions."""
    if len(sys.argv) > 1 and sys.argv[1] == '--list-databases':
        databases = list_databases_from_server_group()
        for db in databases:
            print(db)
    else:
        print("Usage: python -m cdc_generator.helpers.helpers_completions --list-databases")
        sys.exit(1)


if __name__ == "__main__":
    main()
