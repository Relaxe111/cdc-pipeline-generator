"""Helper functions for shell completions (Fish, Bash, etc.)."""

try:
    from cdc_generator.helpers.yaml_loader import yaml  # type: ignore[import-not-found]
except ImportError:
    yaml = None  # type: ignore[assignment]

import os
import sys
from pathlib import Path


def find_server_group_file() -> Path | None:
    """Find source-groups.yaml by searching up from current directory."""
    current = Path(os.getcwd())
    for parent in [current, *current.parents[:3]]:  # Search up to 3 levels
        server_group = parent / "source-groups.yaml"
        if server_group.exists():
            return server_group
    return None


def list_databases_from_server_group() -> list[str]:
    """Extract database names from source-groups.yaml for completions.

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

        databases: set[str] = set()

        for group_data in config.values():
            if not isinstance(group_data, dict) or "pattern" not in group_data:
                continue

            sources = group_data.get("sources", {})
            if isinstance(sources, dict):
                for source_data in sources.values():
                    if not isinstance(source_data, dict):
                        continue
                    for env_name, env_data in source_data.items():
                        if env_name == "schemas" or not isinstance(env_data, dict):
                            continue
                        db_name = env_data.get("database")
                        if isinstance(db_name, str) and db_name.strip():
                            databases.add(db_name.strip())

            db_list = group_data.get("databases", [])
            if isinstance(db_list, list):
                for db in db_list:
                    if isinstance(db, str) and db.strip():
                        databases.add(db.strip())
                    elif isinstance(db, dict):
                        db_name = db.get("name")
                        if isinstance(db_name, str) and db_name.strip():
                            databases.add(db_name.strip())

        return sorted(databases)

    except Exception:
        # Silently fail for completions - don't break the shell
        return []


def main() -> None:
    """CLI entry point for shell completions."""
    if len(sys.argv) > 1 and sys.argv[1] == "--list-databases":
        databases = list_databases_from_server_group()
        for db in databases:
            print(db)
    else:
        print("Usage: python -m cdc_generator.helpers.helpers_completions --list-databases")
        sys.exit(1)


if __name__ == "__main__":
    main()
