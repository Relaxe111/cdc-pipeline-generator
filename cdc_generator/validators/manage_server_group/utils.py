"""Utility functions for schema generation and completions."""

import json
import subprocess
from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
)
from cdc_generator.helpers.service_config import get_project_root

from .types import DatabaseInfo


def _services_dir() -> Path:
    return get_project_root() / "services"


def _service_schema_file() -> Path:
    return get_project_root() / ".vscode" / "service-schema.json"


def _completions_script() -> Path:
    return get_project_root() / "scripts" / "generate-completions.sh"


def regenerate_all_validation_schemas(server_group_names: list[str] | None = None) -> None:
    """Deprecated no-op: static .vscode/schemas are now used for YAML validation."""
    _ = server_group_names
    print_info("Static VS Code schemas enabled; skipping dynamic validation schema generation")


def update_vscode_schema(databases: list[DatabaseInfo]) -> bool:
    """Deprecated no-op: static .vscode/schemas are now used for YAML validation."""
    _ = databases
    print_info("Static VS Code schemas enabled; skipping dynamic VS Code schema updates")
    return True


def update_completions() -> bool:
    """Regenerate Fish shell completions."""
    try:
        completions_script = _completions_script()
        if not completions_script.exists():
            return True

        result = subprocess.run(['bash', str(completions_script)], capture_output=True, text=True)

        if result.returncode == 0:
            print_success("✓ Regenerated Fish shell completions")

            # Reload completions (optional - only works if fish is available)
            try:
                # Check if fish is available
                fish_check = subprocess.run(['which', 'fish'], capture_output=True)
                if fish_check.returncode == 0:
                    subprocess.run(
                        ['fish', '-c', 'complete -c cdc -e; and source ~/.config/fish/completions/cdc.fish'],
                        capture_output=True,
                        timeout=5
                    )
                    print_info("  (Run 'complete -c cdc -e; and source ~/.config/fish/completions/cdc.fish' to reload)")
            except:
                # Fish not available or reload failed - not critical
                print_info("  (Run 'complete -c cdc -e; and source ~/.config/fish/completions/cdc.fish' to reload)")

            return True
        print_info(f"Completions generation returned: {result.returncode}")
        return False

    except Exception as e:
        print_error(f"Failed to update completions: {e}")
        return False
