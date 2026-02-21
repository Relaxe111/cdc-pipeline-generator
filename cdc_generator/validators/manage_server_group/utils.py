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
    """Regenerate validation schemas for services using the specified server groups.

    Args:
        server_group_names: List of server group names to filter by. If None, regenerates all.
    """
    services_dir = _services_dir()
    if not services_dir.exists():
        return

    # Find all service YAML files
    service_files = list(services_dir.glob("*.yaml"))
    if not service_files:
        return

    print_header("Regenerating Validation Schemas")

    for service_file in service_files:
        service_name = service_file.stem
        print_info(f"\n→ Regenerating schema for service: {service_name}")

        try:
            # Load service config to check server group type
            try:
                import yaml  # type: ignore[import-not-found]
            except ImportError:
                yaml = None  # type: ignore[assignment]
            with service_file.open() as f:
                service_config = yaml.safe_load(f)  # type: ignore[misc]

            # Get server group name to determine type
            server_group_name = service_config.get('server_group')
            if not server_group_name:
                print_info(f"  ⊘ Skipped {service_name} (no server_group defined)")
                continue

            # Filter by server group if specified
            if server_group_names and server_group_name not in server_group_names:
                continue

            # Load server groups to check type
            from .config import get_single_server_group, load_server_groups
            server_groups_config = load_server_groups()
            server_group = get_single_server_group(server_groups_config)

            if not server_group:
                print_info(f"  ⊘ Skipped {service_name} (no server group found in configuration)")
                continue

            # Verify the server group name matches
            if server_group.get('name') != server_group_name:
                print_info(f"  ⊘ Skipped {service_name} (server group mismatch: expected '{server_group_name}', found '{server_group.get('name')}')")
                continue

            pattern = server_group.get('pattern')

            # Only db-per-tenant services need a reference customer
            if pattern == 'db-per-tenant' and 'reference' not in service_config:
                print_info(f"  ⊘ Skipped {service_name} (db-per-tenant requires reference customer)")
                continue

            # Generate with --all to include all schemas
            # NOTE: This import would need to be updated based on actual generator structure
            from cdc_generator.cli.service import generate_service_validation_schema

            success = generate_service_validation_schema(
                service=service_name,
                env='nonprod',
                schema_filter=None  # None means all schemas
            )

            if success:
                print_success(f"  ✓ {service_name} validation schema updated")
            else:
                print_info(f"  ⚠ {service_name} schema generation returned False")

        except Exception as e:
            print_info(f"  ⚠ Failed to regenerate schema for {service_name}: {e}")

    print_success("\n✓ Validation schema regeneration complete")


def update_vscode_schema(databases: list[DatabaseInfo]) -> bool:
    """Update .vscode/service-schema.json with database names."""
    try:
        service_schema_file = _service_schema_file()
        if not service_schema_file.exists():
            return True

        with service_schema_file.open() as f:
            schema = json.load(f)

        # Update database enum
        db_names = sorted([db['name'] for db in databases])

        # Find and update the database name enum in the schema
        # This is a simplified approach - adjust based on actual schema structure
        if 'definitions' in schema and 'database' in schema['definitions']:
            schema['definitions']['database']['enum'] = db_names

        with service_schema_file.open('w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"✓ Updated VS Code schema with {len(db_names)} databases")
        return True

    except Exception as e:
        print_error(f"Failed to update VS Code schema: {e}")
        return False


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
