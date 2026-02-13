"""Service removal handlers for manage-service."""

import argparse
import shutil

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
)
from cdc_generator.helpers.service_config import get_project_root


def handle_remove_service(args: argparse.Namespace) -> int:
    """Remove a service configuration and related local artifacts."""
    if not args.service:
        print_error("❌ Error: --service is required for --remove-service")
        return 1

    service_name = str(args.service)
    project_root = get_project_root()

    service_file = project_root / "services" / f"{service_name}.yaml"
    if not service_file.exists():
        print_error(
            f"❌ Service '{service_name}' does not exist: {service_file}"
        )
        return 1

    # 1) Remove services/<service>.yaml
    service_file.unlink()
    print_success(f"✓ Removed {service_file.relative_to(project_root)}")

    # 2) Keep source-groups.yaml untouched by design.
    print_info("Keeping source-groups.yaml unchanged")

    # 3) Remove service-schemas/<service>/
    schemas_dir = project_root / "service-schemas" / service_name
    if schemas_dir.exists():
        shutil.rmtree(schemas_dir)
        print_success(f"✓ Removed {schemas_dir.relative_to(project_root)}/")
    else:
        print_info("No service-schemas directory found for service")

    print_info("\nNext steps:")
    print_info("  • Run 'cdc generate' to refresh generated artifacts")
    print_info("  • Review sink mappings in other services if they referenced this service")

    return 0
