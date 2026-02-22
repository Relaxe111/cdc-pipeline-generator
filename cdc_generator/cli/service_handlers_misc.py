"""Misc handlers for manage-services config (no-service and interactive)."""

import argparse

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
)
from cdc_generator.validators.manage_service.interactive_mode import (
    run_interactive_mode,
)


def handle_no_service() -> int:
    """Show available services when no --service given."""
    from cdc_generator.helpers.service_config import get_project_root

    services_dir = get_project_root() / "services"
    if services_dir.exists():
        service_files = sorted(services_dir.glob("*.yaml"))
        if service_files:
            print_header("Available Services")
            for sf in service_files:
                print(f"  • {sf.stem}")
            print()

    print_error("❌ Error: --service is required")
    print_info(
        "Usage: cdc manage-services config --service <name> [options]"
    )
    print_info(
        "Run 'cdc manage-services config --help' for more information"
    )
    return 1


def handle_list_services() -> int:
    """List all services from services/*.yaml in the current project."""
    from cdc_generator.helpers.service_config import get_project_root

    services_dir = get_project_root() / "services"
    if not services_dir.exists():
        print_info("No services directory found")
        return 0

    service_files = sorted(services_dir.glob("*.yaml"))
    if not service_files:
        print_info("No services found")
        return 0

    print_header("Available Services")
    for sf in service_files:
        print(f"  • {sf.stem}")
    print()
    print_info(f"Total services: {len(service_files)}")
    return 0


def handle_interactive(args: argparse.Namespace) -> int:
    """Run interactive mode (legacy workflow)."""
    return run_interactive_mode(args)
