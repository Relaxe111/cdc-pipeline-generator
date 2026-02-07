"""Misc handlers for manage-service (no-service and interactive)."""

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
        "Usage: cdc manage-service --service <name> [options]"
    )
    print_info(
        "Run 'cdc manage-service --help' for more information"
    )
    return 1


def handle_interactive(args: argparse.Namespace) -> int:
    """Run interactive mode (legacy workflow)."""
    return run_interactive_mode(args)
