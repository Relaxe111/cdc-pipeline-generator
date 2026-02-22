"""Validation handlers for manage-services config."""

import argparse

from cdc_generator.helpers.helpers_logging import print_error, print_info
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.validators.manage_service.schema_generator import (
    generate_service_validation_schema,
)
from cdc_generator.validators.manage_service.validation import (
    validate_hierarchy_no_duplicates,
    validate_service_config,
)


def handle_validate_config(args: argparse.Namespace) -> int:
    """Comprehensive validation of service config.

    If args.service is None, validates all services in services/ directory.
    """
    if args.service:
        # Validate single service
        return 0 if validate_service_config(args.service) else 1

    # Validate all services
    services_dir = get_project_root() / "services"
    if not services_dir.exists():
        print_error("No services directory found")
        return 1

    service_files = sorted(services_dir.glob("*.yaml"))
    if not service_files:
        print_error("No service files found in services/")
        return 1

    print_info(f"Validating {len(service_files)} service(s)...\n")

    results: dict[str, bool] = {}
    for service_file in service_files:
        service_name = service_file.stem
        print_info(f"{'=' * 80}")
        results[service_name] = validate_service_config(service_name)
        print()  # Blank line between services

    # Summary
    print_info(f"{'=' * 80}")
    print_info("Validation Summary")
    print_info(f"{'=' * 80}\n")

    passed = [s for s, ok in results.items() if ok]
    failed = [s for s, ok in results.items() if not ok]

    if passed:
        print_info(f"✓ Passed ({len(passed)}): {', '.join(passed)}")
    if failed:
        print_error(f"✗ Failed ({len(failed)}): {', '.join(failed)}")

    return 0 if all(results.values()) else 1


def handle_validate_hierarchy(args: argparse.Namespace) -> int:
    """Validate hierarchical inheritance (no duplicate values)."""
    return 0 if validate_hierarchy_no_duplicates(args.service) else 1


def handle_generate_validation(args: argparse.Namespace) -> int:
    """Generate JSON Schema for service YAML validation."""
    if not args.all and not args.schema:
        print_error(
            "Error: --generate-validation requires either "
            + "--all (for all schemas) or --schema <name>"
        )
        return 1

    schema_filter = None if args.all else args.schema
    ok = generate_service_validation_schema(
        args.service, args.env, schema_filter,
    )
    return 0 if ok else 1
