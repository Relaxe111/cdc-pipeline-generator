"""Validation handlers for manage-service."""

import argparse

from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.validators.manage_service.schema_generator import (
    generate_service_validation_schema,
)
from cdc_generator.validators.manage_service.validation import (
    validate_hierarchy_no_duplicates,
    validate_service_config,
)


def handle_validate_config(args: argparse.Namespace) -> int:
    """Comprehensive validation of service config."""
    return 0 if validate_service_config(args.service) else 1


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
