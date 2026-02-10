"""Bloblang validation handler for manage-service."""

from __future__ import annotations

import argparse

from cdc_generator.validators.manage_service.bloblang_validator import (
    validate_service_bloblang,
)


def handle_validate_bloblang(args: argparse.Namespace) -> int:
    """Handle --validate-bloblang command.

    Args:
        args: Parsed command-line arguments

    Returns:
        0 if validation passed, 1 if failed
    """
    success = validate_service_bloblang(args.service)
    return 0 if success else 1
