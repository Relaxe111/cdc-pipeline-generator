"""Service creation handlers for manage-service."""

import argparse
from pathlib import Path
from typing import cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_warning,
)
from cdc_generator.validators.manage_service.service_creator import (
    create_service,
)


def _detect_server_group(
    service_name: str,
    project_root: Path,
) -> tuple[str | None, set[str], set[str]]:
    """Detect server group for a service from source-groups.yaml.

    Returns:
        (server_group, defined_services, existing_services)
    """
    server_groups_file = project_root / "source-groups.yaml"
    existing_services = _collect_existing_services(project_root)

    if not server_groups_file.exists():
        return None, set(), existing_services

    from cdc_generator.helpers.yaml_loader import load_yaml_file

    try:
        raw_data = load_yaml_file(server_groups_file)
    except (FileNotFoundError, ValueError):
        return None, set(), existing_services

    server_groups_data = cast(dict[str, object], raw_data)
    server_group, defined_services = _extract_server_group(
        server_groups_data,
        service_name,
    )

    if not server_group:
        server_group = _fallback_single_server_group(
            server_groups_data,
        )
        if server_group:
            print_info(
                f"Using only server group: {server_group}"
            )

    return server_group, defined_services, existing_services


def _extract_server_group(
    server_groups_data: dict[str, object],
    service_name: str,
) -> tuple[str | None, set[str]]:
    """Extract server group and defined services from source groups."""
    server_group: str | None = None
    defined_services: set[str] = set()

    for sg_name_key, sg_data_val in server_groups_data.items():
        if not isinstance(sg_data_val, dict):
            continue
        sg_dict = cast(dict[str, object], sg_data_val)
        sources_val = sg_dict.get("sources")
        if not isinstance(sources_val, dict):
            continue
        src_dict = cast(dict[str, object], sources_val)
        for src_key in src_dict:
            defined_services.add(src_key)
            if src_key == service_name:
                server_group = sg_name_key
                print_info(
                    "Auto-detected server group: "
                    + f"{server_group}"
                )

    return server_group, defined_services


def _fallback_single_server_group(
    server_groups_data: dict[str, object],
) -> str | None:
    """Return the only server group name if exactly one exists."""
    if len(server_groups_data) != 1:
        return None

    first_key = next(iter(server_groups_data))
    first_val = server_groups_data[first_key]
    if isinstance(first_val, dict) and "sources" in first_val:
        return str(first_key)

    return None


def _collect_existing_services(project_root: Path) -> set[str]:
    """Collect existing service names from services/*.yaml."""
    services_dir = project_root / "services"
    if not services_dir.exists():
        return set()

    return {svc_file.stem for svc_file in services_dir.glob("*.yaml")}


def handle_create_service(args: argparse.Namespace) -> int:
    """Create a new service configuration file."""
    if not args.service:
        print("❌ Error: --service is required for --create-service")
        return 1

    from cdc_generator.helpers.service_config import get_project_root

    project_root = get_project_root()
    services_dir = project_root / "services"
    service_file = services_dir / f"{args.service}.yaml"

    if service_file.exists():
        print_error(
            f"❌ Service '{args.service}' already exists: "
            + f"{service_file}"
        )
        print_info("To modify it, edit the file directly or use:")
        print_info(
            f"  cdc manage-service --service {args.service} "
            + "--add-source-table <schema.table>"
        )
        return 1

    server_group, defined_services, existing_services = (
        _detect_server_group(args.service, project_root)
    )

    if defined_services:
        missing = defined_services - existing_services
        if not missing:
            print_warning(
                "⚠️  All services defined in source-groups.yaml "
                + "already have configuration files"
            )
            svc_str = ", ".join(sorted(existing_services))
            print_info(
                f"Existing services: {svc_str}"
            )
        elif args.service not in defined_services:
            print_error(
                f"❌ Service '{args.service}' "
                + "not found in source-groups.yaml"
            )
            if missing:
                missing_str = ", ".join(sorted(missing))
                print_info(
                    "Services defined but not yet created: "
                    + f"{missing_str}"
                )
                print_info(
                    f"Did you mean one of these? {missing_str}"
                )
            return 1

    if not server_group:
        print_error(
            "❌ Could not find server group "
            + f"for service '{args.service}'"
        )
        print_error("Add service mapping to source-groups.yaml")
        return 1

    server_name = getattr(args, "server", None) or "default"
    create_service(args.service, server_group, server=server_name)
    return 0
