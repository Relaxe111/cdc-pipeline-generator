"""Service removal handlers for manage-service."""

import argparse
import shutil

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import (
    load_yaml_file,
    save_yaml_file,
)


def _remove_from_source_groups(service_name: str) -> bool:
    """Remove service from source-groups.yaml sources entries.

    Returns:
        True if any source-group entry was updated.
    """
    source_groups_file = get_project_root() / "source-groups.yaml"
    if not source_groups_file.exists():
        return False

    try:
        raw_data = load_yaml_file(source_groups_file)
    except (FileNotFoundError, ValueError) as exc:
        print_warning(f"Could not read source-groups.yaml: {exc}")
        return False

    changed = False
    for _sg_name, sg_data in raw_data.items():
        if not isinstance(sg_data, dict):
            continue
        sources_raw = sg_data.get("sources")
        if not isinstance(sources_raw, dict):
            continue
        if service_name in sources_raw:
            del sources_raw[service_name]
            changed = True

    if changed:
        save_yaml_file(raw_data, source_groups_file)

    return changed


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

    # 2) Remove source-groups.yaml sources.<service> entries
    removed_from_source_groups = _remove_from_source_groups(service_name)
    if removed_from_source_groups:
        print_success("✓ Removed service from source-groups.yaml sources")
    else:
        print_info("No source-groups.yaml sources entry found for service")

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
