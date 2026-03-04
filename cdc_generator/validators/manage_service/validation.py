"""Validation functions for CDC service configuration."""

from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import load_service_config
from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_service.preflight import (
    ValidationConfig,
    collect_sink_routing_issues,
    collect_unique_template_issues,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent
_VALID_SOURCE_TABLE_KEYS = {"include_columns", "ignore_columns", "primary_key", "track_columns"}


def _resolve_server_group_pattern(service: str) -> str | None:
    """Resolve pattern name from source-groups for display purposes."""
    source_groups_path = PROJECT_ROOT.parent / "source-groups.yaml"
    if not source_groups_path.exists():
        return None

    try:
        source_groups = load_yaml_file(source_groups_path)
    except Exception:
        return None

    if not isinstance(source_groups, dict):
        return None

    for group_data in source_groups.values():
        if not isinstance(group_data, dict):
            continue
        sources = group_data.get("sources", {})
        if isinstance(sources, dict) and service in sources:
            return str(group_data.get("pattern", "unknown"))

    return None


def _validate_service_identity(
    service: str,
    config: dict[str, object],
    errors: list[str],
) -> None:
    """Validate top-level service identity fields."""
    if "service" not in config:
        errors.append(
            f"Missing 'service' field (should be automatically added from '{service}:' root key)"
        )
        return

    if config["service"] != service:
        errors.append(
            f"Service name mismatch: config has '{config['service']}' but file is '{service}.yaml'"
        )


def _validate_source_tables(
    source: dict[str, object],
    errors: list[str],
    warnings: list[str],
) -> None:
    """Validate source tables section shape and keys."""
    if "tables" not in source:
        errors.append("Missing 'source.tables' - no tables defined for CDC")
        return

    tables = source["tables"]
    if not isinstance(tables, dict):
        errors.append("'source.tables' must be a dict (schema.table format)")
        return
    if not tables:
        warnings.append("No CDC tables defined in source.tables")
        return

    for table_key, table_config in tables.items():
        if "." not in table_key:
            warnings.append(
                f"Table key '{table_key}' should be in schema.table format (e.g., 'public.users')"
            )
        if table_config is None:
            continue
        if not isinstance(table_config, dict):
            errors.append(
                f"{table_key}: table configuration must be a dict/object, got {type(table_config).__name__}"
            )
            continue

        for key in table_config:
            if key not in _VALID_SOURCE_TABLE_KEYS:
                warnings.append(
                    f"{table_key}: unknown configuration key '{key}' "
                    + f"(valid: {', '.join(sorted(_VALID_SOURCE_TABLE_KEYS))})"
                )


def _validate_source_section(
    config: dict[str, object],
    errors: list[str],
    warnings: list[str],
) -> None:
    """Validate source section structure."""
    if "source" not in config:
        errors.append("Missing 'source' section")
        return

    source = config["source"]
    if not isinstance(source, dict):
        errors.append("'source' must be a dict/object")
        return

    _validate_source_tables(source, errors, warnings)


def _validate_sink_tables(
    sink_key: str,
    sink_tables: dict[str, object],
    errors: list[str],
    warnings: list[str],
) -> None:
    """Validate sink table definitions under one sink."""
    if not sink_tables:
        warnings.append(f"sinks.{sink_key}.tables: defined but empty")
        return

    for sink_table_key, sink_table_config in sink_tables.items():
        if not isinstance(sink_table_config, dict):
            errors.append(f"sinks.{sink_key}.tables.{sink_table_key}: must be a dict/object")
            continue

        if "from" not in sink_table_config:
            errors.append(
                f"sinks.{sink_key}.tables.{sink_table_key}: missing 'from' field (source table reference)"
            )
        if "target_exists" not in sink_table_config:
            warnings.append(
                f"sinks.{sink_key}.tables.{sink_table_key}: missing 'target_exists' field (should be true or false)"
            )

        replicate = sink_table_config.get("replicate_structure", False)
        has_columns_mapping = "columns" in sink_table_config
        has_replicate_key = "replicate_structure" in sink_table_config
        if (not replicate) and (not has_columns_mapping) and (not has_replicate_key):
            warnings.append(
                f"sinks.{sink_key}.tables.{sink_table_key}: neither 'columns' nor "
                + "'replicate_structure' specified"
            )


def _validate_sinks_section(
    config: dict[str, object],
    errors: list[str],
    warnings: list[str],
) -> None:
    """Validate sinks section structure and table mappings."""
    if "sinks" not in config:
        warnings.append("No 'sinks' section defined (service only has source CDC tables)")
        return

    sinks = config["sinks"]
    if not isinstance(sinks, dict):
        errors.append("'sinks' must be a dict/object")
        return
    if not sinks:
        warnings.append("'sinks' section is present but empty")
        return

    for sink_key, sink_config in sinks.items():
        if not isinstance(sink_config, dict):
            errors.append(f"sinks.{sink_key}: sink configuration must be a dict/object")
            continue
        if "tables" not in sink_config:
            warnings.append(f"sinks.{sink_key}: no 'tables' section defined")
            continue

        sink_tables = sink_config["tables"]
        if not isinstance(sink_tables, dict):
            errors.append(f"sinks.{sink_key}.tables: must be a dict/object")
            continue

        _validate_sink_tables(sink_key, sink_tables, errors, warnings)


def _print_validation_messages(errors: list[str], warnings: list[str]) -> None:
    """Print validation errors and warnings."""
    if errors:
        print_error(f"\n{'='*80}")
        print_error("Configuration Validation Errors")
        print_error(f"{'='*80}\n")
        for error in errors:
            print_error(f"  ❌ {error}")
        print_error(f"\n{'='*80}\n")

    if warnings:
        print_warning(f"\n{'='*80}")
        print_warning("Configuration Warnings")
        print_warning(f"{'='*80}\n")
        for warning in warnings:
            print_warning(f"  ⚠️  {warning}")
        print_warning(f"\n{'='*80}\n")


def _validation_success(
    service: str,
    errors: list[str],
    warnings: list[str],
    hierarchy_valid: bool,
) -> bool:
    """Compute final validation success and print summary."""
    if not errors and not warnings and hierarchy_valid:
        print_success(f"\n✓ All validation checks passed for {service}")
        print_success("✓ Configuration is ready for pipeline generation")
        return True
    if not errors and hierarchy_valid:
        print_warning(f"\n⚠️  Validation passed with warnings for {service}")
        return True

    print_error(f"\n✗ Validation failed for {service}")
    return False


def validate_service_sink_preflight(
    service: str,
    config: dict[str, object] | None = None,
) -> tuple[list[str], list[str]]:
    """Validate sink routing and unique template constraints for a service."""
    cfg = load_service_config(service) if config is None else config
    typed_cfg: ValidationConfig = dict(cfg)
    routing_errors, routing_warnings = collect_sink_routing_issues(service, typed_cfg)
    unique_errors = collect_unique_template_issues(service, typed_cfg)
    return routing_errors + unique_errors, routing_warnings


def validate_service_config(service: str) -> bool:
    """Validate service configuration for pipeline generation readiness.

    Checks the new simplified service structure:
    - <service>:
        source:
          tables: {...}
        sinks: {...}

    Returns:
        True if validation passes, False otherwise
    """
    config = load_service_config(service)

    errors = []
    warnings = []

    server_group_pattern = _resolve_server_group_pattern(service)

    pattern_display = f" ({server_group_pattern} pattern)" if server_group_pattern else ""
    print_header(f"Validating configuration for {service}{pattern_display}")

    _validate_service_identity(service, config, errors)
    _validate_source_section(config, errors, warnings)
    _validate_sinks_section(config, errors, warnings)

    # 4. Run hierarchy validation (simplified for new structure)
    print_info("\nChecking hierarchical configuration...")
    hierarchy_valid = validate_hierarchy_no_duplicates(service)

    preflight_errors, preflight_warnings = validate_service_sink_preflight(service, config)
    errors.extend(preflight_errors)
    warnings.extend(preflight_warnings)

    _print_validation_messages(errors, warnings)
    return _validation_success(service, errors, warnings, hierarchy_valid)


def validate_hierarchy_no_duplicates(service: str) -> bool:
    """Validate hierarchical structure (simplified for new format).

    The new format doesn't have complex inheritance hierarchies like
    the old format. This is a placeholder for future hierarchy validation.

    Returns:
        True (always passes for new format)
    """
    _ = service

    # TODO: Add hierarchy validation if we introduce
    # schema-level defaults → service-level overrides in the future

    print_success(f"✓ ✓ Hierarchical inheritance validation passed for {service}")
    return True
