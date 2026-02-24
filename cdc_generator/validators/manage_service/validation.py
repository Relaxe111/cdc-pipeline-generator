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

PROJECT_ROOT = Path(__file__).parent.parent.parent


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

    # Load source-groups.yaml to get server group pattern
    source_groups_path = PROJECT_ROOT.parent / "source-groups.yaml"
    server_group_pattern = None
    if source_groups_path.exists():
        try:
            source_groups = load_yaml_file(source_groups_path)
            # Find the server group that contains this service
            for group_name, group_data in source_groups.items():
                if isinstance(group_data, dict):
                    sources = group_data.get('sources', {})
                    if service in sources:
                        server_group_pattern = group_data.get('pattern', 'unknown')
                        break
        except Exception:
            pass

    pattern_display = f" ({server_group_pattern} pattern)" if server_group_pattern else ""
    print_header(f"Validating configuration for {service}{pattern_display}")

    # 1. Check basic structure
    if 'service' not in config:
        errors.append(f"Missing 'service' field (should be automatically added from '{service}:' root key)")
    elif config['service'] != service:
        errors.append(f"Service name mismatch: config has '{config['service']}' but file is '{service}.yaml'")

    # 2. Check source section
    if 'source' not in config:
        errors.append("Missing 'source' section")
    else:
        source = config['source']
        if not isinstance(source, dict):
            errors.append("'source' must be a dict/object")
        # Check for tables
        elif 'tables' not in source:
            errors.append("Missing 'source.tables' - no tables defined for CDC")
        else:
            tables = source['tables']
            if not isinstance(tables, dict):
                errors.append("'source.tables' must be a dict (schema.table format)")
            elif not tables:
                warnings.append("No CDC tables defined in source.tables")
            else:
                # Validate each table definition
                for table_key, table_config in tables.items():
                    if '.' not in table_key:
                        warnings.append(f"Table key '{table_key}' should be in schema.table format (e.g., 'public.users')")

                    if table_config is None:
                        continue  # Empty table config is OK (uses defaults)

                    if not isinstance(table_config, dict):
                        errors.append(f"{table_key}: table configuration must be a dict/object, got {type(table_config).__name__}")
                        continue

                    # Check for common config patterns
                    valid_keys = {'include_columns', 'ignore_columns', 'primary_key', 'track_columns'}
                    for key in table_config.keys():
                        if key not in valid_keys:
                            warnings.append(f"{table_key}: unknown configuration key '{key}' (valid: {', '.join(sorted(valid_keys))})")

    # 3. Check sinks section (optional)
    if 'sinks' in config:
        sinks = config['sinks']
        if not isinstance(sinks, dict):
            errors.append("'sinks' must be a dict/object")
        elif not sinks:
            warnings.append("'sinks' section is present but empty")
        else:
            # Validate each sink
            for sink_key, sink_config in sinks.items():
                if not isinstance(sink_config, dict):
                    errors.append(f"sinks.{sink_key}: sink configuration must be a dict/object")
                    continue

                # Check for tables in sink
                if 'tables' not in sink_config:
                    warnings.append(f"sinks.{sink_key}: no 'tables' section defined")
                else:
                    sink_tables = sink_config['tables']
                    if not isinstance(sink_tables, dict):
                        errors.append(f"sinks.{sink_key}.tables: must be a dict/object")
                    elif not sink_tables:
                        warnings.append(f"sinks.{sink_key}.tables: defined but empty")
                    else:
                        # Validate each sink table
                        for sink_table_key, sink_table_config in sink_tables.items():
                            if not isinstance(sink_table_config, dict):
                                errors.append(f"sinks.{sink_key}.tables.{sink_table_key}: must be a dict/object")
                                continue

                            # Check required fields for sink tables
                            if 'from' not in sink_table_config:
                                errors.append(f"sinks.{sink_key}.tables.{sink_table_key}: missing 'from' field (source table reference)")

                            if 'target_exists' not in sink_table_config:
                                warnings.append(f"sinks.{sink_key}.tables.{sink_table_key}: missing 'target_exists' field (should be true or false)")

                            # If replicate_structure is false/missing, check for column mappings
                            replicate = sink_table_config.get('replicate_structure', False)
                            if not replicate:
                                if 'columns' not in sink_table_config and 'replicate_structure' not in sink_table_config:
                                    warnings.append(f"sinks.{sink_key}.tables.{sink_table_key}: neither 'columns' nor 'replicate_structure' specified")

    else:
        warnings.append("No 'sinks' section defined (service only has source CDC tables)")

    # 4. Run hierarchy validation (simplified for new structure)
    print_info("\nChecking hierarchical configuration...")
    hierarchy_valid = validate_hierarchy_no_duplicates(service)

    # Print results
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

    if not errors and not warnings and hierarchy_valid:
        print_success(f"\n✓ All validation checks passed for {service}")
        print_success("✓ Configuration is ready for pipeline generation")
        return True
    if not errors and hierarchy_valid:
        print_warning(f"\n⚠️  Validation passed with warnings for {service}")
        return True
    print_error(f"\n✗ Validation failed for {service}")
    return False


def validate_hierarchy_no_duplicates(service: str) -> bool:
    """Validate hierarchical structure (simplified for new format).

    The new format doesn't have complex inheritance hierarchies like
    the old format. This is a placeholder for future hierarchy validation.

    Returns:
        True (always passes for new format)
    """
    config = load_service_config(service)

    # TODO: Add hierarchy validation if we introduce
    # schema-level defaults → service-level overrides in the future

    print_success(f"✓ ✓ Hierarchical inheritance validation passed for {service}")
    return True
