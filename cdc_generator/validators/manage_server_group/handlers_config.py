"""CLI handlers for configuration management (exclude patterns, env mappings)."""

from argparse import Namespace

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_success,
    print_warning,
)

from .config import (
    load_database_exclude_patterns,
    load_env_mappings,
    load_schema_exclude_patterns,
    load_table_exclude_patterns,
    save_database_exclude_patterns,
    save_env_mappings,
    save_schema_exclude_patterns,
    save_table_exclude_patterns,
)


def handle_add_ignore_pattern(args: Namespace) -> int:
    """Handle adding pattern(s) to the database exclude list.

    Supports comma-separated patterns for bulk addition.

    Args:
        args: Parsed arguments with add_to_ignore_list string

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        >>> args = Namespace(add_to_ignore_list='test_%,staging_%')
        >>> handle_add_ignore_pattern(args)
        0
    """
    if not args.add_to_ignore_list:
        print_error("No pattern specified")
        return 1

    patterns = load_database_exclude_patterns()

    # Support comma-separated patterns
    input_patterns = [p.strip() for p in args.add_to_ignore_list.split(',')]

    added: list[str] = []
    skipped: list[str] = []

    for pattern in input_patterns:
        if not pattern:
            continue

        if pattern in patterns:
            skipped.append(pattern)
            continue

        patterns.append(pattern)
        added.append(pattern)

    if added:
        save_database_exclude_patterns(patterns)
        print_success(f"✓ Added {len(added)} pattern(s) to database exclude list:")
        for p in added:
            print_info(f"  • {p}")

    if skipped:
        print_warning(f"Already in list ({len(skipped)}): {', '.join(skipped)}")

    if not added and not skipped:
        print_error("No valid patterns provided")
        return 1

    print_info(f"\nCurrent database exclude patterns: {patterns}")

    return 0


def handle_add_schema_exclude(args: Namespace) -> int:
    """Handle adding pattern(s) to the schema exclude list.

    Supports comma-separated patterns for bulk addition.

    Args:
        args: Parsed arguments with add_to_schema_excludes string

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        >>> args = Namespace(add_to_schema_excludes='sys,information_schema')
        >>> handle_add_schema_exclude(args)
        0
    """
    if not args.add_to_schema_excludes:
        print_error("No pattern specified")
        return 1

    patterns = load_schema_exclude_patterns()

    # Support comma-separated patterns
    input_patterns = [p.strip() for p in args.add_to_schema_excludes.split(',')]

    added: list[str] = []
    skipped: list[str] = []

    for pattern in input_patterns:
        if not pattern:
            continue

        if pattern in patterns:
            skipped.append(pattern)
            continue

        patterns.append(pattern)
        added.append(pattern)

    if added:
        save_schema_exclude_patterns(patterns)
        print_success(f"✓ Added {len(added)} pattern(s) to schema exclude list:")
        for p in added:
            print_info(f"  • {p}")

    if skipped:
        print_warning(f"Already in list ({len(skipped)}): {', '.join(skipped)}")

    if not added and not skipped:
        print_error("No valid patterns provided")
        return 1

    print_info(f"\nCurrent schema exclude patterns: {patterns}")

    return 0


def handle_add_table_exclude(args: Namespace) -> int:
    """Handle adding pattern(s) to the table exclude list.

    Supports comma-separated patterns for bulk addition.
    """
    if not args.add_to_table_excludes:
        print_error("No pattern specified")
        return 1

    patterns = load_table_exclude_patterns()
    input_patterns = [p.strip() for p in args.add_to_table_excludes.split(',')]

    added: list[str] = []
    skipped: list[str] = []

    for pattern in input_patterns:
        if not pattern:
            continue

        if pattern in patterns:
            skipped.append(pattern)
            continue

        patterns.append(pattern)
        added.append(pattern)

    if added:
        save_table_exclude_patterns(patterns)
        print_success(f"✓ Added {len(added)} pattern(s) to table exclude list:")
        for pattern in added:
            print_info(f"  • {pattern}")

    if skipped:
        print_warning(f"Already in list ({len(skipped)}): {', '.join(skipped)}")

    if not added and not skipped:
        print_error("No valid patterns provided")
        return 1

    print_info(f"\nCurrent table exclude patterns: {patterns}")
    return 0


def parse_env_mapping(mapping_str: str) -> dict[str, str]:
    """Parse comma-separated environment mapping string into dict.

    Format: "from:to,from:to,..."

    Args:
        mapping_str: Comma-separated mappings in format "from:to"

    Returns:
        Dict mapping source env suffix to target env name

    Raises:
        ValueError: If no valid mappings found

    Example:
        >>> parse_env_mapping("staging:stage,production:prod")
        {'staging': 'stage', 'production': 'prod'}
    """
    if not mapping_str or not mapping_str.strip():
        raise ValueError("Environment mapping string cannot be empty")

    env_mappings: dict[str, str] = {}
    errors: list[str] = []

    for idx, pair_raw in enumerate(mapping_str.split(','), 1):
        pair = pair_raw.strip()
        if not pair:
            continue

        if ':' not in pair:
            errors.append(f"Mapping #{idx} '{pair}': missing colon (expected format 'from:to')")
            continue

        if pair.count(':') > 1:
            errors.append(f"Mapping #{idx} '{pair}': multiple colons found (use format 'from:to')")
            continue

        from_env, to_env = pair.split(':', 1)
        from_env = from_env.strip()
        to_env = to_env.strip()

        if not from_env:
            errors.append(f"Mapping #{idx} '{pair}': empty source environment")
            continue

        if not to_env:
            errors.append(f"Mapping #{idx} '{pair}': empty target environment")
            continue

        # Valid mapping
        env_mappings[from_env] = to_env

    # Report all errors
    for error in errors:
        print_error(f"  {error}")

    if not env_mappings:
        raise ValueError(f"No valid mappings found. Found {len(errors)} error(s).")

    return env_mappings


def handle_add_env_mapping(args: Namespace) -> int:
    """Handle adding environment mapping(s) to the server group.

    Args:
        args: Parsed arguments with add_env_mapping string

    Returns:
        Exit code (0 for success, 1 for error)

    Example:
        >>> args = Namespace(add_env_mapping='staging:stage,production:prod')
        >>> handle_add_env_mapping(args)
        0
    """
    if not args.add_env_mapping:
        print_error("No mapping specified")
        return 1

    try:
        new_mappings = parse_env_mapping(args.add_env_mapping)
    except ValueError as e:
        print_error(f"Invalid mapping format: {e}")
        print_info("\nFormat: 'from:to,from:to,...'")
        print_info("Example: cdc manage-source-groups --add-env-mapping 'staging:stage,production:prod'")
        return 1

    # Load existing mappings
    mappings = load_env_mappings()

    added: list[str] = []
    updated: list[str] = []

    for from_env, to_env in new_mappings.items():
        if from_env in mappings:
            if mappings[from_env] != to_env:
                old_val = mappings[from_env]
                mappings[from_env] = to_env
                updated.append(f"{from_env}: {old_val} → {to_env}")
            # else: same value, skip
        else:
            mappings[from_env] = to_env
            added.append(f"{from_env} → {to_env}")

    if added or updated:
        save_env_mappings(mappings)

        if added:
            print_success(f"✓ Added {len(added)} environment mapping(s):")
            for m in added:
                print_info(f"  • {m}")

        if updated:
            print_success(f"✓ Updated {len(updated)} environment mapping(s):")
            for m in updated:
                print_info(f"  • {m}")
    else:
        print_warning("No changes made (mappings already exist with same values)")

    print_info("\nCurrent env_mappings:")
    for from_env, to_env in sorted(mappings.items()):
        print_info(f"  {from_env} → {to_env}")

    return 0
