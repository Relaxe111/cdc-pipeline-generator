"""Extraction pattern utilities for server group management."""

from argparse import Namespace
from typing import Any


def build_extraction_pattern_config(args: Namespace, pattern: str) -> dict[str, Any]:
    """Build extraction pattern configuration from command-line arguments.

    Args:
        args: Parsed arguments with optional env, strip_patterns, env_mapping, description
        pattern: The regex pattern

    Returns:
        Pattern configuration dictionary
    """
    pattern_config: dict[str, Any] = {'pattern': pattern}

    if hasattr(args, 'env') and args.env:
        pattern_config['env'] = args.env

    if hasattr(args, 'strip_patterns') and args.strip_patterns:
        patterns = [s.strip() for s in args.strip_patterns.split(',') if s.strip()]
        if patterns:
            pattern_config['strip_patterns'] = patterns

    if hasattr(args, 'env_mapping') and args.env_mapping:
        # Parse env_mapping list in format ['from:to', 'from2:to2']
        env_map: dict[str, str] = {}
        for mapping_str in args.env_mapping:
            if ':' in mapping_str:
                from_env, to_env = mapping_str.split(':', 1)
                env_map[from_env.strip()] = to_env.strip()
        if env_map:
            pattern_config['env_mapping'] = env_map

    if hasattr(args, 'description') and args.description:
        pattern_config['description'] = args.description

    return pattern_config


def display_pattern_info(pattern_config: dict[str, Any], pattern: str) -> None:
    """Display extraction pattern information.

    Args:
        pattern_config: Pattern configuration dict
        pattern: The regex pattern
    """
    from cdc_generator.helpers.helpers_logging import print_info

    print_info(f"  Pattern: {pattern}")
    if pattern_config.get('env'):
        print_info(f"  Fixed env: {pattern_config['env']}")
    if pattern_config.get('strip_suffixes'):
        print_info(f"  Strip suffixes: {', '.join(pattern_config['strip_suffixes'])}")
    if pattern_config.get('description'):
        print_info(f"  Description: {pattern_config['description']}")


def validate_pattern_index(
    index_str: str,
    patterns: list[Any],
) -> tuple[bool, int]:
    """Validate pattern index is valid integer within range.

    Args:
        index_str: String representation of index
        patterns: List of patterns

    Returns:
        Tuple of (is_valid, index_value)
    """
    from cdc_generator.helpers.helpers_logging import print_error

    try:
        index = int(index_str)
    except ValueError:
        print_error(f"Invalid index '{index_str}'. Must be an integer.")
        return (False, -1)

    if not patterns:
        print_error("No extraction patterns configured for this server")
        return (False, -1)

    if index < 0 or index >= len(patterns):
        print_error(f"Invalid index {index}. Must be between 0 and {len(patterns) - 1}")
        return (False, -1)

    return (True, index)


def display_extraction_pattern(
    idx: int,
    pattern_config: dict[str, Any],
) -> None:
    """Display details of a single extraction pattern.

    Args:
        idx: Pattern index
        pattern_config: Pattern configuration dict
    """
    from cdc_generator.helpers.helpers_logging import print_info

    print_info(f"  [{idx}] Pattern: {pattern_config.get('pattern', '(missing)')}")
    if pattern_config.get('env'):
        print_info(f"      Fixed env: {pattern_config['env']}")
    if pattern_config.get('strip_patterns'):
        patterns_str = ', '.join(pattern_config['strip_patterns'])
        print_info(f"      Strip patterns: {patterns_str}")
    if pattern_config.get('env_mapping'):
        mappings = ', '.join(
            [f"{k}→{v}" for k, v in pattern_config['env_mapping'].items()]
        )
        print_info(f"      Env mapping: {mappings}")
    if pattern_config.get('description'):
        print_info(f"      Description: {pattern_config['description']}")


def display_server_patterns(
    server_name: str,
    server_config: dict[str, Any],
) -> bool:
    """Display extraction patterns for a server.

    Args:
        server_name: Name of the server
        server_config: Server configuration dict

    Returns:
        True if server has patterns, False otherwise
    """
    from cdc_generator.helpers.helpers_logging import print_info, print_warning

    patterns = server_config.get('extraction_patterns', [])

    print()
    print_info(f"📍 Server: {server_name}")

    if patterns:
        for idx, pattern_config in enumerate(patterns):
            display_extraction_pattern(idx, pattern_config)
        return True

    # No patterns
    print_warning("  (no extraction patterns configured)")
    # Check for old single pattern
    single_pattern = server_config.get('extraction_pattern')
    if single_pattern:
        print_info(f"  Legacy single pattern: {single_pattern}")
    return False


def display_pattern_help() -> None:
    """Display help text for adding extraction patterns."""
    from cdc_generator.helpers.helpers_logging import print_info

    print()
    print_info("💡 Add extraction patterns for a server:")
    print_info(
        "   cdc manage-source-groups --add-extraction-pattern default "
        + "'^(?P<service>\\w+)_(?P<env>\\w+)$'"
    )
    print_info(
        "   cdc manage-source-groups --add-extraction-pattern prod "
        + "'^(?P<service>\\w+)_db_prod_adcuris$' \\"
    )
    print_info("       --env prod_adcuris --strip-suffixes '_db'")
