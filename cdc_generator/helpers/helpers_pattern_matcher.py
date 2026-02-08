"""Pattern-based database name extraction.

Provides multi-pattern extraction for decomposing database names into
service and environment identifiers using ordered regex patterns.
"""

import re

from cdc_generator.validators.manage_server_group.types import ExtractionPattern


def match_extraction_patterns(
    db_name: str,
    patterns: list[ExtractionPattern],
    server_name: str = "default"
) -> tuple[str, str] | None:
    """Extract service and environment from database name using ordered patterns.

    Patterns are tried in order. First match wins. Each pattern can:
    - Capture service and/or env using named groups: (?P<service>...), (?P<env>...)
    - Override captured env with fixed env field
    - Strip patterns from captured service name (e.g., '_db')
    - Fallback to server_name for env if no env captured/specified

    Args:
        db_name: Database name to decompose
        patterns: Ordered list of extraction patterns (most specific first)
        server_name: Server name to use as env fallback (default: "default")

    Returns:
        (service, env) tuple if any pattern matches, None otherwise

    Rules:
    - If pattern has fixed 'env' field, it must appear in the regex pattern
    - If no env captured and no fixed env, fallback to server_name
    - Strip patterns applied to service name after extraction
    - Env mapping applied to final env value

    Examples:
        >>> patterns = [
        ...     {
        ...         'pattern': r'^(?P<service>\\w+)_db_prod_adcuris$',
        ...         'env': 'prod_adcuris',
        ...         'strip_patterns': ['_db$']
        ...     },
        ...     {
        ...         'pattern': r'^(?P<service>\\w+)_(?P<env>\\w+)$'
        ...     },
        ...     {
        ...         'pattern': r'^(?P<service>\\w+)$'  # Single word, fallback to server_name
        ...     }
        ... ]
        >>> match_extraction_patterns('auth_db_prod_adcuris', patterns, 'prod')
        ('auth', 'prod_adcuris')
        >>> match_extraction_patterns('myservice_dev', patterns, 'default')
        ('myservice', 'dev')
        >>> match_extraction_patterns('auth', patterns, 'prod')
        ('auth', 'prod')
    """
    for pattern_config in patterns:
        regex = pattern_config.get("pattern")
        if not regex:
            continue

        # Validate: if env is hardcoded, it must appear in the pattern
        fixed_env = pattern_config.get("env")
        if fixed_env and fixed_env not in regex:
            # Skip invalid pattern (env not in regex)
            continue

        match = re.match(regex, db_name)
        if not match:
            continue

        # Extract service from named group (required)
        service = match.group("service") if "service" in match.groupdict() else None
        if not service:
            continue

        # Apply strip_patterns to service name (regex-based)
        strip_patterns = pattern_config.get("strip_patterns", [])
        for pattern_to_strip in strip_patterns:
            service = re.sub(pattern_to_strip, "", service)

        # Determine env: priority order
        # 1. Fixed env from config
        # 2. Captured env from regex
        # 3. Fallback to server_name
        env = fixed_env
        if not env:
            env = match.group("env") if "env" in match.groupdict() else None
        if not env:
            env = server_name  # Fallback to server name

        # Apply env_mapping if configured (per-pattern transformation)
        env_mapping = pattern_config.get("env_mapping", {})
        env = env_mapping.get(env, env) if env_mapping else env

        return (service, env)

    return None


def match_single_pattern(db_name: str, pattern: str) -> tuple[str, str] | None:
    """Extract service and environment from database name using single regex pattern.

    Backward compatibility helper for extraction_pattern field.
    Expects pattern with named groups: (?P<service>...) and (?P<env>...)

    Args:
        db_name: Database name to decompose
        pattern: Regex pattern with named groups

    Returns:
        (service, env) tuple if pattern matches, None otherwise

    Examples:
        >>> match_single_pattern('myservice_dev', r'^(?P<service>\\w+)_(?P<env>\\w+)$')
        ('myservice', 'dev')
    """
    match = re.match(pattern, db_name)
    if not match:
        return None

    service = match.group("service") if "service" in match.groupdict() else None
    env = match.group("env") if "env" in match.groupdict() else None

    if service and env:
        return (service, env)

    return None
