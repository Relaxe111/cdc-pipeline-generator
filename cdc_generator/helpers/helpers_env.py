"""Shared helpers for environment variable generation and .env file management.

Used by both source-group and sink-group CLI handlers when adding servers.
Generates consistent env variable placeholder names and appends them to .env.
"""

from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root

# ============================================================================
# Env Variable Name Generation
# ============================================================================


def source_server_env_vars(
    db_type: str,
    server_name: str,
    kafka_topology: str = "shared",
) -> dict[str, str]:
    """Generate env variable placeholders for a source server.

    Args:
        db_type: Database type ('postgres' or 'mssql')
        server_name: Server name (e.g., 'default', 'nonprod', 'prod')
        kafka_topology: 'shared' or 'per-server'

    Returns:
        Dict with keys: host, port, user, password, kafka_bootstrap_servers
        Values are env var placeholders like '${POSTGRES_SOURCE_HOST_NONPROD}'

    Example:
        >>> source_server_env_vars('postgres', 'nonprod')
        {'host': '${POSTGRES_SOURCE_HOST_NONPROD}', ...}
    """
    prefix = "POSTGRES_SOURCE" if db_type == "postgres" else "MSSQL_SOURCE"

    # Non-default servers get a postfix
    if server_name != "default":
        postfix = f"_{server_name.upper()}"
        placeholders = {
            "host": f"${{{prefix}_HOST{postfix}}}",
            "port": f"${{{prefix}_PORT{postfix}}}",
            "user": f"${{{prefix}_USER{postfix}}}",
            "password": f"${{{prefix}_PASSWORD{postfix}}}",
        }
    else:
        placeholders = {
            "host": f"${{{prefix}_HOST}}",
            "port": f"${{{prefix}_PORT}}",
            "user": f"${{{prefix}_USER}}",
            "password": f"${{{prefix}_PASSWORD}}",
        }

    # Kafka bootstrap servers: depends on topology
    if kafka_topology == "per-server":
        kafka_postfix = f"_{server_name.upper()}"
        placeholders["kafka_bootstrap_servers"] = (
            f"${{KAFKA_BOOTSTRAP_SERVERS{kafka_postfix}}}"
        )
    else:
        placeholders["kafka_bootstrap_servers"] = "${KAFKA_BOOTSTRAP_SERVERS}"

    return placeholders


def sink_server_env_vars(
    db_type: str,
    group_name: str,
    server_name: str,
) -> dict[str, str]:
    """Generate env variable placeholders for a sink server.

    Args:
        db_type: Database type ('postgres' or 'mssql')
        group_name: Sink group name without 'sink_' prefix (e.g., 'asma')
        server_name: Server name (e.g., 'default', 'nonprod', 'prod')

    Returns:
        Dict with keys: host, port, user, password
        Values are env var placeholders like '${POSTGRES_SINK_HOST_ASMA_NONPROD}'

    Example:
        >>> sink_server_env_vars('postgres', 'asma', 'nonprod')
        {'host': '${POSTGRES_SINK_HOST_ASMA_NONPROD}', ...}
    """
    db_prefix = "POSTGRES" if db_type == "postgres" else db_type.upper()
    group_part = group_name.upper().replace("-", "_")
    server_part = server_name.upper().replace("-", "_")

    env_base = f"{db_prefix}_SINK"

    return {
        "host": f"${{{env_base}_HOST_{group_part}_{server_part}}}",
        "port": f"${{{env_base}_PORT_{group_part}_{server_part}}}",
        "user": f"${{{env_base}_USER_{group_part}_{server_part}}}",
        "password": f"${{{env_base}_PASSWORD_{group_part}_{server_part}}}",
    }


# ============================================================================
# .env File Management
# ============================================================================


def _strip_env_syntax(placeholder: str) -> str:
    """Strip ${...} wrapper from env var placeholder.

    Example:
        >>> _strip_env_syntax('${POSTGRES_SOURCE_HOST_NONPROD}')
        'POSTGRES_SOURCE_HOST_NONPROD'
    """
    return placeholder.removeprefix("${").removesuffix("}")


def _read_existing_env_vars(env_path: Path) -> set[str]:
    """Read existing variable names from .env file.

    Skips comments and empty lines. Only extracts the KEY from KEY=VALUE.
    """
    existing: set[str] = set()
    if not env_path.exists():
        return existing

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            existing.add(key)

    return existing


def append_env_vars_to_dotenv(
    placeholders: dict[str, str],
    section_label: str,
) -> int:
    """Append env variable placeholders to .env file.

    Only adds variables that don't already exist in the file.
    Creates the file if it doesn't exist.

    Args:
        placeholders: Dict of field→placeholder (e.g., {'host': '${PG_HOST}'})
        section_label: Comment header for the section
            (e.g., 'Source Server: nonprod (postgres)')

    Returns:
        Number of new variables added
    """
    env_path = get_project_root() / ".env"

    existing = _read_existing_env_vars(env_path)

    # Collect only new variables
    new_lines: list[str] = []
    for _field, placeholder in placeholders.items():
        var_name = _strip_env_syntax(placeholder)
        if var_name not in existing:
            new_lines.append(f"{var_name}=")

    if not new_lines:
        return 0

    # Build the section block
    section_block = f"\n# {section_label}\n"
    section_block += "\n".join(new_lines) + "\n"

    # Append to .env (create if needed)
    with env_path.open("a") as f:
        f.write(section_block)

    return len(new_lines)


def print_env_update_summary(
    count: int,
    placeholders: dict[str, str],
) -> None:
    """Print summary of .env file update.

    Args:
        count: Number of new variables added
        placeholders: All placeholders (for display when nothing new)
    """
    if count > 0:
        print_success(f"Added {count} env variable(s) to .env")
        for _field, placeholder in placeholders.items():
            var_name = _strip_env_syntax(placeholder)
            print_info(f"  {var_name}=")
    else:
        print_warning("All env variables already exist in .env")


def remove_env_vars_from_dotenv(
    placeholders: dict[str, str],
) -> int:
    """Remove env variables and their section comment from .env file.

    Removes lines matching the variable names and any preceding comment
    line that becomes orphaned (no variables left after it).

    Args:
        placeholders: Dict of field->placeholder (e.g., {'host': '${PG_HOST}'})

    Returns:
        Number of variables removed
    """
    env_path = get_project_root() / ".env"
    if not env_path.exists():
        return 0

    var_names = {
        _strip_env_syntax(p) for p in placeholders.values()
    }

    lines = env_path.read_text().splitlines()
    filtered: list[str] = []
    removed = 0

    for line in lines:
        stripped = line.strip()
        # Check if this line is a variable assignment we want to remove
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in var_names:
                removed += 1
                continue
        filtered.append(line)

    if removed == 0:
        return 0

    # Clean up orphaned section comments (comment followed by blank/EOF)
    cleaned: list[str] = []
    for i, line in enumerate(filtered):
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("# ="):
            # Check if next non-empty line is another comment or end of file
            next_content = ""
            for j in range(i + 1, len(filtered)):
                if filtered[j].strip():
                    next_content = filtered[j].strip()
                    break
            # Keep if next content is a variable, drop if it's another
            # section comment, empty, or EOF
            if next_content and not next_content.startswith("#"):
                cleaned.append(line)
                continue
            # Check if this looks like a section label (e.g. "# Sink Server:")
            if "Server:" in stripped or "server:" in stripped:
                continue  # Drop orphaned section header
        cleaned.append(line)

    # Remove trailing blank lines
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()

    env_path.write_text("\n".join(cleaned) + "\n")
    return removed


def print_env_removal_summary(
    count: int,
    placeholders: dict[str, str],
) -> None:
    """Print summary of .env variable removal.

    Args:
        count: Number of variables removed
        placeholders: All placeholders (for display)
    """
    if count > 0:
        print_success(f"Removed {count} env variable(s) from .env")
        for _field, placeholder in placeholders.items():
            var_name = _strip_env_syntax(placeholder)
            print_info(f"  {var_name}")
    else:
        print_info("No matching env variables found in .env")
