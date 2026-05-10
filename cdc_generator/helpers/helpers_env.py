"""Shared helpers for environment variable generation and project env file management.

Used by both source-group and sink-group CLI handlers when adding servers.
Generates consistent env variable placeholder names and keeps .env and
.env.example aligned.
"""

import os
from pathlib import Path

from cdc_generator.helpers.helpers_logging import (
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.service_config import get_project_root

# ============================================================================
# .env Loading (startup)
# ============================================================================

_MIN_QUOTED_VALUE_LENGTH = 2


def _strip_env_value(value: str) -> str:
    """Strip surrounding single or double quotes from an env value."""
    if (
        len(value) >= _MIN_QUOTED_VALUE_LENGTH
        and value[0] == value[-1]
        and value[0] in {'"', "'"}
    ):
        return value[1:-1]
    return value


def load_project_dotenv() -> int:
    """Load .env from the project root into os.environ.

    Scans upwards from the current working directory (via get_project_root)
    and reads ``.env``.  Existing environment variables are never overwritten
    (``os.environ.setdefault`` semantics).  Comments, blank lines, and lines
    without ``=`` are skipped.  Surrounding single/double quotes are stripped.

    Called once at CLI startup so that every sub-command sees the same
    resolved environment.

    Returns:
        Number of new variables loaded into os.environ.
    """
    project_root = get_project_root()
    env_path = project_root / ".env"
    if not env_path.exists():
        return 0

    loaded = 0
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if key not in os.environ:
            os.environ[key] = _strip_env_value(value.strip())
            loaded += 1

    return loaded


# ============================================================================
# Env Variable Name Generation
# ============================================================================


def source_server_env_vars(
    db_type: str,
    server_name: str,
    broker_topology: str | None = "shared",
) -> dict[str, str]:
    """Generate env variable placeholders for a source server.

    Args:
        db_type: Database type ('postgres' or 'mssql')
        server_name: Server name (e.g., 'default', 'nonprod', 'prod')
        broker_topology: Broker fan-out mode when topology is redpanda

    Returns:
        Dict with keys: host, port, user, password and, when applicable,
        kafka_bootstrap_servers
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
    if broker_topology == "per-server":
        kafka_postfix = f"_{server_name.upper()}"
        placeholders["kafka_bootstrap_servers"] = (
            f"${{KAFKA_BOOTSTRAP_SERVERS{kafka_postfix}}}"
        )
    elif broker_topology is not None:
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

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            existing.add(key)

    return existing


def _managed_env_paths() -> tuple[Path, Path]:
    """Return the project env files kept in sync by CLI mutations."""
    project_root = get_project_root()
    return project_root / ".env", project_root / ".env.example"


def _append_env_vars_to_file(
    env_path: Path,
    placeholders: dict[str, str],
    section_label: str,
) -> set[str]:
    """Append missing env variables to a single env-like file."""
    existing = _read_existing_env_vars(env_path)

    added_vars: list[str] = []
    for placeholder in placeholders.values():
        var_name = _strip_env_syntax(placeholder)
        if var_name not in existing:
            added_vars.append(var_name)

    if not added_vars:
        return set()

    section_block = f"\n# {section_label}\n"
    section_block += "\n".join(f"{var_name}=" for var_name in added_vars) + "\n"

    with env_path.open("a", encoding="utf-8") as file_handle:
        file_handle.write(section_block)

    return set(added_vars)


def _remove_env_vars_from_file(
    env_path: Path,
    var_names: set[str],
) -> set[str]:
    """Remove variables and orphaned section headers from a single env-like file."""
    if not env_path.exists():
        return set()

    lines = env_path.read_text(encoding="utf-8").splitlines()
    filtered: list[str] = []
    removed_vars: set[str] = set()

    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in var_names:
                removed_vars.add(key)
                continue
        filtered.append(line)

    if not removed_vars:
        return set()

    cleaned: list[str] = []
    for index, line in enumerate(filtered):
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("# ="):
            next_content = ""
            for next_index in range(index + 1, len(filtered)):
                if filtered[next_index].strip():
                    next_content = filtered[next_index].strip()
                    break
            if next_content and not next_content.startswith("#"):
                cleaned.append(line)
                continue
            if "Server:" in stripped or "server:" in stripped:
                continue
        cleaned.append(line)

    while cleaned and not cleaned[-1].strip():
        cleaned.pop()

    env_path.write_text("\n".join(cleaned) + "\n", encoding="utf-8")
    return removed_vars


def append_env_vars_to_dotenv(
    placeholders: dict[str, str],
    section_label: str,
) -> int:
    """Append env variable placeholders to project env files.

    Only adds variables that don't already exist in each file.
    Creates the file if it doesn't exist.

    Args:
        placeholders: Dict of field→placeholder (e.g., {'host': '${PG_HOST}'})
        section_label: Comment header for the section
            (e.g., 'Source Server: nonprod (postgres)')

    Returns:
        Number of unique variables added across managed env files
    """
    added_vars: set[str] = set()
    for env_path in _managed_env_paths():
        added_vars.update(
            _append_env_vars_to_file(env_path, placeholders, section_label),
        )

    return len(added_vars)


def print_env_update_summary(
    count: int,
    placeholders: dict[str, str],
) -> None:
    """Print summary of project env file updates.

    Args:
        count: Number of new variables added
        placeholders: All placeholders (for display when nothing new)
    """
    if count > 0:
        print_success(
            f"Added {count} env variable(s) to .env / .env.example",
        )
        for _field, placeholder in placeholders.items():
            var_name = _strip_env_syntax(placeholder)
            print_info(f"  {var_name}=")
    else:
        print_warning("All env variables already exist in .env / .env.example")


def remove_env_vars_from_dotenv(
    placeholders: dict[str, str],
) -> int:
    """Remove env variables and orphaned section comments from project env files.

    Removes lines matching the variable names and any preceding comment
    line that becomes orphaned (no variables left after it).

    Args:
        placeholders: Dict of field->placeholder (e.g., {'host': '${PG_HOST}'})

    Returns:
        Number of unique variables removed across managed env files
    """
    var_names = {
        _strip_env_syntax(p) for p in placeholders.values()
    }

    removed_vars: set[str] = set()
    for env_path in _managed_env_paths():
        removed_vars.update(_remove_env_vars_from_file(env_path, var_names))

    return len(removed_vars)


def print_env_removal_summary(
    count: int,
    placeholders: dict[str, str],
) -> None:
    """Print summary of project env variable removal.

    Args:
        count: Number of variables removed
        placeholders: All placeholders (for display)
    """
    if count > 0:
        print_success(
            f"Removed {count} env variable(s) from .env / .env.example",
        )
        for _field, placeholder in placeholders.items():
            var_name = _strip_env_syntax(placeholder)
            print_info(f"  {var_name}")
    else:
        print_info("No matching env variables found in .env / .env.example")
