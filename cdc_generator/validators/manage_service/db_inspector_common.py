"""Common database inspection utilities for CDC pipeline."""

import os
import re
from typing import Any, cast

from cdc_generator.helpers.helpers_logging import (
    print_error,
    print_info,
    print_warning,
)
from cdc_generator.helpers.service_config import (
    get_project_root,
    load_service_config,
)
from cdc_generator.validators.manage_server_group.config import (
    get_server_group_for_service,
    load_server_groups,
)

_MISSING_ENV_VARS: set[str] = set()
_ENV_WARNING_STATE = {"printed": False}


def expand_env_vars(value: str | int | None) -> str | int | None:
    """Expand ${VAR} and $VAR patterns in environment variables.

    Args:
        value: Value to expand (can be string or other type)

    Returns:
        Expanded value if string, original value otherwise
    """
    if not isinstance(value, str):
        return value

    # Replace ${VAR} with $VAR for os.path.expandvars
    value = value.replace('${', '$').replace('}', '')
    expanded = os.path.expandvars(value)

    # Check if expansion actually happened (variable was set)
    if expanded == value and "$" in value:
        # Variable wasn't expanded - it's not in the environment
        var_name = value.replace("$", "").split("/")[0].split(":")[0]
        _MISSING_ENV_VARS.add(var_name)

    return expanded


def _print_env_var_warnings_once() -> None:
    """Print missing/available env vars once per run.

    Aggregates all missing env vars detected by expand_env_vars() and prints
    a single warning block with available database-related env vars.
    """
    if _ENV_WARNING_STATE["printed"] or not _MISSING_ENV_VARS:
        return

    _ENV_WARNING_STATE["printed"] = True
    missing = ", ".join(sorted(_MISSING_ENV_VARS))
    print_warning(
        "⚠️ Environment variables not set: " + missing
    )
    print_warning(
        "   - Using literal values from config"
    )
    print_warning(
        "   - 💡 In Docker, ensure variables are in .env and restart the "
        + "container."
    )

    keywords = [
        "MSSQL",
        "POSTGRES",
        "DB",
        "DATABASE",
        "HOST",
        "PORT",
        "USER",
        "PASSWORD",
    ]
    relevant_vars = {
        k: v
        for k, v in os.environ.items()
        if any(keyword in k.upper() for keyword in keywords)
    }

    if relevant_vars:
        print_warning("\n   Available database-related environment variables:")
        for k, v in sorted(relevant_vars.items()):
            display_value = (
                "***"
                if "PASSWORD" in k.upper() or "PASS" in k.upper()
                else v
            )
            print_warning(f"     - {k}={display_value}")
    else:
        print_warning(
            "\n   No database-related environment variables found in the container."
        )


def get_service_db_config(
    service: str,
    env: str = "nonprod",
) -> dict[str, Any] | None:
    """Get database connection configuration for a service.

    Args:
        service: Service name
        env: Environment name (default: nonprod)

    Returns:
        Dictionary with connection config or None if not found
    """
    try:
        config = load_service_config(service)

        # Get server group configuration using typed loaders
        server_groups_data = load_server_groups()
        server_group_name = get_server_group_for_service(service, server_groups_data)

        if not server_group_name:
            print_error(f"Could not find server group for service '{service}'")
            return None

        server_group = server_groups_data[server_group_name]
        db_type = server_group.get("type", "postgres")  # 'mssql' or 'postgres'

        # Get validation database from service config
        source_raw = config.get("source")
        source = (
            cast(dict[str, Any], source_raw)
            if isinstance(source_raw, dict)
            else {}
        )
        validation_database = source.get("validation_database")
        if not validation_database:
            print_error(
                "No validation_database found in service config for "
                + f"'{service}'"
            )
            return None

        # Find the environment with this database in source-groups.yaml sources
        sources = server_group.get("sources", {})
        service_sources = sources.get(service, {})

        # Find which environment has this database
        env_config_found: dict[str, Any] | None = None
        for env_name, env_data in service_sources.items():
            if env_name == "schemas":
                continue
            env_data_dict = cast(dict[str, Any], env_data)
            if env_data_dict.get("database") == validation_database:
                # Get server configuration
                server_name = str(env_data_dict.get("server", "default"))
                servers = server_group.get("servers", {})
                server_config = servers.get(server_name, {})

                # Build connection config based on database type
                if db_type == "postgres":
                    host_val = expand_env_vars(server_config.get("host"))
                    port_val = expand_env_vars(server_config.get("port"))
                    user_raw = server_config.get("user") or server_config.get(
                        "username",
                    )
                    user_val = expand_env_vars(user_raw)
                    password_val = expand_env_vars(
                        server_config.get("password"),
                    )
                    env_config_found = {
                        "postgres": {
                            "host": str(host_val or "localhost"),
                            "port": int(port_val or 5432),
                            "user": str(user_val or "postgres"),
                            "password": str(password_val or ""),
                            "database": validation_database,
                        },
                        "database_name": validation_database,
                    }
                else:  # mssql
                    host_val = expand_env_vars(server_config.get("host"))
                    port_val = expand_env_vars(server_config.get("port"))
                    user_raw = server_config.get("user") or server_config.get(
                        "username",
                    )
                    user_val = expand_env_vars(user_raw)
                    password_val = expand_env_vars(
                        server_config.get("password"),
                    )
                    env_config_found = {
                        "mssql": {
                            "host": str(host_val or "localhost"),
                            "port": int(port_val or 1433),
                            "user": str(user_val or "sa"),
                            "password": str(password_val or ""),
                        },
                        "database_name": validation_database,
                    }
                break

        if not env_config_found:
            print_error(
                "Could not find environment config for database "
                + f"'{validation_database}' in source-groups.yaml "
                + f"(env: {env})"
            )
            return None

        result = {
            "env_config": env_config_found,
            "config": config,
        }
        _print_env_var_warnings_once()
        return result

    except Exception as e:
        print_error(f"Failed to load service config: {e}")
        return None



def get_connection_params(
    db_config: dict[str, Any],
    db_type: str,
) -> dict[str, Any] | None:
    """Extract connection parameters from database config.

    Args:
        db_config: Database configuration from get_service_db_config()
        db_type: Database type ('mssql' or 'postgres')

    Returns:
        Dictionary with connection parameters or None if not found
    """
    env_config = db_config.get("env_config", {})

    if db_type == "mssql":
        mssql = env_config.get("mssql", {})
        database = env_config.get("database_name")

        port_val = expand_env_vars(mssql.get("port", "1433"))
        return {
            "host": str(
                expand_env_vars(mssql.get("host", "localhost"))
                or "localhost"
            ),
            "port": (
                int(port_val)
                if port_val and str(port_val).isdigit()
                else 1433
            ),
            "user": str(
                expand_env_vars(mssql.get("user", "sa"))
                or "sa"
            ),
            "password": str(
                expand_env_vars(mssql.get("password", ""))
                or ""
            ),
            "database": database,
        }

    if db_type == "postgres":
        postgres = env_config.get("postgres", {})
        database = (
            postgres.get("name")
            or postgres.get("database")
            or env_config.get("database_name")
        )

        # Try to get explicit connection params first
        host = postgres.get("host")
        port = postgres.get("port")
        user = postgres.get("user")
        password = postgres.get("password")

        # If host not explicit, try to parse from URL
        if not host and "url" in postgres:
            url = expand_env_vars(postgres["url"])
            url_str = str(url) if url else ""
            # Parse postgresql://user:password@host:port/database
            match = re.match(
                r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+?)(\?|$)",
                url_str,
            )
            if match:
                user = user if user else str(match.group(1))
                password = password if password else str(match.group(2))
                host = str(match.group(3))
                port = int(match.group(4))
                database = database if database else str(match.group(5))

        # Expand environment variables
        host_val = expand_env_vars(host) if host else None
        host = str(host_val or "localhost")
        port_val = expand_env_vars(port) if port else None
        port = int(port_val) if port_val and str(port_val).isdigit() else 5432
        user_val = expand_env_vars(user) if user else None
        user = str(user_val or "postgres")
        password_val = expand_env_vars(password) if password else None
        password = str(password_val or "")

        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    print_error(f"Unsupported database type: {db_type}")
    return None


# ---------------------------------------------------------------------------
# Sink database inspection helpers
# ---------------------------------------------------------------------------

_SINK_KEY_PARTS = 2


def get_available_sinks(service: str) -> list[str]:
    """Get list of available sink keys from a service config.

    Args:
        service: Service name

    Returns:
        List of sink key strings (e.g., ['sink_asma.calendar'])
    """
    try:
        config = load_service_config(service)
    except FileNotFoundError:
        return []

    sinks_raw = config.get("sinks")
    if not isinstance(sinks_raw, dict):
        return []

    sinks = cast(dict[str, object], sinks_raw)
    return [str(k) for k in sinks]


def _parse_sink_key(sink_key: str) -> tuple[str, str] | None:
    """Parse 'sink_group.target_service' → (sink_group, target_service).

    Returns None if the format is invalid.
    """
    parts = sink_key.split(".", 1)
    if len(parts) != _SINK_KEY_PARTS:
        return None
    return parts[0], parts[1]


def _load_sink_groups() -> dict[str, Any]:
    """Load sink-groups.yaml from project root.

    Returns:
        Dictionary of sink group configurations, or empty dict.
    """
    sink_file = get_project_root() / "sink-groups.yaml"
    if not sink_file.exists():
        print_error("sink-groups.yaml not found")
        return {}

    from cdc_generator.helpers.yaml_loader import load_yaml_file

    return cast(dict[str, Any], load_yaml_file(sink_file))


def _resolve_sink_server_env(
    sink_group_config: dict[str, Any],
    env: str,
) -> dict[str, Any] | None:
    """Find the best matching server for the given environment.

    Tries exact env match first, then falls back to common names.

    Args:
        sink_group_config: Sink group configuration from sink-groups.yaml
        env: Environment name (e.g., 'nonprod', 'prod')

    Returns:
        Server configuration dict, or None if not found
    """
    servers = sink_group_config.get("servers", {})
    if not servers:
        return None

    # Exact match first
    if env in servers:
        return cast(dict[str, Any], servers[env])

    # Map common aliases
    env_aliases: dict[str, list[str]] = {
        "nonprod": ["default", "dev", "stage", "test"],
        "prod": ["default"],
    }
    for alias in env_aliases.get(env, []):
        if alias in servers:
            return cast(dict[str, Any], servers[alias])

    # Fallback: first server
    first_key = next(iter(servers))
    print_info(
        f"No server for env '{env}', using '{first_key}'"
    )
    return cast(dict[str, Any], servers[first_key])


def _resolve_sink_database_name(
    sink_group_config: dict[str, Any],
    target_service: str,
    env: str,
) -> str | None:
    """Resolve the database name for a sink target service.

    Looks up source-groups.yaml sources under the sink's source_group
    to find the database for the target service and environment.

    Args:
        sink_group_config: Sink group configuration
        target_service: Target service name from sink key
        env: Environment name

    Returns:
        Database name, or None if not found
    """
    source_group_name = sink_group_config.get("source_group")
    if not source_group_name:
        return None

    try:
        source_groups = load_server_groups()
    except (FileNotFoundError, ValueError):
        return None

    source_group = source_groups.get(source_group_name)
    if not source_group:
        print_error(
            f"Source group '{source_group_name}' not found in source-groups.yaml"
        )
        return None

    sources = source_group.get("sources", {})
    service_source = sources.get(target_service)
    if not service_source:
        print_error(
            f"Service '{target_service}' not found in "
            + f"source group '{source_group_name}' sources"
        )
        return None

    # Try exact env match, then fallback aliases
    env_aliases: dict[str, list[str]] = {
        "nonprod": ["dev", "stage", "test"],
    }
    envs_to_try = [env, *env_aliases.get(env, [])]

    for env_name in envs_to_try:
        env_data = service_source.get(env_name)
        if isinstance(env_data, dict):
            env_dict = cast(dict[str, Any], env_data)
            db = env_dict.get("database")
            if db:
                return str(db)

    print_error(
        f"No database found for service '{target_service}' in env '{env}'"
    )
    return None


def _resolve_source_ref_server(
    source_ref: str,
) -> dict[str, Any] | None:
    """Resolve a source_ref to concrete server config.

    Args:
        source_ref: Reference in format '<group>/<server>'

    Returns:
        Server configuration dict, or None if not found
    """
    parts = source_ref.split("/")
    if len(parts) != _SINK_KEY_PARTS:
        print_error(f"Invalid source_ref format: '{source_ref}'")
        return None

    group_name, server_name = parts
    try:
        source_groups = load_server_groups()
    except (FileNotFoundError, ValueError):
        return None

    group = source_groups.get(group_name)
    if not group:
        print_error(f"Source group '{group_name}' not found")
        return None

    servers = group.get("servers", {})
    server_config = servers.get(server_name)
    if not server_config:
        print_error(
            f"Server '{server_name}' not found "
            + f"in source group '{group_name}'"
        )
        return None

    return cast(dict[str, Any], dict(server_config))


def get_sink_db_config(
    service: str,
    sink_key: str,
    env: str = "nonprod",
) -> dict[str, Any] | None:
    """Get database connection configuration for a sink.

    Resolves connection from sink-groups.yaml servers and database name
    from source-groups.yaml sources (via the sink's source_group).

    Args:
        service: Service name (to validate sink key exists)
        sink_key: Sink key (e.g., 'sink_asma.calendar')
        env: Environment name (default: nonprod)

    Returns:
        Dictionary with 'env_config', 'db_type', and 'schemas'
        or None if not found
    """
    resolved = _resolve_sink_group_and_key(
        service, sink_key,
    )
    if not resolved:
        return None

    sink_group_name, target_service, sink_group = resolved
    db_type = str(sink_group.get("type", "postgres"))

    result = _build_sink_connection(
        sink_group_name, target_service,
        sink_group, db_type, env,
    )
    _print_env_var_warnings_once()
    return result


def _resolve_sink_group_and_key(
    service: str,
    sink_key: str,
) -> tuple[str, str, dict[str, Any]] | None:
    """Validate and resolve sink key to group config.

    Args:
        service: Service name
        sink_key: Sink key (e.g., 'sink_asma.calendar')

    Returns:
        (sink_group_name, target_service, sink_group) or None
    """
    # Validate sink key exists in service config
    available = get_available_sinks(service)
    if sink_key not in available:
        print_error(
            f"Sink '{sink_key}' not found in service '{service}'"
        )
        if available:
            print_info(
                "Available sinks: "
                + ", ".join(available)
            )
        return None

    # Parse sink key
    parsed = _parse_sink_key(sink_key)
    if not parsed:
        print_error(
            f"Invalid sink key '{sink_key}'. "
            + "Expected format: sink_group.target_service"
        )
        return None

    sink_group_name, target_service = parsed

    # Load sink groups
    sink_groups = _load_sink_groups()
    if not sink_groups:
        return None

    sink_group = sink_groups.get(sink_group_name)
    if not sink_group:
        print_error(
            f"Sink group '{sink_group_name}' not found in sink-groups.yaml"
        )
        return None

    return sink_group_name, target_service, sink_group


def _build_sink_connection(
    sink_group_name: str,
    target_service: str,
    sink_group: dict[str, Any],
    db_type: str,
    env: str,
) -> dict[str, Any] | None:
    """Build sink connection config from resolved components.

    Args:
        sink_group_name: Sink group name
        target_service: Target service name
        sink_group: Sink group configuration
        db_type: Database type
        env: Environment name

    Returns:
        Connection config dict or None on error
    """
    # Resolve server connection
    server_config = _resolve_sink_server_env(sink_group, env)
    if not server_config:
        print_error(
            "No server found in sink group "
            + f"'{sink_group_name}' for env '{env}'"
        )
        return None

    # Handle source_ref servers (inherited from source group)
    if "source_ref" in server_config:
        resolved = _resolve_source_ref_server(
            str(server_config["source_ref"]),
        )
        if not resolved:
            return None
        # Apply overrides from sink config
        for key, value in server_config.items():
            if key != "source_ref":
                resolved[key] = value
        server_config = resolved

    # Resolve database name
    database = _resolve_sink_database_name(
        sink_group, target_service, env,
    )
    if not database:
        return None

    # Resolve schemas from source-groups.yaml
    schemas = _resolve_sink_schemas(
        sink_group, target_service,
    )

    # Build connection config
    host = str(
        expand_env_vars(server_config.get("host"))
        or "localhost"
    )
    port_val = expand_env_vars(
        server_config.get("port"),
    )
    port = (
        int(port_val)
        if port_val and str(port_val).isdigit()
        else 5432
        if db_type == "postgres"
        else 1433
    )
    user_val = expand_env_vars(
        server_config.get("user")
        or server_config.get("username"),
    )
    user = str(user_val or "postgres")
    password_val = expand_env_vars(
        server_config.get("password"),
    )
    password = str(password_val or "")

    env_config: dict[str, Any] = {
        "database_name": database,
    }
    conn_key = db_type if db_type == "mssql" else "postgres"
    env_config[conn_key] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }

    return {
        "env_config": env_config,
        "db_type": db_type,
        "schemas": schemas,
    }


def _resolve_sink_schemas(
    sink_group_config: dict[str, Any],
    target_service: str,
) -> list[str]:
    """Resolve allowed schemas for a sink target service.

    Looks up schemas from source-groups.yaml sources.

    Args:
        sink_group_config: Sink group configuration
        target_service: Target service name

    Returns:
        List of allowed schema names
    """
    source_group_name = sink_group_config.get("source_group")
    if not source_group_name:
        return []

    try:
        source_groups = load_server_groups()
    except (FileNotFoundError, ValueError):
        return []

    source_group = source_groups.get(source_group_name)
    if not source_group:
        return []

    sources = source_group.get("sources", {})
    service_source = sources.get(target_service, {})
    return list(service_source.get("schemas", []))
