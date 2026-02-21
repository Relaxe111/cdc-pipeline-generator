#!/usr/bin/env python3
"""
Shared module for loading service and customer configurations.
Supports both new service-based format (services/) and legacy format (2-customers/).
"""

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.yaml_loader import yaml


def get_project_root() -> Path:
    """Get the project root directory of the implementation.

    Searches upwards from the current working directory for a directory containing
    known markers: 'source-groups.yaml', 'services/', or '2-customers/'.
    This allows the tool to work correctly from any subdirectory within an implementation repo.

    Returns:
        Path to implementation root directory

    Note:
        As a fallback, returns current directory instead of raising error,
        so new files are created where the command is executed.
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        server_group = parent / "source-groups.yaml"
        services_dir = parent / "services"
        customers_dir = parent / "2-customers"
        if server_group.exists() or services_dir.is_dir() or customers_dir.is_dir():
            return parent

    # Fallback to current directory (allows new implementation scaffolding)
    return current


def load_service_config(service_name: str = "adopus") -> dict[str, object]:
    """Load service configuration from services/, preserving comments.

    Supports both formats:
    - New: {service_name: {source: {...}, customers: [...]}}
    - Legacy: {service: service_name, source: {...}, customers: [...]}

    Always returns legacy format for backward compatibility.
    """
    services_dir = get_project_root() / "services"
    service_path = services_dir / f"{service_name}.yaml"
    if not service_path.exists():
        raise FileNotFoundError(f"Service config not found: {service_path}")
    with service_path.open() as f:
        raw_config = yaml.load(f)

    # Check if new format (service name as root key)
    if isinstance(raw_config, dict) and service_name in raw_config:
        # New format: extract service config and add 'service' field for backward compatibility
        raw_config_dict = cast(dict[str, Any], raw_config)
        service_config = raw_config_dict[service_name]
        if isinstance(service_config, dict):
            config = cast(dict[str, object], dict(cast(dict[str, Any], service_config)))
            config['service'] = service_name
            return _normalize_loaded_service_config(config)

    # Legacy format: already has 'service' field
    if isinstance(raw_config, dict):
        return _normalize_loaded_service_config(cast(dict[str, object], dict(cast(dict[str, Any], raw_config))))
    return raw_config  # type: ignore[return-value]


def _normalize_loaded_service_config(config: dict[str, object]) -> dict[str, object]:
    """Normalize service config for runtime usage.

    For db-per-tenant services, customers are derived from source-groups sources
    and no longer depend on service-level ``customers`` YAML blocks.
    """
    server_group_name_raw = config.get('server_group')
    server_group_name = str(server_group_name_raw).strip()
    if not server_group_name:
        return config

    source_groups = _load_source_groups_file()
    source_groups_dict = cast(dict[str, Any], source_groups)
    server_group_raw = source_groups_dict.get(server_group_name)
    if not isinstance(server_group_raw, dict):
        return config

    server_group = cast(dict[str, Any], server_group_raw)
    pattern = _resolve_server_group_pattern(server_group)
    if pattern != 'db-per-tenant':
        return config

    derived_customers = _derive_customers_from_source_group(server_group)
    config['customers'] = derived_customers
    return config


def _load_source_groups_file() -> dict[str, object]:
    """Load source-groups.yaml as a dictionary."""
    source_groups_path = get_project_root() / "source-groups.yaml"
    if not source_groups_path.exists():
        return {}

    with source_groups_path.open() as source_groups_file:
        raw = yaml.load(source_groups_file)

    if isinstance(raw, dict):
        return dict(raw)
    return {}


def _resolve_server_group_pattern(server_group: dict[str, Any]) -> str:
    """Resolve normalized server group pattern from supported keys."""
    raw_pattern = server_group.get('server_group_type', server_group.get('pattern', ''))
    return str(raw_pattern).strip().lower()


def _derive_customers_from_source_group(
    server_group: dict[str, Any],
) -> list[dict[str, object]]:
    """Build customers list from source-groups sources map."""
    sources_raw = server_group.get('sources', {})
    if not isinstance(sources_raw, dict):
        return []

    sources = cast(dict[str, Any], sources_raw)
    customers: list[dict[str, object]] = []
    for source_name_raw, source_entry_raw in sources.items():
        source_name = str(source_name_raw).strip()
        if not source_name or not isinstance(source_entry_raw, dict):
            continue

        customer_name = source_name.casefold()
        source_entry = cast(dict[str, Any], source_entry_raw)
        customer_id = _resolve_customer_id_from_source_entry(source_entry)

        customers.append(
            {
                'name': customer_name,
                'schema': customer_name,
                'customer_id': customer_id,
            }
        )

    customers.sort(key=lambda customer: str(customer.get('name', '')))
    return customers


def _resolve_customer_id_from_source_entry(source_entry: dict[str, Any]) -> object | None:
    """Extract customer_id from source entry.

    Prefers environment-level ``customer_id`` values, then top-level fallback.
    """
    top_level_customer_id = source_entry.get('customer_id')

    for env_name, env_cfg in source_entry.items():
        if env_name == 'schemas' or not isinstance(env_cfg, dict):
            continue
        env_cfg_dict = cast(dict[str, Any], env_cfg)
        env_customer_id = env_cfg_dict.get('customer_id')
        if env_customer_id is not None:
            return env_customer_id

    return top_level_customer_id


def merge_customer_config(service_config: dict[str, object], customer_name: str) -> dict[str, object]:
    """Merge shared service config with customer-specific overrides.

    Returns a dict compatible with old customer YAML format for backward compatibility.
    """
    normalized_service_config = _normalize_loaded_service_config(dict(service_config))

    # Find customer in service config
    customers_raw = normalized_service_config.get('customers', [])
    if not isinstance(customers_raw, list):
        raise ValueError(f"Customer '{customer_name}' not found in service config")
    customers = cast(list[object], customers_raw)
    customer_data: dict[str, object] | None = None
    normalized_customer_name = customer_name.casefold()
    for customer_entry in customers:
        if not isinstance(customer_entry, dict):
            continue
        customer_entry_dict = cast(dict[str, Any], customer_entry)
        candidate_name_raw = customer_entry_dict.get('name')
        candidate_name = str(candidate_name_raw).casefold() if candidate_name_raw is not None else ''
        if candidate_name == normalized_customer_name:
            customer_data = cast(dict[str, object], customer_entry_dict)
            break

    if not customer_data:
        raise ValueError(f"Customer '{customer_name}' not found in service config")

    # Flatten hierarchical source_tables structure to old format for backward compatibility
    # New format: [{schema: "dbo", tables: [{name: "Actor", primary_key: "actno"}]}]
    # Or simplified: [{schema: "dbo", tables: ["Actor", "Fraver"]}]  (when no extra properties)
    # Old format: [{schema: "dbo", table: "Actor", primary_key: "actno"}]
    source_tables_hierarchical = normalized_service_config.get('shared', {}).get('source_tables', [])  # type: ignore[attr-defined]
    ignore_tables = normalized_service_config.get('shared', {}).get('ignore_tables', [])  # type: ignore[attr-defined]

    source_tables_flat = []
    for schema_group in source_tables_hierarchical:  # type: ignore[attr-defined]
        schema_name = schema_group.get('schema')  # type: ignore[attr-defined]
        for table in schema_group.get('tables', []):  # type: ignore[attr-defined]
            # Handle both string format ("Actor") and object format ({name: "Actor", ...})
            if isinstance(table, str):
                table_name = table
                table_dict = {'name': table}
            else:
                table_name = table.get('name')  # type: ignore[attr-defined]
                table_dict = table  # type: ignore[assignment]

            # Check if table should be ignored (ignore_tables has priority)
            should_ignore = False
            for ignore_entry in ignore_tables:  # type: ignore[attr-defined]
                if isinstance(ignore_entry, str):
                    # Simple format: just table name (assumes dbo schema)
                    if table_name == ignore_entry and schema_name == 'dbo':
                        should_ignore = True
                        break
                elif (
                    isinstance(ignore_entry, dict)
                    and cast(dict[str, Any], ignore_entry).get('table') == table_name
                    and cast(dict[str, Any], ignore_entry).get('schema', 'dbo') == schema_name
                ):
                    should_ignore = True
                    break

            if not should_ignore:
                table_config = {  # type: ignore[var-annotated]
                    'schema': schema_name,
                    'table': table_name,
                    'primary_key': table_dict.get('primary_key')  # type: ignore[attr-defined]
                }

                # Handle column filtering (ignore_columns has priority over include_columns)
                ignore_cols = table_dict.get('ignore_columns')  # type: ignore[attr-defined]
                include_cols = table_dict.get('include_columns')  # type: ignore[attr-defined]

                if ignore_cols:
                    table_config['ignore_columns'] = ignore_cols  # type: ignore[index]
                elif include_cols:
                    table_config['include_columns'] = include_cols  # type: ignore[index]

                source_tables_flat.append(table_config)  # type: ignore[arg-type]

    # Start with backward-compatible structure
    merged = {  # type: ignore[var-annotated]
        'customer': customer_name,
        'schema': customer_data.get('schema', normalized_customer_name),
        'customer_id': customer_data.get('customer_id'),
        'cdc_tables': source_tables_flat,
        'environments': {}
    }

    derived_environments = _derive_customer_environments_from_source_groups(
        normalized_service_config,
        customer_name,
    )
    merged['environments'] = derived_environments

    return merged  # type: ignore[return-value]


def _derive_customer_environments_from_source_groups(
    service_config: dict[str, object],
    customer_name: str,
) -> dict[str, dict[str, object]]:
    """Build customer environments from source-groups.yaml.

    This is the canonical source for db-per-tenant environment/database/server mapping.
    """
    server_group_name_raw = service_config.get('server_group')
    server_group_name = str(server_group_name_raw).strip()
    if not server_group_name:
        return {}

    source_groups_path = get_project_root() / "source-groups.yaml"
    if not source_groups_path.exists():
        return {}

    with source_groups_path.open() as source_groups_file:
        source_groups = yaml.load(source_groups_file)

    if not isinstance(source_groups, dict):
        return {}

    source_groups_dict = cast(dict[str, Any], source_groups)
    server_group = source_groups_dict.get(server_group_name)
    if not isinstance(server_group, dict):
        return {}

    server_group_dict = cast(dict[str, Any], server_group)
    sources_raw = server_group_dict.get('sources', {})
    if not isinstance(sources_raw, dict):
        return {}

    source_entry = _find_source_entry_for_customer(cast(dict[str, Any], sources_raw), customer_name)
    if source_entry is None:
        return {}

    servers_raw = server_group_dict.get('servers', {})
    servers: dict[str, object] = {}
    if isinstance(servers_raw, dict):
        servers = cast(dict[str, object], dict(cast(dict[str, Any], servers_raw)))

    environments: dict[str, dict[str, object]] = {}
    for env_name_raw, env_cfg_raw in source_entry.items():
        env_name = str(env_name_raw).strip()
        if not env_name or env_name == 'schemas' or not isinstance(env_cfg_raw, dict):
            continue

        env_cfg = cast(dict[str, Any], env_cfg_raw)
        database_name_raw = env_cfg.get('database')
        database_name = str(database_name_raw).strip() if database_name_raw is not None else ''
        server_name_raw = env_cfg.get('server', 'default')
        server_name = str(server_name_raw).strip() or 'default'

        env_data: dict[str, object] = {
            'existing_mssql': True,
            'database': {'name': database_name},
            'topic_prefix': f"{env_name}.{customer_name}.{database_name}",
        }

        server_cfg_raw = servers.get(server_name)
        if isinstance(server_cfg_raw, dict):
            server_cfg = cast(dict[str, Any], server_cfg_raw)
            mssql_config = {
                'host': server_cfg.get('host', ''),
                'port': server_cfg.get('port', ''),
                'user': server_cfg.get('user', ''),
                'password': server_cfg.get('password', ''),
            }
            env_data['mssql'] = mssql_config

            kafka_bootstrap = server_cfg.get('kafka_bootstrap_servers')
            if kafka_bootstrap is not None:
                env_data['kafka'] = {
                    'bootstrap_servers': kafka_bootstrap,
                }

        environments[env_name] = env_data

    return environments


def _find_source_entry_for_customer(
    sources: dict[str, Any],
    customer_name: str,
) -> dict[str, object] | None:
    """Find source entry by exact/case-insensitive customer key."""
    if customer_name in sources and isinstance(sources[customer_name], dict):
        return cast(dict[str, object], dict(cast(dict[str, Any], sources[customer_name])))

    normalized_name = customer_name.casefold()
    for source_name_raw, source_cfg_raw in sources.items():
        source_name = str(source_name_raw)
        if source_name.casefold() == normalized_name and isinstance(source_cfg_raw, dict):
            return cast(dict[str, object], dict(cast(dict[str, Any], source_cfg_raw)))

    return None


def load_customer_config(customer: str) -> dict[str, Any]:
    """Load customer configuration - supports both new and legacy format.

    Priority:
    1. Try new service-based format (services/adopus.yaml)
    2. Fall back to legacy format (2-customers/{customer}.yaml)
    """
    # Try new service-based format first
    try:
        service_config = load_service_config("adopus")
        return merge_customer_config(service_config, customer)
    except (FileNotFoundError, ValueError) as exc:
        # Fall back to old format (individual customer files)
        customers_dir = get_project_root() / "2-customers"
        config_path = customers_dir / f"{customer}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"Customer config not found in service or legacy format: {customer}"
            ) from exc
        with config_path.open() as f:
            return yaml.safe_load(f)  # type: ignore[return-value,attr-defined]


def get_all_customers() -> list[str]:
    """Get list of all customers.

    Priority:
    1. Read from service config (services/adopus.yaml)
    2. Fall back to directory listing (2-customers/)
    """
    try:
        service_config = load_service_config("adopus")
        customers = service_config.get('customers', [])
        if not isinstance(customers, list):
            return []

        names: list[str] = []
        for customer_entry in cast(list[object], customers):
            if not isinstance(customer_entry, dict):
                continue
            customer_entry_dict = cast(dict[str, Any], customer_entry)
            customer_name = customer_entry_dict.get('name')
            if isinstance(customer_name, str) and customer_name:
                names.append(customer_name)
        return names
    except FileNotFoundError:
        # Fall back to old format (directory listing)
        customers_dir = get_project_root() / "2-customers"
        if not customers_dir.exists():
            return []
        return [f.stem for f in customers_dir.glob("*.yaml")]


def get_customer_environments(customer: str) -> list[str]:
    """Get list of environments configured for a customer."""
    config = load_customer_config(customer)
    return list(config.get('environments', {}).keys())
