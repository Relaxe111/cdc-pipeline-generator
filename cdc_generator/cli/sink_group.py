#!/usr/bin/env python3
"""Manage sink server group configuration file (sink-groups.yaml).

This command helps manage sink destinations for CDC pipelines.
Sink groups can either inherit from source groups (db-shared pattern)
or be standalone (analytics warehouse, webhooks, etc.).

Usage:
    # 🏗️  Auto-scaffold all sink groups from source groups (db-shared only)
    cdc manage-sink-groups --create

    # 🔗 Create inherited sink group from specific source group
    cdc manage-sink-groups --create --source-group foo

    # ➕ Add new standalone sink group (auto-prefixes with 'sink_')
    cdc manage-sink-groups --add-new-sink-group analytics --type postgres

    # 📋 List all sink groups
    cdc manage-sink-groups --list

    # ℹ️  Show information about a sink group
    cdc manage-sink-groups --info sink_analytics

    # 🖥️  Add a server to a sink group
    cdc manage-sink-groups --sink-group sink_analytics --add-server default \\
        --host localhost --port 5432 --user postgres --password secret

    # 🗑️  Remove a server from a sink group
    cdc manage-sink-groups --sink-group sink_analytics --remove-server default

    # ❌ Remove a sink group
    cdc manage-sink-groups --remove sink_analytics

    # ✅ Validate sink group configuration
    cdc manage-sink-groups --validate

Examples:
    # 🏗️  Auto-scaffold (creates sink_foo from source foo if db-shared)
    cdc manage-sink-groups --create

    # 🔗 Create specific inherited sink from source group 'foo'
    cdc manage-sink-groups --create --source-group foo

    # 📊 Add new standalone analytics warehouse (creates 'sink_analytics')
    cdc manage-sink-groups --add-new-sink-group analytics --type postgres

    # 🌍 Add production server to sink_analytics
    cdc manage-sink-groups --sink-group sink_analytics --add-server prod \\
        --host analytics.example.com --port 5432 --user analytics_user
"""

import argparse
import sys
from pathlib import Path
from typing import Any, cast

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root))

from cdc_generator.core.sink_types import SinkServerConfig
from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.helpers.helpers_sink_groups import (
    create_inherited_sink_group,
    create_standalone_sink_group,
    load_sink_groups,
    resolve_sink_group,
    save_sink_groups,
    should_inherit_services,
    validate_sink_group_structure,
)
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.helpers.yaml_loader import ConfigDict, load_yaml_file


def get_sink_file_path() -> Path:
    """Get path to sink-groups.yaml in current implementation."""
    return get_project_root() / "sink-groups.yaml"


def get_source_group_file_path() -> Path:
    """Get path to source-groups.yaml in current implementation."""
    root = get_project_root()
    source_file = root / "source-groups.yaml"
    if not source_file.exists():
        print_error(f"Source server group file not found: {source_file}")
        print_info("This command must be run from an implementation directory.")
        sys.exit(1)
    return source_file


def _auto_scaffold_sink_groups(
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
) -> int:
    """Auto-scaffold sink groups for all db-shared source groups."""
    print_header("Auto-scaffolding sink groups from source groups")

    created_count = 0
    for source_group_name, source_group in source_groups.items():
        if not should_inherit_services(source_group):
            pattern = source_group.get("pattern", "unknown")
            msg = (f"Skipping '{source_group_name}' (pattern: {pattern})"
                   " — inheritance only for db-shared")
            print_info(msg)
            continue

        sink_group_name = f"sink_{source_group_name}"

        if sink_group_name in sink_groups:
            print_warning(f"Sink group '{sink_group_name}' already exists, skipping")
            continue

        source_services = source_group.get("sources", {})
        if not isinstance(source_services, dict):
            print_warning(f"Invalid sources for '{source_group_name}', skipping")
            continue

        new_sink_group = create_inherited_sink_group(
            source_group_name,
            source_group,
            source_services,
        )

        sink_groups[sink_group_name] = new_sink_group
        created_count += 1
        print_success(f"Created '{sink_group_name}' from source group '{source_group_name}'")

    if created_count == 0:
        print_warning("No new sink groups created")
        print_info("Use --source-group or --name to create specific sink groups")
        return 0

    save_sink_groups(sink_groups, sink_file)
    print_success(f"\nCreated {created_count} sink group(s) in {sink_file}")
    print_info("\nNext steps:")
    print_info("1. Edit services/*.yaml to add sink configurations")
    print_info("2. Run 'cdc generate' to create sink pipelines")

    return 0


def _create_inherited_sink_group_from_source(
    args: argparse.Namespace,
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
    source_file: Path,
) -> int:
    """Create inherited sink group from specific source group."""
    source_group_name = args.source_group

    if source_group_name not in source_groups:
        print_error(f"Source group '{source_group_name}' not found in {source_file}")
        print_info(f"Available source groups: {list(source_groups.keys())}")
        return 1

    source_group = source_groups[source_group_name]

    if not should_inherit_services(source_group):
        pattern = source_group.get("pattern", "unknown")
        msg = (f"Source group '{source_group_name}' has pattern '{pattern}'. "
               "Inheriting services only makes sense for 'db-shared' pattern.")
        print_warning(msg)
        print_info("Recommendation: Use --name to create a standalone sink group instead.")
        return 1

    sink_group_name = f"sink_{source_group_name}"

    if sink_group_name in sink_groups:
        print_warning(f"Sink group '{sink_group_name}' already exists.")
        return 1

    source_services = source_group.get("sources", {})
    if not isinstance(source_services, dict):
        print_error(f"Invalid sources configuration for '{source_group_name}'")
        return 1

    new_sink_group = create_inherited_sink_group(
        source_group_name,
        source_group,
        source_services,
    )

    sink_groups[sink_group_name] = new_sink_group

    servers = new_sink_group.get("servers", {})
    print_success(f"Created inherited sink group '{sink_group_name}'")
    print_info(f"Source group: {source_group_name}")
    print_info(f"Pattern: {source_group.get('pattern')}")
    print_info(f"Inherited servers: {list(servers.keys())}")
    print_info(f"Inherited services: {new_sink_group.get('_inherited_services', [])}")
    print_info("\nNext steps:")
    print_info("1. Edit services/*.yaml to add sink configurations referencing this sink group")
    print_info("2. Run 'cdc generate' to create sink pipelines")

    save_sink_groups(sink_groups, sink_file)
    print_success(f"Saved to {sink_file}")
    return 0


def _create_standalone_sink(
    args: argparse.Namespace,
    sink_groups: dict[str, Any],
    source_groups: dict[str, ConfigDict],
    sink_file: Path,
    source_file: Path,
) -> int:
    """Create standalone sink group."""
    base_name = args.add_new_sink_group
    # Always add sink_ prefix
    sink_group_name = f"sink_{base_name}" if not base_name.startswith("sink_") else base_name
    sink_type = args.type or "postgres"
    pattern = args.pattern or "db-shared"
    environment_aware = args.environment_aware
    source_group_name = args.for_source_group

    # Validation: environment_aware is required for db-shared pattern
    if pattern == "db-shared" and not environment_aware:
        print_error("--environment-aware flag is required when --pattern is db-shared")
        print_info("Use: cdc manage-sink-groups --add-new-sink-group <name> --pattern db-shared --environment-aware")
        return 1

    if not source_group_name:
        if not source_groups:
            print_error("No source groups found in source-groups.yaml")
            return 1
        source_group_name = next(iter(source_groups.keys()))
        print_info(f"No --for-source-group specified, using '{source_group_name}'")

    if source_group_name not in source_groups:
        print_error(f"Source group '{source_group_name}' not found in {source_file}")
        print_info(f"Available source groups: {list(source_groups.keys())}")
        return 1

    if sink_group_name in sink_groups:
        print_warning(f"Sink group '{sink_group_name}' already exists.")
        return 1

    new_sink_group = create_standalone_sink_group(
        sink_group_name,
        source_group_name,
        sink_type,
        pattern,
        environment_aware,
        getattr(args, 'database_exclude_patterns', None),
        getattr(args, 'schema_exclude_patterns', None),
    )

    sink_groups[sink_group_name] = new_sink_group

    print_success(f"Created standalone sink group '{sink_group_name}'")
    print_info(f"Type: {sink_type}")
    print_info(f"Pattern: {pattern}")
    print_info(f"Environment Aware: {environment_aware}")
    print_info(f"Source group: {source_group_name}")
    if getattr(args, 'database_exclude_patterns', None):
        print_info(f"Database exclude patterns: {args.database_exclude_patterns}")
    if getattr(args, 'schema_exclude_patterns', None):
        print_info(f"Schema exclude patterns: {args.schema_exclude_patterns}")
    print_info("\nNext steps:")
    print_info(f"1. Add servers to sink group '{sink_group_name}' using --add-server")
    print_info("2. Edit services/*.yaml to add sink configurations")
    print_info("3. Run 'cdc generate' to create sink pipelines")

    save_sink_groups(sink_groups, sink_file)
    print_success(f"Saved to {sink_file}")
    return 0


def handle_create(args: argparse.Namespace) -> int:
    """Create a new sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    # Load existing sink groups (or create empty)
    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        sink_groups = {}

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Route to appropriate create handler
    if not args.source_group:
        return _auto_scaffold_sink_groups(sink_groups, source_groups, sink_file)

    if args.source_group:
        return _create_inherited_sink_group_from_source(
            args, sink_groups, source_groups, sink_file, source_file
        )

    print_error("Unexpected state in handle_create")
    return 1



def handle_add_new_sink_group(args: argparse.Namespace) -> int:
    """Add a new standalone sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    # Load existing sink groups (or create empty)
    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        sink_groups = {}

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    return _create_standalone_sink(
        args, sink_groups, source_groups, sink_file, source_file
    )


def handle_list(_args: argparse.Namespace) -> int:
    """List all sink groups."""
    sink_file = get_sink_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_info(f"No sink groups file found: {sink_file}")
        print_info("Use --create to create your first sink group.")
        return 0

    if not sink_groups:
        print_info("No sink groups configured.")
        return 0

    print_header("Sink Groups")
    for sink_group_name, sink_group in sink_groups.items():
        source_group = sink_group.get("source_group", "N/A")
        sink_type = sink_group.get("type", "N/A")
        pattern = sink_group.get("pattern", "N/A")
        server_count = len(sink_group.get("servers", {}))
        source_count = len(sink_group.get("sources", {}))

        print(f"\n{sink_group_name}:")
        print(f"  Source Group:  {source_group}")
        print(f"  Pattern:       {pattern}")
        print(f"  Type:          {sink_type}")
        print(f"  Servers:       {server_count}")
        print(f"  Sources:       {source_count}")

        if sink_group.get("_inherited_services"):
            inherited = sink_group.get("_inherited_services", [])
            if isinstance(inherited, list):
                inherited_str = cast(list[str], inherited)
                print(f"  Inherited:     {', '.join(inherited_str)}")

    return 0


def handle_inspect_command(args: argparse.Namespace) -> int:
    """Inspect databases on a standalone sink server."""
    from cdc_generator.validators.manage_server_group.db_inspector import (
        list_mssql_databases,
        list_postgres_databases,
    )

    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    # Require --sink-group argument
    if not args.sink_group:
        print_error("Error: --inspect requires --sink-group <name>")
        print_info("Usage: cdc manage-sink-groups --inspect --sink-group <name>")
        return 1

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    sink_group_name = args.sink_group
    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found.")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]

    # Validate: inspection only for standalone sink groups
    # Inherited sink groups have source_ref in servers (created via --create)
    # Standalone sink groups have their own server configs (created via --add-new-sink-group)
    servers = sink_group.get("servers", {})
    if servers:
        first_server = next(iter(servers.values()))
        if "source_ref" in first_server:
            print_error(f"Error: Cannot inspect inherited sink group '{sink_group_name}'")
            print_info("Inspection is only available for standalone sink groups (created with --add-new-sink-group)")
            print_info("Inherited sink groups (created with --create) use source_ref and inherit from source groups.")
            return 1

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    # Get server to inspect
    server_name = args.server or "default"
    servers = resolved.get("servers", {})
    if server_name not in servers:
        print_error(f"Server '{server_name}' not found in sink group '{sink_group_name}'")
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    server_config = servers[server_name]
    sink_type = resolved.get("type", "postgres")

    # Get exclude patterns
    database_exclude_patterns = resolved.get("database_exclude_patterns")
    schema_exclude_patterns = resolved.get("schema_exclude_patterns")

    print_header(f"Inspecting Sink Server: {server_name} ({sink_type})")

    try:
        if sink_type == "mssql":
            databases = list_mssql_databases(
                server_config=server_config,  # type: ignore[arg-type]
                server_group_config=resolved,  # type: ignore[arg-type]
                include_pattern=args.include_pattern,
                database_exclude_patterns=database_exclude_patterns,
                schema_exclude_patterns=schema_exclude_patterns,
                server_name=server_name,
            )
        elif sink_type == "postgres":
            databases = list_postgres_databases(
                server_config=server_config,  # type: ignore[arg-type]
                server_group_config=resolved,  # type: ignore[arg-type]
                include_pattern=args.include_pattern,
                database_exclude_patterns=database_exclude_patterns,
                schema_exclude_patterns=schema_exclude_patterns,
                server_name=server_name,
            )
        else:
            print_error(f"Inspection not supported for sink type: {sink_type}")
            print_info("Only 'postgres' and 'mssql' sink types support inspection.")
            return 1

        # Display results
        print_success(f"\nFound {len(databases)} database(s):\n")
        for db in databases:
            schemas_str = ", ".join(db["schemas"])
            print(f"  {db['name']}")
            print(f"    Service:     {db['service']}")
            print(f"    Environment: {db['environment'] or 'N/A'}")
            print(f"    Schemas:     {schemas_str}")
            print(f"    Tables:      {db['table_count']}")
            print()

        print_info("Note: Use this information to manually configure sink destinations.")
        print_info("Future enhancement: Add databases to sink group configuration automatically.")

    except ImportError as e:
        print_error(f"Database driver not installed: {e}")
        return 1
    except Exception as e:
        print_error(f"Failed to inspect databases: {e}")
        return 1

    return 0


def handle_info_command(args: argparse.Namespace) -> int:
    """Show detailed information about a sink group."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    sink_group_name = args.info
    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found.")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]

    # Load source groups for resolution
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    # Resolve sink group
    resolved = resolve_sink_group(sink_group_name, sink_group, source_groups)

    print_header(f"Sink Group: {sink_group_name}")
    print(f"Source Group:      {resolved.get('source_group', 'N/A')}")
    print(f"Pattern:           {resolved.get('pattern', 'N/A')}")
    print(f"Type:              {resolved.get('type', 'N/A')}")
    print(f"Kafka Topology:    {resolved.get('kafka_topology', 'N/A')}")
    print(f"Environment Aware: {resolved.get('environment_aware', False)}")
    print(f"Description:       {resolved.get('description', 'N/A')}")
    
    # Show exclude patterns if present
    if resolved.get('database_exclude_patterns'):
        db_patterns = resolved.get('database_exclude_patterns', [])
        print(f"Database Exclude:  {', '.join(db_patterns)}")
    if resolved.get('schema_exclude_patterns'):
        schema_patterns = resolved.get('schema_exclude_patterns', [])
        print(f"Schema Exclude:    {', '.join(schema_patterns)}")

    if resolved.get("_inherited_from"):
        print(f"Inherited From:   {resolved.get('_inherited_from', 'N/A')}")

    print("\nServers:")
    for server_name, server_config in resolved.get("servers", {}).items():
        print(f"\n  {server_name}:")
        if "_source_ref" in server_config:
            print(f"    Source Ref:  {server_config['_source_ref']}")
        print(f"    Type:        {server_config.get('type', 'N/A')}")
        print(f"    Host:        {server_config.get('host', 'N/A')}")
        print(f"    Port:        {server_config.get('port', 'N/A')}")

    print("\nSources:")
    sources = resolved.get("sources", {})
    if not sources:
        print("  (none configured)")
    else:
        for service_name, source in sources.items():
            print(f"\n  {service_name}:")
            source_dict = cast(dict[str, Any], source)
            for env_name, env_config in source_dict.items():
                if env_name == "schemas":
                    print(f"    Schemas: {env_config}")
                elif isinstance(env_config, dict):
                    env_dict = cast(dict[str, Any], env_config)
                    server = env_dict.get("server", "N/A")
                    database = env_dict.get("database", "N/A")
                    schema = env_dict.get("schema", "N/A")
                    print(f"    {env_name}:")
                    print(f"      Server:   {server}")
                    print(f"      Database: {database}")
                    print(f"      Schema:   {schema}")

    return 0


def handle_validate_command(_args: argparse.Namespace) -> int:
    """Validate sink group configuration."""
    sink_file = get_sink_file_path()
    source_file = get_source_group_file_path()

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_info(f"No sink groups file found: {sink_file}")
        return 0

    # Load source groups
    source_groups = cast(dict[str, ConfigDict], load_yaml_file(source_file))

    print_header("Validating Sink Groups")

    all_valid = True
    for sink_group_name, sink_group in sink_groups.items():
        print(f"\nValidating '{sink_group_name}'...")

        # Structure validation
        errors = validate_sink_group_structure(sink_group_name, sink_group)
        if errors:
            all_valid = False
            for error in errors:
                print_error(f"  ✗ {error}")
        else:
            print_success("  ✓ Structure valid")

        # Resolution validation
        try:
            resolve_sink_group(sink_group_name, sink_group, source_groups)
            print_success("  ✓ All references resolve successfully")
        except ValueError as e:
            all_valid = False
            print_error(f"  ✗ Resolution failed: {e}")

    if all_valid:
        print_success("\n✓ All sink groups are valid")
        return 0

    print_error("\n✗ Validation failed")
    return 1


def _build_server_config(args: argparse.Namespace, sink_group: dict[str, Any]) -> dict[str, object]:
    """Build server configuration from arguments.

    If host/port/user/password are not provided, generates env variable placeholders
    using pattern: ${<DB_TYPE>_SINK_<FIELD>_<SERVERGROUP>_<SERVER>}

    Example: sink_asma + nonprod + postgres → ${POSTGRES_SINK_HOST_ASMA_NONPROD}
    
    Note: Type is inherited from sink group level, not duplicated at server level.
    """
    server_config: dict[str, object] = {}

    # Determine server type for env var generation (but don't add to config)
    server_type = sink_group.get("type", "postgres")

    # Get sink group name and server name for env var generation
    # Strip 'sink_' prefix if present
    sink_group_name = args.sink_group
    if sink_group_name.startswith("sink_"):
        sink_group_name = sink_group_name[5:]
    sink_group_name = sink_group_name.upper().replace("-", "_")
    server_name = args.add_server.upper().replace("-", "_")

    # Determine DB type prefix (POSTGRES or MSSQL)
    db_type = "POSTGRES" if server_type == "postgres" else server_type.upper()

    # Generate env variable pattern: ${<DB_TYPE>_SINK_<FIELD>_<SERVERGROUP>_<SERVER>}
    env_base = f"${{{db_type}_SINK"

    # Use provided values or generate env variable placeholders
    host_env = f"{env_base}_HOST_{sink_group_name}_{server_name}}}"
    port_env = f"{env_base}_PORT_{sink_group_name}_{server_name}}}"
    user_env = f"{env_base}_USER_{sink_group_name}_{server_name}}}"
    password_env = f"{env_base}_PASSWORD_{sink_group_name}_{server_name}}}"

    server_config["host"] = args.host if args.host else host_env
    server_config["port"] = args.port if args.port else port_env
    server_config["user"] = args.user if args.user else user_env
    server_config["password"] = args.password if args.password else password_env

    # Add extraction patterns if provided
    if getattr(args, 'extraction_patterns', None):
        # Parse patterns from command line (expecting JSON-like format or simple patterns)
        # For now, store as-is - validation/parsing happens elsewhere
        server_config["extraction_patterns"] = args.extraction_patterns

    return server_config


def _validate_server_config(
    server_config: dict[str, object],
) -> str | None:
    """Validate server config, return error message or None if valid.

    Note: Type is inherited from sink group level.
    Host/port are optional - will use env variable placeholders if not provided.
    """
    # Type is inherited from sink group, not required at server level
    # Host/port are optional (env vars used as defaults)
    return None


def handle_add_server_command(args: argparse.Namespace) -> int:
    """Add a server to a sink group."""
    sink_file = get_sink_file_path()

    if not args.sink_group or not args.add_server:
        print_error("--add-server requires --sink-group and server name")
        return 1

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    sink_group_name = args.sink_group
    server_name = args.add_server

    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    sink_group = sink_groups[sink_group_name]
    servers = sink_group.get("servers", {})

    if server_name in servers:
        print_warning(f"Server '{server_name}' already exists in sink group '{sink_group_name}'")
        return 1

    # Build and validate server config
    server_config = _build_server_config(args, cast(dict[str, Any], sink_group))
    error = _validate_server_config(server_config)
    if error:
        print_error(error)
        return 1

    servers[server_name] = cast(SinkServerConfig, server_config)
    sink_group["servers"] = servers  # type: ignore[typeddict-item]

    save_sink_groups(sink_groups, sink_file)

    server_type = sink_group.get("type", "postgres")
    print_success(f"Added server '{server_name}' to sink group '{sink_group_name}'")
    print_info(f"Server type: {server_type} (inherited from sink group)")
    
    # Show what was configured
    host = server_config.get("host", "N/A")
    port = server_config.get("port", "N/A")
    if args.host:
        print_info(f"Host: {host}:{port}")
    else:
        print_info(f"Host: {host} (env variable)")
        print_info(f"Port: {port} (env variable)")
        print_info(f"User: {server_config.get('user', 'N/A')} (env variable)")
        print_warning("Remember to set environment variables at runtime")

    return 0


def _check_server_references(
    server_name: str,
    sink_group: dict[str, Any]
) -> list[str]:
    """Check if server is referenced in sources, return list of references."""
    sources = sink_group.get("sources", {})
    references: list[str] = []
    for service_name, source in sources.items():
        if isinstance(source, dict):
            source_dict = cast(dict[str, Any], source)
            for env_name, env_config in source_dict.items():
                if env_name != "schemas" and isinstance(env_config, dict):
                    env_dict = cast(dict[str, Any], env_config)
                    if env_dict.get("server") == server_name:
                        references.append(f"{service_name}.{env_name}")
    return references


def handle_remove_server_command(args: argparse.Namespace) -> int:
    """Remove a server from a sink group."""
    sink_file = get_sink_file_path()

    if not args.sink_group or not args.remove_server:
        print_error("--remove-server requires --sink-group and server name")
        return 1

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    sink_group_name = args.sink_group
    server_name = args.remove_server

    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found")
        return 1

    sink_group = sink_groups[sink_group_name]
    servers = sink_group.get("servers", {})

    if server_name not in servers:
        print_error(f"Server '{server_name}' not found in sink group '{sink_group_name}'")
        print_info(f"Available servers: {list(servers.keys())}")
        return 1

    # Check if server is referenced in sources
    references = _check_server_references(server_name, cast(dict[str, Any], sink_group))

    if references:
        print_error(f"Cannot remove server '{server_name}' — referenced in sources:")
        for ref in references:
            print_error(f"  - {ref}")
        return 1

    del servers[server_name]
    save_sink_groups(sink_groups, sink_file)

    print_success(f"Removed server '{server_name}' from sink group '{sink_group_name}'")

    return 0


def handle_remove_sink_group_command(args: argparse.Namespace) -> int:
    """Remove a sink group."""
    sink_file = get_sink_file_path()

    if not args.remove:
        print_error("Sink group name required")
        return 1

    try:
        sink_groups = load_sink_groups(sink_file)
    except FileNotFoundError:
        print_error(f"Sink groups file not found: {sink_file}")
        return 1

    sink_group_name = args.remove

    if sink_group_name not in sink_groups:
        print_error(f"Sink group '{sink_group_name}' not found")
        print_info(f"Available sink groups: {list(sink_groups.keys())}")
        return 1

    # Confirm removal
    print_warning(f"This will remove sink group '{sink_group_name}'")

    del sink_groups[sink_group_name]
    save_sink_groups(sink_groups, sink_file)

    print_success(f"Removed sink group '{sink_group_name}'")

    return 0


def main() -> int:
    """CLI entry point for manage-sink-groups command."""
    
    # Build colorized description
    description = f"""{Colors.CYAN}{Colors.BOLD}🎯 Manage sink server group configuration (sink-groups.yaml){Colors.RESET}

{Colors.DIM}This command helps manage sink destinations for CDC pipelines.
Sink groups can either inherit from source groups (db-shared pattern)
or be standalone (analytics warehouse, webhooks, etc.).{Colors.RESET}

{Colors.YELLOW}Usage Examples:{Colors.RESET}
    {Colors.GREEN}🏗️  Auto-scaffold all sink groups{Colors.RESET}
    $ cdc manage-sink-groups --create

    {Colors.BLUE}🔗 Create inherited sink group{Colors.RESET}
    $ cdc manage-sink-groups --create --source-group foo

    {Colors.GREEN}➕ Add new standalone sink group{Colors.RESET}
    $ cdc manage-sink-groups --add-new-sink-group analytics --type postgres

    {Colors.CYAN}📋 List all sink groups{Colors.RESET}
    $ cdc manage-sink-groups --list

    {Colors.BLUE}ℹ️  Show sink group information{Colors.RESET}
    $ cdc manage-sink-groups --info sink_analytics

    {Colors.GREEN}🖥️  Add a server{Colors.RESET}
    $ cdc manage-sink-groups --sink-group sink_analytics --add-server default \\
        --host localhost --port 5432 --user postgres --password secret

    {Colors.RED}🗑️  Remove a server{Colors.RESET}
    $ cdc manage-sink-groups --sink-group sink_analytics --remove-server default

    {Colors.RED}❌ Remove a sink group{Colors.RESET}
    $ cdc manage-sink-groups --remove sink_analytics

    {Colors.GREEN}✅ Validate configuration{Colors.RESET}
    $ cdc manage-sink-groups --validate
"""
    
    parser = argparse.ArgumentParser(
        description=description,
        prog="cdc manage-sink-groups",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Create actions
    parser.add_argument(
        "--create",
        action="store_true",
        help=f"{Colors.GREEN}🏗️  Create a new sink group{Colors.RESET}",
    )
    parser.add_argument(
        "--source-group",
        metavar="NAME",
        help=f"{Colors.BLUE}🔗 Source group to inherit from (for inherited sink groups){Colors.RESET}",
    )
    parser.add_argument(
        "--add-new-sink-group",
        metavar="NAME",
        help=f"{Colors.GREEN}➕ Add new standalone sink group (auto-prefixes with 'sink_'){Colors.RESET}",
    )
    parser.add_argument(
        "--type",
        choices=["postgres", "mssql", "http_client", "http_server"],
        default="postgres",
        help=f"{Colors.YELLOW}🗂️  Type of sink (default: postgres){Colors.RESET}",
    )
    parser.add_argument(
        "--pattern",
        choices=["db-shared", "db-per-tenant"],
        default="db-shared",
        help=f"{Colors.CYAN}🏗️  Pattern for sink group (default: db-shared){Colors.RESET}",
    )
    parser.add_argument(
        "--environment-aware",
        action="store_true",
        help=f"{Colors.GREEN}🌍 Enable environment-aware grouping (required for db-shared){Colors.RESET}",
    )
    parser.add_argument(
        "--database-exclude-patterns",
        nargs="+",
        metavar="PATTERN",
        help=f"{Colors.YELLOW}🚫 Regex patterns for excluding databases (space-separated){Colors.RESET}",
    )
    parser.add_argument(
        "--schema-exclude-patterns",
        nargs="+",
        metavar="PATTERN",
        help=f"{Colors.YELLOW}🚫 Regex patterns for excluding schemas (space-separated){Colors.RESET}",
    )
    parser.add_argument(
        "--for-source-group",
        metavar="NAME",
        help=f"{Colors.CYAN}📡 Source group this standalone sink consumes from{Colors.RESET}",
    )

    # List/Info actions
    parser.add_argument(
        "--list",
        action="store_true",
        help=f"{Colors.CYAN}📋 List all sink groups{Colors.RESET}",
    )
    parser.add_argument(
        "--info",
        metavar="NAME",
        help=f"{Colors.BLUE}ℹ️  Show detailed information about a sink group{Colors.RESET}",
    )

    # Inspection actions (standalone sink groups only)
    parser.add_argument(
        "--inspect",
        action="store_true",
        help=f"{Colors.CYAN}🔍 Inspect databases on sink server (standalone sink groups only){Colors.RESET}",
    )
    parser.add_argument(
        "--server",
        metavar="NAME",
        help=f"{Colors.BLUE}🖥️  Server name to inspect (default: 'default'){Colors.RESET}",
    )
    parser.add_argument(
        "--include-pattern",
        metavar="PATTERN",
        help=f"{Colors.GREEN}✅ Only include databases matching regex pattern{Colors.RESET}",
    )

    # Validation
    parser.add_argument(
        "--validate",
        action="store_true",
        help=f"{Colors.GREEN}✅ Validate sink group configuration{Colors.RESET}",
    )

    # Server management
    parser.add_argument(
        "--sink-group",
        metavar="NAME",
        help=f"{Colors.CYAN}🎯 Sink group to operate on (for --add-server, --remove-server){Colors.RESET}",
    )
    parser.add_argument(
        "--add-server",
        metavar="NAME",
        help=f"{Colors.GREEN}🖥️  Add a server to a sink group (requires --sink-group){Colors.RESET}",
    )
    parser.add_argument(
        "--remove-server",
        metavar="NAME",
        help=f"{Colors.RED}🗑️  Remove a server from a sink group (requires --sink-group){Colors.RESET}",
    )
    parser.add_argument(
        "--host",
        metavar="HOST",
        help=f"{Colors.BLUE}🌐 Server host{Colors.RESET}",
    )
    parser.add_argument(
        "--port",
        metavar="PORT",
        help=f"{Colors.BLUE}🔌 Server port{Colors.RESET}",
    )
    parser.add_argument(
        "--user",
        metavar="USER",
        help=f"{Colors.BLUE}👤 Server user{Colors.RESET}",
    )
    parser.add_argument(
        "--password",
        metavar="PASSWORD",
        help=f"{Colors.YELLOW}🔑 Server password{Colors.RESET}",
    )
    parser.add_argument(
        "--extraction-patterns",
        nargs="+",
        metavar="PATTERN",
        help=f"{Colors.CYAN}🔍 Regex extraction patterns for server (space-separated, use quotes){Colors.RESET}",
    )

    # Sink group management
    parser.add_argument(
        "--remove",
        metavar="NAME",
        help=f"{Colors.RED}❌ Remove a sink group{Colors.RESET}",
    )

    args = parser.parse_args()

    # Route to appropriate handler
    handlers = {
        "create": (args.create, lambda: handle_create(args)),
        "add_new_sink_group": (args.add_new_sink_group, lambda: handle_add_new_sink_group(args)),
        "list": (args.list, lambda: handle_list(args)),
        "info": (args.info, lambda: handle_info_command(args)),
        "inspect": (args.inspect, lambda: handle_inspect_command(args)),
        "validate": (args.validate, lambda: handle_validate_command(args)),
        "add_server": (args.add_server, lambda: handle_add_server_command(args)),
        "remove_server": (args.remove_server, lambda: handle_remove_server_command(args)),
        "remove": (args.remove, lambda: handle_remove_sink_group_command(args)),
    }

    for condition, handler in handlers.values():
        if condition:
            return handler()

    # No action specified
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
