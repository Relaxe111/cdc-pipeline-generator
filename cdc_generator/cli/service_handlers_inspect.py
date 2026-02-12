"""Source database inspection handlers for manage-service."""

import argparse

from cdc_generator.helpers.helpers_logging import (
    Colors,
    print_error,
    print_header,
    print_info,
    print_success,
    print_warning,
)
from cdc_generator.validators.manage_server_group.config import (
    get_server_group_for_service,
    load_server_groups,
)
from cdc_generator.validators.manage_server_group.types import (
    ServerGroupConfig,
    ServerGroupFile,
    SourceConfig,
)
from cdc_generator.validators.manage_service.db_inspector_common import (
    ValidationEnvMissingError,
)
from cdc_generator.validators.manage_service.mssql_inspector import (
    inspect_mssql_schema,
)
from cdc_generator.validators.manage_service.postgres_inspector import (
    inspect_postgres_schema,
)
from cdc_generator.validators.manage_service.schema_saver import (
    save_detailed_schema,
)

_VALIDATION_ENV_MISSING_EXIT_CODE = 2


def _resolve_inspect_db_type(
    service: str,
) -> tuple[str | None, str | None, ServerGroupFile]:
    """Resolve db type and server group for inspect.

    Returns:
        (db_type, server_group, server_groups_data)
    """
    db_type: str | None = None
    server_group: str | None = None
    server_groups_data: ServerGroupFile = {}

    try:
        server_groups_data = load_server_groups()
        server_group = get_server_group_for_service(
            service, server_groups_data,
        )

        if server_group and server_group in server_groups_data:
            sg = server_groups_data[server_group]
            raw_type = sg.get("type")
            db_type = str(raw_type) if raw_type else None
    except (FileNotFoundError, ValueError):
        pass

    return db_type, server_group, server_groups_data


def _get_allowed_schemas(
    service: str,
    server_group: str,
    server_groups_data: ServerGroupFile,
) -> list[str] | int:
    """Get allowed schemas for a service from source-groups.yaml.

    Returns:
        list[str] on success, or int exit-code on error.
    """
    sg_data = server_groups_data[server_group]
    pattern = sg_data.get("pattern")
    sources = sg_data.get("sources", {})

    if pattern == "db-per-tenant":
        return _schemas_for_per_tenant(
            sg_data, sources, server_group,
        )

    if pattern == "db-shared":
        return _schemas_for_shared(
            service, sources, server_group,
        )

    return []


def _schemas_for_per_tenant(
    sg_data: ServerGroupConfig,
    sources: dict[str, SourceConfig],
    server_group: str,
) -> list[str] | int:
    """Get schemas for db-per-tenant pattern."""
    database_ref = sg_data.get("database_ref")
    if not database_ref:
        print_error(
            "No database_ref defined for "
            + f"server group '{server_group}'"
        )
        return 1
    if database_ref in sources:
        source_config = sources[database_ref]
        return source_config.get("schemas", [])
    print_error(
        f"Reference database '{database_ref}' not found "
        + f"in sources for server group '{server_group}'"
    )
    return 1


def _schemas_for_shared(
    service: str,
    sources: dict[str, SourceConfig],
    server_group: str,
) -> list[str] | int:
    """Get schemas for db-shared pattern."""
    if service in sources:
        source_config = sources[service]
        return source_config.get("schemas", [])
    print_error(
        f"Service '{service}' not found in sources "
        + f"for server group '{server_group}'"
    )
    return 1


def handle_inspect(args: argparse.Namespace) -> int:
    """Inspect database schema and list available tables.

    If args.service is None, inspects all services in services/ directory.
    """
    if args.service:
        # Inspect single service
        return _inspect_single_service(args)

    # Inspect all services - default to --all if not specified
    if not args.all and not args.schema:
        args.all = True

    # Inspect all services
    from cdc_generator.helpers.service_config import get_project_root

    services_dir = get_project_root() / "services"
    if not services_dir.exists():
        print_error("No services directory found")
        return 1

    service_files = sorted(services_dir.glob("*.yaml"))
    if not service_files:
        print_error("No service files found in services/")
        return 1

    print_info(f"Inspecting {len(service_files)} service(s)...\n")

    results: dict[str, bool] = {}
    validation_env_missing = False  # Track if validation_env is missing

    for service_file in service_files:
        service_name = service_file.stem
        print_info(f"{'=' * 80}")
        args_copy = argparse.Namespace(**vars(args))
        args_copy.service = service_name
        result = _inspect_single_service(args_copy)
        results[service_name] = result == 0

        # Check if failure was due to missing validation_env (exit code 2)
        if result == _VALIDATION_ENV_MISSING_EXIT_CODE:
            validation_env_missing = True

        print()  # Blank line between services

    # Summary
    print_info(f"{'=' * 80}")
    print_info("Inspection Summary")
    print_info(f"{'=' * 80}\n")

    passed = [s for s, ok in results.items() if ok]
    failed = [s for s, ok in results.items() if not ok]

    if passed:
        print_info(f"✓ Completed ({len(passed)}): {', '.join(passed)}")
    if failed:
        print_error(f"✗ Failed ({len(failed)}): {', '.join(failed)}")

        # Show consolidated validation_env error if applicable
        if validation_env_missing:
            _show_validation_env_help()

    return 0 if all(results.values()) else 1


def _show_validation_env_help() -> None:
    """Show consolidated help message for missing validation_env."""
    print()
    print_error("❌ Configuration Error: validation_env not set")
    print()
    print_info("   The 'validation_env' field is required in source-groups.yaml to determine")
    print_info("   which environment's database to use for inspection and validation.")
    print()
    print_info("   You can set it using the CLI command:")
    print()
    print_success("     cdc manage-source-groups --set-validation-env <env>")
    print()
    print_info("   Or list available environments:")
    print()
    print_success("     cdc manage-source-groups --list-envs")
    print()


def _inspect_single_service(args: argparse.Namespace) -> int:
    """Inspect database schema for a single service.

    Returns:
        0 on success
        1 on general failure
        2 on missing validation_env (special case for consolidated error)
    """
    db_type, server_group, server_groups_data = (
        _resolve_inspect_db_type(args.service)
    )

    if not db_type:
        print_error(
            "Could not determine database type "
            + f"for service '{args.service}'"
        )
        print_error(
            "Service must be defined in source-groups.yaml sources"
        )
        return 1

    if not args.all and not args.schema:
        print_error(
            "Error: --inspect requires either "
            + "--all (for all schemas) or --schema <name>"
        )
        return 1

    # Get allowed schemas
    allowed_schemas: list[str] = []
    if server_group:
        result = _get_allowed_schemas(
            args.service, server_group, server_groups_data,
        )
        if isinstance(result, int):
            return result
        allowed_schemas = result

    schema: str | None = args.schema

    if schema and schema not in allowed_schemas:
        print_error(
            f"Schema '{schema}' not allowed "
            + f"for service '{args.service}'"
        )
        print_error(
            f"Allowed schemas: {', '.join(allowed_schemas)}"
        )
        return 1

    schema_msg = (
        f"schemas: {', '.join(allowed_schemas)}"
        if args.all
        else f"schema: {schema}"
    )
    print_header(
        f"Inspecting {db_type.upper()} schema "
        + f"for {args.service} ({schema_msg})"
    )

    return _run_inspection(args, db_type, allowed_schemas, schema)


def _run_inspection(
    args: argparse.Namespace,
    db_type: str,
    allowed_schemas: list[str],
    schema: str | None,
) -> int:
    """Execute the inspection and print results.

    Returns:
        0 on success
        1 on general failure
        2 on missing validation_env
    """
    try:
        if db_type == "mssql":
            tables = inspect_mssql_schema(args.service, args.env)
        elif db_type == "postgres":
            tables = inspect_postgres_schema(args.service, args.env)
        else:
            print_error(f"Unsupported database type: {db_type}")
            return 1
    except ValidationEnvMissingError:
        # Don't print error here - will be shown consolidated at the end
        return _VALIDATION_ENV_MISSING_EXIT_CODE

    if tables:
        if args.all:
            tables = [
                t for t in tables
                if t["TABLE_SCHEMA"] in allowed_schemas
            ]
        else:
            tables = [
                t for t in tables
                if t["TABLE_SCHEMA"] == schema
            ]

        if not tables:
            if args.all:
                schemas_str = ", ".join(allowed_schemas)
                print_warning(
                    "No tables found in allowed schemas: "
                    + schemas_str
                )
            else:
                print_warning(
                    f"No tables found in schema '{schema}'"
                )
            return 1

        if args.save:
            ok = save_detailed_schema(
                args.service,
                args.env,
                schema or "",
                tables,
                db_type,
            )
            return 0 if ok else 1

        print_success(f"Found {len(tables)} tables:\n")
        current_schema: str | None = None
        for table in tables:
            tbl_schema = table["TABLE_SCHEMA"]
            if tbl_schema != current_schema:
                print(
                    f"\n{Colors.CYAN}[{tbl_schema}]{Colors.RESET}"
                )
                current_schema = tbl_schema
            col_count = table["COLUMN_COUNT"]
            print(
                f"  {table['TABLE_NAME']} ({col_count} columns)"
            )

    return 0 if tables else 1
