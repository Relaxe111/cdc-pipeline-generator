"""Service creation and scaffolding."""

from typing import Any, cast

import yaml

from cdc_generator.helpers.helpers_logging import print_header, print_success
from cdc_generator.helpers.service_config import get_project_root
from cdc_generator.validators.manage_server_group.config import load_server_groups


def create_service(
    service_name: str,
    server_group: str,
    _server: str = "default",
    validation_database_override: str | None = None,
) -> None:
    """Create a new service configuration file.

    Args:
        service_name: Name of the service to create
        server_group: Server group name (e.g., 'adopus', 'asma')
        _server: Server name for multi-server setups (default: 'default')
        validation_database_override: Explicit validation database override
    """
    project_root = get_project_root()
    services_dir = project_root / 'services'
    services_dir.mkdir(exist_ok=True)

    service_file = services_dir / f'{service_name}.yaml'

    # Load source-groups.yaml using typed loader
    try:
        server_groups_data = load_server_groups()
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"source-groups.yaml not found. Run 'cdc scaffold' first: {exc}"
        ) from exc

    # Find the server group and get its type
    validation_database = None
    schemas = []
    pattern = None

    # New structure: server group at root level with sources
    if server_group in server_groups_data:
        group = server_groups_data[server_group]
        pattern = group.get('pattern')

        # Extract schemas and validation database from sources
        sources = group.get('sources', {})

        if pattern == 'db-per-tenant':
            database_ref = group.get('database_ref')
            if validation_database_override:
                validation_database = validation_database_override
                source_for_database = _find_source_by_database(sources, validation_database)
                if source_for_database is not None:
                    schemas = _extract_schemas_from_source(source_for_database)
                else:
                    schemas = ['dbo']
            else:
                # Use database_ref for validation
                if database_ref and database_ref in sources:
                    source_config = cast(dict[str, Any], sources[database_ref])
                    schemas = _extract_schemas_from_source(source_config)
                    validation_database = _extract_database_from_source(source_config)

            if not validation_database:
                raise ValueError(
                    f"Could not find validation database for server group '{server_group}'.\n" +
                    f"Expected: sources.{database_ref}.<env>.database in source-groups.yaml"
                )

        elif pattern == 'db-shared':
            # Find the source that matches this service name
            if service_name in sources:
                source_config = cast(dict[str, Any], sources[service_name])
                schemas = _extract_schemas_from_source(source_config, default_schema='public')
                validation_database = validation_database_override or _extract_database_from_source(source_config)

            if not validation_database:
                raise ValueError(
                    "Could not find database for service "
                    + f"'{service_name}' in server group "
                    + f"'{server_group}'.\n"
                    + f"Expected: sources.{service_name}."
                    + "<env>.database in source-groups.yaml"
                )

    if not pattern:
        raise ValueError(f"Server group '{server_group}' not found in source-groups.yaml")

    # Check if service exists - update mode
    update_mode = service_file.exists()

    if update_mode:
        print_header(f"Updating {pattern} service: {service_name}")
        # Load existing service file
        with service_file.open() as f:
            existing_service = yaml.safe_load(f)
    else:
        print_header(f"Creating new {pattern} service: {service_name}")
        existing_service = None

    if pattern == 'db-per-tenant':
        template: dict[str, Any] = {
            'source': {
                'validation_database': validation_database,
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'dbo.Actor': {primary_key: 'actno', ignore_columns: []}
                    # 'dbo.User': {primary_key: 'userid'}
                }
            }
        }

    else:  # db-shared
        template: dict[str, Any] = {
            'source': {
                'validation_database': validation_database,
                'tables': {
                    # Schema-qualified table names as keys
                    # Example:
                    # 'public.users': {primary_key: 'id'}
                    # 'logs.events': {primary_key: 'event_id', ignore_columns: ['debug_data']}
                }
            }
        }

    # If updating, merge with existing configuration
    if update_mode and existing_service:
        # Update validation_database if found in source-groups.yaml
        if validation_database and 'source' in existing_service:
            existing_service['source']['validation_database'] = validation_database

        # Update source with extracted schemas
        if schemas:
            # Ensure source exists
            if 'source' not in existing_service:
                existing_service['source'] = {}

            # Preserve existing tables (already in flat schema.table format)
            if 'tables' not in existing_service['source']:
                existing_service['source']['tables'] = {}

            # Migrate old source_tables structure if present
            if 'source_tables' in existing_service:
                migrated_tables = {}
                for schema_entry in existing_service['source_tables']:
                    schema_name = schema_entry.get('schema')
                    tables = schema_entry.get('tables', [])
                    for table in tables:
                        if isinstance(table, str):
                            table_name = table
                            migrated_tables[f"{schema_name}.{table_name}"] = {}
                        else:
                            table_name = table.get('name')
                            table_props = {k: v for k, v in table.items() if k != 'name'}
                            migrated_tables[f"{schema_name}.{table_name}"] = table_props

                # Merge migrated tables with existing flat tables
                existing_service['source']['tables'].update(migrated_tables)
                # Remove old structure
                del existing_service['source_tables']

        template = existing_service

    # Write YAML with header comment and proper formatting
    sep = "=" * 76
    header_comment = f"""# {sep}
# CDC Service Configuration - Auto-managed
# {sep}
# ⚠️  This file is mostly READ-ONLY - modify only through CDC commands:
#
#   cdc manage-services config --service {service_name} --add-source-table <schema.table>
#   cdc manage-services config --service {service_name} --remove-table <schema.table>
#   cdc manage-services config --create-service {service_name}
#
# 📝 MANUAL EDITS ALLOWED:
#   - source.tables - You can manually add/edit table entries (use schema.table format)
#   - Table properties: primary_key, ignore_columns, include_columns
#   - environments - Environment-specific settings (kafka, etc.)
#
# 🚫 DO NOT MANUALLY EDIT:
#   - Service name is derived from filename ({service_name}.yaml)
#   - source.validation_database (auto-populated from source-groups.yaml)
#
# [i] NOTE:
#   - server_group: Auto-detected (only one per implementation)
#   - server: Determined by environment (from source-groups.yaml)
#   - source.type: From source-groups.yaml type field
#   - Database connections: From source-groups.yaml servers configuration
# {sep}

"""

    with service_file.open('w') as f:
        f.write(header_comment)
        # Remove 'service' field if present (redundant in new format)
        template_to_save = {k: v for k, v in template.items() if k != 'service'}
        # Wrap in service name key
        wrapped_template = {service_name: template_to_save}
        yaml.dump(wrapped_template, f, default_flow_style=False, sort_keys=False, indent=2)

    action = "Updated" if update_mode else "Created"
    print_success(f"✓ {action} service configuration: {service_file}")

    if not update_mode:
        print_success("\nNext steps:")
        if pattern == 'db-per-tenant':
            print_success(
                "  1. Configure customers/environments in source-groups.yaml "
                + "(sources.<customer>.<env>)"
            )
            print_success(
                "  2. Add CDC tables: cdc manage-services config "
                + f"--service {service_name} "
                + "--add-source-table <schema.table>"
            )
            print_success(
                "  3. Validate: cdc manage-services config "
                + f"--service {service_name} --validate-config"
            )
            print_success("  4. Generate pipelines: cdc generate")
        else:
            print_success(
                "  1. Add CDC tables: cdc manage-services config "
                + f"--service {service_name} "
                + "--add-source-table <schema.table>"
            )
            print_success(
                "  2. Validate: cdc manage-services config "
                + f"--service {service_name} --validate-config"
            )
            print_success("  3. Generate pipelines: cdc generate")


def _extract_schemas_from_source(
    source_config: dict[str, Any],
    default_schema: str = 'dbo',
) -> list[str]:
    """Extract schema list from a source entry."""
    schemas_raw = source_config.get('schemas')
    if isinstance(schemas_raw, list):
        schemas = [str(schema).strip() for schema in schemas_raw if str(schema).strip()]
        if schemas:
            return schemas
    return [default_schema]


def _extract_database_from_source(source_config: dict[str, Any]) -> str | None:
    """Extract preferred database from source entry environments."""
    for env_name, env_config in source_config.items():
        if env_name == 'schemas' or not isinstance(env_config, dict):
            continue
        env_config_dict = cast(dict[str, Any], env_config)
        if env_config_dict.get('server') == 'default' and env_config_dict.get('database'):
            return str(env_config_dict.get('database', '')).strip() or None

    for env_name, env_config in source_config.items():
        if env_name == 'schemas' or not isinstance(env_config, dict):
            continue
        env_config_dict = cast(dict[str, Any], env_config)
        if env_config_dict.get('database'):
            return str(env_config_dict.get('database', '')).strip() or None

    return None


def _find_source_by_database(
    sources: dict[str, Any],
    database_name: str,
) -> dict[str, Any] | None:
    """Find source entry containing the given database in any env."""
    target = database_name.strip()
    if not target:
        return None

    for source_cfg in sources.values():
        if not isinstance(source_cfg, dict):
            continue
        source_dict = cast(dict[str, Any], source_cfg)
        for env_name, env_config in source_dict.items():
            if env_name == 'schemas' or not isinstance(env_config, dict):
                continue
            env_dict = cast(dict[str, Any], env_config)
            database_value = env_dict.get('database')
            if database_value is not None and str(database_value).strip() == target:
                return source_dict

    return None
