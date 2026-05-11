"""Mini JSON Schema generators for keys (service, server_group, database_name, schema_name, table_name)."""

import json
from typing import Any

import yaml  # type: ignore

from cdc_generator.helpers.helpers_logging import print_error, print_info, print_success
from cdc_generator.helpers.service_config import get_project_root


def _get_server_groups(data: object) -> dict[str, dict[str, Any]]:
    """Return server groups from either wrapped or top-level source-groups YAML."""
    if not isinstance(data, dict):
        return {}

    wrapped_groups = data.get("server_group")
    if isinstance(wrapped_groups, dict):
        return {
            str(group_name): group
            for group_name, group in wrapped_groups.items()
            if isinstance(group, dict)
        }

    return {
        str(group_name): group
        for group_name, group in data.items()
        if isinstance(group, dict)
    }


def _build_string_enum_schema(
    schema_id: str,
    title: str,
    description: str,
    values: list[str],
) -> dict[str, Any]:
    """Build a valid string schema, omitting enum when no values were discovered."""
    schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": schema_id,
        "title": title,
        "description": description,
        "type": "string",
    }
    if values:
        schema["enum"] = values
    else:
        schema["description"] = description + " (no values discovered)"
    return schema


def _extract_group_service_names(group_name: str, group: dict[str, Any]) -> set[str]:
    """Extract service names from a server group across supported layouts."""
    services: set[str] = set()
    group_type = group.get("pattern")

    if group_type == "db-per-tenant":
        services.add(group_name)

    sources = group.get("sources")
    if isinstance(sources, dict):
        for source_name in sources.keys():
            if isinstance(source_name, str) and source_name:
                services.add(source_name)

    databases = group.get("databases")
    if isinstance(databases, list):
        for db in databases:
            if not isinstance(db, dict):
                continue
            service_name = db.get("service")
            if isinstance(service_name, str) and service_name:
                services.add(service_name)

    return services


def _extract_group_database_names(group: dict[str, Any]) -> list[str]:
    """Extract database names from a server group across supported layouts."""
    db_names: list[str] = []
    group_type = group.get("pattern")
    database_ref = group.get("database_ref")
    if group_type == "db-per-tenant" and isinstance(database_ref, str) and database_ref:
        db_names.append(database_ref)

    databases = group.get("databases")
    if isinstance(databases, list):
        for db in databases:
            if isinstance(db, str) and db:
                db_names.append(db)
                continue
            if not isinstance(db, dict):
                continue
            db_name = db.get("name")
            if isinstance(db_name, str) and db_name:
                db_names.append(db_name)

    sources = group.get("sources")
    if isinstance(sources, dict):
        for source_cfg in sources.values():
            if not isinstance(source_cfg, dict):
                continue
            for env_cfg in source_cfg.values():
                if not isinstance(env_cfg, dict):
                    continue
                db_name = env_cfg.get("database")
                if isinstance(db_name, str) and db_name:
                    db_names.append(db_name)

    return sorted(set(db_names))


def _extract_group_database_schema_pairs(group: dict[str, Any]) -> list[tuple[str, list[str]]]:
    """Extract database/schema pairs across supported source-group layouts."""
    pairs: list[tuple[str, list[str]]] = []
    group_type = group.get("pattern")
    database_ref = group.get("database_ref")

    databases = group.get("databases")
    if isinstance(databases, list):
        for db in databases:
            if not isinstance(db, dict):
                continue
            db_name = db.get("name")
            schemas = db.get("schemas")
            if isinstance(db_name, str) and db_name and isinstance(schemas, list):
                typed_schemas = [schema for schema in schemas if isinstance(schema, str) and schema]
                if typed_schemas:
                    pairs.append((db_name, typed_schemas))

    sources = group.get("sources")
    if isinstance(sources, dict):
        for source_cfg in sources.values():
            if not isinstance(source_cfg, dict):
                continue
            schemas_obj = source_cfg.get("schemas")
            if not isinstance(schemas_obj, list):
                continue
            schemas = [schema for schema in schemas_obj if isinstance(schema, str) and schema]
            if not schemas:
                continue
            for env_name, env_cfg in source_cfg.items():
                if env_name == "schemas" or not isinstance(env_cfg, dict):
                    continue
                db_name = env_cfg.get("database")
                if isinstance(db_name, str) and db_name:
                    pairs.append((db_name, schemas))

    if group_type == "db-per-tenant" and isinstance(database_ref, str) and database_ref:
        matching_pair = next((pair for pair in pairs if pair[0] == database_ref), None)
        if matching_pair is not None:
            return [matching_pair]

    return pairs


def generate_service_enum_schema() -> bool:
    """Generate mini schema for 'service' key from source-groups.yaml.

    Extracts service names based on pattern:
    - db-per-tenant: uses group name as service name
    - db-shared: uses 'databases[].service' values

    Saves to .vscode/schemas/keys/service.schema.json

    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        project_root = get_project_root()
        server_groups_file = project_root / 'source-groups.yaml'
        if not server_groups_file.exists():
            print_error(f"source-groups.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        services: set[str] = set()
        server_groups = _get_server_groups(data)

        for group_name, group in server_groups.items():
            services.update(_extract_group_service_names(group_name, group))

        # Create the mini schema for service key only
        sorted_services = sorted(list(services))
        schema = _build_string_enum_schema(
            schema_id="service.schema.json",
            title="Service Name Validation",
            description="Valid service names from source-groups.yaml (auto-generated)",
            values=sorted_services,
        )

        # Save to keys directory
        keys_dir = project_root / '.vscode' / 'schemas' / 'keys'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / 'service.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated service mini schema: {output_file}")
        print_info(f"  {len(sorted_services)} services: {', '.join(sorted_services)}")

        return True

    except Exception as e:
        print_error(f"Failed to generate service mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_server_group_enum_schema() -> bool:
    """Generate mini schema for 'server_group' key from source-groups.yaml.

    Extracts all server group names.

    Saves to .vscode/schemas/keys/server_group.schema.json

    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        project_root = get_project_root()
        server_groups_file = project_root / 'source-groups.yaml'
        if not server_groups_file.exists():
            print_error(f"source-groups.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        server_groups = sorted(_get_server_groups(data).keys())

        # Create the mini schema for server_group key only
        schema = _build_string_enum_schema(
            schema_id="server_group.schema.json",
            title="Server Group Validation",
            description="Valid server group names from source-groups.yaml (auto-generated)",
            values=server_groups,
        )

        # Save to keys directory
        keys_dir = project_root / '.vscode' / 'schemas' / 'keys'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / 'server_group.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated server_group mini schema: {output_file}")
        print_info(f"  {len(server_groups)} groups: {', '.join(sorted(server_groups))}")

        return True

    except Exception as e:
        print_error(f"Failed to generate server_group mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_database_name_schemas() -> bool:
    """Generate mini schemas for 'validation_database' per server group.

    Creates one mini schema per server group with database names from that group.

    Saves to .vscode/schemas/keys/database_name/{server_group}.schema.json

    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        project_root = get_project_root()
        server_groups_file = project_root / 'source-groups.yaml'
        if not server_groups_file.exists():
            print_error(f"source-groups.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        keys_dir = project_root / '.vscode' / 'schemas' / 'keys' / 'database_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        generated_count = 0

        for group_name, group in _get_server_groups(data).items():

            db_names = _extract_group_database_names(group)

            if not db_names:
                continue

            # Create mini schema for this server group's databases
            schema: dict[str, Any] = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"database_name/{group_name}.schema.json",
                "title": f"Database Name Validation ({group_name})",
                "description": f"Valid database names from server group '{group_name}' (auto-generated)",
                "type": "string",
                "enum": db_names
            }

            output_file = keys_dir / f'{group_name}.schema.json'
            with open(output_file, 'w') as f:
                json.dump(schema, f, indent=2)

            print_success(f"Generated database_name/{group_name} mini schema: {output_file}")
            print_info(f"  {len(db_names)} databases")
            generated_count += 1

        return generated_count > 0

    except Exception as e:
        print_error(f"Failed to generate database_name mini schemas: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_schema_name_schemas() -> bool:
    """Generate mini schemas for 'schema' field per database.

    For db-per-tenant: Only generates from database_ref (all customer DBs have same schema)
    For db-shared: Generates for all databases
    Optimization: Databases with identical schema lists share a schema file

    Saves to .vscode/schemas/keys/schema_name/{database}.schema.json or shared variants

    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        project_root = get_project_root()
        server_groups_file = project_root / 'source-groups.yaml'
        if not server_groups_file.exists():
            print_error(f"source-groups.yaml not found at {server_groups_file}")
            return False

        with open(server_groups_file) as f:
            data = yaml.safe_load(f)

        keys_dir = project_root / '.vscode' / 'schemas' / 'keys' / 'schema_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        # Group databases by their schema list (sorted tuple for consistent matching)
        schema_groups: dict[tuple[str, ...], list[str]] = {}  # tuple(sorted_schemas) -> [db_names]
        databases_to_generate: list[tuple[str, list[str]]] = []

        # First pass: collect all databases and group by schema list
        for _group_name, group in _get_server_groups(data).items():
            for db_name, schemas in _extract_group_database_schema_pairs(group):
                schema_key: tuple[str, ...] = tuple(sorted(schemas))
                if schema_key not in schema_groups:
                    schema_groups[schema_key] = []
                schema_groups[schema_key].append(db_name)
                databases_to_generate.append((db_name, schemas))

        # Create shared schemas for schema lists used by 2+ databases
        shared_schemas_created: dict[tuple[str, ...], str] = {}  # schema_key -> shared_filename
        shared_dir = keys_dir / 'shared'
        shared_dir.mkdir(parents=True, exist_ok=True)

        for schema_key, db_names in schema_groups.items():
            if len(db_names) >= 2:  # Shared if used by 2+ databases
                # Create a readable filename from schema list
                schemas_list: list[str] = list(schema_key)
                if len(schemas_list) == 1:
                    shared_name = schemas_list[0]
                else:
                    shared_name = '_'.join(schemas_list)

                shared_file = shared_dir / f'{shared_name}.schema.json'
                schema: dict[str, Any] = {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "$id": f"schema_name/shared/{shared_name}.schema.json",
                    "title": f"Schema Name Validation (shared: {', '.join(schemas_list)})",
                    "description": f"Shared schema for databases with schemas: {', '.join(schemas_list)} (used by {len(db_names)} databases)",
                    "type": "string",
                    "enum": schemas_list
                }

                with open(shared_file, 'w') as f:
                    json.dump(schema, f, indent=2)

                shared_schemas_created[schema_key] = shared_name

        # Second pass: generate individual schemas or skip if shared exists
        generated_count = 0
        skipped_for_shared: list[str] = []

        for db_name, schemas in databases_to_generate:
            schema_key = tuple(sorted(schemas))

            # If shared version exists, skip individual file creation
            if schema_key in shared_schemas_created:
                skipped_for_shared.append(db_name)
                # Remove old individual file if it exists
                old_file = keys_dir / f'{db_name}.schema.json'
                if old_file.exists():
                    old_file.unlink()
                continue

            # Create mini schema for this database's schemas (unique schema list)
            schema: dict[str, Any] = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "$id": f"schema_name/{db_name}.schema.json",
                "title": f"Schema Name Validation ({db_name})",
                "description": f"Valid schema names from database '{db_name}' (auto-generated)",
                "type": "string",
                "enum": sorted(schemas)
            }

            output_file = keys_dir / f'{db_name}.schema.json'
            with open(output_file, 'w') as f:
                json.dump(schema, f, indent=2)

            generated_count += 1

        if generated_count > 0 or shared_schemas_created:
            msg = f"Generated {generated_count} schema_name mini schemas"
            if shared_schemas_created:
                shared_names = [name for name in shared_schemas_created.values()]
                msg += f" + {len(shared_schemas_created)} shared ({', '.join(sorted(shared_names))})"
                if skipped_for_shared:
                    msg += f" [replaced {len(skipped_for_shared)} individual schemas]"
            print_success(msg)

        return generated_count > 0 or len(shared_schemas_created) > 0

    except Exception as e:
        print_error(f"Failed to generate schema_name mini schemas: {e}")
        import traceback
        traceback.print_exc()
        return False


def generate_table_names_enum_schema(service: str, schemas_data: dict[str, Any]) -> bool:
    """Generate mini schema for table names (schema.table format) for a service.

    Args:
        service: Service name (e.g., 'adopus', 'proxy')
        schemas_data: Dict[schema_name, Dict[table_name, table_metadata]]

    Saves to .vscode/schemas/keys/table_name/{service}.schema.json

    Returns:
        True if schema generation succeeded, False otherwise
    """
    try:
        # Build list of schema.table qualified names
        table_names: list[str] = []
        for schema_name, tables in schemas_data.items():
            for table_name in tables.keys():
                qualified_name = f"{schema_name}.{table_name}"
                table_names.append(qualified_name)

        if not table_names:
            print_info(f"No tables found for service '{service}'")
            return True

        # Create the mini schema
        schema: dict[str, Any] = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": f"table_name/{service}.schema.json",
            "title": f"Table Name Validation ({service})",
            "description": f"Valid schema-qualified table names for service '{service}' (schema.table format, auto-generated from service-schemas/{service})",
            "type": "string",
            "enum": sorted(table_names)
        }

        # Save to keys/table_name directory
        project_root = get_project_root()
        keys_dir = project_root / '.vscode' / 'schemas' / 'keys' / 'table_name'
        keys_dir.mkdir(parents=True, exist_ok=True)

        output_file = keys_dir / f'{service}.schema.json'
        with open(output_file, 'w') as f:
            json.dump(schema, f, indent=2)

        print_success(f"Generated table_name mini schema: {output_file}")
        print_info(f"  {len(table_names)} tables: {', '.join(sorted(table_names)[:5])}{'...' if len(table_names) > 5 else ''}")

        return True

    except Exception as e:
        print_error(f"Failed to generate table_name mini schema: {e}")
        import traceback
        traceback.print_exc()
        return False
