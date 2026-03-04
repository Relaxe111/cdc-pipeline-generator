#!/usr/bin/env python3
"""
Generate Bento pipeline configurations from templates and customer configs.

Usage:
    python generate_pipelines.py                    # Generate all customers, all environments
    python generate_pipelines.py avansas            # Generate all environments for avansas
    python generate_pipelines.py avansas local      # Generate only local for avansas
    python generate_pipelines.py --list             # List all customers
"""

import argparse
import re
import sys
import traceback
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from cdc_generator.core.column_template_operations import (
    ResolvedTransform,
    resolve_column_templates,
    resolve_transforms,
)
from cdc_generator.core.sink_env_routing import resolve_sink_env_key
from cdc_generator.core.source_ref_resolver import parse_source_ref, resolve_source_ref
from cdc_generator.helpers.helpers_batch import build_staging_case
from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import (
    get_all_customers,
    get_project_root,
    load_customer_config,
    load_service_config,
)
from cdc_generator.helpers.yaml_loader import ConfigValue, load_yaml_file
from cdc_generator.validators.bloblang_parser import extract_root_assignments

# Import validation functions
from cdc_generator.validators.manage_service.validation import validate_service_config


def get_services_for_customers(customers: list[str]) -> set[str]:
    """Determine which services are used by the given customers.

    Returns set of service names that need to be validated.
    """
    services: set[str] = set()
    services_dir = get_project_root() / "services"

    if not services_dir.exists():
        return services

    target_customers = {customer.casefold() for customer in customers}

    # Check each service file to see if it contains any of our customers
    for service_file in services_dir.glob("*.yaml"):
        try:
            service_config = load_service_config(service_file.stem)
            service_customers = service_config.get('customers', [])
            if not isinstance(service_customers, list):
                continue
            service_customers_list = cast(list[ConfigValue], service_customers)

            customer_names: set[str] = set()
            customer_value: ConfigValue
            for customer_value in service_customers_list:
                if not isinstance(customer_value, dict):
                    continue
                customer_dict = cast(dict[str, ConfigValue], customer_value)
                name = customer_dict.get('name')
                if isinstance(name, str):
                    customer_names.add(name.casefold())

            # If any of our target customers are in this service, include it
            if target_customers.intersection(customer_names):
                services.add(service_file.stem)
        except Exception:
            continue

    return services


def normalize_for_comparison(content: str) -> str:
    """Remove timestamp line from content for comparison purposes."""
    lines = content.split('\n')
    normalized_lines = [
        line for line in lines
        if not line.startswith('# Generated:')
    ]
    return '\n'.join(normalized_lines)


def should_write_file(file_path: Path, new_content: str) -> bool:
    """Check if file should be written by comparing content (ignoring timestamp)."""
    if not file_path.exists():
        return True  # New file, write it

    existing_content = file_path.read_text()
    return normalize_for_comparison(existing_content) != normalize_for_comparison(new_content)


def preserve_env_vars(value: object) -> str:
    """Preserve environment variable placeholders for runtime resolution by Bento.

    Also provides backward compatibility by converting old ${env:VAR} format to ${VAR}.
    """
    if isinstance(value, str):
        # Backward compatibility: convert ${env:VAR} to ${VAR} if found
        return re.sub(r'\$\{env:([A-Z_][A-Z0-9_]*)\}', r'${\1}', value)
    return str(value) if value is not None else ""

PROJECT_ROOT = get_project_root()
TEMPLATES_DIR = PROJECT_ROOT / "pipelines" / "templates"
SERVICES_DIR = PROJECT_ROOT / "services"

PIPELINES_GENERATED_DIR = PROJECT_ROOT / "pipelines" / "generated"
GENERATED_SOURCES_DIR = PIPELINES_GENERATED_DIR / "sources"
GENERATED_SINKS_DIR = PIPELINES_GENERATED_DIR / "sinks"
SINK_REF_PARTS_COUNT = 2


def load_template(template_name: str) -> str:
    """Load a template file as string."""
    template_path = TEMPLATES_DIR / template_name
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text()


def load_generated_table_definitions() -> dict[str, Any]:
    """Load table definitions from canonical services/_schemas/ tree.

    Returns a table-name keyed dict with normalized ``fields`` entries:
        {
            "Actor": {
                "fields": [{"mssql": "[actno]", "postgres": "actno"}, ...]
            }
        }
    """
    generated_dir = SERVICES_DIR / '_schemas'

    if not generated_dir.exists():
        return {}

    tables_by_name: dict[str, Any] = {}
    for yaml_file in sorted(generated_dir.rglob('*.yaml')):
        path_parts = set(yaml_file.parts)
        if '_definitions' in path_parts or '_bloblang' in path_parts or 'adapters' in path_parts:
            continue

        table_def = load_yaml_file(yaml_file)
        if table_def and table_def:
            table_def_dict = cast(dict[str, object], table_def)
            table_name = table_def_dict.get('table')
            columns = table_def_dict.get('columns')
            if not isinstance(table_name, str) or not isinstance(columns, list):
                continue
            columns_list = cast(list[object], columns)

            fields: list[dict[str, str]] = []
            col_raw: object
            for col_raw in columns_list:
                if not isinstance(col_raw, dict):
                    continue
                col = cast(dict[str, Any], col_raw)
                col_name_raw = col.get('name')
                if not isinstance(col_name_raw, str) or not col_name_raw:
                    continue
                postgres_name = normalize_table_name(col_name_raw)
                fields.append(
                    {
                        'mssql': f'[{col_name_raw}]',
                        'postgres': postgres_name,
                    },
                )

            if fields:
                tables_by_name[table_name] = {
                    'name': table_name,
                    'fields': fields,
                }

    return tables_by_name


def get_primary_key_from_schema(
    service_name: str,
    schema_name: str,
    table_name: str,
) -> tuple[str | list[str] | None, str | None]:
    """Read primary_key from canonical service schema table definition.

    Returns:
        tuple: (primary_key, source) where source is 'schema' or None
    """
    table_schema_path = SERVICES_DIR / '_schemas' / service_name / schema_name / f'{table_name}.yaml'

    if not table_schema_path.exists():
        raise ValueError(
            "Missing table schema definition for primary key resolution: "
            + f"{table_schema_path}. "
            + "Generation requires services/_schemas/{service}/{schema}/{Table}.yaml "
            + "with a valid primary_key."
        )

    table_def_dict = cast(dict[str, Any], load_yaml_file(table_schema_path))

    # 1) Prefer explicit top-level primary_key
    top_level_pk = table_def_dict.get('primary_key')
    if isinstance(top_level_pk, str) and top_level_pk:
        return top_level_pk, 'schema'
    if isinstance(top_level_pk, list):
        top_level_pk_list = cast(list[object], top_level_pk)
        pk_list = [str(pk) for pk in top_level_pk_list if str(pk)]
        if pk_list:
            return pk_list, 'schema'

    # 2) Fallback to columns[].primary_key flags in schema file
    columns = table_def_dict.get('columns', [])
    pk_columns: list[str] = []
    if isinstance(columns, list):
        columns_list = cast(list[object], columns)
        col: object
        for col in columns_list:
            if not isinstance(col, dict):
                continue
            col_dict = cast(dict[str, Any], col)
            if col_dict.get('primary_key') is True:
                col_name = col_dict.get('name')
                if isinstance(col_name, str) and col_name:
                    pk_columns.append(col_name)

    if pk_columns:
        if len(pk_columns) == 1:
            return pk_columns[0], 'schema'
        return pk_columns, 'schema'

    raise ValueError(
        "Missing primary key metadata in schema definition: "
        + f"{table_schema_path}. "
        + "Add top-level primary_key or mark columns with primary_key: true."
    )


def substitute_variables(template: str, variables: dict[str, Any]) -> str:
    """Replace {{VAR}} placeholders with values from variables dict."""
    result = template
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        result = result.replace(placeholder, str(value))
    return result


def resolve_postgres_url_from_sink_groups(
    service_name: str,
    env_name: str,
    target_sink_env: str | None = None,
) -> str:
    """Build consolidated sink PostgreSQL URL from sink-groups.yaml.

    Uses service sinks reference like ``sink_asma.directory`` and resolves
    server credentials + environment database from sink-groups.
    """
    service_cfg = load_service_config(service_name)
    sinks_raw = service_cfg.get('sinks', {})
    if not isinstance(sinks_raw, dict) or not sinks_raw:
        raise ValueError(
            f"Service '{service_name}' has no sinks configuration; cannot resolve sink DSN"
        )
    sinks = cast(dict[str, Any], sinks_raw)

    sink_ref = str(next(iter(sinks)))
    sink_ref_parts = sink_ref.split('.', 1)
    if len(sink_ref_parts) != SINK_REF_PARTS_COUNT:
        raise ValueError(
            f"Invalid sink reference '{sink_ref}' in service '{service_name}'. "
            + "Expected format '<sink_group>.<source>'"
        )

    sink_group_name, sink_source_name = sink_ref_parts

    sink_groups_path = PROJECT_ROOT / 'sink-groups.yaml'
    if not sink_groups_path.exists():
        raise ValueError(f"Missing sink-groups.yaml at {sink_groups_path}")

    sink_groups_data = cast(dict[str, Any], load_yaml_file(sink_groups_path))
    sink_group_raw = sink_groups_data.get(sink_group_name)
    if not isinstance(sink_group_raw, dict):
        raise ValueError(
            f"Sink group '{sink_group_name}' not found in sink-groups.yaml"
        )

    sink_group = cast(dict[str, Any], sink_group_raw)
    sources_raw = sink_group.get('sources', {})
    if not isinstance(sources_raw, dict):
        raise ValueError(
            f"Sink group '{sink_group_name}' has invalid 'sources' section"
        )

    sources = cast(dict[str, Any], sources_raw)
    source_raw = sources.get(sink_source_name)
    if not isinstance(source_raw, dict):
        raise ValueError(
            f"Sink source '{sink_source_name}' not found in sink group '{sink_group_name}'"
        )

    source_cfg = cast(dict[str, Any], source_raw)
    source_env_key = resolve_sink_env_key(
        source_cfg,
        env_name,
        target_sink_env=target_sink_env,
    )
    source_env_raw = source_cfg.get(source_env_key)
    if not isinstance(source_env_raw, dict):
        raise ValueError(
            f"Invalid sink source env config for '{sink_group_name}.{sink_source_name}.{source_env_key}'"
        )

    source_env_cfg = cast(dict[str, Any], source_env_raw)
    server_name = source_env_cfg.get('server')
    database_name = source_env_cfg.get('database')
    if not isinstance(server_name, str) or not isinstance(database_name, str):
        raise ValueError(
            f"Missing server/database for sink source '{sink_group_name}.{sink_source_name}.{source_env_key}'"
        )

    servers_raw = sink_group.get('servers', {})
    if not isinstance(servers_raw, dict):
        raise ValueError(f"Sink group '{sink_group_name}' has invalid 'servers' section")
    servers = cast(dict[str, Any], servers_raw)
    server_cfg_raw = servers.get(server_name)
    if not isinstance(server_cfg_raw, dict):
        raise ValueError(
            f"Server '{server_name}' not found in sink group '{sink_group_name}'"
        )

    server_cfg = cast(dict[str, Any], server_cfg_raw)

    host_raw = server_cfg.get('host')
    port_raw = server_cfg.get('port')
    user_raw = server_cfg.get('user')
    password_raw = server_cfg.get('password')

    if host_raw in (None, '') or port_raw in (None, '') or user_raw in (None, '') or password_raw in (None, ''):
        raise ValueError(
            f"Missing sink server credentials for '{sink_group_name}.{server_name}'. "
            + "Required: host, port, user, password"
        )

    host = preserve_env_vars(host_raw)
    port = preserve_env_vars(port_raw)
    user = preserve_env_vars(user_raw)
    password = preserve_env_vars(password_raw)
    database = preserve_env_vars(database_name)

    return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=disable"


def _load_source_groups_config() -> dict[str, Any]:
    source_groups_path = PROJECT_ROOT / 'source-groups.yaml'
    if not source_groups_path.exists():
        return {}
    return cast(dict[str, Any], load_yaml_file(source_groups_path))


def _find_source_name_case_insensitive(group_name: str, customer_name: str) -> str | None:
    source_groups = _load_source_groups_config()
    group_raw = source_groups.get(group_name)
    if not isinstance(group_raw, dict):
        return None

    group_cfg = cast(dict[str, Any], group_raw)
    sources_raw = group_cfg.get('sources', {})
    if not isinstance(sources_raw, dict):
        return None

    sources = cast(dict[str, Any], sources_raw)
    target = customer_name.casefold()
    for source_name in sources:
        if str(source_name).casefold() == target:
            return str(source_name)
    return None


def _escape_bloblang_string(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"')


def _resolve_template_expr(
    template_value: str,
    value_source: str,
    customer_name: str,
    env_name: str,
    server_group_name: str,
) -> str:
    if value_source == 'source_ref':
        parsed_ref = parse_source_ref(template_value)
        if parsed_ref is None:
            raise ValueError(f"Invalid source reference in column template: {template_value}")

        source_name = _find_source_name_case_insensitive(server_group_name, customer_name)
        if not source_name:
            raise ValueError(
                f"Could not resolve source name for customer '{customer_name}' in group '{server_group_name}'"
            )

        resolved = resolve_source_ref(
            parsed_ref,
            source_name=source_name,
            env=env_name,
            config=_load_source_groups_config(),
        )
        resolved_text = str(resolved).strip()
        if resolved_text.casefold() in {'none', 'null', ''}:
            raise ValueError(
                "Source reference resolved to null/empty value: "
                + f"{template_value} for customer '{customer_name}' env '{env_name}'. "
                + "Populate source-groups.yaml with concrete per-source value before generation."
            )
        return f'"{_escape_bloblang_string(resolved_text)}"'

    if value_source == 'env':
        env_match = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", template_value.strip())
        if env_match:
            return f'env("{env_match.group(1)}")'
        return f'"{_escape_bloblang_string(template_value)}"'

    if value_source == 'sql':
        lowered = template_value.strip().casefold()
        if lowered == 'now()':
            return 'now()'
        if lowered == 'gen_random_uuid()':
            return 'uuid_v4()'
        return f'"{_escape_bloblang_string(template_value)}"'

    return template_value


def _build_sink_table_enrichment(
    table_cfg: dict[str, Any],
    customer_name: str,
    env_name: str,
    server_group_name: str,
) -> tuple[list[str], list[str], list[str]]:
    extra_columns: list[str] = []
    extra_args: list[str] = []
    processor_steps: list[str] = []

    for resolved_template in resolve_column_templates(cast(dict[str, object], table_cfg)):
        extra_columns.append(resolved_template.name)
        extra_args.append(f'this.{resolved_template.name}')
        expr = _resolve_template_expr(
            template_value=resolved_template.value,
            value_source=resolved_template.template.value_source,
            customer_name=customer_name,
            env_name=env_name,
            server_group_name=server_group_name,
        )
        processor_steps.append(f'root.{resolved_template.name} = {expr}')

    for resolved_transform in resolve_transforms(cast(dict[str, object], table_cfg)):
        for output_name in sorted(extract_root_assignments(resolved_transform.bloblang)):
            if output_name not in extra_columns:
                extra_columns.append(output_name)
                extra_args.append(f'this.{output_name}')
        if resolved_transform.execution_stage == 'sink':
            processor_steps.append(resolved_transform.bloblang)

    return extra_columns, extra_args, processor_steps


def _select_sink_table_cfg_for_source(
    service_cfg: dict[str, Any],
    source_table: str,
) -> dict[str, Any] | None:
    sinks_raw = service_cfg.get('sinks', {})
    if not isinstance(sinks_raw, dict):
        return None

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return None

    sink_root_raw = sinks.get(next(iter(sinks)))
    if not isinstance(sink_root_raw, dict):
        return None

    sink_root = cast(dict[str, Any], sink_root_raw)
    tables_raw = sink_root.get('tables', {})
    if not isinstance(tables_raw, dict):
        return None

    tables = cast(dict[str, Any], tables_raw)
    exact_match: dict[str, Any] | None = None
    fallback: dict[str, Any] | None = None

    for sink_table_key, sink_table_cfg_raw in tables.items():
        if not isinstance(sink_table_cfg_raw, dict):
            continue
        sink_table_cfg = cast(dict[str, Any], sink_table_cfg_raw)
        from_ref = str(sink_table_cfg.get('from', '')).strip()
        if not from_ref:
            continue

        source_ref_table = from_ref.split('.', 1)[1] if '.' in from_ref else from_ref
        if source_ref_table.casefold() != source_table.casefold():
            continue

        fallback = sink_table_cfg
        target_table = str(sink_table_key).split('.', 1)[1] if '.' in str(sink_table_key) else str(sink_table_key)
        if target_table.casefold() == source_table.casefold():
            exact_match = sink_table_cfg
            break

    return exact_match if exact_match is not None else fallback


def _collect_sink_table_cfgs_for_source(
    service_cfg: dict[str, Any],
    source_table: str,
) -> list[dict[str, Any]]:
    sinks_raw = service_cfg.get('sinks', {})
    if not isinstance(sinks_raw, dict):
        return []

    sinks = cast(dict[str, Any], sinks_raw)
    if not sinks:
        return []

    sink_root_raw = sinks.get(next(iter(sinks)))
    if not isinstance(sink_root_raw, dict):
        return []

    sink_root = cast(dict[str, Any], sink_root_raw)
    tables_raw = sink_root.get('tables', {})
    if not isinstance(tables_raw, dict):
        return []

    tables = cast(dict[str, Any], tables_raw)
    matching: list[dict[str, Any]] = []
    for sink_table_cfg_raw in tables.values():
        if not isinstance(sink_table_cfg_raw, dict):
            continue
        sink_table_cfg = cast(dict[str, Any], sink_table_cfg_raw)
        from_ref = str(sink_table_cfg.get('from', '')).strip()
        if not from_ref:
            continue
        source_ref_table = from_ref.split('.', 1)[1] if '.' in from_ref else from_ref
        if source_ref_table.casefold() == source_table.casefold():
            matching.append(sink_table_cfg)

    return matching


def _build_runtime_processor_case(
    schema_name: str,
    table_name: str,
    processor_steps: Iterable[str],
) -> str:
    steps = list(processor_steps)
    if not steps:
        return ""

    bloblang_parts = "\n".join(f"        {step}" for step in steps)
    return (
        f'- check: \'this.__routing_schema == "{schema_name}" && '
        + f'this.__routing_table == "{table_name}"\'\n'
        + '  processors:\n'
        + '    - bloblang: |\n'
        + bloblang_parts
    )


def _build_runtime_processors_block(processor_cases: list[str]) -> str:
    if not processor_cases:
        return ""

    indented_cases = "\n".join(
        "          " + processor_case.replace("\n", "\n          ")
        for processor_case in processor_cases
    )
    return "- switch:\n        cases:\n" + indented_cases


def _build_source_transform_processors(
    table_cfgs: list[dict[str, Any]],
) -> str:
    if not table_cfgs:
        return ""

    source_stage: list[ResolvedTransform] = []
    seen_refs: set[str] = set()
    for table_cfg in table_cfgs:
        for transform in resolve_transforms(cast(dict[str, object], table_cfg)):
            if transform.execution_stage != 'source':
                continue
            if transform.bloblang_ref in seen_refs:
                continue
            seen_refs.add(transform.bloblang_ref)
            source_stage.append(transform)

    if not source_stage:
        return ""

    blocks: list[str] = [
        "    # Apply source-stage transforms configured for this table",
    ]

    for transform in source_stage:
        bloblang = transform.bloblang.replace("\n", "\n        ")
        blocks.append("    - bloblang: |\n        " + bloblang)

    blocks.extend([
        "    # Normalize transformed output to an array and split into messages",
        "    - bloblang: 'root = if this.type() == \"array\" { this } else { [ this ] }'",
        "    - unarchive:",
        "        format: json_array",
    ])

    return "\n" + "\n".join(blocks)


def generate_customer_pipelines(
    customer: str,
    environments: list[str] | None = None,
) -> None:
    """Generate Bento SOURCE pipeline config for a customer.

    Note: Sink pipelines are generated separately as consolidated per-environment files.
    """
    print(f"\n📦 Customer: {customer}")
    print("-" * 60)

    # Load customer config
    try:
        config = load_customer_config(customer)
    except FileNotFoundError as e:
        print(f"   ✗ {e}")
        return

    customer_name = config.get("customer", customer)
    schema = config.get("schema", customer)
    service_name = str(config.get('service', '')).strip()
    service_cfg = load_service_config(service_name) if service_name else {}

    # Load generated table definitions
    generated_tables = load_generated_table_definitions()

    # Load source template only (sink is consolidated)
    source_template = load_template("source-pipeline.yaml")

    # Determine which environments to generate
    env_configs = config.get("environments", {})
    if environments:
        env_configs = {k: v for k, v in env_configs.items() if k in environments}

    if not env_configs:
        print("   ⚠️  No environments configured")
        return

    # Generate for each environment
    for env_name, env_config in env_configs.items():
        print(f"\n   🌍 Environment: {env_name}")

        # Build variable substitution map
        variables = {
            "CUSTOMER": customer_name,
            "ENV": env_name,
            "SCHEMA": schema,
            "DATABASE_NAME": preserve_env_vars(env_config.get("database_name", customer_name)),
            "TOPIC_PREFIX": preserve_env_vars(
                env_config.get("topic_prefix", f"{env_name}-{customer_name}")
            ),

            # MSSQL connection (preserves ${VAR} format if present)
            "MSSQL_HOST": preserve_env_vars(env_config.get("mssql", {}).get("host", "localhost")),
            "MSSQL_PORT": preserve_env_vars(env_config.get("mssql", {}).get("port", 1433)),
            "MSSQL_USER": preserve_env_vars(env_config.get("mssql", {}).get("user", "sa")),
            "MSSQL_PASSWORD": preserve_env_vars(
                env_config.get("mssql", {}).get("password", "password")
            ),

            # PostgreSQL connection
            "POSTGRES_URL": preserve_env_vars(
                env_config.get("postgres", {}).get(
                    "url",
                    (
                        "postgres://postgres:postgres@postgres:5432/"
                        "consolidated_db?sslmode=disable"
                    ),
                )
            ),

            # Kafka/Redpanda
            "KAFKA_BOOTSTRAP_SERVERS": preserve_env_vars(
                env_config.get("kafka", {}).get(
                    "bootstrap_servers",
                    env_config.get("kafka_bootstrap_servers", "localhost:19092"),
                )
            ),

            # CDC Tables - build comma-separated list for table.include.list
            "TABLE_INCLUDE_LIST": build_table_include_list(config),

            # Sink topics - dynamic list based on configured tables
            "SINK_TOPICS": build_sink_topics(
                config,
                preserve_env_vars(env_config.get("topic_prefix", f"{env_name}-{customer_name}")),
                generated_tables
            ),
        }

        # Build source table inputs for multi-table CDC polling
        source_inputs = build_source_table_inputs(config, variables, generated_tables, service_cfg)
        variables["SOURCE_TABLE_INPUTS"] = source_inputs

        # Build table routing map for main pipeline (pass service name for schema lookup)
        service_name = config.get('service', 'adopus')  # Default to adopus for now
        table_routing = build_table_routing_map(service_name, config, variables, generated_tables)
        variables["TABLE_ROUTING"] = table_routing

        # Substitute variables in source template only
        source_pipeline = substitute_variables(source_template, variables)

        # Create output directory for customer source
        output_dir = GENERATED_SOURCES_DIR / env_name / customer_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write source pipeline file with DO NOT EDIT warning
        source_path = output_dir / "source-pipeline.yaml"

        header = """# ============================================================================
# DO NOT EDIT THIS FILE - IT IS AUTO-GENERATED
# ============================================================================
# This file is automatically generated from templates and customer configs
#
# To make changes:
#   1. Edit source files: services/<service>.yaml and pipelines/templates/
#   2. Run: cdc manage-pipelines generate --all
#
# Customer: {customer}
# Environment: {env}
# Generated: {timestamp}
# ============================================================================

"""

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header_formatted = header.format(customer=customer_name, env=env_name, timestamp=timestamp)

        source_content = header_formatted + source_pipeline

        # Only write files if content has actually changed (ignoring timestamp)
        if should_write_file(source_path, source_content):
            source_path.write_text(source_content)
            print(f"      ✓ Generated: {source_path.relative_to(PROJECT_ROOT)}")
        else:
            print(f"      ⊘ Unchanged: {source_path.relative_to(PROJECT_ROOT)}")


def _load_customer_env_config(
    customer: str,
    env_name: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load customer config and selected environment config."""
    try:
        config = load_customer_config(customer)
    except FileNotFoundError:
        return None, None

    envs_raw = config.get("environments", {})
    envs = cast(dict[str, Any], envs_raw) if isinstance(envs_raw, dict) else {}
    env_config_raw = envs.get(env_name)
    if not isinstance(env_config_raw, dict):
        return config, None

    return config, cast(dict[str, Any], env_config_raw)


def _build_customer_consolidated_routes(
    customer: str,
    env_name: str,
    schema_name: str,
    config: dict[str, Any],
    env_config: dict[str, Any],
    postgres_url: str,
    generated_tables: dict[str, Any],
) -> tuple[list[str], list[str], list[str], int, list[tuple[str, str]], int]:
    """Build sink topics, table cases and runtime processors for one customer."""
    topics: list[str] = []
    table_cases: list[str] = []
    runtime_cases: list[str] = []
    customer_generated_routes = 0
    skipped_routes = 0
    skipped_entries: list[tuple[str, str]] = []

    topic_prefix = preserve_env_vars(env_config.get("topic_prefix", f"{env_name}.{customer}"))
    source_tables_raw = config.get("cdc_tables", [])
    source_tables = cast(list[dict[str, Any]], source_tables_raw) if isinstance(source_tables_raw, list) else []
    service_name = str(config.get("service", ""))
    service_cfg = load_service_config(service_name) if service_name else {}
    server_group_name = str(config.get("server_group", service_name))

    for table_config in source_tables:
        table_name = str(table_config.get("table", "")).strip()
        if not table_name:
            continue
        table_schema = str(table_config.get("schema", "dbo"))

        table_def = generated_tables.get(table_name)
        if not table_def or "fields" not in table_def:
            print(f"   [WARNING] No generated field metadata for {table_name} - skipping")
            continue

        fields = table_def["fields"]
        mssql_fields = [field["mssql"] for field in fields]
        postgres_fields = [field["postgres"] for field in fields]

        sink_table_cfg = _select_sink_table_cfg_for_source(service_cfg, table_name)
        extra_columns: list[str] = []
        extra_args: list[str] = []
        processor_steps: list[str] = []
        if sink_table_cfg is not None:
            try:
                extra_columns, extra_args, processor_steps = _build_sink_table_enrichment(
                    sink_table_cfg,
                    customer_name=customer,
                    env_name=env_name,
                    server_group_name=server_group_name,
                )
            except ValueError as enrichment_error:
                skipped_routes += 1
                skipped_entries.append((f"{table_schema}.{table_name}", str(enrichment_error)))
                continue

        runtime_case = _build_runtime_processor_case(
            schema_name=schema_name,
            table_name=table_name,
            processor_steps=processor_steps,
        )
        if runtime_case:
            runtime_cases.append(runtime_case)

        table_cases.append(
            build_staging_case(
                table_name=table_name,
                schema=schema_name,
                postgres_url=postgres_url,
                postgres_fields=postgres_fields,
                mssql_fields=mssql_fields,
                extra_columns=extra_columns,
                extra_args=extra_args,
            )
        )
        customer_generated_routes += 1
        topics.append(f'"{topic_prefix}.{table_schema}.{table_name}"')

    return topics, table_cases, runtime_cases, customer_generated_routes, skipped_entries, skipped_routes


def _print_customer_skip_summary(
    customer: str,
    env_name: str,
    customer_generated_routes: int,
    skipped_entries: list[tuple[str, str]],
) -> None:
    """Print consolidated summary for skipped customer routes."""
    if not skipped_entries:
        return

    first_table, first_reason = skipped_entries[0]
    if customer_generated_routes == 0:
        print_error(
            f"Skipping customer '{customer}' for env '{env_name}' due to unresolved source refs "
            + f"({len(skipped_entries)} table routes skipped). "
            + f"First issue [{first_table}]: {first_reason}"
        )
        return

    print_error(
        f"Customer '{customer}' in env '{env_name}': skipped {len(skipped_entries)} table routes "
        + f"due to unresolved source refs. First issue [{first_table}]: {first_reason}"
    )


def _render_consolidated_sink_content(
    env_name: str,
    customers: list[str],
    all_topics: list[str],
    all_table_cases: list[str],
    runtime_processor_cases: list[str],
    sink_template: str,
) -> str:
    """Render consolidated sink content with generated header and substitutions."""
    topics_yaml = "\n".join([f"      - {topic}" for topic in all_topics])
    table_cases_yaml = "\n".join(all_table_cases).replace("\n", "\n      ")

    variables = {
        "ENV": env_name,
        "SINK_TOPICS": "\n" + topics_yaml,
        "TABLE_CASES": table_cases_yaml,
        "SINK_RUNTIME_PROCESSORS": _build_runtime_processors_block(runtime_processor_cases),
    }
    sink_pipeline = substitute_variables(sink_template, variables)

    header = """# ============================================================================
# DO NOT EDIT THIS FILE - IT IS AUTO-GENERATED
# ============================================================================
# CONSOLIDATED SINK - Handles ALL customers for this environment
#
# To make changes:
#   1. Edit source files: services/<service>.yaml
#   2. Edit template: pipelines/templates/sink-pipeline.yaml
#   3. Run: cdc manage-pipelines generate --all
#
# Environment: {env}
# Customers: {customers}
# Generated: {timestamp}
# ============================================================================

"""

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    customer_list = ", ".join(
        sorted([
            customer
            for customer in customers
            if load_customer_config(customer).get("environments", {}).get(env_name)
        ])
    )
    header_formatted = header.format(env=env_name, customers=customer_list, timestamp=timestamp)
    return header_formatted + sink_pipeline


def _print_consolidated_sink_skip_summary(
    skipped_routes: int,
    skipped_customers: int,
) -> None:
    """Print post-generation skip summary for consolidated sink."""
    if skipped_routes:
        print_error(f"   ⚠ Skipped routes due to unresolved source refs: {skipped_routes}")
    if skipped_customers:
        print_error(f"   ⚠ Skipped customers due to invalid sink routing: {skipped_customers}")


def generate_consolidated_sink(
    env_name: str,
    customers: list[str] | None = None,
) -> None:
    """Generate a single consolidated sink pipeline for an environment.

    Aggregates all customer topics and table routing into one sink per environment.
    """
    print(f"\n🔗 Consolidated Sink: {env_name}")
    print("-" * 60)

    if customers is None:
        customers = get_all_customers()

    # Load generated table definitions
    generated_tables = load_generated_table_definitions()

    # Load sink template
    sink_template = load_template("sink-pipeline.yaml")

    # Aggregate all topics and table cases across customers
    all_topics: list[str] = []
    all_table_cases: list[str] = []
    runtime_processor_cases: list[str] = []
    postgres_url = None  # Resolved from sink-groups (preferred) or customer config fallback
    skipped_routes = 0
    skipped_customers = 0

    for customer in customers:
        config, env_config = _load_customer_env_config(customer, env_name)
        if config is None or env_config is None:
            continue

        schema = str(config.get("schema", customer))
        service_name = str(config.get("service", ""))

        try:
            customer_postgres_url = _resolve_consolidated_postgres_url(
                service_name=service_name,
                customer=customer,
                env_name=env_name,
                env_config=env_config,
            )
        except ValueError as sink_route_error:
            skipped_customers += 1
            cdc_tables = cast(list[dict[str, Any]], config.get("cdc_tables", []))
            skipped_routes += len(cdc_tables)
            print_error(
                f"Skipping customer '{customer}' for env '{env_name}' due to sink routing error: "
                + str(sink_route_error)
            )
            continue

        if postgres_url is None:
            postgres_url = customer_postgres_url
        elif postgres_url != customer_postgres_url:
            skipped_customers += 1
            cdc_tables = cast(list[dict[str, Any]], config.get("cdc_tables", []))
            skipped_routes += len(cdc_tables)
            print_error(
                f"Skipping customer '{customer}' for env '{env_name}' due to mismatched sink route target. "
                + "Consolidated sink requires one resolved sink target per environment."
            )
            continue

        (
            customer_topics,
            customer_table_cases,
            customer_runtime_cases,
            customer_generated_routes,
            customer_skipped,
            customer_skipped_routes,
        ) = _build_customer_consolidated_routes(
            customer=customer,
            env_name=env_name,
            schema_name=schema,
            config=config,
            env_config=env_config,
            postgres_url=postgres_url,
            generated_tables=generated_tables,
        )
        all_topics.extend(customer_topics)
        all_table_cases.extend(customer_table_cases)
        runtime_processor_cases.extend(customer_runtime_cases)
        skipped_routes += customer_skipped_routes
        _print_customer_skip_summary(customer, env_name, customer_generated_routes, customer_skipped)

    if not all_topics:
        print(f"   ⚠️  No customers configured for environment {env_name}")
        return

    output_dir = GENERATED_SINKS_DIR / env_name
    output_dir.mkdir(parents=True, exist_ok=True)
    sink_path = output_dir / "sink-pipeline.yaml"
    sink_content = _render_consolidated_sink_content(
        env_name=env_name,
        customers=customers,
        all_topics=all_topics,
        all_table_cases=all_table_cases,
        runtime_processor_cases=runtime_processor_cases,
        sink_template=sink_template,
    )

    if should_write_file(sink_path, sink_content):
        sink_path.write_text(sink_content)
        print(f"   ✓ Generated: {sink_path.relative_to(PROJECT_ROOT)}")
        print(f"   📋 Topics: {len(all_topics)}, Table Routes: {len(all_table_cases)}")
        _print_consolidated_sink_skip_summary(skipped_routes, skipped_customers)
    else:
        print(f"   ⊘ Unchanged: {sink_path.relative_to(PROJECT_ROOT)}")
        _print_consolidated_sink_skip_summary(skipped_routes, skipped_customers)


def build_table_include_list(config: dict[str, Any]) -> str:
    """Build comma-separated table include list from CDC tables config."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        # Default fallback
        return "dbo.Actor,dbo.AdgangLinjer"

    # Build list: schema.table
    table_list = [f"{t['schema']}.{t['table']}" for t in source_tables]
    return ",".join(table_list)


def build_sink_topics(
    config: dict[str, Any],
    topic_prefix: str,
    generated_tables: dict[str, Any],
) -> str:
    """Build comma-separated list of Kafka topics for the sink to subscribe to."""
    source_tables = config.get('cdc_tables', [])

    # topics list for yaml
    topics: list[str] = []

    if not source_tables:
        # Default fallback topics
        topics = [
            f"{topic_prefix}.dbo.Actor",
            f"{topic_prefix}.dbo.AdgangLinjer"
        ]
    else:
        for t in source_tables:
            table_name = t['table']
            schema = t.get('schema', 'dbo')

            # Use generated metadata if available
            table_def = generated_tables.get(table_name)
            if table_def and 'topic_format' in table_def:
                topic = table_def['topic_format'].replace("{TOPIC_PREFIX}", topic_prefix)
                topics.append(topic)
            else:
                # Fallback to standard format
                topics.append(f"{topic_prefix}.{schema}.{table_name}")

    # Format for YAML list
    return "\n".join([f"      - \"{topic}\"" for topic in topics]).strip()


def build_source_table_inputs(
    config: dict[str, Any],
    variables: dict[str, Any],
    generated_tables: dict[str, Any],
    service_cfg: dict[str, Any] | None = None,
) -> str:
    """Build generate + sql_raw inputs for continuous CDC polling with LSN tracking."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    inputs: list[str] = []
    # Build DSN using variables from customer config (preserves env var references)
    mssql_user = variables['MSSQL_USER']
    mssql_password = variables['MSSQL_PASSWORD']
    mssql_host = variables['MSSQL_HOST']
    mssql_port = variables['MSSQL_PORT']
    mssql_database = variables['DATABASE_NAME']
    dsn = f"sqlserver://{mssql_user}:{mssql_password}@{mssql_host}:{mssql_port}?database={mssql_database}"

    for table_config in source_tables:
        table_name = table_config['table']
        schema = table_config.get('schema', 'dbo')

        sink_table_cfgs = _collect_sink_table_cfgs_for_source(service_cfg or {}, table_name)
        source_transform_processors = _build_source_transform_processors(sink_table_cfgs)

        # Get field definitions from generated table metadata
        table_def = generated_tables.get(table_name)
        if not table_def or 'fields' not in table_def:
            print(f"[WARNING] No generated field metadata for {table_name} - skipping source input")
            continue

        fields = table_def['fields']

        # Build column list with CDC metadata columns (with brackets for MSSQL)
        columns = [
            '"[__$start_lsn]"',
            '"[__$end_lsn]"',
            '"[__$seqval]"',
            '"[__$operation]"',
            '"[__$update_mask]"',
        ]

        # Add all business columns from table definition
        for field in fields:
            mssql_col = field['mssql']
            columns.append(f'"{mssql_col}"')

        # Build using generate input with sql_raw processor for LSN-filtered CDC polling
        table_label = table_name.lower()
        cache_key = f"{table_label}_last_lsn"

        # Build column list for SELECT - convert LSN to hex string to avoid encoding
        # Use CONVERT(VARCHAR(22), [__$start_lsn], 1) to get '0x' prefixed hex string
        select_columns = (
            "CONVERT(VARCHAR(22), [__$start_lsn], 1) AS __lsn_hex, "
            "[__$operation], [__$update_mask]"
        )
        for field in fields:
            mssql_col = field['mssql']
            select_columns += f", {mssql_col}"

        input_yaml = f"""- label: {table_label}_cdc
  generate:
    interval: 5s
    mapping: 'root = {{}}'
  processors:
    # Initialize with default LSN
    - bloblang: 'root.last_lsn = "0x00000000000000000000"'
    # Try to get last processed LSN from cache
    - try:
        - branch:
            request_map: 'root = ""'
            processors:
              - cache:
                  resource: lsn_cache
                  operator: get
                  key: "{cache_key}"
            result_map: 'root.last_lsn = content().string()'
    # Query CDC table for records with LSN > last processed
    # sql_raw returns result set as JSON array
    # IMPORTANT: TOP 100000 limits batch size to prevent memory issues with large CDC backlogs
    - sql_raw:
        driver: mssql
        dsn: "{dsn}"
        query: |
          SELECT TOP ${{MSSQL_CDC_SELECT_TOP:-100000}} {select_columns}
          FROM cdc.{schema}_{table_name}_CT
          WHERE [__$start_lsn] > CONVERT(VARBINARY(10), $1, 1)
          ORDER BY [__$start_lsn], [__$seqval]
        args_mapping: 'root = [ this.last_lsn ]'
    # Ensure result is always an array
    # (sql_raw returns empty object when no rows, but we need empty array)
    - bloblang: 'root = if this.type() == "object" {{ [] }} else {{ this }}'
    # Split the array result into individual messages (one per CDC row)
    - unarchive:
        format: json_array
{source_transform_processors}
    # Set table metadata and capture the LSN for cache update (already hex string from SQL)
    - bloblang: |
        meta source_table = "{table_name}"
        meta max_lsn = this.get("__lsn_hex")
        root = this"""

        inputs.append(input_yaml)

    # Return inputs with consistent indentation
    # The template has {{SOURCE_TABLE_INPUTS}} at an indentation of 6 spaces.
    # We want the First line to have NO extra indentation (it replaces {{VAR}}),
    # and subsequent lines to have 6 spaces.

    result = ""
    for i, inp in enumerate(inputs):
        if i > 0:
            result += "\n\n      " # Space between inputs, with 6 spaces indentation

        # Add the input, and for every newline add 6 spaces
        result += inp.replace("\n", "\n      ")

    return result


def build_lsn_init_values(config: dict[str, Any]) -> str:
    """Build LSN cache initialization values for each CDC table."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    init_values: list[str] = []
    for table_config in source_tables:
        table_name = table_config['table']
        table_label = table_name.lower()
        cache_key = f"{table_label}_last_lsn"

        # Initialize each table's LSN to zero (start from beginning)
        init_values.append(f'        {cache_key}: "0x00000000000000000000"')

    return "\n".join(init_values)


def build_table_routing_map(
    service_name: str,
    config: dict[str, Any],
    variables: dict[str, Any],
    generated_tables: dict[str, Any],
) -> str:
    """Build bloblang case statements for routing each table to correct topic with correct key."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    topic_prefix = variables['TOPIC_PREFIX']
    cases: list[str] = []

    for table_config in source_tables:
        table_name = table_config['table']
        schema = table_config.get('schema', 'dbo')

        # Get table definition
        table_def = generated_tables.get(table_name)
        if not table_def:
            continue

        # Use generated metadata if available, otherwise fallback to manual construction
        key_expr = table_def.get('kafka_key')
        topic_format = table_def.get('topic_format')

        if key_expr and topic_format:
            # Use metadata from auto-generated definition
            # Replace placeholder with actual prefix
            full_topic = topic_format.replace("{TOPIC_PREFIX}", topic_prefix)
        else:
            # Try to get primary key from multiple sources (priority order):
            # 1. User-specified in YAML
            # 2. Canonical table schema file in services/_schemas
            primary_key: str | list[str] | None = table_def.get('primary_key')
            pk_source = 'yaml' if primary_key else None

            if not primary_key:
                # Try to read from canonical service schema file
                schema_pk, _source = get_primary_key_from_schema(
                    service_name, schema, table_name
                )
                if schema_pk:
                    primary_key = schema_pk
                    pk_source = 'schema'
                    print(
                        f"  [i] {schema}.{table_name}: "
                        + f"Using primary_key from schema: {primary_key}"
                    )

            if not primary_key:
                raise ValueError(
                    f"{schema}.{table_name}: No primary_key found. "
                    + "Generation requires schema metadata under "
                    + f"services/_schemas/{service_name}/{schema}/{table_name}.yaml"
                )

            # Validate if user specified primary_key matches canonical schema
            if pk_source == 'yaml':
                schema_pk, _ = get_primary_key_from_schema(service_name, schema, table_name)
                if schema_pk and schema_pk != primary_key:
                    print(
                        f"  ⚠️  {schema}.{table_name}: primary_key mismatch! "
                        + f"YAML={primary_key}, Schema={schema_pk}"
                    )

            # Build kafka key expression (use payload.after or payload.before for the data)
            if isinstance(primary_key, list):
                # Composite key - cast items to str for type safety
                pk_list: list[str] = [str(pk) for pk in primary_key]
                pk_parts = [f'$data.{pk}.string().or(\"\")' for pk in pk_list]
                key_expr = ' + \"|\" + '.join(pk_parts)
            else:
                # Single key
                key_expr = f'$data.{primary_key}.string().or(\"\")'

            full_topic = f"{topic_prefix}.{schema}.{table_name}"

        case = f'''$table_name == "{table_name}" => {{
  "topic": "{full_topic}",
  "key": {key_expr}
}}'''
        cases.append(case)

    # Indent by 10 spaces (8 for match + 2 for case)
    # Re-aligning: the placeholder {{TABLE_ROUTING}} is at 8 spaces.
    # We want the output to be at 10 spaces.
    # So we add 2 spaces to the first line and 10 to others.
    result = ""
    for i, case in enumerate(cases):
        if i > 0:
            result += "\n\n          "
        else:
            result += "  " # Add 2 spaces for the first line to reach col 10

        result += case.replace("\n", "\n          ")

    return result


def build_sink_table_cases(
    config: dict[str, Any],
    schema: str,
    postgres_url: str,
    generated_tables: dict[str, Any],
) -> str:
    """
    Build dynamic switch cases for sink pipeline using staging table pattern.

    All CDC operations (INSERT, UPDATE, DELETE) are written to staging tables (stg_TableName).
    A stored procedure (sp_merge_tablename) handles deduplication and merge to final table.

    This pattern provides:
    - High throughput via batched inserts to UNLOGGED staging tables
    - Proper deduplication using DISTINCT ON with timestamp ordering
    - Gap detection via kafka offset tracking
    """
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    cases: list[str] = []

    for table_config in source_tables:
        table_name = table_config['table']

        # Get fields from generated table definition
        table_def = generated_tables.get(table_name)
        if not table_def or 'fields' not in table_def:
            # Skip tables without generated field metadata
            print(f"[WARNING] No generated field metadata for {table_name} - skipping")
            continue

        fields = table_def['fields']

        # Build field lists from generated table definition
        mssql_fields: list[str] = []
        postgres_fields: list[str] = []

        for field in fields:
            mssql_name = field['mssql']
            postgres_name = field['postgres']
            mssql_fields.append(mssql_name)
            postgres_fields.append(postgres_name)

        # Generate staging table case (all operations go to staging)
        staging_case = build_staging_case(
            table_name=table_name,
            schema=schema,
            postgres_url=postgres_url,
            postgres_fields=postgres_fields,
            mssql_fields=mssql_fields
        )

        cases.append(staging_case)

    # Indent everything by 6 spaces to match the template cases block
    result = "\n".join(cases)
    # First line not prefixed as it replaces {{VAR}} already at col 6
    return result.replace("\n", "\n      ")


def normalize_table_name(name: str) -> str:
    """Normalize Norwegian special characters only (keep original casing and structure)

    Replaces:
    - å/Å -> a/A
    - ø/Ø -> o/O
    - æ/Æ -> ae/AE
    """
    replacements = {
        'å': 'a', 'Å': 'A',
        'ø': 'o', 'Ø': 'O',
        'æ': 'ae', 'Æ': 'AE'
    }

    result = name
    for norwegian_char, replacement in replacements.items():
        result = result.replace(norwegian_char, replacement)

    return result


def _parse_generation_scope(
    args: argparse.Namespace,
) -> tuple[list[str], list[str] | None]:
    """Resolve customers and environments from CLI args."""
    if args.all_customers or args.customer is None:
        customers = get_all_customers()
        environments = [args.environment] if args.environment else None
    elif args.customer and args.environment:
        customers = [args.customer]
        environments = [args.environment]
    else:
        customers = [args.customer]
        environments = None

    return customers, environments


def _validate_services_for_customers(customers: list[str]) -> None:
    """Validate service configurations required by selected customers."""
    services_to_validate = get_services_for_customers(customers)
    if not services_to_validate:
        return

    print(
        f"\n📋 Validating {len(services_to_validate)} service(s): "
        + f"{', '.join(sorted(services_to_validate))}"
    )
    validation_failed = False

    for service_name in sorted(services_to_validate):
        print(f"\n  → Validating {service_name}...")
        if not validate_service_config(service_name):
            validation_failed = True
            print(f"    ✗ Validation failed for {service_name}")

    if validation_failed:
        print(
            "\n❌ Service validation failed. "
            + "Fix errors before generating pipelines."
        )
        print("   Run: cdc manage-services config --service <name> --validate-config")
        sys.exit(1)

    print("\n  ✅ All service configurations validated successfully\n")


def _generate_sources(customers: list[str], environments: list[str] | None) -> bool:
    """Generate per-customer source pipelines and return failure flag."""
    generation_failed = False
    for customer in customers:
        try:
            generate_customer_pipelines(customer, environments)
        except Exception as error:
            generation_failed = True
            print(f"\n   ✗ Error generating {customer}: {error}")
            traceback.print_exc()
    return generation_failed


def _collect_target_environments(customers: list[str], environments: list[str] | None) -> set[str]:
    """Collect environments to generate consolidated sinks for."""
    env_set: set[str] = set()
    for customer in customers:
        try:
            config = load_customer_config(customer)
            customer_envs = list(config.get("environments", {}).keys())
            if environments:
                customer_envs = [env for env in customer_envs if env in environments]
            env_set.update(customer_envs)
        except FileNotFoundError:
            continue
    return env_set


def _generate_sinks(env_set: set[str]) -> bool:
    """Generate consolidated sink pipelines and return failure flag."""
    generation_failed = False
    all_customers = get_all_customers()
    for env_name in sorted(env_set):
        try:
            generate_consolidated_sink(env_name, all_customers)
        except Exception as error:
            generation_failed = True
            print(f"\n   ✗ Error generating consolidated sink for {env_name}: {error}")
            traceback.print_exc()
    return generation_failed


def _resolve_consolidated_postgres_url(
    service_name: str,
    customer: str,
    env_name: str,
    env_config: dict[str, Any],
) -> str:
    """Resolve Postgres URL for consolidated sink generation."""
    resolved_service_name = service_name
    if not resolved_service_name:
        matched_services = sorted(get_services_for_customers([customer]))
        if len(matched_services) == 1:
            resolved_service_name = matched_services[0]

    if resolved_service_name:
        try:
            target_sink_env_raw = env_config.get("target_sink_env")
            target_sink_env = (
                str(target_sink_env_raw).strip()
                if isinstance(target_sink_env_raw, str) and target_sink_env_raw.strip()
                else None
            )
            return resolve_postgres_url_from_sink_groups(
                resolved_service_name,
                env_name,
                target_sink_env=target_sink_env,
            )
        except ValueError as error:
            if "has no sinks configuration" not in str(error):
                raise

    raw_url = preserve_env_vars(
        env_config.get(
            "postgres",
            {},
        ).get(
            "url",
            (
                "postgresql://${POSTGRES_SINK_USER:-postgres}:${POSTGRES_SINK_PASSWORD:-postgres}"
                + "@${POSTGRES_SINK_HOST:-postgres}:${POSTGRES_SINK_PORT:-5432}"
                + "/${POSTGRES_SINK_DB:-postgres}?sslmode=disable"
            ),
        )
    )

    if '&options=' in raw_url:
        return raw_url.split('&options=')[0]
    if '?currentSchema=' in raw_url:
        return raw_url.split('?currentSchema=')[0]
    return raw_url


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines generate",
        description="Generate source and sink pipelines from templates",
    )
    parser.add_argument("customer", nargs="?", help="Generate for a specific customer")
    parser.add_argument("environment", nargs="?", help="Generate for a specific environment")
    parser.add_argument("--list", action="store_true", help="List all available customers")
    parser.add_argument("--all", dest="all_customers", action="store_true", help="Generate for all customers")
    parser.add_argument("--force", action="store_true", help="Reserved for compatibility (currently no-op)")
    args = parser.parse_args()

    if args.list:
        customers = get_all_customers()
        print("Available customers:")
        for customer in sorted(customers):
            print(f"  - {customer}")
        return

    customers, environments = _parse_generation_scope(args)

    if not customers:
        print("No customers found. Create customer configs in services/<service>.yaml")
        return

    print("=" * 60)
    print("🚀 Bento Pipeline Generator")
    print("=" * 60)

    _validate_services_for_customers(customers)
    source_failed = _generate_sources(customers, environments)
    env_set = _collect_target_environments(customers, environments)
    sink_failed = _generate_sinks(env_set)
    generation_failed = source_failed or sink_failed

    if generation_failed:
        print("\n" + "=" * 60)
        print("❌ Pipeline generation failed")
        print("   Fix schema/primary key errors and retry generation.")
        print("=" * 60)
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ Pipeline generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
