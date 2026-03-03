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
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_batch import build_staging_case
from cdc_generator.helpers.service_config import (
    get_all_customers,
    get_project_root,
    load_customer_config,
    load_service_config,
)
from cdc_generator.helpers.yaml_loader import ConfigValue, load_yaml_file

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


def _resolve_sink_env_key(source_cfg: dict[str, Any], env_name: str) -> str:
    """Resolve sink-groups source environment key from generator env name."""
    if env_name in source_cfg:
        return env_name

    env_aliases: dict[str, list[str]] = {
        'default': ['dev', 'nonprod', 'stage', 'test'],
        'nonprod': ['dev', 'stage', 'test'],
        'local': ['dev', 'test'],
        'prod': ['prod', 'prod-adcuris'],
    }

    for candidate in env_aliases.get(env_name, []):
        if candidate in source_cfg:
            return candidate

    raise ValueError(
        f"No sink-groups source environment mapping for env '{env_name}'. "
        + f"Available: {[key for key in source_cfg if key != 'schemas']}"
    )


def resolve_postgres_url_from_sink_groups(service_name: str, env_name: str) -> str:
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
    source_env_key = _resolve_sink_env_key(source_cfg, env_name)
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
        source_inputs = build_source_table_inputs(config, variables, generated_tables)
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
    postgres_url = None  # Resolved from sink-groups (preferred) or customer config fallback

    for customer in customers:
        try:
            config = load_customer_config(customer)
        except FileNotFoundError:
            continue

        schema = config.get("schema", customer)
        service_name = str(config.get('service', ''))
        env_config = config.get("environments", {}).get(env_name)

        if not env_config:
            continue  # Customer doesn't have this environment

        # Get PostgreSQL URL (same for all customers in consolidated setup)
        # Strip customer-specific URL parameters like search_path or currentSchema
        # since we use schema-qualified table names instead
        if postgres_url is None:
            postgres_url = _resolve_consolidated_postgres_url(
                service_name=service_name,
                customer=customer,
                env_name=env_name,
            )

        # Build topics for this customer
        topic_prefix = preserve_env_vars(env_config.get("topic_prefix", f"{env_name}.{customer}"))
        source_tables = config.get('cdc_tables', [])

        for t in source_tables:
            table_name = t['table']
            table_schema = t.get('schema', 'dbo')
            # topic_prefix already includes database name, just add schema.table
            topic = f'"{topic_prefix}.{table_schema}.{table_name}"'
            all_topics.append(topic)

        # Build table cases for this customer
        for table_config in source_tables:
            table_name = table_config['table']

            table_def = generated_tables.get(table_name)
            if not table_def or 'fields' not in table_def:
                print(f"   [WARNING] No generated field metadata for {table_name} - skipping")
                continue

            fields = table_def['fields']
            mssql_fields = [f['mssql'] for f in fields]
            postgres_fields = [f['postgres'] for f in fields]

            staging_case = build_staging_case(
                table_name=table_name,
                schema=schema,
                postgres_url=postgres_url,
                postgres_fields=postgres_fields,
                mssql_fields=mssql_fields
            )
            all_table_cases.append(staging_case)

    if not all_topics:
        print(f"   ⚠️  No customers configured for environment {env_name}")
        return

    # Build consolidated variables
    topics_yaml = "\n".join([f"      - {t}" for t in all_topics])
    table_cases_yaml = "\n".join(all_table_cases)
    # Indent table cases to match template
    table_cases_yaml = table_cases_yaml.replace("\n", "\n      ")

    variables = {
        "ENV": env_name,
        "SINK_TOPICS": "\n" + topics_yaml,
        "TABLE_CASES": table_cases_yaml,
    }

    # Substitute variables in sink template
    sink_pipeline = substitute_variables(sink_template, variables)

    # Create output directory for consolidated sink
    output_dir = GENERATED_SINKS_DIR / env_name
    output_dir.mkdir(parents=True, exist_ok=True)

    sink_path = output_dir / "sink-pipeline.yaml"

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
            c for c in customers
            if load_customer_config(c).get("environments", {}).get(env_name)
        ])
    )
    header_formatted = header.format(env=env_name, customers=customer_list, timestamp=timestamp)

    sink_content = header_formatted + sink_pipeline

    if should_write_file(sink_path, sink_content):
        sink_path.write_text(sink_content)
        print(f"   ✓ Generated: {sink_path.relative_to(PROJECT_ROOT)}")
        print(f"   📋 Topics: {len(all_topics)}, Table Routes: {len(all_table_cases)}")
    else:
        print(f"   ⊘ Unchanged: {sink_path.relative_to(PROJECT_ROOT)}")


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
) -> str:
    """Resolve Postgres URL for consolidated sink generation."""
    resolved_service_name = service_name
    if not resolved_service_name:
        matched_services = sorted(get_services_for_customers([customer]))
        if len(matched_services) == 1:
            resolved_service_name = matched_services[0]

    if not resolved_service_name:
        raise ValueError(
            f"Unable to resolve service for customer '{customer}'. "
            + "Consolidated sink DSN must be resolved from sink-groups.yaml (no localhost fallback)."
        )

    return resolve_postgres_url_from_sink_groups(resolved_service_name, env_name)


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
