#!/usr/bin/env python3
"""
Generate Redpanda Connect pipeline configurations from templates and customer configs.

Usage:
    python generate_pipelines.py                    # Generate all customers, all environments
    python generate_pipelines.py avansas            # Generate all environments for avansas
    python generate_pipelines.py avansas local      # Generate only local for avansas
    python generate_pipelines.py --list             # List all customers
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.helpers_batch import build_staging_case
from cdc_generator.helpers.service_config import get_all_customers, load_customer_config
from cdc_generator.helpers.yaml_loader import ConfigValue, load_yaml_file

# Import validation functions
from cdc_generator.validators.manage_service.validation import validate_service_config


def get_services_for_customers(customers: list[str]) -> set[str]:
    """Determine which services are used by the given customers.

    Returns set of service names that need to be validated.
    """
    services: set[str] = set()
    services_dir = Path(__file__).parent.parent / "services"

    if not services_dir.exists():
        return services

    # Check each service file to see if it contains any of our customers
    for service_file in services_dir.glob("*.yaml"):
        try:
            service_config = load_yaml_file(service_file)
            service_customers = service_config.get('customers', [])
            if not isinstance(service_customers, list):
                continue

            customer_names: set[str] = set()
            c: ConfigValue
            for c in service_customers:
                if not isinstance(c, dict):
                    continue
                name = c.get('name')
                if isinstance(name, str):
                    customer_names.add(name)

            # If any of our target customers are in this service, include it
            if any(customer in customer_names for customer in customers):
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
    """Preserve environment variable placeholders for runtime resolution by Redpanda Connect.

    Also provides backward compatibility by converting old ${env:VAR} format to ${VAR}.
    """
    if isinstance(value, str):
        # Backward compatibility: convert ${env:VAR} to ${VAR} if found
        return re.sub(r'\$\{env:([A-Z_][A-Z0-9_]*)\}', r'${\1}', value)
    return str(value) if value is not None else ""

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
TEMPLATES_DIR = PROJECT_ROOT / "pipeline-templates"
SERVICES_DIR = PROJECT_ROOT / "services"
CUSTOMERS_DIR = PROJECT_ROOT / "2-customers"  # Legacy support during migration

# Generated files go to root-level generated/ folder
GENERATED_ROOT = PROJECT_ROOT / "generated"
GENERATED_DIR = GENERATED_ROOT / "pipelines" / "multi-tenant"


def load_template(template_name: str) -> str:
    """Load a template file as string."""
    template_path = TEMPLATES_DIR / template_name
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text()


def load_generated_table_definitions() -> dict[str, Any]:
    """Load generated table definitions from generated/table-definitions/"""
    generated_dir = GENERATED_ROOT / 'table-definitions'

    if not generated_dir.exists():
        return {}

    tables_by_name: dict[str, Any] = {}
    for yaml_file in sorted(generated_dir.glob('*.yaml')):
        table_def = load_yaml_file(yaml_file)
        if table_def and table_def:
            table_def_dict = cast(dict[str, object], table_def)
            table_name = table_def_dict.get('name')
            if isinstance(table_name, str):
                tables_by_name[table_name] = table_def

    return tables_by_name


def get_primary_key_from_schema(
    _service_name: str,
    schema_name: str,
    table_name: str,
) -> tuple[str | list[str] | None, str | None]:
    """Read primary_key from generated validation schema.

    Returns:
        tuple: (primary_key, source) where source is 'schema', 'fallback', or None
    """
    # Find validation schema file
    schemas_dir = PROJECT_ROOT / '.vscode' / 'schemas'

    if not schemas_dir.exists():
        return None, None

    # Find schema file matching service (might have different database name)
    schema_files = list(schemas_dir.glob('*.service-validation.schema.json'))

    for schema_file in schema_files:
        try:
            with schema_file.open() as f:
                validation_schema = json.load(f)

            # Navigate to table definitions in shared.source_tables
            shared = validation_schema.get('properties', {}).get('shared', {})
            source_tables = shared.get('properties', {}).get('source_tables', {})
            items = source_tables.get('items', {})

            # Look for schema group
            if 'anyOf' in items:
                for schema_group in items['anyOf']:
                    schema_def = schema_group.get('properties', {}).get('schema', {})
                    if schema_def.get('const') == schema_name:
                        # Found matching schema, now find table
                        tables = schema_group.get('properties', {}).get('tables', {})
                        table_items = tables.get('items', {})

                        if 'anyOf' in table_items:
                            for table_def in table_items['anyOf']:
                                table_name_def = table_def.get('properties', {}).get('name', {})
                                if table_name_def.get('const') == table_name:
                                    # Found the table, extract primary_key
                                    pk_def = table_def.get('properties', {}).get('primary_key', {})
                                    pk_desc = pk_def.get('description', '')

                                    # Parse primary key from description
                                    # Format: "Primary key: 'column_name'" or
                                    # "Primary key: ['col1', 'col2']"
                                    if 'Primary key:' in pk_desc:
                                        pk_str = pk_desc.split('Primary key:')[1].strip()
                                        try:
                                            import ast
                                            pk_value = ast.literal_eval(pk_str)
                                            return pk_value, 'schema'
                                        except Exception:
                                            pass
        except Exception:
            continue

    return None, None


def substitute_variables(template: str, variables: dict[str, Any]) -> str:
    """Replace {{VAR}} placeholders with values from variables dict."""
    result = template
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        result = result.replace(placeholder, str(value))
    return result


def generate_customer_pipelines(
    customer: str,
    environments: list[str] | None = None,
) -> None:
    """Generate Redpanda Connect SOURCE pipeline config for a customer.

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
        output_dir = GENERATED_DIR / env_name / customer_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Write source pipeline file with DO NOT EDIT warning
        source_path = output_dir / "source-pipeline.yaml"

        header = """# ============================================================================
# DO NOT EDIT THIS FILE - IT IS AUTO-GENERATED
# ============================================================================
# This file is automatically generated from templates and customer configs
#
# To make changes:
#   1. Edit source files: 2-customers/{customer}.yaml or pipeline-templates/
#   2. Run: python3 scripts/3-generate-pipelines.py
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
            print(f"      ✓ Generated: {source_path.relative_to(SCRIPT_DIR.parent)}")
        else:
            print(f"      ⊘ Unchanged: {source_path.relative_to(SCRIPT_DIR.parent)}")


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
    postgres_url = None  # Will be set from first customer's config

    for customer in customers:
        try:
            config = load_customer_config(customer)
        except FileNotFoundError:
            continue

        schema = config.get("schema", customer)
        env_config = config.get("environments", {}).get(env_name)

        if not env_config:
            continue  # Customer doesn't have this environment

        # Get PostgreSQL URL (same for all customers in consolidated setup)
        # Strip customer-specific URL parameters like search_path or currentSchema
        # since we use schema-qualified table names instead
        if postgres_url is None:
            raw_url = preserve_env_vars(
                env_config.get("postgres", {}).get("url",
                    "postgresql://postgres:postgres@localhost:5432/postgres?sslmode=disable")
            )
            # Remove customer-specific parameters (search_path, currentSchema, options)
            # Keep only the base URL with sslmode
            if '&options=' in raw_url:
                postgres_url = raw_url.split('&options=')[0]
            elif '?currentSchema=' in raw_url:
                postgres_url = raw_url.split('?currentSchema=')[0]
            else:
                postgres_url = raw_url

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
    output_dir = GENERATED_DIR / env_name
    output_dir.mkdir(parents=True, exist_ok=True)

    sink_path = output_dir / "sink-pipeline.yaml"

    header = """# ============================================================================
# DO NOT EDIT THIS FILE - IT IS AUTO-GENERATED
# ============================================================================
# CONSOLIDATED SINK - Handles ALL customers for this environment
#
# To make changes:
#   1. Edit customer files in 2-customers/*.yaml
#   2. Edit template: pipeline-templates/sink-pipeline.yaml
#   3. Run: python3 scripts/3-generate-pipelines.py
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
        print(f"   ✓ Generated: {sink_path.relative_to(SCRIPT_DIR.parent)}")
        print(f"   📋 Topics: {len(all_topics)}, Table Routes: {len(all_table_cases)}")
    else:
        print(f"   ⊘ Unchanged: {sink_path.relative_to(SCRIPT_DIR.parent)}")


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
          SELECT TOP 100000 {select_columns}
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
            # 2. Validation schema (auto-detected from database)
            # 3. Fallback to 'id'
            primary_key: str | list[str] | None = table_def.get('primary_key')
            pk_source = 'yaml' if primary_key else None

            if not primary_key:
                # Try to read from validation schema
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
                # Final fallback
                primary_key = 'id'
                pk_source = 'fallback'
                print(f"  ⚠️  {schema}.{table_name}: No primary_key found, using fallback 'id'")

            # Validate if user specified primary_key matches schema
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


def main() -> None:
    args = sys.argv[1:]

    # Handle --help or -h flag
    if "--help" in args or "-h" in args:
        print("""
Usage:
    python 3-generate-pipelines.py                    # Generate all customers, all environments
    python 3-generate-pipelines.py avansas            # Generate all environments for avansas
    python 3-generate-pipelines.py avansas local      # Generate only local for avansas
    python 3-generate-pipelines.py --list             # List all customers

Options:
    --list      List all available customers
    --help, -h  Show this help message

Output Structure:
    generated/pipelines/{env}/{customer}/source-pipeline.yaml  # Per customer source
    generated/pipelines/{env}/sink-pipeline.yaml               # Consolidated sink per env
""")
        return

    # Handle --list flag
    if "--list" in args:
        customers = get_all_customers()
        print("Available customers:")
        for c in sorted(customers):
            print(f"  - {c}")
        return

    # Determine customers and environments to generate
    if len(args) == 0:
        # Generate all
        customers = get_all_customers()
        environments = None
    elif len(args) == 1:
        # Generate all environments for specific customer
        customers = [args[0]]
        environments = None
    else:
        # Generate specific environment for specific customer
        customers = [args[0]]
        environments = [args[1]]

    if not customers:
        print("No customers found. Create customer configs in 2-customers/<customer>.yaml")
        return

    print("=" * 60)
    print("🚀 Redpanda Connect Pipeline Generator")
    print("=" * 60)

    # Validate service configurations for customers being generated
    services_to_validate = get_services_for_customers(customers)

    if services_to_validate:
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
            print("   Run: cdc manage-service --service <name> --validate-config")
            sys.exit(1)

        print("\n  ✅ All service configurations validated successfully\n")

    # Generate source pipelines per customer
    for customer in customers:
        try:
            generate_customer_pipelines(customer, environments)
        except Exception as e:
            print(f"\n   ✗ Error generating {customer}: {e}")
            import traceback
            traceback.print_exc()

    # Generate consolidated sinks per environment
    # Collect all unique environments from the customers being processed
    env_set: set[str] = set()
    for customer in customers:
        try:
            config = load_customer_config(customer)
            customer_envs = list(config.get("environments", {}).keys())
            if environments:
                # Only include environments that were requested
                customer_envs = [e for e in customer_envs if e in environments]
            env_set.update(customer_envs)
        except FileNotFoundError:
            continue

    for env_name in sorted(env_set):
        try:
            # For consolidated sink, use ALL customers (not just the ones being regenerated)
            # This ensures the sink always has complete topic/routing coverage
            all_customers = get_all_customers()
            generate_consolidated_sink(env_name, all_customers)
        except Exception as e:
            print(f"\n   ✗ Error generating consolidated sink for {env_name}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print("✅ Pipeline generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
