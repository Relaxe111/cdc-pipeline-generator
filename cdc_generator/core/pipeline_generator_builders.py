"""Builder helpers for pipeline generation templates and routing blocks."""

from __future__ import annotations

from typing import Any

from cdc_generator.core.pipeline_generator_common import get_primary_key_from_schema
from cdc_generator.core.pipeline_generator_transforms import (
    build_source_transform_processors,
    collect_sink_table_cfgs_for_source,
)
from cdc_generator.helpers.helpers_batch import build_staging_case


def build_table_include_list(config: dict[str, Any]) -> str:
    """Build comma-separated table include list from CDC tables config."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return "dbo.Actor,dbo.AdgangLinjer"

    table_list = [f"{t['schema']}.{t['table']}" for t in source_tables]
    return ",".join(table_list)


def build_sink_topics(
    config: dict[str, Any],
    topic_prefix: str,
    generated_tables: dict[str, Any],
) -> str:
    """Build comma-separated list of Kafka topics for the sink to subscribe to."""
    source_tables = config.get('cdc_tables', [])
    topics: list[str] = []

    if not source_tables:
        topics = [
            f"{topic_prefix}.dbo.Actor",
            f"{topic_prefix}.dbo.AdgangLinjer",
        ]
    else:
        for table_cfg in source_tables:
            table_name = table_cfg['table']
            schema = table_cfg.get('schema', 'dbo')

            table_def = generated_tables.get(table_name)
            if table_def and 'topic_format' in table_def:
                topic = table_def['topic_format'].replace("{TOPIC_PREFIX}", topic_prefix)
                topics.append(topic)
            else:
                topics.append(f"{topic_prefix}.{schema}.{table_name}")

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
    mssql_user = variables['MSSQL_USER']
    mssql_password = variables['MSSQL_PASSWORD']
    mssql_host = variables['MSSQL_HOST']
    mssql_port = variables['MSSQL_PORT']
    mssql_database = variables['DATABASE_NAME']
    dsn = f"sqlserver://{mssql_user}:{mssql_password}@{mssql_host}:{mssql_port}?database={mssql_database}"

    for table_config in source_tables:
        table_name = table_config['table']
        schema = table_config.get('schema', 'dbo')

        sink_table_cfgs = collect_sink_table_cfgs_for_source(service_cfg or {}, table_name)
        source_transform_processors = build_source_transform_processors(sink_table_cfgs)

        table_def = generated_tables.get(table_name)
        if not table_def or 'fields' not in table_def:
            print(f"[WARNING] No generated field metadata for {table_name} - skipping source input")
            continue

        fields = table_def['fields']
        table_label = table_name.lower()
        cache_key = f"{table_label}_last_lsn"

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

    result = ""
    for index, entry in enumerate(inputs):
        if index > 0:
            result += "\n\n      "
        result += entry.replace("\n", "\n      ")

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
        init_values.append(f'        {cache_key}: "0x00000000000000000000"')

    return "\n".join(init_values)


def build_table_routing_map(
    service_name: str,
    config: dict[str, Any],
    variables: dict[str, Any],
    generated_tables: dict[str, Any],
) -> str:
    """Build bloblang case statements for routing each table to correct topic/key."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    topic_prefix = variables['TOPIC_PREFIX']
    cases: list[str] = []

    for table_config in source_tables:
        table_name = table_config['table']
        schema = table_config.get('schema', 'dbo')
        table_def = generated_tables.get(table_name)
        if not table_def:
            continue

        key_expr = table_def.get('kafka_key')
        topic_format = table_def.get('topic_format')

        if key_expr and topic_format:
            full_topic = topic_format.replace("{TOPIC_PREFIX}", topic_prefix)
        else:
            primary_key: str | list[str] | None = table_def.get('primary_key')
            pk_source = 'yaml' if primary_key else None

            if not primary_key:
                schema_pk, _source = get_primary_key_from_schema(service_name, schema, table_name)
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

            if pk_source == 'yaml':
                schema_pk, _ = get_primary_key_from_schema(service_name, schema, table_name)
                if schema_pk and schema_pk != primary_key:
                    print(
                        f"  ⚠️  {schema}.{table_name}: primary_key mismatch! "
                        + f"YAML={primary_key}, Schema={schema_pk}"
                    )

            if isinstance(primary_key, list):
                pk_list: list[str] = [str(pk) for pk in primary_key]
                pk_parts = [f'$data.{pk}.string().or(\"\")' for pk in pk_list]
                key_expr = ' + \"|\" + '.join(pk_parts)
            else:
                key_expr = f'$data.{primary_key}.string().or(\"\")'

            full_topic = f"{topic_prefix}.{schema}.{table_name}"

        case = f'''$table_name == "{table_name}" => {{
  "topic": "{full_topic}",
  "key": {key_expr}
}}'''
        cases.append(case)

    result = ""
    for index, case in enumerate(cases):
        if index > 0:
            result += "\n\n          "
        else:
            result += "  "
        result += case.replace("\n", "\n          ")

    return result


def build_sink_table_cases(
    config: dict[str, Any],
    schema: str,
    postgres_url: str,
    generated_tables: dict[str, Any],
) -> str:
    """Build dynamic switch cases for sink pipeline using staging table pattern."""
    source_tables = config.get('cdc_tables', [])
    if not source_tables:
        return ""

    cases: list[str] = []

    for table_config in source_tables:
        table_name = table_config['table']

        table_def = generated_tables.get(table_name)
        if not table_def or 'fields' not in table_def:
            print(f"[WARNING] No generated field metadata for {table_name} - skipping")
            continue

        fields = table_def['fields']
        mssql_fields: list[str] = []
        postgres_fields: list[str] = []

        for field in fields:
            mssql_fields.append(field['mssql'])
            postgres_fields.append(field['postgres'])

        staging_case = build_staging_case(
            table_name=table_name,
            schema=schema,
            postgres_url=postgres_url,
            postgres_fields=postgres_fields,
            mssql_fields=mssql_fields,
        )
        cases.append(staging_case)

    result = "\n".join(cases)
    return result.replace("\n", "\n      ")