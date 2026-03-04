"""Consolidated sink helper functions for pipeline generation."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from cdc_generator.core.pipeline_generator_common import (
    get_services_for_customers,
    preserve_env_vars,
    resolve_postgres_url_from_sink_groups,
    substitute_variables,
)
from cdc_generator.core.pipeline_generator_transforms import (
    build_runtime_processor_case,
    build_runtime_processors_block,
    build_sink_table_enrichment,
    select_sink_table_cfg_for_source,
)
from cdc_generator.helpers.helpers_batch import build_staging_case
from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import load_customer_config, load_service_config


def load_customer_env_config(
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


def build_customer_consolidated_routes(
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

        sink_table_cfg = select_sink_table_cfg_for_source(service_cfg, table_name)
        extra_columns: list[str] = []
        extra_args: list[str] = []
        processor_steps: list[str] = []
        if sink_table_cfg is not None:
            try:
                extra_columns, extra_args, processor_steps = build_sink_table_enrichment(
                    sink_table_cfg,
                    customer_name=customer,
                    env_name=env_name,
                    server_group_name=server_group_name,
                )
            except ValueError as enrichment_error:
                skipped_routes += 1
                skipped_entries.append((f"{table_schema}.{table_name}", str(enrichment_error)))
                continue

        runtime_case = build_runtime_processor_case(
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


def print_customer_skip_summary(
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


def render_consolidated_sink_content(
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
        "SINK_RUNTIME_PROCESSORS": build_runtime_processors_block(runtime_processor_cases),
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


def print_consolidated_sink_skip_summary(
    skipped_routes: int,
    skipped_customers: int,
) -> None:
    """Print post-generation skip summary for consolidated sink."""
    if skipped_routes:
        print_error(f"   ⚠ Skipped routes due to unresolved source refs: {skipped_routes}")
    if skipped_customers:
        print_error(f"   ⚠ Skipped customers due to invalid sink routing: {skipped_customers}")


def resolve_consolidated_postgres_url(
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