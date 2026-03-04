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
import sys
import traceback
from datetime import datetime
from typing import Any, cast

from cdc_generator.core.pipeline_generator_builders import (
    build_sink_topics,
    build_source_table_inputs,
    build_table_include_list,
    build_table_routing_map,
)
from cdc_generator.core.pipeline_generator_common import (
    get_services_for_customers,
    load_generated_table_definitions,
    load_template,
    preserve_env_vars,
    should_write_file,
    substitute_variables,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    build_customer_consolidated_routes as _build_customer_consolidated_routes,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    load_customer_env_config as _load_customer_env_config,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    print_consolidated_sink_skip_summary as _print_consolidated_sink_skip_summary,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    print_customer_skip_summary as _print_customer_skip_summary,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    render_consolidated_sink_content as _render_consolidated_sink_content,
)
from cdc_generator.core.pipeline_generator_consolidated import (
    resolve_consolidated_postgres_url as _resolve_consolidated_postgres_url,
)
from cdc_generator.helpers.helpers_logging import print_error
from cdc_generator.helpers.service_config import (
    get_all_customers,
    get_project_root,
    load_customer_config,
    load_service_config,
)

# Import validation functions
from cdc_generator.validators.manage_service.validation import validate_service_config

PROJECT_ROOT = get_project_root()
PIPELINES_GENERATED_DIR = PROJECT_ROOT / "pipelines" / "generated"
GENERATED_SOURCES_DIR = PIPELINES_GENERATED_DIR / "sources"
GENERATED_SINKS_DIR = PIPELINES_GENERATED_DIR / "sinks"


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
