#!/usr/bin/env python3
"""Verify pipeline configuration and generated outputs.

Usage:
    cdc manage-pipelines verify
    cdc manage-pipelines verify --full
    cdc manage-pipelines verify --sink
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from cdc_generator.helpers.psycopg2_loader import (
    PostgresNotAvailableError,
    create_postgres_connection,
    has_psycopg2,
)
from cdc_generator.helpers.service_config import (
    get_all_customers,
    get_project_root,
    load_customer_config,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cdc manage-pipelines verify",
        description="Verify pipeline configuration and generated artifacts",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full generation and validate generated output files",
    )
    parser.add_argument(
        "--sink",
        action="store_true",
        help="Verify sink PostgreSQL connectivity from environment configs",
    )
    parser.add_argument(
        "--service",
        default=None,
        help="Optional service name filter for --sink checks",
    )
    return parser.parse_args()


def _load_yaml_safely(path: Path) -> list[str]:
    try:
        load_yaml_file(path)
    except Exception as exc:
        return [f"Invalid YAML: {path} ({exc})"]
    return []


def _check_template_placeholders(template_path: Path) -> list[str]:
    errors: list[str] = []

    if not template_path.exists():
        return [f"Missing template file: {template_path}"]

    content = template_path.read_text(encoding="utf-8")

    if content.count("{{") != content.count("}}"):
        errors.append(f"Unbalanced placeholder braces in {template_path}")

    found = re.findall(r"\{\{([A-Z0-9_]+)\}\}", content)
    if not found:
        errors.append(f"No template placeholders found in {template_path}")

    return errors


def _validate_yaml_and_structure(project_root: Path) -> list[str]:
    errors: list[str] = []

    source_groups_path = project_root / "source-groups.yaml"
    if not source_groups_path.exists():
        errors.append(f"Missing file: {source_groups_path}")
    else:
        errors.extend(_load_yaml_safely(source_groups_path))

    services_dir = project_root / "services"
    if not services_dir.exists():
        errors.append(f"Missing directory: {services_dir}")
    else:
        for service_path in sorted(services_dir.glob("*.yaml")):
            errors.extend(_load_yaml_safely(service_path))

    templates_dir = project_root / "pipelines" / "templates"
    source_template = templates_dir / "source-pipeline.yaml"
    sink_template = templates_dir / "sink-pipeline.yaml"
    errors.extend(
        _check_template_placeholders(source_template),
    )
    errors.extend(
        _check_template_placeholders(sink_template),
    )

    return errors


def _run_full_generation(project_root: Path) -> list[str]:
    errors: list[str] = []
    generator_script = Path(__file__).resolve().parents[1] / "core" / "pipeline_generator.py"
    result = subprocess.run(
        [sys.executable, str(generator_script)],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        errors.append("Pipeline generation failed during --full verification")
        if result.stdout:
            errors.append(f"Generator stdout:\n{result.stdout.strip()}")
        if result.stderr:
            errors.append(f"Generator stderr:\n{result.stderr.strip()}")

    return errors


def _validate_generated_outputs(project_root: Path) -> list[str]:
    errors: list[str] = []
    generated_root = project_root / "pipelines" / "generated"
    source_dir = generated_root / "sources"
    sink_dir = generated_root / "sinks"

    if not source_dir.exists():
        errors.append(f"Missing generated sources directory: {source_dir}")
    if not sink_dir.exists():
        errors.append(f"Missing generated sinks directory: {sink_dir}")

    source_files = sorted(source_dir.rglob("*.yaml")) if source_dir.exists() else []
    sink_files = sorted(sink_dir.rglob("*.yaml")) if sink_dir.exists() else []

    if not source_files:
        errors.append("No source pipeline files were generated")
    if not sink_files:
        errors.append("No sink pipeline files were generated")

    unresolved_pattern = re.compile(r"\{\{[^}]+\}\}")
    for yaml_path in [*source_files, *sink_files]:
        content = yaml_path.read_text(encoding="utf-8")
        if unresolved_pattern.search(content):
            errors.append(f"Unresolved placeholders found in generated file: {yaml_path}")

    return errors


def _resolve_env_value(value: str) -> str:
    def replace_var(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{([A-Z_][A-Z0-9_]*)\}", replace_var, value)


def _extract_postgres_urls(service_filter: str | None) -> list[tuple[str, str, str]]:
    urls: list[tuple[str, str, str]] = []

    for customer in get_all_customers():
        config: dict[str, Any] = load_customer_config(customer)
        service_name = str(config.get("service", ""))
        if service_filter and service_name != service_filter:
            continue

        environments = cast(dict[str, Any], config.get("environments", {}))

        for env_name_raw, env_config_raw in environments.items():
            env_name = str(env_name_raw)
            env_config = cast(dict[str, Any], env_config_raw)
            postgres = cast(dict[str, Any], env_config.get("postgres", {}))
            url = postgres.get("url")
            if isinstance(url, str) and url.strip():
                urls.append((customer, str(env_name), _resolve_env_value(url.strip())))

    return urls


def _extract_sink_server_connections(
    project_root: Path,
    service_filter: str | None,
) -> list[tuple[str, str, int, str, str, str]]:
    sink_groups_path = project_root / "sink-groups.yaml"
    if not sink_groups_path.exists():
        return []

    sink_groups_data = cast(dict[str, Any], load_yaml_file(sink_groups_path))

    connections: list[tuple[str, str, int, str, str, str]] = []
    for sink_group_name_raw, sink_group_raw in sink_groups_data.items():
        sink_group_name = str(sink_group_name_raw)
        sink_group = cast(dict[str, Any], sink_group_raw) if isinstance(sink_group_raw, dict) else {}
        sources_raw = sink_group.get("sources", {})
        if service_filter and isinstance(sources_raw, dict) and service_filter not in sources_raw:
            continue

        servers_raw = sink_group.get("servers", {})
        servers = cast(dict[str, Any], servers_raw) if isinstance(servers_raw, dict) else {}
        for server_name_raw, server_cfg_raw in servers.items():
            server_name = str(server_name_raw)
            server_cfg = cast(dict[str, Any], server_cfg_raw) if isinstance(server_cfg_raw, dict) else {}

            host = server_cfg.get("host")
            user = server_cfg.get("user")
            password = server_cfg.get("password")
            port_raw = server_cfg.get("port", 5432)
            database_raw = server_cfg.get("database", "postgres")

            if not isinstance(host, str) or not isinstance(user, str) or not isinstance(password, str):
                continue

            resolved_host = _resolve_env_value(host)
            resolved_user = _resolve_env_value(user)
            resolved_password = _resolve_env_value(password)
            resolved_db = _resolve_env_value(str(database_raw))

            try:
                resolved_port = int(_resolve_env_value(str(port_raw)))
            except ValueError:
                continue

            label = f"{sink_group_name}/{server_name}"
            connections.append(
                (
                    label,
                    resolved_host,
                    resolved_port,
                    resolved_user,
                    resolved_password,
                    resolved_db,
                ),
            )

    return connections


def _verify_single_postgres_url(url: str) -> str | None:
    if "${" in url:
        return f"Unresolved environment variable in PostgreSQL URL: {url}"

    parsed = urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return f"Unsupported PostgreSQL URL scheme: {parsed.scheme}"

    host = parsed.hostname
    port = parsed.port or 5432
    username = parsed.username
    password = parsed.password
    dbname = parsed.path.lstrip("/") if parsed.path else ""

    if not host or not username or password is None or not dbname:
        return f"Incomplete PostgreSQL URL (need host/user/password/db): {url}"

    try:
        conn = create_postgres_connection(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=password,
            connect_timeout=3,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        finally:
            conn.close()
    except PostgresNotAvailableError as exc:
        return str(exc)
    except Exception as exc:
        return f"Connection failed for {host}:{port}/{dbname} ({exc})"

    return None


def _verify_sink_connectivity(service_filter: str | None) -> list[str]:
    errors: list[str] = []

    if not has_psycopg2:
        return ["psycopg2 is not installed. Install it with: pip install psycopg2-binary"]

    url_entries = _extract_postgres_urls(service_filter)
    if not url_entries:
        server_connections = _extract_sink_server_connections(get_project_root(), service_filter)
        if not server_connections:
            return [
                "No sink PostgreSQL connection settings found in customer env configs or sink-groups.yaml",
            ]

        for label, host, port, user, password, dbname in server_connections:
            if "${" in host or "${" in user or "${" in password:
                errors.append(f"[{label}] Unresolved environment variable in sink server credentials")
                continue
            try:
                conn = create_postgres_connection(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                    connect_timeout=3,
                )
                conn.close()
            except Exception as exc:
                errors.append(f"[{label}] Connection failed for {host}:{port}/{dbname} ({exc})")

        return errors

    seen: set[str] = set()
    unique_urls: list[tuple[str, str, str]] = []
    for customer, env_name, url in url_entries:
        if url in seen:
            continue
        seen.add(url)
        unique_urls.append((customer, env_name, url))

    for customer, env_name, url in unique_urls:
        error = _verify_single_postgres_url(url)
        if error is not None:
            errors.append(f"[{customer}/{env_name}] {error}")

    return errors


def main() -> int:
    args = _parse_args()

    if args.full and args.sink:
        print("❌ Use only one mode: either --full or --sink")
        return 2

    project_root = get_project_root()

    if args.sink:
        print("🔍 Verifying sink PostgreSQL connectivity...")
        errors = _verify_sink_connectivity(args.service)
    elif args.full:
        print("🔍 Running full verification (light checks + generation validation)...")
        errors = _validate_yaml_and_structure(project_root)
        if not errors:
            errors.extend(_run_full_generation(project_root))
        if not errors:
            errors.extend(_validate_generated_outputs(project_root))
    else:
        print("🔍 Running light verification (YAML + structure + placeholders)...")
        errors = _validate_yaml_and_structure(project_root)

    if errors:
        print("\n❌ Verification failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("\n✅ Verification passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
