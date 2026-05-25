"""Focused tests for the canonical ``cdc fdw`` bootstrap flow."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import cast

import pytest

from cdc_generator.cli.fdw import main as fdw_main
from cdc_generator.helpers.autocompletions.sinks import list_target_sink_envs_for_service
from cdc_generator.helpers.fdw_bootstrap import (
    FdwBootstrapRequest,
    build_fdw_bootstrap_plan,
    render_fdw_bootstrap_sql,
)

_SOURCE_GROUPS_YAML = """adopus:
  pattern: db-per-tenant
  type: mssql
  servers:
    default:
      host: ${MSSQL_SOURCE_HOST}
      port: ${MSSQL_SOURCE_PORT}
      user: ${MSSQL_SOURCE_USER}
      password: ${MSSQL_SOURCE_PASSWORD}
  sources:
    Test:
      schemas:
        - dbo
      default:
        server: default
        database: AdOpusTest
        customer_id: 4d43855c-afa9-45ca-9e31-382dbde9681b
        target_sink_env: dev
    FretexDev:
      schemas:
        - dbo
      default:
        server: default
        database: AdOpusFretexDev
        customer_id: 04ed3971-ea9a-49e0-a0ba-5170c16a8d64
        target_sink_env: stage
"""

_SERVICE_YAML = """adopus:
  source:
    validation_database: AdOpusTest
    tables:
      dbo.Actor: {}
      dbo.Soknad: {}
  shared:
    source_tables:
      - schema: dbo
        tables:
          - name: Actor
          - name: Soknad
    ignore_tables: []
  server_group: adopus
  sinks:
    sink_asma.directory:
      tables:
        adopus.Actor:
          from: dbo.Actor
"""

_ACTOR_SCHEMA_YAML = """database: AdOpusTest
schema: dbo
service: adopus
table: Actor
columns:
  - name: actno
    type: int
    nullable: false
    default_value: null
    primary_key: true
  - name: Navn
    type: varchar
    nullable: true
    default_value: null
    primary_key: false
  - name: changedt
    type: datetime
    nullable: true
    default_value: null
    primary_key: false
"""

_SOKNAD_SCHEMA_YAML = """database: AdOpusTest
schema: dbo
service: adopus
table: Soknad
columns:
  - name: SoknadId
    type: int
    nullable: false
    default_value: null
    primary_key: true
  - name: Navn
    type: nvarchar
    nullable: true
    default_value: null
    primary_key: false
"""

_DOTENV = """MSSQL_SOURCE_HOST=10.90.37.9
MSSQL_SOURCE_PORT=49852
MSSQL_SOURCE_USER=cdc_pipeline_admin
MSSQL_SOURCE_PASSWORD=supersecret
POSTGRES_SINK_HOST_ASMA_NONPROD=10.90.37.20
POSTGRES_SINK_PORT_ASMA_NONPROD=5432
POSTGRES_SINK_USER_ASMA_NONPROD=pg_user_nonprod
POSTGRES_SINK_PASSWORD_ASMA_NONPROD=pg_password_nonprod
POSTGRES_SINK_HOST_ASMA_PROD=10.90.37.30
POSTGRES_SINK_PORT_ASMA_PROD=5432
POSTGRES_SINK_USER_ASMA_PROD=pg_user_prod
POSTGRES_SINK_PASSWORD_ASMA_PROD=pg_password_prod
"""

_SINK_GROUPS_YAML = """sink_asma:
  source_group: adopus
  pattern: db-shared
  type: postgres
  servers:
    nonprod:
      host: ${POSTGRES_SINK_HOST_ASMA_NONPROD}
      port: ${POSTGRES_SINK_PORT_ASMA_NONPROD}
      user: ${POSTGRES_SINK_USER_ASMA_NONPROD}
      password: ${POSTGRES_SINK_PASSWORD_ASMA_NONPROD}
    prod:
      host: ${POSTGRES_SINK_HOST_ASMA_PROD}
      port: ${POSTGRES_SINK_PORT_ASMA_PROD}
      user: ${POSTGRES_SINK_USER_ASMA_PROD}
      password: ${POSTGRES_SINK_PASSWORD_ASMA_PROD}
  sources:
    directory:
      schemas:
        - adopus
        - cdc_management
      dev:
        server: nonprod
        database: directory_dev
      stage:
        server: nonprod
        database: directory_stage
      prod:
        server: prod
        database: directory_prod
"""


def _write_fdw_project(project_root: Path) -> None:
    services_dir = project_root / "services"
    schemas_dir = services_dir / "_schemas" / "adopus" / "dbo"
    schemas_dir.mkdir(parents=True, exist_ok=True)

    (project_root / "source-groups.yaml").write_text(_SOURCE_GROUPS_YAML, encoding="utf-8")
    (project_root / "sink-groups.yaml").write_text(_SINK_GROUPS_YAML, encoding="utf-8")
    (project_root / ".env").write_text(_DOTENV, encoding="utf-8")
    (services_dir / "adopus.yaml").write_text(_SERVICE_YAML, encoding="utf-8")
    (schemas_dir / "Actor.yaml").write_text(_ACTOR_SCHEMA_YAML, encoding="utf-8")
    (schemas_dir / "Soknad.yaml").write_text(_SOKNAD_SCHEMA_YAML, encoding="utf-8")


@pytest.fixture()
def fdw_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a minimal implementation repo for FDW bootstrap tests."""
    _write_fdw_project(tmp_path)
    monkeypatch.setenv("MSSQL_SOURCE_HOST", "10.90.37.9")
    monkeypatch.setenv("MSSQL_SOURCE_PORT", "49852")
    monkeypatch.setenv("MSSQL_SOURCE_USER", "cdc_pipeline_admin")
    monkeypatch.setenv("MSSQL_SOURCE_PASSWORD", "supersecret")
    monkeypatch.setenv("POSTGRES_SINK_HOST_ASMA_NONPROD", "10.90.37.20")
    monkeypatch.setenv("POSTGRES_SINK_PORT_ASMA_NONPROD", "5432")
    monkeypatch.setenv("POSTGRES_SINK_USER_ASMA_NONPROD", "pg_user_nonprod")
    monkeypatch.setenv("POSTGRES_SINK_PASSWORD_ASMA_NONPROD", "pg_password_nonprod")
    monkeypatch.setenv("POSTGRES_SINK_HOST_ASMA_PROD", "10.90.37.30")
    monkeypatch.setenv("POSTGRES_SINK_PORT_ASMA_PROD", "5432")
    monkeypatch.setenv("POSTGRES_SINK_USER_ASMA_PROD", "pg_user_prod")
    monkeypatch.setenv("POSTGRES_SINK_PASSWORD_ASMA_PROD", "pg_password_prod")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_build_fdw_bootstrap_plan_derives_sources_and_tables(
    fdw_project: Path,
) -> None:
    """Plan should derive customer sources and mapped FDW table columns."""
    del fdw_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        "default",
        FdwBootstrapRequest(),
    )

    assert plan.service_name == "adopus"
    assert plan.target_schema_name == "adopus"
    assert len(plan.source_plans) == 2
    assert len(plan.table_plans) == 2

    source_by_name = {source_plan.customer_name: source_plan for source_plan in plan.source_plans}
    test_source = source_by_name["Test"]
    assert test_source.fdw_server_name == "mssql_default_test"
    assert test_source.fdw_schema_name == "fdw_default_test"
    assert test_source.host == "10.90.37.9"
    assert test_source.environment_profile_name == "default"

    actor_plan = next(table_plan for table_plan in plan.table_plans if table_plan.logical_table_name == "Actor")
    assert actor_plan.foreign_table_name == "Actor_CT"
    assert actor_plan.base_foreign_table_name == "Actor_base"
    assert actor_plan.remote_table_name == "dbo_Actor_CT"
    assert actor_plan.columns[0] == ("__$start_lsn", "bytea")
    assert actor_plan.base_columns[0] == ("actno", "integer")
    assert ("actno", "integer") in actor_plan.columns
    assert ("Navn", "varchar") in actor_plan.columns
    assert ("changedt", "timestamp") in actor_plan.columns


def test_render_fdw_bootstrap_sql_includes_metadata_and_foreign_tables(
    fdw_project: Path,
) -> None:
    """Rendered SQL should include metadata registration and FDW DDL."""
    del fdw_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        "default",
        FdwBootstrapRequest(tables=("Actor",)),
    )
    sql_text = render_fdw_bootstrap_sql(plan)

    assert "--   - extension tds_fdw must already exist in the target PostgreSQL database" in sql_text
    assert 'INSERT INTO "cdc_management"."source_instance"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."Actor_CT"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."Actor_base"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."cdc_min_lsn_Actor"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."cdc_max_lsn"' in sql_text
    assert "SELECT sys.fn_cdc_get_min_lsn(''dbo_Actor'') AS min_lsn" in sql_text
    assert "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn" in sql_text
    assert 'CREATE ROLE "cdc_runner"' not in sql_text
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."customer_registry"' not in sql_text
    assert 'CREATE SCHEMA IF NOT EXISTS "cdc_management";' not in sql_text
    assert "CREATE EXTENSION IF NOT EXISTS tds_fdw;" not in sql_text


def test_fdw_cli_sql_supports_multiple_runner_roles(
    fdw_project: Path,
) -> None:
    """Repeated --runner-role options should create one mapping per PostgreSQL role."""
    output_path = fdw_project / "generated" / "fdw" / "adopus-default-dev-fdw.sql"

    result = fdw_main(
        [
            "sql",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--runner-role",
            "cdc_runner",
            "--runner-role",
            "postgres",
        ]
    )

    assert result == 0
    sql_text = output_path.read_text(encoding="utf-8")
    assert "-- Runner roles: cdc_runner, postgres" in sql_text
    assert 'CREATE USER MAPPING FOR "cdc_runner"' in sql_text
    assert 'ALTER USER MAPPING FOR "cdc_runner"' in sql_text
    assert 'CREATE USER MAPPING FOR "postgres"' in sql_text
    assert 'ALTER USER MAPPING FOR "postgres"' in sql_text


def test_build_fdw_bootstrap_plan_can_infer_routes_from_target_sink_env(
    fdw_project: Path,
) -> None:
    """Route selection should work without an explicit source env filter."""
    del fdw_project

    plan = build_fdw_bootstrap_plan(
        "adopus",
        None,
        FdwBootstrapRequest(target_sink_env="dev"),
    )

    assert plan.source_env is None
    assert plan.resolved_source_envs == ("default",)
    assert plan.resolved_server_names == ("default",)
    assert [source_plan.customer_name for source_plan in plan.source_plans] == ["Test"]
    assert plan.source_plans[0].target_sink_env == "dev"


def test_fdw_cli_can_infer_single_available_service(
    fdw_project: Path,
) -> None:
    """fdw commands should not require --service when only one service exists."""
    del fdw_project

    result = fdw_main(
        [
            "plan",
            "--target-sink-env",
            "dev",
        ]
    )

    assert result == 0


def test_list_target_sink_envs_for_service_reads_sink_group_envs(
    fdw_project: Path,
) -> None:
    """FDW target sink env completion should come from sink-groups.yaml."""
    del fdw_project

    assert list_target_sink_envs_for_service("adopus") == ["dev", "prod", "stage"]


def test_fdw_cli_sql_writes_metadata_only_output(
    fdw_project: Path,
) -> None:
    """The fdw CLI should write generated SQL to the default metadata path."""
    output_path = fdw_project / "generated" / "fdw" / "adopus-default-stage-metadata.sql"

    result = fdw_main(
        [
            "sql",
            "--service",
            "adopus",
            "--metadata-only",
            "--target-sink-env",
            "stage",
        ]
    )

    assert result == 0
    sql_text = output_path.read_text(encoding="utf-8")
    assert 'INSERT INTO "cdc_management"."source_table_registration"' in sql_text
    assert "CREATE SERVER" not in sql_text
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."customer_registry"' not in sql_text


def test_fdw_cli_sql_uses_structured_default_output_name(
    fdw_project: Path,
) -> None:
    """The fdw CLI should derive the default output file from service/source/sink."""
    result = fdw_main(
        [
            "sql",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )

    assert result == 0
    output_path = fdw_project / "generated" / "fdw" / "adopus-default-dev-fdw.sql"
    assert output_path.exists()


def test_fdw_cli_sql_concatenates_multiple_resolved_server_names(
    fdw_project: Path,
) -> None:
    """Auto-generated filenames should include all resolved source server names."""
    multi_server_source_groups = """adopus:
  pattern: db-per-tenant
  type: mssql
  servers:
    nonprod:
      host: ${MSSQL_SOURCE_HOST}
      port: ${MSSQL_SOURCE_PORT}
      user: ${MSSQL_SOURCE_USER}
      password: ${MSSQL_SOURCE_PASSWORD}
    prod:
      host: ${MSSQL_SOURCE_HOST}
      port: ${MSSQL_SOURCE_PORT}
      user: ${MSSQL_SOURCE_USER}
      password: ${MSSQL_SOURCE_PASSWORD}
  sources:
    Test:
      schemas:
        - dbo
      nonprod:
        server: nonprod
        database: AdOpusTest
        customer_id: 4d43855c-afa9-45ca-9e31-382dbde9681b
        target_sink_env: dev
    FretexDev:
      schemas:
        - dbo
      prod:
        server: prod
        database: AdOpusFretexDev
        customer_id: 04ed3971-ea9a-49e0-a0ba-5170c16a8d64
        target_sink_env: dev
"""
    (fdw_project / "source-groups.yaml").write_text(multi_server_source_groups, encoding="utf-8")

    result = fdw_main(
        [
            "sql",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )

    assert result == 0
    output_path = fdw_project / "generated" / "fdw" / "adopus-nonprod_prod-dev-fdw.sql"
    assert output_path.exists()


def test_fdw_cli_apply_uses_resolved_sink_target_and_default_sql_path(
    fdw_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fdw apply should resolve the sink DB from sink-groups and call psql."""
    sql_result = fdw_main(
        [
            "sql",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )
    assert sql_result == 0

    output_path = fdw_project / "generated" / "fdw" / "adopus-default-dev-fdw.sql"
    assert output_path.exists()
    captured: dict[str, object] = {}

    monkeypatch.setattr("cdc_generator.cli.fdw.shutil.which", lambda _value: "/usr/bin/psql")

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured["check"] = check
        captured["env"] = env
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr("cdc_generator.cli.fdw.subprocess.run", fake_run)

    apply_result = fdw_main(
        [
            "apply",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
        ]
    )

    assert apply_result == 0
    assert captured["command"] == [
        "/usr/bin/psql",
        "-h",
        "10.90.37.20",
        "-p",
        "5432",
        "-U",
        "pg_user_nonprod",
        "-d",
        "directory_dev",
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        "generated/fdw/adopus-default-dev-fdw.sql",
    ]
    assert captured["check"] is False
    assert cast(dict[str, str], captured["env"])["PGPASSWORD"] == "pg_password_nonprod"


def test_fdw_cli_apply_refreshes_default_sql_for_current_runner_roles(
    fdw_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fdw apply should rewrite the default SQL file from the current CLI arguments."""
    del fdw_project

    captured: dict[str, object] = {}
    output_path = Path("generated/fdw/adopus-default-dev-fdw.sql")

    monkeypatch.setattr("cdc_generator.cli.fdw.shutil.which", lambda _value: "/usr/bin/psql")

    def fake_run(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured["check"] = check
        captured["env"] = env
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr("cdc_generator.cli.fdw.subprocess.run", fake_run)

    apply_result = fdw_main(
        [
            "apply",
            "--service",
            "adopus",
            "--target-sink-env",
            "dev",
            "--runner-role",
            "cdc_runner",
            "--runner-role",
            "postgres",
        ]
    )

    assert apply_result == 0
    assert output_path.exists()
    sql_text = output_path.read_text(encoding="utf-8")
    assert 'CREATE USER MAPPING FOR "cdc_runner"' in sql_text
    assert 'CREATE USER MAPPING FOR "postgres"' in sql_text
    assert captured["command"] == [
        "/usr/bin/psql",
        "-h",
        "10.90.37.20",
        "-p",
        "5432",
        "-U",
        "pg_user_nonprod",
        "-d",
        "directory_dev",
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        "generated/fdw/adopus-default-dev-fdw.sql",
    ]
