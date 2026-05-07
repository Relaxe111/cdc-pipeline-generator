"""Focused tests for the canonical ``cdc fdw`` bootstrap flow."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_generator.cli.fdw import main as fdw_main
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
    FretexDev:
      schemas:
      - dbo
      default:
        server: default
        database: AdOpusFretexDev
        customer_id: 04ed3971-ea9a-49e0-a0ba-5170c16a8d64
"""

_SERVICE_YAML = """adopus:
  source:
    validation_database: AdOpusTest
    tables:
      dbo.Actor: {}
      dbo.Soknad: {}
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
"""


def _write_fdw_project(project_root: Path) -> None:
    services_dir = project_root / "services"
    schemas_dir = services_dir / "_schemas" / "adopus" / "dbo"
    schemas_dir.mkdir(parents=True, exist_ok=True)

    (project_root / "source-groups.yaml").write_text(_SOURCE_GROUPS_YAML)
    (project_root / ".env").write_text(_DOTENV)
    (services_dir / "adopus.yaml").write_text(_SERVICE_YAML)
    (schemas_dir / "Actor.yaml").write_text(_ACTOR_SCHEMA_YAML)
    (schemas_dir / "Soknad.yaml").write_text(_SOKNAD_SCHEMA_YAML)


@pytest.fixture()
def fdw_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a minimal implementation repo for FDW bootstrap tests."""
    _write_fdw_project(tmp_path)
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

    actor_plan = next(
        table_plan for table_plan in plan.table_plans if table_plan.logical_table_name == "Actor"
    )
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

    assert 'CREATE EXTENSION IF NOT EXISTS tds_fdw;' in sql_text
    assert 'INSERT INTO "cdc_management"."source_instance"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."Actor_CT"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."Actor_base"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."cdc_min_lsn_Actor"' in sql_text
    assert 'CREATE FOREIGN TABLE "fdw_default_test"."cdc_max_lsn"' in sql_text
    assert "SELECT sys.fn_cdc_get_min_lsn(''dbo_Actor'') AS min_lsn" in sql_text
    assert 'SELECT sys.fn_cdc_get_max_lsn() AS max_lsn' in sql_text


def test_fdw_cli_sql_writes_metadata_only_output(
    fdw_project: Path,
) -> None:
    """The fdw CLI should write generated SQL to the requested output file."""
    output_path = fdw_project / "generated" / "fdw" / "bootstrap.sql"

    result = fdw_main([
        "sql",
        "--service",
        "adopus",
        "--metadata-only",
        "--output",
        str(output_path),
    ])

    assert result == 0
    sql_text = output_path.read_text(encoding="utf-8")
    assert 'INSERT INTO "cdc_management"."source_table_registration"' in sql_text
    assert 'CREATE SERVER' not in sql_text