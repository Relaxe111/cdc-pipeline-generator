"""Focused tests for native FDW runtime migration generation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from cdc_generator.core.migration_generator import generate_migrations
from cdc_generator.core.migration_generator.data_structures import MigrationColumn

_SERVICE_CONFIG: dict[str, object] = {
    "service": "native_test",
    "source": {
        "tables": {
            "dbo.Actor": {},
        },
    },
    "sinks": {
        "sink_test.db": {
            "tables": {
                "adopus.Actor": {
                    "from": "dbo.Actor",
                    "replicate_structure": True,
                    "column_templates": [
                        {"template": "customer_id", "name": "customer_id"},
                    ],
                },
            },
        },
    },
}


def _write_native_project(tmp_path: Path) -> Path:
    (tmp_path / "source-groups.yaml").write_text(
        "adopus:\n"
        "  pattern: db-per-tenant\n"
        "  type: mssql\n"
        "  sources:\n"
        "    Test:\n"
        "      schemas:\n"
        "        - dbo\n",
        encoding="utf-8",
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_test:\n"
        "  type: postgres\n"
        "  sources:\n"
        "    db:\n"
        "      dev:\n"
        "        database: native_dev\n",
        encoding="utf-8",
    )
    schema_dir = tmp_path / "services" / "_schemas" / "native_test" / "dbo"
    schema_dir.mkdir(parents=True, exist_ok=True)
    (schema_dir / "Actor.yaml").write_text(
        "table: Actor\n"
        "columns:\n"
        "  - name: actno\n"
        "    type: int\n"
        "    nullable: false\n"
        "    primary_key: true\n"
        "  - name: Navn\n"
        "    type: varchar\n"
        "    nullable: true\n",
        encoding="utf-8",
    )
    return schema_dir.parent


def _inject_customer_id_column(
    columns: list[MigrationColumn],
    _table_cfg: dict[str, object],
) -> list[MigrationColumn]:
    """Test helper to inject the resolved customer_id template column."""
    if any(column.name.casefold() == "customer_id" for column in columns):
        return columns
    return [
        *columns,
        MigrationColumn(name="customer_id", type="UUID", nullable=False),
    ]


def test_generate_native_runtime_writes_expected_files(tmp_path: Path) -> None:
    """FDW topology should derive the native runtime and emit expected SQL."""
    schema_base = _write_native_project(tmp_path)
    output_dir = tmp_path / "migrations"

    with (
        patch(
            "cdc_generator.core.migration_generator.get_project_root",
            return_value=tmp_path,
        ),
        patch(
            "cdc_generator.core.migration_generator.load_service_config",
            return_value=_SERVICE_CONFIG,
        ),
        patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        ),
        patch(
            "cdc_generator.core.migration_generator.table_processing.add_column_template_columns",
            side_effect=_inject_customer_id_column,
        ),
    ):
        result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            topology="fdw",
        )

    assert result.errors == []

    sink_dir = output_dir / "sink_test.db"
    native_infra_sql = (sink_dir / "00-infrastructure" / "03-native-cdc-runtime.sql").read_text(
        encoding="utf-8",
    )
    final_table_sql = (sink_dir / "01-tables" / "Actor.sql").read_text(
        encoding="utf-8",
    )
    staging_sql = (sink_dir / "01-tables" / "Actor-staging.sql").read_text(
        encoding="utf-8",
    )
    manifest_text = (sink_dir / "manifest.yaml").read_text(encoding="utf-8")

    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_checkpoint"' in native_infra_sql
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_schedule_policy"' in native_infra_sql
    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "cdc_management"."native_cdc_runtime_state"' in native_infra_sql
    assert 'ALTER TABLE "cdc_management"."native_cdc_runtime_state" SET UNLOGGED' in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."claim_due_native_cdc_work"' in native_infra_sql
    assert 'CREATE OR REPLACE PROCEDURE "cdc_management"."renew_native_cdc_lease"' in native_infra_sql
    assert 'CREATE OR REPLACE VIEW "cdc_management"."v_native_cdc_health"' in native_infra_sql
    assert 'ADD COLUMN IF NOT EXISTS "jitter_millis" integer' in native_infra_sql
    assert "interval '1 millisecond'" in native_infra_sql
    assert 'runtime_mode: "native"' in manifest_text
    assert 'topology_kind: "mssql_fdw_pull"' in manifest_text
    assert 'runtime_engine: "postgres_native"' in manifest_text
    assert '00-infrastructure/03-native-cdc-runtime.sql' in manifest_text

    assert 'PRIMARY KEY ("customer_id", "actno")' in final_table_sql
    assert '"__source_start_lsn" BYTEA' in final_table_sql
    assert '"__source_seqval" BYTEA' in final_table_sql
    assert '"__cdc_operation" INTEGER' in final_table_sql

    assert 'CREATE OR REPLACE FUNCTION "adopus"."pull_actor_batch"' in staging_sql
    assert 'CREATE OR REPLACE PROCEDURE "adopus"."sp_merge_actor"' in staging_sql
    assert 'cdc_min_lsn_Actor' in staging_sql
    assert 'INSERT INTO "adopus"."stg_Actor"' in staging_sql


def test_generate_native_runtime_renders_configured_policy_seed(tmp_path: Path) -> None:
    """Native runtime should render explicit source-table native_cdc policy metadata."""
    schema_base = _write_native_project(tmp_path)
    output_dir = tmp_path / "migrations"
    service_config: dict[str, object] = {
        **_SERVICE_CONFIG,
        "source": {
            "tables": {
                "dbo.Actor": {
                    "native_cdc": {
                        "enabled": False,
                        "schedule_profile": "hot",
                        "poll_interval_seconds": 5,
                        "min_poll_interval_seconds": 1,
                        "max_poll_interval_seconds": 600,
                        "max_rows_per_pull": 2000,
                        "lease_seconds": 180,
                        "poll_priority": 10,
                        "jitter_millis": 25,
                        "max_backoff_seconds": 1200,
                        "business_hours_profile_key": "weekday_hot",
                    },
                },
            },
        },
    }

    with (
        patch(
            "cdc_generator.core.migration_generator.get_project_root",
            return_value=tmp_path,
        ),
        patch(
            "cdc_generator.core.migration_generator.load_service_config",
            return_value=service_config,
        ),
        patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        ),
        patch(
            "cdc_generator.core.migration_generator.table_processing.add_column_template_columns",
            side_effect=_inject_customer_id_column,
        ),
    ):
        result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            runtime_mode="native",
        )

    assert result.errors == []

    native_infra_sql = (
        output_dir / "sink_test.db" / "00-infrastructure" / "03-native-cdc-runtime.sql"
    ).read_text(encoding="utf-8")

    assert "'weekday_hot'" in native_infra_sql
    assert "'hot'" in native_infra_sql
    assert "2000" in native_infra_sql
    assert "1200" in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."resolve_native_cdc_schedule_policy"' in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."sync_native_cdc_registration_state"' in native_infra_sql
    assert 'CREATE TRIGGER "trg_sync_native_cdc_registration_state"' in native_infra_sql


def test_native_runtime_requires_customer_id_template(tmp_path: Path) -> None:
    """Native runtime mode should reject shared tables without customer_id."""
    schema_base = _write_native_project(tmp_path)
    output_dir = tmp_path / "migrations"
    service_config: dict[str, object] = {
        **_SERVICE_CONFIG,
        "sinks": {
            "sink_test.db": {
                "tables": {
                    "adopus.Actor": {
                        "from": "dbo.Actor",
                        "replicate_structure": True,
                    },
                },
            },
        },
    }

    with (
        patch(
            "cdc_generator.core.migration_generator.get_project_root",
            return_value=tmp_path,
        ),
        patch(
            "cdc_generator.core.migration_generator.load_service_config",
            return_value=service_config,
        ),
        patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        ),
    ):
        result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            runtime_mode="native",
        )

    assert result.errors == [
        "Table adopus.Actor: native runtime requires a customer_id column template",
    ]
