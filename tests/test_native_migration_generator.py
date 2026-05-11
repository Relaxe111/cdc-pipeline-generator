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
                },
            },
        },
    },
}

_EXISTING_TARGET_SERVICE_CONFIG: dict[str, object] = {
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
                    "target_exists": True,
                },
            },
        },
    },
}


def _write_native_project(tmp_path: Path) -> Path:
    (tmp_path / "source-groups.yaml").write_text(
        "adopus:\n  pattern: db-per-tenant\n  type: mssql\n  sources:\n    Test:\n      schemas:\n        - dbo\n",
        encoding="utf-8",
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_test:\n  type: postgres\n  sources:\n    db:\n      dev:\n        database: native_dev\n",
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
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_bootstrap_state"' in native_infra_sql
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_schedule_policy"' in native_infra_sql
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_tier_assignment"' in native_infra_sql
    assert 'CREATE TABLE IF NOT EXISTS "cdc_management"."native_cdc_activity_rollup_hourly"' in native_infra_sql
    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "cdc_management"."native_cdc_runtime_state"' in native_infra_sql
    assert 'ALTER TABLE "cdc_management"."native_cdc_runtime_state" SET UNLOGGED' in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."claim_due_native_cdc_work"' in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."bootstrap_native_cdc_tables"' in native_infra_sql
    assert 'CREATE OR REPLACE PROCEDURE "cdc_management"."renew_native_cdc_lease"' in native_infra_sql
    assert 'CREATE OR REPLACE VIEW "cdc_management"."v_native_cdc_health"' in native_infra_sql
    assert 'ADD COLUMN IF NOT EXISTS "tier_mode" text' in native_infra_sql
    assert 'ADD COLUMN IF NOT EXISTS "manual_schedule_profile" text' in native_infra_sql
    assert 'ADD COLUMN IF NOT EXISTS "jitter_millis" integer' in native_infra_sql
    assert "interval '1 millisecond'" in native_infra_sql
    assert 'runtime_mode: "native"' in manifest_text
    assert 'topology_kind: "mssql_fdw_pull"' in manifest_text
    assert 'runtime_engine: "postgres_native"' in manifest_text
    assert "00-infrastructure/03-native-cdc-runtime.sql" in manifest_text
    # Phase A: Native mode must NOT include legacy broker management SQL
    assert "02-cdc-management.sql" not in manifest_text, "Native runtime manifest must not reference legacy broker management SQL"
    # Phase B: Table count must be non-zero
    assert "table_count: 1" in manifest_text, "Native runtime manifest must have non-zero table count"

    assert 'PRIMARY KEY ("customer_id", "actno")' in final_table_sql
    assert '"__source_start_lsn" BYTEA' in final_table_sql
    assert '"__source_seqval" BYTEA' in final_table_sql
    assert '"__cdc_operation" INTEGER' in final_table_sql

    assert 'CREATE OR REPLACE FUNCTION "adopus"."pull_actor_batch"' in staging_sql
    assert 'CREATE OR REPLACE FUNCTION "adopus"."bootstrap_actor_snapshot"' in staging_sql
    assert 'CREATE OR REPLACE PROCEDURE "adopus"."sp_merge_actor"' in staging_sql
    assert "Actor_base" in staging_sql
    assert "cdc_max_lsn" in staging_sql
    assert "cdc_min_lsn_Actor" in staging_sql
    assert "native_cdc_bootstrap_state" in staging_sql
    assert 'INSERT INTO "adopus"."Actor"' in staging_sql
    assert 'INSERT INTO "adopus"."stg_Actor"' in staging_sql

    # Phase C: Bootstrap defaults to 'pending', not 'completed'
    assert "COALESCE(bootstrap." in native_infra_sql
    compact = native_infra_sql.replace(" ", "").replace("\n", "")
    assert "bootstrap_status\",'pending')" in compact, "Bootstrap status must default to 'pending', not implicitly 'completed'"
    # Phase C: No more unsafe implicit completed bootstrap from enabled registration
    assert 'WHEN reg."enabled" OR policy."enabled" THEN \'completed\'' not in native_infra_sql, (
        "Enabled registration must not imply completed bootstrap"
    )

    # Phase C: Health view must expose unsafe-state columns
    assert '"unsafe_enabled_without_bootstrap"' in native_infra_sql
    assert '"unsafe_bootstrap_completed_without_checkpoint"' in native_infra_sql
    assert '"unsafe_failed_bootstrap"' in native_infra_sql
    assert '"unsafe_pending_bootstrap_enabled"' in native_infra_sql
    assert '"unsafe_stalled_bootstrap"' in native_infra_sql

    # Phase E: claim_due_native_cdc_work uses policy.max_rows_per_pull as primary source
    assert 'policy."max_rows_per_pull"' in compact, "Claim must use policy.max_rows_per_pull, not only hardcoded tier presets"
    claim_start = native_infra_sql.index(
        'CREATE OR REPLACE FUNCTION "cdc_management"."claim_due_native_cdc_work"',
    )
    claim_end = native_infra_sql.index("$$;\n", claim_start)
    claim_sql = native_infra_sql[claim_start:claim_end]
    assert '"jitter_millis" integer' in claim_sql
    assert '"max_backoff_seconds" integer' in claim_sql

    # Phase D: sp_merge_<table> checkpoint update is inside merge procedure (not pull)
    assert 'UPDATE "cdc_management"."native_cdc_checkpoint"' in staging_sql
    assert "sp_merge_actor" in staging_sql
    # Checkpoint advancement should happen AFTER the merge operations
    ckpt_line_idx = staging_sql.index('UPDATE "cdc_management"."native_cdc_checkpoint"')
    merge_line_idx = staging_sql.index("ON CONFLICT")
    assert ckpt_line_idx > merge_line_idx, "Checkpoint advancement must occur after merge/apply, not before"


def test_generate_native_runtime_keeps_existing_target_tables_and_cleans_stale_legacy_sql(
    tmp_path: Path,
) -> None:
    """Native mode should still generate per-table SQL for target_exists tables."""
    schema_base = _write_native_project(tmp_path)
    output_dir = tmp_path / "migrations"
    stale_legacy_sql = output_dir / "sink_test.db" / "00-infrastructure" / "02-cdc-management.sql"
    stale_legacy_sql.parent.mkdir(parents=True, exist_ok=True)
    stale_legacy_sql.write_text("-- stale legacy file\n", encoding="utf-8")

    with (
        patch(
            "cdc_generator.core.migration_generator.get_project_root",
            return_value=tmp_path,
        ),
        patch(
            "cdc_generator.core.migration_generator.load_service_config",
            return_value=_EXISTING_TARGET_SERVICE_CONFIG,
        ),
        patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        ),
    ):
        result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            topology="fdw",
        )

    stale_manual_sql = output_dir / "sink_test.db" / "02-manual" / "Actor" / "MANUAL_REQUIRED.sql"
    stale_manual_sql.parent.mkdir(parents=True, exist_ok=True)
    stale_manual_sql.write_text("-- stale manual file\n", encoding="utf-8")

    with (
        patch(
            "cdc_generator.core.migration_generator.get_project_root",
            return_value=tmp_path,
        ),
        patch(
            "cdc_generator.core.migration_generator.load_service_config",
            return_value=_EXISTING_TARGET_SERVICE_CONFIG,
        ),
        patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[schema_base],
        ),
    ):
        second_result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            topology="fdw",
        )

    assert result.errors == []
    assert second_result.errors == []

    sink_dir = output_dir / "sink_test.db"
    manifest_text = (sink_dir / "manifest.yaml").read_text(encoding="utf-8")
    final_table_sql = (sink_dir / "01-tables" / "Actor.sql").read_text(
        encoding="utf-8",
    )
    staging_sql = (sink_dir / "01-tables" / "Actor-staging.sql").read_text(
        encoding="utf-8",
    )

    assert "table_count: 1" in manifest_text
    assert not (sink_dir / "00-infrastructure" / "02-cdc-management.sql").exists()
    assert not (sink_dir / "02-manual" / "Actor" / "MANUAL_REQUIRED.sql").exists()
    assert 'CREATE TABLE IF NOT EXISTS "adopus"."Actor"' in final_table_sql
    assert 'CREATE OR REPLACE FUNCTION "adopus"."pull_actor_batch"' in staging_sql
    assert 'CREATE OR REPLACE PROCEDURE "adopus"."sp_merge_actor"' in staging_sql


def test_generate_native_runtime_renders_resolve_policy_dynamic(
    tmp_path: Path,
) -> None:
    """Native runtime resolve function should query native_cdc_schedule_policy directly."""
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
    ):
        result = generate_migrations(
            "native_test",
            output_dir=output_dir,
            topology="fdw",
            runtime_mode="native",
        )

    assert result.errors == []

    native_infra_sql = (output_dir / "sink_test.db" / "00-infrastructure" / "03-native-cdc-runtime.sql").read_text(encoding="utf-8")

    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."resolve_native_cdc_schedule_policy"' in native_infra_sql
    assert 'CREATE OR REPLACE FUNCTION "cdc_management"."sync_native_cdc_registration_state"' in native_infra_sql
    assert 'CREATE TRIGGER "trg_sync_native_cdc_registration_state"' in native_infra_sql

    # Function body should contain a direct SELECT from the table, not VALUES
    func_start = native_infra_sql.index("LANGUAGE sql")
    func_end = native_infra_sql.index("$$;\n", func_start)
    func_body = native_infra_sql[func_start:func_end]
    assert "native_cdc_schedule_policy" in func_body
    assert "UNION ALL" in func_body
    assert "WHERE NOT EXISTS" in func_body
    assert "VALUES" not in func_body
    # sync call passes source_instance_key + logical_table_name
    assert 'v_registration."source_instance_key"' in native_infra_sql


def test_native_runtime_auto_injects_customer_id_column(tmp_path: Path) -> None:
    """Native runtime mode should auto-inject customer_id without YAML templates."""
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

    assert result.errors == []

    final_table_sql = (output_dir / "sink_test.db" / "01-tables" / "Actor.sql").read_text(
        encoding="utf-8",
    )
    assert '"customer_id" UUID NOT NULL' in final_table_sql
    assert 'PRIMARY KEY ("customer_id", "actno")' in final_table_sql
