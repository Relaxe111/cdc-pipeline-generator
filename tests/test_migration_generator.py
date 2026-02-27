"""Unit tests for migration_generator.py.

Covers:
- MigrationColumn / TableMigration / SinkTarget / GenerationResult dataclasses
- build_columns_from_table_def (MSSQL→PG mapping, ignore_columns, PK detection)
- build_full_column_list (full pipeline with CDC metadata + column templates)
- _add_cdc_metadata_columns
- _compute_checksum / _inject_checksum
- get_sinks / resolve_sink_target
- load_table_definitions
- _derive_target_schemas
- generate_migrations (integration with tmp_path)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from cdc_generator.core.migration_generator import (
    CDC_METADATA_COLUMNS,
    GenerationResult,
    MigrationColumn,
    SinkTarget,
    TableMigration,
    _add_cdc_metadata_columns,
    _build_column_defs_sql,
    _build_create_table_sql,
    _compute_checksum,
    _derive_target_schemas,
    _inject_checksum,
    build_columns_from_table_def,
    build_full_column_list,
    get_sinks,
    load_table_definitions,
    resolve_sink_target,
)

# ---------------------------------------------------------------------------
# Dataclass construction
# ---------------------------------------------------------------------------


class TestDataclasses:
    """Verify dataclass defaults and construction."""

    def test_migration_column_defaults(self) -> None:
        col = MigrationColumn(name="id", type="INTEGER")
        assert col.nullable is True
        assert col.primary_key is False
        assert col.default is None

    def test_migration_column_pk(self) -> None:
        col = MigrationColumn(name="id", type="INTEGER", nullable=False, primary_key=True)
        assert col.primary_key is True
        assert col.nullable is False

    def test_table_migration_defaults(self) -> None:
        tm = TableMigration(
            table_name="Actor",
            target_schema="adopus",
            source_schema="dbo",
            columns=[],
            primary_keys=[],
        )
        assert tm.replicate_structure is True
        assert tm.target_exists is False

    def test_sink_target_defaults(self) -> None:
        st = SinkTarget(sink_name="s.d", sink_group="s", sink_service="d")
        assert st.databases == {}

    def test_generation_result_defaults(self) -> None:
        r = GenerationResult()
        assert r.files_written == 0
        assert r.tables_processed == 0
        assert r.schemas == []
        assert r.sink_targets == []
        assert r.errors == []
        assert r.warnings == []
        assert r.output_dir is None


# ---------------------------------------------------------------------------
# build_columns_from_table_def
# ---------------------------------------------------------------------------


class TestBuildColumnsFromTableDef:
    """Test column extraction from service-schema YAML format."""

    def test_basic_columns(self) -> None:
        """Extracts columns with types from _schemas format."""
        table_def: dict[str, Any] = {
            "table": "Actor",
            "columns": [
                {"name": "actno", "type": "int", "nullable": False, "primary_key": True},
                {"name": "name", "type": "nvarchar", "nullable": True},
            ],
        }
        mapper = MagicMock()
        mapper.map_type.side_effect = lambda t: {"int": "integer", "nvarchar": "varchar"}.get(t, "text")

        cols, pks = build_columns_from_table_def(table_def, type_mapper=mapper)

        assert len(cols) == 2
        assert cols[0].name == "actno"
        assert cols[0].type == "integer"
        assert cols[0].primary_key is True
        assert cols[1].name == "name"
        assert cols[1].type == "varchar"
        assert pks == ["actno"]

    def test_ignore_columns(self) -> None:
        """Columns in ignore list are excluded."""
        table_def: dict[str, Any] = {
            "table": "Actor",
            "columns": [
                {"name": "actno", "type": "int", "primary_key": True},
                {"name": "secret", "type": "nvarchar"},
                {"name": "name", "type": "nvarchar"},
            ],
        }
        cols, _pks = build_columns_from_table_def(
            table_def, ignore_columns=["secret"],
        )
        names = [c.name for c in cols]
        assert "secret" not in names
        assert "actno" in names
        assert "name" in names

    def test_ignore_columns_case_insensitive(self) -> None:
        """Ignore columns comparison is case-insensitive."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [
                {"name": "Secret", "type": "int"},
                {"name": "ok", "type": "int"},
            ],
        }
        cols, _ = build_columns_from_table_def(
            table_def, ignore_columns=["SECRET"],
        )
        assert len(cols) == 1
        assert cols[0].name == "ok"

    def test_no_type_mapper_passthrough(self) -> None:
        """Without a type mapper, raw MSSQL types pass through as-is."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [{"name": "x", "type": "uniqueidentifier"}],
        }
        cols, _ = build_columns_from_table_def(table_def, type_mapper=None)
        assert cols[0].type == "uniqueidentifier"

    def test_fields_format(self) -> None:
        """Supports 'fields' key (table-definitions format) without type mapping."""
        table_def: dict[str, Any] = {
            "fields": [
                {"postgres": "actno", "type": "integer", "primary_key": True},
                {"postgres": "name", "type": "varchar"},
            ],
        }
        cols, pks = build_columns_from_table_def(table_def)
        assert len(cols) == 2
        assert cols[0].name == "actno"
        assert cols[0].type == "integer"
        assert pks == ["actno"]

    def test_empty_columns(self) -> None:
        """Returns empty when no columns found."""
        cols, pks = build_columns_from_table_def({"table": "Empty"})
        assert cols == []
        assert pks == []

    def test_multiple_primary_keys(self) -> None:
        """Supports composite primary keys."""
        table_def: dict[str, Any] = {
            "table": "Composite",
            "columns": [
                {"name": "a", "type": "int", "primary_key": True},
                {"name": "b", "type": "int", "primary_key": True},
                {"name": "c", "type": "varchar"},
            ],
        }
        cols, pks = build_columns_from_table_def(table_def)
        assert pks == ["a", "b"]
        assert len(cols) == 3

    def test_primary_keys_are_deduplicated_case_insensitive(self) -> None:
        """Duplicate PK entries differing by case are deduplicated."""
        table_def: dict[str, Any] = {
            "table": "DupPk",
            "columns": [
                {"name": "Id", "type": "int", "primary_key": True},
                {"name": "id", "type": "int", "primary_key": True},
                {"name": "value", "type": "nvarchar"},
            ],
        }

        _cols, pks = build_columns_from_table_def(table_def)

        assert pks == ["Id"]

    def test_duplicate_columns_are_deduplicated_case_insensitive(self) -> None:
        """Duplicate source columns are deduplicated to prevent duplicate DDL lines."""
        table_def: dict[str, Any] = {
            "table": "DupCol",
            "columns": [
                {"name": "Id", "type": "int", "primary_key": True},
                {"name": "id", "type": "int", "primary_key": True},
                {"name": "Name", "type": "nvarchar"},
            ],
        }

        cols, pks = build_columns_from_table_def(table_def)

        assert [c.name for c in cols] == ["Id", "Name"]
        assert pks == ["Id"]


# ---------------------------------------------------------------------------
# _add_cdc_metadata_columns
# ---------------------------------------------------------------------------


class TestAddCdcMetadataColumns:
    """Test CDC metadata column injection."""

    def test_adds_all_metadata_columns(self) -> None:
        """All 6 CDC metadata columns are appended."""
        cols = [MigrationColumn(name="id", type="INTEGER")]
        result = _add_cdc_metadata_columns(cols)
        assert len(result) == 1 + len(CDC_METADATA_COLUMNS)

    def test_metadata_column_names(self) -> None:
        """Check exact metadata column names."""
        result = _add_cdc_metadata_columns([])
        names = [c.name for c in result]
        assert "__sync_timestamp" in names
        assert "__source" in names
        assert "__source_db" in names
        assert "__source_table" in names
        assert "__source_ts_ms" in names
        assert "__cdc_operation" in names

    def test_no_duplicates(self) -> None:
        """Existing metadata columns are not duplicated."""
        cols = [MigrationColumn(name="__sync_timestamp", type="TIMESTAMP")]
        result = _add_cdc_metadata_columns(cols)
        ts_count = sum(1 for c in result if c.name == "__sync_timestamp")
        assert ts_count == 1

    def test_sync_timestamp_is_not_null(self) -> None:
        """__sync_timestamp has NOT NULL + DEFAULT."""
        result = _add_cdc_metadata_columns([])
        ts = next(c for c in result if c.name == "__sync_timestamp")
        assert ts.nullable is False
        assert ts.default == "CURRENT_TIMESTAMP"


# ---------------------------------------------------------------------------
# _compute_checksum / _inject_checksum
# ---------------------------------------------------------------------------


class TestChecksum:
    """Test SHA256 checksum computation and injection."""

    def test_compute_deterministic(self) -> None:
        """Same content produces same checksum."""
        content = "SELECT 1;\n"
        assert _compute_checksum(content) == _compute_checksum(content)

    def test_compute_different(self) -> None:
        """Different content produces different checksum."""
        assert _compute_checksum("SELECT 1;") != _compute_checksum("SELECT 2;")

    def test_inject_adds_checksum_line(self) -> None:
        """Checksum is injected as a SQL comment."""
        content = (
            "-- " + "=" * 76 + "\n"
            "-- DO NOT EDIT\n"
            "-- " + "=" * 76 + "\n"
            "\nCREATE TABLE t (id INT);\n"
        )
        result = _inject_checksum(content)
        assert "-- Checksum: sha256:" in result

    def test_inject_after_header_block(self) -> None:
        """Checksum line placed after the closing header separator."""
        header_sep = "-- " + "=" * 76
        content = f"{header_sep}\n-- Header\n{header_sep}\n\nSELECT 1;\n"
        result = _inject_checksum(content)
        lines = result.splitlines()
        # Checksum should be on line index 3 (after 2nd separator)
        checksum_idx = next(
            i for i, line in enumerate(lines) if line.startswith("-- Checksum:")
        )
        sep_indices = [i for i, line in enumerate(lines) if line == header_sep]
        assert checksum_idx == sep_indices[1] + 1

    def test_inject_no_header(self) -> None:
        """If no header block, checksum is prepended."""
        content = "SELECT 1;\n"
        result = _inject_checksum(content)
        assert result.startswith("-- Checksum: sha256:")


# ---------------------------------------------------------------------------
# get_sinks
# ---------------------------------------------------------------------------


class TestGetSinks:
    """Test sink extraction from service config."""

    def test_single_sink(self) -> None:
        """Extracts tables grouped by sink name."""
        config: dict[str, object] = {
            "sinks": {
                "sink_asma.directory": {
                    "tables": {
                        "adopus.Actor": {"from": "dbo.Actor"},
                        "adopus.Role": {"from": "dbo.Role"},
                    },
                },
            },
        }
        result = get_sinks(config)
        assert "sink_asma.directory" in result
        assert len(result["sink_asma.directory"]) == 2

    def test_multiple_sinks(self) -> None:
        config: dict[str, object] = {
            "sinks": {
                "s1.db1": {"tables": {"a.T1": {"from": "dbo.T1"}}},
                "s2.db2": {"tables": {"b.T2": {"from": "dbo.T2"}}},
            },
        }
        result = get_sinks(config)
        assert len(result) == 2

    def test_no_sinks(self) -> None:
        assert get_sinks({}) == {}

    def test_empty_tables(self) -> None:
        config: dict[str, object] = {
            "sinks": {"s.d": {"tables": {}}},
        }
        assert get_sinks(config) == {}


# ---------------------------------------------------------------------------
# resolve_sink_target
# ---------------------------------------------------------------------------


class TestResolveSinkTarget:
    """Test sink target resolution from sink-groups.yaml."""

    def test_parses_sink_name(self, tmp_path: Path) -> None:
        """Correctly splits sink_group and sink_service."""
        # No sink-groups.yaml → databases empty
        st = resolve_sink_target("sink_asma.directory", tmp_path)
        assert st.sink_group == "sink_asma"
        assert st.sink_service == "directory"
        assert st.databases == {}

    def test_with_sink_groups_yaml(self, tmp_path: Path) -> None:
        """Resolves per-env database names from sink-groups.yaml."""
        sg_content = (
            "sink_asma:\n"
            "  type: postgres\n"
            "  sources:\n"
            "    directory:\n"
            "      dev:\n"
            "        database: directory_dev\n"
            "      prod:\n"
            "        database: directory_prod\n"
        )
        (tmp_path / "sink-groups.yaml").write_text(sg_content)
        st = resolve_sink_target("sink_asma.directory", tmp_path)
        assert st.databases == {"dev": "directory_dev", "prod": "directory_prod"}

    def test_single_part_sink_name(self, tmp_path: Path) -> None:
        """Handles sink name without dot separator."""
        st = resolve_sink_target("simple", tmp_path)
        assert st.sink_group == "simple"
        assert st.sink_service == ""


# ---------------------------------------------------------------------------
# load_table_definitions
# ---------------------------------------------------------------------------


class TestLoadTableDefinitions:
    """Test table definition loading from services/_schemas/."""

    def test_loads_from_schema_dir(self, tmp_path: Path) -> None:
        """Loads YAML files from _schemas/{service}/{schema}/ dirs."""
        schema_dir = tmp_path / "services" / "_schemas" / "test_svc" / "dbo"
        schema_dir.mkdir(parents=True)
        (schema_dir / "Actor.yaml").write_text(
            "table: Actor\ncolumns:\n  - name: id\n    type: int\n",
        )
        with (
            patch(
                "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
                return_value=[tmp_path / "services" / "_schemas" / "test_svc"],
            ),
        ):
            result = load_table_definitions("test_svc", tmp_path)
        assert "dbo.Actor" in result
        assert result["dbo.Actor"]["table"] == "Actor"

    def test_empty_when_no_schemas(self, tmp_path: Path) -> None:
        """Returns empty dict when no schema dirs exist."""
        with patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[tmp_path / "nonexistent"],
        ):
            assert load_table_definitions("x", tmp_path) == {}

    def test_multiple_schemas(self, tmp_path: Path) -> None:
        """Loads from multiple schema subdirectories."""
        base = tmp_path / "services" / "_schemas" / "svc"
        for schema_name in ("dbo", "hr"):
            d = base / schema_name
            d.mkdir(parents=True)
            (d / "T1.yaml").write_text(f"table: T1_{schema_name}\ncolumns: []\n")

        with patch(
            "cdc_generator.core.migration_generator.get_service_schema_read_dirs",
            return_value=[base],
        ):
            result = load_table_definitions("svc", tmp_path)
        assert "dbo.T1_dbo" in result
        assert "hr.T1_hr" in result


# ---------------------------------------------------------------------------
# _derive_target_schemas
# ---------------------------------------------------------------------------


class TestDeriveTargetSchemas:
    """Test schema extraction from sink table keys."""

    def test_extracts_schemas(self) -> None:
        tables: dict[str, dict[str, Any]] = {
            "adopus.Actor": {},
            "adopus.Role": {},
            "hr.Employee": {},
        }
        assert _derive_target_schemas(tables) == ["adopus", "hr"]

    def test_excludes_public(self) -> None:
        tables: dict[str, dict[str, Any]] = {"public.T1": {}}
        assert _derive_target_schemas(tables) == []

    def test_no_dot_keys(self) -> None:
        tables: dict[str, dict[str, Any]] = {"T1": {}}
        assert _derive_target_schemas(tables) == []


# ---------------------------------------------------------------------------
# _build_column_defs_sql / _build_create_table_sql
# ---------------------------------------------------------------------------


class TestSqlRendering:
    """Test SQL generation helpers."""

    def test_column_defs_basic(self) -> None:
        cols = [
            MigrationColumn(name="id", type="INTEGER", nullable=False),
            MigrationColumn(name="name", type="VARCHAR(255)"),
        ]
        lines = _build_column_defs_sql(cols)
        assert len(lines) == 2
        assert '"id" INTEGER NOT NULL' in lines[0]
        assert '"name" VARCHAR(255)' in lines[1]
        assert "NOT NULL" not in lines[1]

    def test_column_defs_with_default(self) -> None:
        cols = [
            MigrationColumn(
                name="ts", type="TIMESTAMP", nullable=False, default="CURRENT_TIMESTAMP",
            ),
        ]
        lines = _build_column_defs_sql(cols)
        assert "DEFAULT CURRENT_TIMESTAMP" in lines[0]

    def test_create_table_sql_format(self) -> None:
        cols = [
            MigrationColumn(name="id", type="INTEGER", nullable=False, primary_key=True),
            MigrationColumn(name="val", type="TEXT"),
        ]
        sql = _build_create_table_sql(
            target_schema="myschema",
            table_name="MyTable",
            columns=cols,
            primary_keys=["id"],
            source_schema="dbo",
            generated_at="2025-01-01 00:00:00 UTC",
        )
        assert 'CREATE TABLE IF NOT EXISTS "myschema"."MyTable"' in sql
        assert '"id" INTEGER NOT NULL' in sql
        assert 'PRIMARY KEY ("id")' in sql
        assert "idx_MyTable_sync_ts" in sql

    def test_create_table_no_pk(self) -> None:
        cols = [MigrationColumn(name="val", type="TEXT")]
        sql = _build_create_table_sql(
            target_schema="s", table_name="T",
            columns=cols, primary_keys=[],
            source_schema="dbo", generated_at="now",
        )
        assert "PRIMARY KEY" not in sql

    def test_create_table_deduplicates_primary_keys(self) -> None:
        cols = [
            MigrationColumn(name="Id", type="INTEGER", nullable=False, primary_key=True),
            MigrationColumn(name="name", type="TEXT"),
        ]
        sql = _build_create_table_sql(
            target_schema="myschema",
            table_name="MyTable",
            columns=cols,
            primary_keys=["Id", "id"],
            source_schema="dbo",
            generated_at="2025-01-01 00:00:00 UTC",
        )

        assert 'PRIMARY KEY ("Id")' in sql
        assert 'PRIMARY KEY ("Id", "id")' not in sql


# ---------------------------------------------------------------------------
# build_full_column_list
# ---------------------------------------------------------------------------


class TestBuildFullColumnList:
    """Test the full column pipeline (build + templates + metadata)."""

    def test_includes_cdc_metadata(self) -> None:
        """Full pipeline adds CDC metadata columns."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [
                {"name": "id", "type": "int", "primary_key": True},
            ],
        }
        service_config: dict[str, object] = {
            "source": {"tables": {"dbo.T": {}}},
        }
        sink_cfg: dict[str, object] = {}

        cols, pks = build_full_column_list(
            table_def, sink_cfg, service_config, "dbo.T",
        )
        names = [c.name for c in cols]
        assert "__sync_timestamp" in names
        assert "__cdc_operation" in names
        assert pks == ["id"]

    def test_respects_ignore_columns(self) -> None:
        """Full pipeline honours ignore_columns from source config."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [
                {"name": "id", "type": "int", "primary_key": True},
                {"name": "secret", "type": "nvarchar"},
            ],
        }
        service_config: dict[str, object] = {
            "source": {
                "tables": {
                    "dbo.T": {"ignore_columns": ["secret"]},
                },
            },
        }
        sink_cfg: dict[str, object] = {}
        cols, _ = build_full_column_list(
            table_def, sink_cfg, service_config, "dbo.T",
        )
        names = [c.name for c in cols]
        assert "secret" not in names
        assert "id" in names

    @patch("cdc_generator.core.migration_generator.resolve_column_templates")
    def test_preserves_exact_column_template_name(
        self,
        mock_resolve_templates: MagicMock,
    ) -> None:
        """Template column names are preserved exactly as configured."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [{"name": "Id", "type": "int", "primary_key": True}],
        }
        service_config: dict[str, object] = {"source": {"tables": {"dbo.T": {}}}}
        sink_cfg: dict[str, object] = {
            "column_templates": [{"template": "customer_id", "name": "_CustomerExact"}],
        }

        template = MagicMock()
        template.column_type = "uuid"
        template.not_null = True
        template.default = None
        resolved = MagicMock()
        resolved.name = "_CustomerExact"
        resolved.template = template
        mock_resolve_templates.return_value = [resolved]

        cols, _ = build_full_column_list(table_def, sink_cfg, service_config, "dbo.T")
        names = [c.name for c in cols]
        assert "_CustomerExact" in names

    @patch("cdc_generator.core.migration_generator.resolve_transforms")
    def test_preserves_exact_transform_output_names(
        self,
        mock_resolve_transforms: MagicMock,
    ) -> None:
        """Transform-produced output column names keep exact casing/underscore."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [{"name": "Id", "type": "int", "primary_key": True}],
        }
        service_config: dict[str, object] = {"source": {"tables": {"dbo.T": {}}}}
        sink_cfg: dict[str, object] = {
            "transforms": [{"bloblang_ref": "services/_bloblang/x.blobl"}],
        }

        resolved_transform = MagicMock()
        resolved_transform.bloblang = "root._customer = this.Id\nroot.CustomerStatus = \"ok\""
        mock_resolve_transforms.return_value = [resolved_transform]

        cols, _ = build_full_column_list(table_def, sink_cfg, service_config, "dbo.T")
        names = [c.name for c in cols]
        assert "_customer" in names
        assert "CustomerStatus" in names

    @patch("cdc_generator.core.migration_generator.resolve_transforms")
    def test_preserves_expected_output_column_name(
        self,
        mock_resolve_transforms: MagicMock,
    ) -> None:
        """Transform expected_output_column is included without renaming."""
        table_def: dict[str, Any] = {
            "table": "T",
            "columns": [{"name": "Id", "type": "int", "primary_key": True}],
        }
        service_config: dict[str, object] = {"source": {"tables": {"dbo.T": {}}}}
        sink_cfg: dict[str, object] = {
            "transforms": [{"expected_output_column": "_CustomerFromTransform"}],
        }

        mock_resolve_transforms.return_value = []

        cols, _ = build_full_column_list(table_def, sink_cfg, service_config, "dbo.T")
        names = [c.name for c in cols]
        assert "_CustomerFromTransform" in names


# ---------------------------------------------------------------------------
# generate_migrations — integration test with tmp_path
# ---------------------------------------------------------------------------


class TestGenerateMigrations:
    """Integration test for the full generate_migrations flow."""

    def _setup_project(self, tmp_path: Path) -> None:
        """Set up a minimal project structure for generation."""
        # source-groups.yaml
        (tmp_path / "source-groups.yaml").write_text(
            "test:\n  pattern: db-per-tenant\n  type: mssql\n"
            "  sources:\n    proxy:\n      schemas:\n        - dbo\n",
        )
        # sink-groups.yaml
        (tmp_path / "sink-groups.yaml").write_text(
            "sink_test:\n  type: postgres\n  sources:\n"
            "    db:\n      dev:\n        database: test_dev\n",
        )
        # services/test_svc.yaml
        svc_dir = tmp_path / "services"
        svc_dir.mkdir()
        (svc_dir / "test_svc.yaml").write_text(
            "test_svc:\n"
            "  source:\n"
            "    tables:\n"
            "      dbo.Actor:\n"
            "        primary_key: actno\n"
            "  sinks:\n"
            "    sink_test.db:\n"
            "      tables:\n"
            "        myschema.Actor:\n"
            "          from: dbo.Actor\n",
        )
        # Table definition YAML
        schema_dir = tmp_path / "services" / "_schemas" / "test_svc" / "dbo"
        schema_dir.mkdir(parents=True)
        (schema_dir / "Actor.yaml").write_text(
            "table: Actor\n"
            "columns:\n"
            "  - name: actno\n"
            "    type: int\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: name\n"
            "    type: nvarchar\n"
            "    nullable: true\n",
        )

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    @patch("cdc_generator.core.migration_generator.get_service_schema_read_dirs")
    def test_generates_files(
        self,
        mock_schema_dirs: MagicMock,
        mock_load_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Full generation creates expected directory structure."""
        self._setup_project(tmp_path)

        mock_root.return_value = tmp_path
        mock_load_config.return_value = {
            "service": "test_svc",
            "source": {
                "tables": {
                    "dbo.Actor": {"primary_key": "actno"},
                },
            },
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Actor": {"from": "dbo.Actor"},
                    },
                },
            },
        }
        mock_schema_dirs.return_value = [
            tmp_path / "services" / "_schemas" / "test_svc",
        ]

        from cdc_generator.core.migration_generator import generate_migrations

        output = tmp_path / "migrations"
        result = generate_migrations(
            "test_svc", output_dir=output,
        )

        assert result.errors == [], f"Errors: {result.errors}"
        assert result.files_written > 0
        assert result.tables_processed >= 1

        # Check directory structure
        sink_dir = output / "sink_test.db"
        assert sink_dir.exists()
        assert (sink_dir / "manifest.yaml").exists()
        assert (sink_dir / "00-infrastructure" / "01-create-schemas.sql").exists()
        assert (sink_dir / "00-infrastructure" / "02-cdc-management.sql").exists()
        assert (sink_dir / "01-tables" / "Actor.sql").exists()

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    def test_missing_service_config(
        self,
        mock_load: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when service config not found."""
        mock_root.return_value = tmp_path
        mock_load.side_effect = FileNotFoundError("not found")

        from cdc_generator.core.migration_generator import generate_migrations

        result = generate_migrations("missing")
        assert len(result.errors) == 1
        assert "not found" in result.errors[0]

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    def test_no_sinks_returns_error(
        self,
        mock_load: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Returns error when service config has no sinks."""
        mock_root.return_value = tmp_path
        mock_load.return_value = {"source": {"tables": {}}}

        from cdc_generator.core.migration_generator import generate_migrations

        result = generate_migrations("svc")
        assert any("No sink tables" in e for e in result.errors)

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    @patch("cdc_generator.core.migration_generator.get_service_schema_read_dirs")
    def test_checksum_in_generated_files(
        self,
        mock_schema_dirs: MagicMock,
        mock_load_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Generated SQL files contain SHA256 checksum comments."""
        self._setup_project(tmp_path)
        mock_root.return_value = tmp_path
        mock_load_config.return_value = {
            "service": "test_svc",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {"myschema.Actor": {"from": "dbo.Actor"}},
                },
            },
        }
        mock_schema_dirs.return_value = [
            tmp_path / "services" / "_schemas" / "test_svc",
        ]

        from cdc_generator.core.migration_generator import generate_migrations

        output = tmp_path / "migrations"
        generate_migrations("test_svc", output_dir=output)

        actor_sql = (output / "sink_test.db" / "01-tables" / "Actor.sql").read_text()
        assert "-- Checksum: sha256:" in actor_sql

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    @patch("cdc_generator.core.migration_generator.get_service_schema_read_dirs")
    def test_creates_manual_required_file_for_destructive_type_change(
        self,
        mock_schema_dirs: MagicMock,
        mock_load_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Second generation emits table-scoped manual file when type changes."""
        self._setup_project(tmp_path)
        mock_root.return_value = tmp_path
        mock_load_config.return_value = {
            "service": "test_svc",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {"myschema.Actor": {"from": "dbo.Actor"}},
                },
            },
        }
        mock_schema_dirs.return_value = [
            tmp_path / "services" / "_schemas" / "test_svc",
        ]

        from cdc_generator.core.migration_generator import generate_migrations

        output = tmp_path / "migrations"
        first = generate_migrations("test_svc", output_dir=output)
        assert first.errors == []

        actor_yaml = tmp_path / "services" / "_schemas" / "test_svc" / "dbo" / "Actor.yaml"
        actor_yaml.write_text(
            "table: Actor\n"
            "columns:\n"
            "  - name: actno\n"
            "    type: int\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: name\n"
            "    type: int\n"
            "    nullable: true\n",
        )

        second = generate_migrations("test_svc", output_dir=output)
        assert second.errors == []

        manual_file = output / "sink_test.db" / "02-manual" / "Actor" / "MANUAL_REQUIRED.sql"
        assert manual_file.exists()
        manual_content = manual_file.read_text(encoding="utf-8")
        assert "COLUMN_TYPE_CHANGED" in manual_content
        assert "name" in manual_content

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    @patch("cdc_generator.core.migration_generator.get_service_schema_read_dirs")
    def test_creates_manual_required_file_for_removed_table(
        self,
        mock_schema_dirs: MagicMock,
        mock_load_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Generation emits manual file when a previously generated table is removed."""
        self._setup_project(tmp_path)
        mock_root.return_value = tmp_path
        mock_schema_dirs.return_value = [
            tmp_path / "services" / "_schemas" / "test_svc",
        ]

        from cdc_generator.core.migration_generator import generate_migrations

        initial_config = {
            "service": "test_svc",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {"myschema.Actor": {"from": "dbo.Actor"}},
                },
            },
        }
        mock_load_config.return_value = initial_config

        output = tmp_path / "migrations"
        first = generate_migrations("test_svc", output_dir=output)
        assert first.errors == []

        other_yaml = tmp_path / "services" / "_schemas" / "test_svc" / "dbo" / "Other.yaml"
        other_yaml.write_text(
            "table: Other\n"
            "columns:\n"
            "  - name: id\n"
            "    type: int\n"
            "    nullable: false\n"
            "    primary_key: true\n",
            encoding="utf-8",
        )

        removed_config = {
            "service": "test_svc",
            "source": {
                "tables": {
                    "dbo.Other": {"primary_key": "id"},
                },
            },
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Other": {"from": "dbo.Other"},
                    },
                },
            },
        }
        mock_load_config.return_value = removed_config

        second = generate_migrations("test_svc", output_dir=output)
        assert second.errors == []

        manual_file = output / "sink_test.db" / "02-manual" / "Actor" / "MANUAL_REQUIRED.sql"
        assert manual_file.exists()
        manual_content = manual_file.read_text(encoding="utf-8")
        assert "TABLE_REMOVED" in manual_content
        assert "Actor" in manual_content

    @patch("cdc_generator.core.migration_generator.get_project_root")
    @patch("cdc_generator.core.migration_generator.load_service_config")
    @patch("cdc_generator.core.migration_generator.get_service_schema_read_dirs")
    def test_manual_migration_hints_are_included_in_manual_required_sql(
        self,
        mock_schema_dirs: MagicMock,
        mock_load_config: MagicMock,
        mock_root: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Hint block from services yaml is included before fallback suggestions."""
        self._setup_project(tmp_path)
        mock_root.return_value = tmp_path
        mock_schema_dirs.return_value = [
            tmp_path / "services" / "_schemas" / "test_svc",
        ]

        from cdc_generator.core.migration_generator import generate_migrations

        base_config = {
            "service": "test_svc",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Actor": {"from": "dbo.Actor"},
                    },
                },
            },
        }
        mock_load_config.return_value = base_config

        output = tmp_path / "migrations"
        first = generate_migrations("test_svc", output_dir=output)
        assert first.errors == []

        actor_yaml = tmp_path / "services" / "_schemas" / "test_svc" / "dbo" / "Actor.yaml"
        actor_yaml.write_text(
            "table: Actor\n"
            "columns:\n"
            "  - name: actno\n"
            "    type: int\n"
            "    nullable: false\n"
            "    primary_key: true\n"
            "  - name: score\n"
            "    type: int\n"
            "    nullable: true\n",
            encoding="utf-8",
        )

        hinted_config = {
            "service": "test_svc",
            "source": {"tables": {"dbo.Actor": {"primary_key": "actno"}}},
            "sinks": {
                "sink_test.db": {
                    "tables": {
                        "myschema.Actor": {
                            "from": "dbo.Actor",
                            "manual_migration_hints": {
                                "type_changes": [
                                    {
                                        "column": "score",
                                        "using": "NULLIF(trim(\"score\"), '')::integer",
                                    },
                                ],
                            },
                        },
                    },
                },
            },
        }
        mock_load_config.return_value = hinted_config

        second = generate_migrations("test_svc", output_dir=output)
        assert second.errors == []

        manual_file = output / "sink_test.db" / "02-manual" / "Actor" / "MANUAL_REQUIRED.sql"
        assert manual_file.exists()
        manual_content = manual_file.read_text(encoding="utf-8")
        assert "Hint-based SQL from services/<service>.yaml manual_migration_hints" in manual_content
        assert "USING NULLIF(trim(\"score\"), '')::integer" in manual_content
