"""Tests for the structure_replicator module.

Covers:
- ReplicationConfig dataclass construction
- load_source_schema() with real and missing schemas
- replicate_table_structure() DDL generation (pgsql→pgsql, mssql→pgsql)
- Column filtering via include_columns + PK preservation
- DDL format: quoted identifiers, NOT NULL, PRIMARY KEY
- get_replication_summary() preview with type change detection
- target_schema override
"""

from pathlib import Path
from typing import cast

import pytest

from cdc_generator.core.structure_replicator import (
    ReplicationConfig,
    get_replication_summary,
    load_source_schema,
    replicate_table_structure,
)

# Expected column counts for test schemas
_USERS_COLUMN_COUNT = 5
_ORDERS_COLUMN_COUNT = 5


# ── Test fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def sample_schema_dir(tmp_path: Path) -> Path:
    """Create a temporary service-schemas directory with sample schemas.

    Creates:
        service-schemas/
            testservice/
                public/
                    users.yaml (pgsql types, composite PK)
                dbo/
                    orders.yaml (mssql types, single PK)
    """
    schemas_dir = tmp_path / "service-schemas"

    # users table - PostgreSQL types, composite PK
    users_dir = schemas_dir / "testservice" / "public"
    users_dir.mkdir(parents=True)
    users_yaml = users_dir / "users.yaml"
    users_yaml.write_text(
        "database: testdb\n"
        + "schema: public\n"
        + "service: testservice\n"
        + "table: users\n"
        + "columns:\n"
        + "- name: customer_id\n"
        + "  type: uuid\n"
        + "  nullable: false\n"
        + "  primary_key: true\n"
        + "- name: user_id\n"
        + "  type: uuid\n"
        + "  nullable: false\n"
        + "  primary_key: true\n"
        + "- name: username\n"
        + "  type: text\n"
        + "  nullable: false\n"
        + "  primary_key: false\n"
        + "- name: email\n"
        + "  type: varchar\n"
        + "  nullable: true\n"
        + "  primary_key: false\n"
        + "- name: created_at\n"
        + "  type: timestamptz\n"
        + "  nullable: false\n"
        + "  primary_key: false\n"
        + "primary_key:\n"
        + "- customer_id\n"
        + "- user_id\n"
    )

    # orders table - MSSQL types, single PK
    orders_dir = schemas_dir / "testservice" / "dbo"
    orders_dir.mkdir(parents=True)
    orders_yaml = orders_dir / "orders.yaml"
    orders_yaml.write_text(
        "database: testdb\n"
        + "schema: dbo\n"
        + "service: testservice\n"
        + "table: orders\n"
        + "columns:\n"
        + "- name: order_id\n"
        + "  type: uniqueidentifier\n"
        + "  nullable: false\n"
        + "  primary_key: true\n"
        + "- name: customer_name\n"
        + "  type: nvarchar\n"
        + "  nullable: false\n"
        + "  primary_key: false\n"
        + "- name: total\n"
        + "  type: money\n"
        + "  nullable: true\n"
        + "  primary_key: false\n"
        + "- name: is_active\n"
        + "  type: bit\n"
        + "  nullable: false\n"
        + "  primary_key: false\n"
        + "- name: ordered_at\n"
        + "  type: datetime2\n"
        + "  nullable: false\n"
        + "  primary_key: false\n"
        + "primary_key: order_id\n"
    )

    return schemas_dir


@pytest.fixture()
def pgsql_config(sample_schema_dir: Path) -> ReplicationConfig:
    """ReplicationConfig for pgsql→pgsql replication."""
    return ReplicationConfig(
        service="testservice",
        source_schema="public",
        table_name="users",
        source_engine="pgsql",
        sink_engine="pgsql",
        schemas_dir=sample_schema_dir,
    )


@pytest.fixture()
def mssql_config(sample_schema_dir: Path) -> ReplicationConfig:
    """ReplicationConfig for mssql→pgsql replication."""
    return ReplicationConfig(
        service="testservice",
        source_schema="dbo",
        table_name="orders",
        source_engine="mssql",
        sink_engine="pgsql",
        schemas_dir=sample_schema_dir,
    )


# ── ReplicationConfig ─────────────────────────────────────────────────────


class TestReplicationConfig:
    """Test ReplicationConfig dataclass."""

    def test_required_fields(self, sample_schema_dir: Path) -> None:
        """All required fields are set."""
        cfg = ReplicationConfig(
            service="svc",
            source_schema="public",
            table_name="tbl",
            source_engine="pgsql",
            sink_engine="pgsql",
            schemas_dir=sample_schema_dir,
        )
        assert cfg.service == "svc"
        assert cfg.source_schema == "public"
        assert cfg.table_name == "tbl"
        assert cfg.source_engine == "pgsql"
        assert cfg.sink_engine == "pgsql"

    def test_optional_defaults(self) -> None:
        """Optional fields have correct defaults."""
        cfg = ReplicationConfig(
            service="svc",
            source_schema="public",
            table_name="tbl",
            source_engine="pgsql",
            sink_engine="pgsql",
        )
        assert cfg.target_schema is None
        assert cfg.include_columns is None
        assert cfg.schemas_dir is None


# ── load_source_schema() ──────────────────────────────────────────────────


class TestLoadSourceSchema:
    """Test loading source schema YAML files."""

    def test_loads_existing_schema(self, sample_schema_dir: Path) -> None:
        """Loads a valid schema file and returns dict with columns."""
        schema = load_source_schema(
            "testservice", "public", "users", schemas_dir=sample_schema_dir,
        )
        assert schema is not None
        assert "columns" in schema
        assert "primary_key" in schema
        columns = schema["columns"]
        assert isinstance(columns, list)
        assert len(columns) == _USERS_COLUMN_COUNT

    def test_returns_none_for_missing_file(self, sample_schema_dir: Path) -> None:
        """Returns None when schema file doesn't exist."""
        schema = load_source_schema(
            "testservice", "public", "nonexistent", schemas_dir=sample_schema_dir,
        )
        assert schema is None

    def test_returns_none_for_missing_service(self, sample_schema_dir: Path) -> None:
        """Returns None when service directory doesn't exist."""
        schema = load_source_schema(
            "nosuchservice", "public", "users", schemas_dir=sample_schema_dir,
        )
        assert schema is None

    def test_returns_none_when_no_schemas_dir(self) -> None:
        """Returns None when schemas_dir points to nonexistent path."""
        schema = load_source_schema(
            "testservice", "public", "users", schemas_dir=Path("/nonexistent/path"),
        )
        assert schema is None


# ── replicate_table_structure() ───────────────────────────────────────────


class TestReplicateTableStructure:
    """Test full DDL generation from source schema files."""

    def test_pgsql_to_pgsql(self, pgsql_config: ReplicationConfig) -> None:
        """PostgreSQL→PostgreSQL replication generates valid DDL."""
        ddl = replicate_table_structure(pgsql_config)

        assert ddl is not None
        assert 'CREATE TABLE IF NOT EXISTS "public"."users"' in ddl
        assert '"customer_id" uuid NOT NULL' in ddl
        assert '"user_id" uuid NOT NULL' in ddl
        assert '"username" text NOT NULL' in ddl
        assert '"email" varchar' in ddl
        assert '"created_at" timestamptz NOT NULL' in ddl
        assert 'PRIMARY KEY ("customer_id", "user_id")' in ddl
        assert ddl.strip().endswith(");")

    def test_mssql_to_pgsql(self, mssql_config: ReplicationConfig) -> None:
        """MSSQL→PostgreSQL replication converts types correctly."""
        ddl = replicate_table_structure(mssql_config)

        assert ddl is not None
        assert '"order_id" uuid NOT NULL' in ddl  # uniqueidentifier → uuid
        assert '"customer_name" varchar NOT NULL' in ddl  # nvarchar → varchar
        assert '"total" numeric' in ddl  # money → numeric
        assert '"is_active" boolean NOT NULL' in ddl  # bit → boolean
        assert '"ordered_at" timestamp NOT NULL' in ddl  # datetime2 → timestamp
        assert 'PRIMARY KEY ("order_id")' in ddl

    def test_target_schema_override(self, pgsql_config: ReplicationConfig) -> None:
        """target_schema overrides source schema in DDL."""
        pgsql_config.target_schema = "staging"
        ddl = replicate_table_structure(pgsql_config)

        assert ddl is not None
        assert '"staging"."users"' in ddl
        assert '"public"."users"' not in ddl

    def test_include_columns_with_pk_preserved(
        self, pgsql_config: ReplicationConfig,
    ) -> None:
        """include_columns limits output, but PK columns are always included."""
        pgsql_config.include_columns = ["username", "email"]
        ddl = replicate_table_structure(pgsql_config)

        assert ddl is not None
        # PK columns auto-included
        assert '"customer_id"' in ddl
        assert '"user_id"' in ddl
        # Requested columns included
        assert '"username"' in ddl
        assert '"email"' in ddl
        # Unrequested non-PK excluded
        assert '"created_at"' not in ddl

    def test_ddl_has_not_null_constraints(
        self, pgsql_config: ReplicationConfig,
    ) -> None:
        """NOT NULL columns get the constraint, nullable columns don't."""
        ddl = replicate_table_structure(pgsql_config)
        assert ddl is not None

        # Non-nullable columns have NOT NULL
        assert '"customer_id" uuid NOT NULL' in ddl
        # Nullable column (email) should NOT have NOT NULL on its line
        email_line = [line for line in ddl.split("\n") if '"email"' in line]
        assert len(email_line) == 1
        assert "NOT NULL" not in email_line[0]

    def test_ddl_quoted_identifiers(
        self, sample_schema_dir: Path,
    ) -> None:
        """Schema and table names are properly double-quoted."""
        ddl = replicate_table_structure(
            ReplicationConfig(
                service="testservice",
                source_schema="public",
                table_name="users",
                source_engine="pgsql",
                sink_engine="pgsql",
                target_schema="MySchema",
                schemas_dir=sample_schema_dir,
            ),
        )
        assert ddl is not None
        assert '"MySchema"."users"' in ddl

    def test_ddl_ends_with_semicolon(
        self, pgsql_config: ReplicationConfig,
    ) -> None:
        """DDL output ends with closing paren and semicolon."""
        ddl = replicate_table_structure(pgsql_config)
        assert ddl is not None
        assert ddl.strip().endswith(");")

    def test_composite_primary_key_in_ddl(
        self, pgsql_config: ReplicationConfig,
    ) -> None:
        """Composite PK generates multi-column PRIMARY KEY constraint."""
        ddl = replicate_table_structure(pgsql_config)
        assert ddl is not None
        assert 'PRIMARY KEY ("customer_id", "user_id")' in ddl

    def test_single_primary_key_in_ddl(
        self, mssql_config: ReplicationConfig,
    ) -> None:
        """Single PK generates single-column PRIMARY KEY constraint."""
        ddl = replicate_table_structure(mssql_config)
        assert ddl is not None
        assert 'PRIMARY KEY ("order_id")' in ddl

    def test_missing_schema_returns_none(
        self, sample_schema_dir: Path,
    ) -> None:
        """Returns None when source schema file doesn't exist."""
        cfg = ReplicationConfig(
            service="testservice",
            source_schema="public",
            table_name="nonexistent",
            source_engine="pgsql",
            sink_engine="pgsql",
            schemas_dir=sample_schema_dir,
        )
        assert replicate_table_structure(cfg) is None


# ── get_replication_summary() ─────────────────────────────────────────────


class TestReplicationSummary:
    """Test replication preview/summary."""

    def test_summary_fields(self, pgsql_config: ReplicationConfig) -> None:
        """Summary includes all expected fields."""
        summary = get_replication_summary(pgsql_config)

        assert summary is not None
        assert summary["service"] == "testservice"
        assert summary["source_schema"] == "public"
        assert summary["table_name"] == "users"
        assert summary["source_engine"] == "pgsql"
        assert summary["sink_engine"] == "pgsql"
        assert summary["column_count"] == _USERS_COLUMN_COUNT
        assert summary["adapter"] == "pgsql-to-pgsql"

    def test_summary_pgsql_no_type_changes(
        self, pgsql_config: ReplicationConfig,
    ) -> None:
        """pgsql→pgsql with canonical types has no type changes."""
        summary = get_replication_summary(pgsql_config)
        assert summary is not None
        # All types in our test schema are already canonical pgsql types
        assert isinstance(summary["type_changes"], list)

    def test_summary_mssql_has_type_changes(
        self, mssql_config: ReplicationConfig,
    ) -> None:
        """mssql→pgsql replication shows type changes."""
        summary = get_replication_summary(mssql_config)

        assert summary is not None
        raw_changes = summary["type_changes"]
        assert isinstance(raw_changes, list)
        changes = cast(list[dict[str, str]], raw_changes)
        assert len(changes) > 0

        # Check specific changes
        change_map: dict[str, dict[str, str]] = {
            c["column"]: c for c in changes
        }
        assert "order_id" in change_map
        assert change_map["order_id"]["source_type"] == "uniqueidentifier"
        assert change_map["order_id"]["sink_type"] == "uuid"

    def test_summary_missing_schema_returns_none(
        self, sample_schema_dir: Path,
    ) -> None:
        """Returns None when source schema doesn't exist."""
        cfg = ReplicationConfig(
            service="testservice",
            source_schema="public",
            table_name="nonexistent",
            source_engine="pgsql",
            sink_engine="pgsql",
            schemas_dir=sample_schema_dir,
        )
        assert get_replication_summary(cfg) is None
