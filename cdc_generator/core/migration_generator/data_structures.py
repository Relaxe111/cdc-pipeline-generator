"""Data structures for migration generation.

Contains all dataclasses used in migration generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from jinja2 import Environment

from cdc_generator.helpers.type_mapper import TypeMapper

RuntimeMode = Literal["brokered", "native"]


@dataclass
class MigrationColumn:
    """Column definition for migration DDL.

    Attributes:
        name: Column name (PostgreSQL).
        type: PostgreSQL type string.
        nullable: Whether the column allows NULL.
        primary_key: Whether this column is part of the PK.
        default: Optional SQL default expression.
    """

    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    default: str | None = None


@dataclass
class TableMigration:
    """Parsed table info ready for migration generation.

    Attributes:
        table_name: PostgreSQL table name.
        target_schema: PostgreSQL target schema (e.g., 'adopus').
        source_schema: MSSQL source schema (e.g., dbo).
        columns: All columns including CDC metadata and extras.
        primary_keys: List of primary key column names.
        replicate_structure: Whether structure comes from table-definition YAML.
        target_exists: Whether the target table already exists.
        source_table: MSSQL source table name.
        source_key: Full source reference (e.g., 'dbo.Actor').
        foreign_table_name: Expected local FDW table name.
        min_lsn_table_name: Expected local helper FDW table name.
        capture_instance_name: MSSQL CDC capture instance name.
    """

    table_name: str
    target_schema: str
    source_schema: str
    columns: list[MigrationColumn]
    primary_keys: list[str]
    replicate_structure: bool = True
    target_exists: bool = False
    source_table: str | None = None
    source_key: str | None = None
    foreign_table_name: str | None = None
    base_foreign_table_name: str | None = None
    min_lsn_table_name: str | None = None
    capture_instance_name: str | None = None


@dataclass
class SinkTarget:
    """Resolved sink target information.

    Parsed from the service config sink key (e.g., 'sink_asma.directory')
    and enriched with per-environment database names from sink-groups.yaml.

    Attributes:
        sink_name: Full sink key (e.g., 'sink_asma.directory').
        sink_group: Sink group name (e.g., 'sink_asma').
        sink_service: Service/database within the group (e.g., 'directory').
        databases: Per-environment database names (e.g., {'prod': 'directory', 'dev': 'directory_dev'}).
    """

    sink_name: str
    sink_group: str
    sink_service: str
    databases: dict[str, str] = field(default_factory=dict[str, str])


@dataclass(frozen=True)
class NativeCdcPolicySeed:
    """Resolved per-table native CDC policy defaults for SQL rendering."""

    logical_table_name: str
    target_schema_name: str
    target_table_name: str
    enabled: bool = True
    schedule_profile: str = "warm"
    tier_mode: str = "auto"
    manual_schedule_profile: str | None = None
    base_poll_interval_seconds: int = 5
    min_poll_interval_seconds: int = 5
    max_poll_interval_seconds: int = 60
    max_rows_per_pull: int = 1000
    lease_seconds: int = 120
    poll_priority: int = 100
    jitter_millis: int = 250
    max_backoff_seconds: int = 300
    business_hours_profile_key: str | None = None


@dataclass
class ServiceData:
    """Loaded service data shared across all sink targets.

    Attributes:
        service_config: Full parsed service config.
        table_defs: Table definitions from services/_schemas/.
        type_mapper: Optional MSSQL→PG type converter.
    """

    service_config: dict[str, object]
    table_defs: dict[str, dict[str, Any]]
    type_mapper: TypeMapper | None


@dataclass
class GenerationResult:
    """Result of migration generation run.

    Attributes:
        files_written: Number of files written.
        tables_processed: Number of tables processed.
        schemas: List of customer schema names generated.
        sink_targets: Sink targets that were processed.
        errors: List of error messages.
        warnings: List of warning messages.
        output_dir: Path to the output directory.
    """

    files_written: int = 0
    tables_processed: int = 0
    schemas: list[str] = field(default_factory=list[str])
    sink_targets: list[SinkTarget] = field(default_factory=list[SinkTarget])
    errors: list[str] = field(default_factory=list[str])
    warnings: list[str] = field(default_factory=list[str])
    output_dir: Path | None = None


@dataclass
class RenderContext:
    """Shared context for all template rendering in a single run.

    Attributes:
        jinja_env: Jinja2 environment.
        output_dir: Per-sink output directory.
        generated_at: Timestamp string.
        db_user: Database user for GRANT statements.
        sink_target: Resolved sink target info.
        runtime_mode: Generated SQL runtime model.
    """

    jinja_env: Environment
    output_dir: Path
    generated_at: str
    db_user: str
    sink_target: SinkTarget
    runtime_mode: RuntimeMode = "brokered"
    native_cdc_policy_seeds: list[NativeCdcPolicySeed] = field(default_factory=list[NativeCdcPolicySeed])


@dataclass
class ExistingColumnDef:
    """Column definition parsed from an existing generated CREATE TABLE file."""

    name: str
    type: str
    nullable: bool


@dataclass
class ManualMigrationHints:
    """Optional per-table hints from services/<service>.yaml for manual SQL generation."""

    renames: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])
    type_casts: dict[str, str] = field(default_factory=dict[str, str])
    pre_not_null_sql: dict[str, str] = field(default_factory=dict[str, str])