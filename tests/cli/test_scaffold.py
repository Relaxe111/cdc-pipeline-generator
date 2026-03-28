"""End-to-end tests for ``cdc scaffold`` command.

Tests the full scaffold flow through a real **fish** shell, exactly as
a user would type in the dev container terminal.

Coverage matrix
---------------
- All 4 pattern x source-type combos (db-per-tenant/db-shared x mssql/postgres)
- Extraction pattern: regex written, empty string omitted
- source-groups.yaml deep content (connection placeholders, kafka, description)
- ``--broker-topology per-server``
- Local sink runtime files (tds_fdw Postgres image + pgAdmin assets)
- ``--environment-aware`` validation
- ``--update`` mode (basic + creates missing dirs + merges settings + empty project)
- Error paths (missing each required flag individually, duplicate, invalid values)
- Generated file content (.env.example, docker-compose.yml, README, .gitignore, settings.json)
- File-skip behaviour (existing files not overwritten, docker-compose backed up)
"""

import json
import subprocess
from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc

# Mark all tests in this module as CLI end-to-end tests
pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scaffold_db_per_tenant_mssql(
    run_cdc: RunCdc, name: str = "testproj",
) -> subprocess.CompletedProcess[str]:
    """Run a standard db-per-tenant + mssql scaffold."""
    return run_cdc(
        "scaffold", name,
        "--pattern", "db-per-tenant",
        "--source-type", "mssql",
        "--extraction-pattern", f"^{name}_(?P<customer>[^_]+)$",
    )


def _scaffold_db_shared_postgres(
    run_cdc: RunCdc, name: str = "sharedproj",
) -> subprocess.CompletedProcess[str]:
    """Run a standard db-shared + postgres scaffold."""
    return run_cdc(
        "scaffold", name,
        "--pattern", "db-shared",
        "--source-type", "postgres",
        "--extraction-pattern", "",
        "--environment-aware",
    )


def _assert_directories_exist(project_root: Path, directories: list[str]) -> None:
    """Assert that all expected directories exist."""
    for directory in directories:
        dir_path = project_root / directory
        assert dir_path.is_dir(), f"Directory missing: {directory}"


def _assert_files_exist(project_root: Path, files: list[str]) -> None:
    """Assert that all expected files exist."""
    for file_path in files:
        full_path = project_root / file_path
        assert full_path.is_file(), f"File missing: {file_path}"


def _read_file(project_root: Path, file_path: str) -> str:
    """Read file contents from project root."""
    return (project_root / file_path).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Expected structure
# ---------------------------------------------------------------------------

EXPECTED_DIRECTORIES = [
    "services",
    "services/_schemas",
    "services/_schemas/_definitions",
    "services/_schemas/adapters",
    "services/_bloblang",
    "services/_bloblang/examples",
    "pipelines",
    "pipelines/templates",
    "pipelines/generated",
    "pipelines/generated/sources",
    "pipelines/generated/sinks",
    "generated/schemas",
    "generated/pg-migrations",
    "_docs",
    "pgadmin",
    ".vscode",
]

EXPECTED_FILES = [
    "source-groups.yaml",
    "docker-compose.yml",
    "Dockerfile.pg17-tds-fdw",
    "Dockerfile.pgadmin",
    ".env.example",
    "README.md",
    "_docs/PROJECT_STRUCTURE.md",
    "_docs/ENV_VARIABLES.md",
    "_docs/CDC_CLI.md",
    "_docs/CDC_CLI_FLOW.md",
    "_docs/architecture/MIGRATIONS.md",
    "_docs/architecture/DESTRUCTIVE_CHANGES.md",
    ".gitignore",
    ".vscode/settings.json",
    "pgadmin/entrypoint.sh",
    "pgadmin/servers.json",
    "pgadmin/pgpass",
    "services/_schemas/column-templates.yaml",
    "services/_schemas/transform-rules.yaml",
    "services/_schemas/_definitions/map-mssql-pgsql.yaml",
    "pipelines/templates/source-pipeline.yaml",
    "pipelines/templates/sink-pipeline.yaml",
]


# ═══════════════════════════════════════════════════════════════════════════
# 1. Pattern x source-type combinations (structure + dirs + files)
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldDbPerTenantMssql:
    """cdc scaffold --pattern db-per-tenant --source-type mssql."""

    def test_cdc_scaffold_db_per_tenant_mssql_creates_all_dirs_and_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Full scaffold creates all directories and files."""
        result = _scaffold_db_per_tenant_mssql(run_cdc, "testproject")

        assert result.returncode == 0, (
            f"Scaffold failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        _assert_directories_exist(isolated_project, EXPECTED_DIRECTORIES)
        _assert_files_exist(isolated_project, EXPECTED_FILES)

    def test_cdc_scaffold_db_per_tenant_mssql_env_example_contains_mssql_vars(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes MSSQL_SOURCE_* environment variables."""
        _scaffold_db_per_tenant_mssql(run_cdc)
        content = _read_file(isolated_project, ".env.example")
        assert "MSSQL_SOURCE_HOST" in content
        assert "MSSQL_SOURCE_PORT" in content
        assert "MSSQL_SOURCE_USER" in content
        assert "MSSQL_SOURCE_PASSWORD" in content


class TestScaffoldDbPerTenantPostgres:
    """cdc scaffold --pattern db-per-tenant --source-type postgres."""

    def test_cdc_scaffold_db_per_tenant_postgres_creates_all_dirs_and_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """db-per-tenant + postgres creates all directories and files."""
        result = run_cdc(
            "scaffold", "pgmulti",
            "--pattern", "db-per-tenant",
            "--source-type", "postgres",
            "--extraction-pattern", "^pgmulti_(?P<customer>[^_]+)$",
        )
        assert result.returncode == 0, (
            f"Scaffold failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        _assert_directories_exist(isolated_project, EXPECTED_DIRECTORIES)
        _assert_files_exist(isolated_project, EXPECTED_FILES)

    def test_cdc_scaffold_db_per_tenant_postgres_env_example_contains_postgres_vars(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes POSTGRES_SOURCE_* environment variables."""
        run_cdc(
            "scaffold", "pgmulti",
            "--pattern", "db-per-tenant",
            "--source-type", "postgres",
            "--extraction-pattern", "^pgmulti_(?P<customer>[^_]+)$",
        )
        content = _read_file(isolated_project, ".env.example")
        assert "POSTGRES_SOURCE_HOST" in content
        assert "POSTGRES_SOURCE_PORT" in content
        assert "POSTGRES_SOURCE_USER" in content
        assert "POSTGRES_SOURCE_PASSWORD" in content

    def test_cdc_scaffold_db_per_tenant_postgres_source_groups_has_postgres_type(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml has type: postgres for db-per-tenant + postgres."""
        run_cdc(
            "scaffold", "pgmulti",
            "--pattern", "db-per-tenant",
            "--source-type", "postgres",
            "--extraction-pattern", "^pgmulti_(?P<customer>[^_]+)$",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "pattern: db-per-tenant" in content
        assert "type: postgres" in content


class TestScaffoldDbSharedPostgres:
    """cdc scaffold --pattern db-shared --source-type postgres --environment-aware."""

    def test_cdc_scaffold_db_shared_postgres_creates_all_dirs_and_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Full scaffold creates all directories and files."""
        result = _scaffold_db_shared_postgres(run_cdc, "sharedproj")
        assert result.returncode == 0, (
            f"Scaffold failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        _assert_directories_exist(isolated_project, EXPECTED_DIRECTORIES)
        _assert_files_exist(isolated_project, EXPECTED_FILES)

    def test_cdc_scaffold_db_shared_postgres_env_example_contains_postgres_vars(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes POSTGRES_SOURCE_* environment variables."""
        _scaffold_db_shared_postgres(run_cdc)
        content = _read_file(isolated_project, ".env.example")
        assert "POSTGRES_SOURCE_HOST" in content
        assert "POSTGRES_SOURCE_PORT" in content

    def test_cdc_scaffold_db_shared_postgres_source_groups_has_environment_aware(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml has environment_aware: true for db-shared."""
        _scaffold_db_shared_postgres(run_cdc, "asmatest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "asmatest:" in content
        assert "pattern: db-shared" in content
        assert "type: postgres" in content
        assert "environment_aware: true" in content


class TestScaffoldDbSharedMssql:
    """cdc scaffold --pattern db-shared --source-type mssql --environment-aware."""

    def test_cdc_scaffold_db_shared_mssql_creates_all_dirs_and_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """db-shared + mssql creates all directories and files."""
        result = run_cdc(
            "scaffold", "mssqlshared",
            "--pattern", "db-shared",
            "--source-type", "mssql",
            "--extraction-pattern", "",
            "--environment-aware",
        )
        assert result.returncode == 0, (
            f"Scaffold failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        _assert_directories_exist(isolated_project, EXPECTED_DIRECTORIES)
        _assert_files_exist(isolated_project, EXPECTED_FILES)

    def test_cdc_scaffold_db_shared_mssql_env_example_contains_mssql_vars(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes MSSQL_SOURCE_* variables for db-shared + mssql."""
        run_cdc(
            "scaffold", "mssqlshared",
            "--pattern", "db-shared",
            "--source-type", "mssql",
            "--extraction-pattern", "",
            "--environment-aware",
        )
        content = _read_file(isolated_project, ".env.example")
        assert "MSSQL_SOURCE_HOST" in content
        assert "MSSQL_SOURCE_PORT" in content

    def test_cdc_scaffold_db_shared_mssql_source_groups_has_mssql_type(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml has type: mssql for db-shared + mssql."""
        run_cdc(
            "scaffold", "mssqlshared",
            "--pattern", "db-shared",
            "--source-type", "mssql",
            "--extraction-pattern", "",
            "--environment-aware",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "pattern: db-shared" in content
        assert "type: mssql" in content
        assert "environment_aware: true" in content


# ═══════════════════════════════════════════════════════════════════════════
# 2. source-groups.yaml deep content verification
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldSourceGroupsContent:
    """Verify source-groups.yaml content beyond basic pattern/type."""

    def test_cdc_scaffold_extraction_pattern_regex_written_to_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Non-empty extraction pattern regex is written to source-groups.yaml."""
        run_cdc(
            "scaffold", "regexproj",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^regexproj_(?P<customer>[^_]+)$",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "extraction_pattern:" in content
        assert "regexproj_" in content

    def test_cdc_scaffold_empty_extraction_pattern_omits_key_from_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Empty extraction pattern '' causes key to be omitted from source-groups.yaml."""
        _scaffold_db_shared_postgres(run_cdc, "nopat")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "nopat:" in content
        assert "extraction_pattern" not in content

    def test_cdc_scaffold_source_groups_has_default_server_with_env_placeholders(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml contains servers.default with env var placeholders."""
        _scaffold_db_per_tenant_mssql(run_cdc, "srvtest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "servers:" in content
        assert "default:" in content
        assert "${MSSQL_SOURCE_HOST}" in content
        assert "${MSSQL_SOURCE_PORT}" in content
        assert "${MSSQL_SOURCE_USER}" in content
        assert "${MSSQL_SOURCE_PASSWORD}" in content

    def test_cdc_scaffold_source_groups_has_broker_topology_for_redpanda(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """redpanda scaffolds persist broker_topology."""
        _scaffold_db_per_tenant_mssql(run_cdc, "kafkatest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "broker_topology:" in content

    def test_cdc_scaffold_source_groups_has_default_topology(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml persists the default user-facing topology."""
        _scaffold_db_per_tenant_mssql(run_cdc, "topologydefault")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "topology: redpanda" in content

    def test_cdc_scaffold_source_groups_has_empty_sources_section(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml contains empty sources: {} section."""
        _scaffold_db_per_tenant_mssql(run_cdc, "srctest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "sources:" in content

    def test_cdc_scaffold_source_groups_has_description(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml contains description field matching pattern."""
        _scaffold_db_per_tenant_mssql(run_cdc, "desctest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "description:" in content
        assert "Multi-tenant" in content

    def test_cdc_scaffold_db_shared_source_groups_has_shared_description(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """db-shared source-groups.yaml has 'Shared' in description."""
        _scaffold_db_shared_postgres(run_cdc, "shareddesc")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "Shared" in content

    def test_cdc_scaffold_source_groups_has_metadata_header(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml has metadata comment header."""
        _scaffold_db_per_tenant_mssql(run_cdc, "hdrtest")
        content = _read_file(isolated_project, "source-groups.yaml")
        # The file should start with comment lines (metadata header)
        assert content.startswith("#")

    def test_cdc_scaffold_source_groups_has_kafka_bootstrap_servers_placeholder(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """source-groups.yaml default server has kafka_bootstrap_servers placeholder."""
        _scaffold_db_per_tenant_mssql(run_cdc, "kbtest")
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "kafka_bootstrap_servers:" in content
        assert "${KAFKA_BOOTSTRAP_SERVERS}" in content

    def test_cdc_scaffold_fdw_omits_broker_fields_from_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """fdw scaffolds should not persist redpanda-only broker fields."""
        run_cdc(
            "scaffold", "fdwclean",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^fdwclean_(?P<customer>[^_]+)$",
            "--topology", "fdw",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "topology: fdw" in content
        assert "broker_topology:" not in content
        assert "kafka_bootstrap_servers:" not in content


# ═══════════════════════════════════════════════════════════════════════════
# 3. Kafka topology
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldBrokerTopology:
    """Test --broker-topology flag."""

    def test_cdc_scaffold_broker_topology_per_server_in_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--broker-topology per-server writes per-server bootstrap in source-groups.yaml."""
        run_cdc(
            "scaffold", "brokeralias",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^brokeralias_(?P<customer>[^_]+)$",
            "--broker-topology", "per-server",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "broker_topology: per-server" in content
        assert "KAFKA_BOOTSTRAP_SERVERS_DEFAULT" in content

    def test_cdc_scaffold_broker_topology_per_server_env_example_has_per_server_entries(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--broker-topology per-server generates per-server entries in .env.example."""
        run_cdc(
            "scaffold", "persvr2",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^persvr2_(?P<customer>[^_]+)$",
            "--broker-topology", "per-server",
        )
        content = _read_file(isolated_project, ".env.example")
        assert "per-server" in content.lower() or "KAFKA_BOOTSTRAP_SERVERS_DEFAULT" in content

    def test_cdc_scaffold_broker_topology_shared_explicit_in_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--broker-topology shared (explicit) writes shared bootstrap to source-groups.yaml."""
        run_cdc(
            "scaffold", "sharedkafka",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^sk_(?P<customer>[^_]+)$",
            "--broker-topology", "shared",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "broker_topology: shared" in content
        assert "${KAFKA_BOOTSTRAP_SERVERS}" in content

    def test_cdc_scaffold_rejects_broker_topology_for_fdw(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """broker-topology should be rejected when scaffold topology is not redpanda."""
        result = run_cdc(
            "scaffold", "fdwbrokerbad",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^fdwbrokerbad_(?P<customer>[^_]+)$",
            "--topology", "fdw",
            "--broker-topology", "per-server",
        )
        assert result.returncode != 0


# ═══════════════════════════════════════════════════════════════════════════
# 3b. User-facing topology
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldTopology:
    """Test --topology flag."""

    def test_cdc_scaffold_topology_fdw_in_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--topology fdw writes the explicit topology for MSSQL projects."""
        run_cdc(
            "scaffold", "fdwproj",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^fdwproj_(?P<customer>[^_]+)$",
            "--topology", "fdw",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "topology: fdw" in content
        assert "broker_topology:" not in content

    def test_cdc_scaffold_topology_fdw_env_example_omits_redpanda_section(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """fdw scaffolds should not emit Redpanda env settings."""
        run_cdc(
            "scaffold", "fdwenv",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^fdwenv_(?P<customer>[^_]+)$",
            "--topology", "fdw",
        )
        content = _read_file(isolated_project, ".env.example")
        assert "KAFKA_BOOTSTRAP_SERVERS" not in content
        assert "REDPANDA_SCHEMA_REGISTRY" not in content

    def test_cdc_scaffold_topology_pg_native_in_source_groups(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--topology pg_native writes the explicit topology for PostgreSQL projects."""
        run_cdc(
            "scaffold", "pgnativeproj",
            "--pattern", "db-shared",
            "--source-type", "postgres",
            "--extraction-pattern", "",
            "--environment-aware",
            "--topology", "pg_native",
        )
        content = _read_file(isolated_project, "source-groups.yaml")
        assert "topology: pg_native" in content

    def test_cdc_scaffold_rejects_topology_source_type_mismatch(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """Unsupported topology/source-type combinations should fail fast."""
        result = run_cdc(
            "scaffold", "badtopology",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "",
            "--topology", "pg_native",
        )
        assert result.returncode != 0


# ═══════════════════════════════════════════════════════════════════════════
# 4. Generated file content verification
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldGeneratedFileContent:
    """Verify content of generated non-YAML files."""

    def test_cdc_scaffold_docker_compose_contains_server_group_name(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """docker-compose.yml references the server group name."""
        _scaffold_db_per_tenant_mssql(run_cdc, "dctest")
        content = _read_file(isolated_project, "docker-compose.yml")
        assert "dctest" in content
        assert "services:" in content

    def test_cdc_scaffold_docker_compose_uses_fdw_postgres_and_pgadmin(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """docker-compose.yml should build the FDW Postgres target and pgAdmin UI."""
        _scaffold_db_per_tenant_mssql(run_cdc, "fdwlocal")
        content = _read_file(isolated_project, "docker-compose.yml")
        assert "Dockerfile.pg17-tds-fdw" in content
        assert "Dockerfile.pgadmin" in content
        assert "pgadmin:" in content
        assert "adminer:" not in content

    def test_cdc_scaffold_readme_contains_pattern_and_project_name(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """README.md contains the project name and pattern description."""
        _scaffold_db_per_tenant_mssql(run_cdc, "readmetest")
        content = _read_file(isolated_project, "README.md")
        # Project name (title-cased) should appear
        assert "readmetest" in content.lower()
        assert "db-per-tenant" in content

    def test_cdc_scaffold_readme_references_docs_index_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """README.md references generated _docs files with short summaries."""
        _scaffold_db_per_tenant_mssql(run_cdc, "docsref")
        content = _read_file(isolated_project, "README.md")
        assert "_docs/PROJECT_STRUCTURE.md" in content
        assert "_docs/ENV_VARIABLES.md" in content
        assert "_docs/CDC_CLI.md" in content
        assert "_docs/CDC_CLI_FLOW.md" in content
        assert "_docs/architecture/DESTRUCTIVE_CHANGES.md" in content

    def test_cdc_scaffold_gitignore_contains_expected_patterns(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.gitignore has standard CDC patterns."""
        _scaffold_db_per_tenant_mssql(run_cdc, "gitest")
        content = _read_file(isolated_project, ".gitignore")
        assert ".env" in content
        assert "__pycache__/" in content
        assert ".lsn_cache/" in content
        assert "pipelines/generated/*" in content

    def test_cdc_scaffold_vscode_settings_contains_yaml_association(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.vscode/settings.json has files.associations for YAML."""
        _scaffold_db_per_tenant_mssql(run_cdc, "vstest")
        content = _read_file(isolated_project, ".vscode/settings.json")
        settings = json.loads(content)
        assert "files.associations" in settings
        assert "*.yaml" in settings["files.associations"]

    def test_cdc_scaffold_vscode_settings_has_readonly_includes(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.vscode/settings.json marks config files as read-only."""
        _scaffold_db_per_tenant_mssql(run_cdc, "rotest")
        content = _read_file(isolated_project, ".vscode/settings.json")
        settings = json.loads(content)
        assert "files.readonlyInclude" in settings
        assert settings["files.readonlyInclude"].get("source-groups.yaml") is True

    def test_cdc_scaffold_env_example_has_target_postgres_section(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes target PostgreSQL config section."""
        _scaffold_db_per_tenant_mssql(run_cdc, "tgttest")
        content = _read_file(isolated_project, ".env.example")
        assert "POSTGRES_LOCAL_USER" in content
        assert "POSTGRES_LOCAL_PASSWORD" in content
        assert "POSTGRES_LOCAL_DB" in content

    def test_cdc_scaffold_env_example_has_pgadmin_section(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes pgAdmin configuration defaults."""
        _scaffold_db_per_tenant_mssql(run_cdc, "pgadminenv")
        content = _read_file(isolated_project, ".env.example")
        assert "PGADMIN_DEFAULT_EMAIL" in content
        assert "PGADMIN_DEFAULT_PASSWORD" in content
        assert "PGADMIN_PORT" in content

    def test_cdc_scaffold_env_example_has_kafka_section(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """.env.example includes Kafka/Redpanda configuration section."""
        _scaffold_db_per_tenant_mssql(run_cdc, "kafkaenv")
        content = _read_file(isolated_project, ".env.example")
        assert "KAFKA_BOOTSTRAP_SERVERS" in content
        assert "REDPANDA_SCHEMA_REGISTRY" in content


# ═══════════════════════════════════════════════════════════════════════════
# 5. Pipeline templates
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldTemplates:
    """Test that scaffold generates pipeline templates with real content."""

    def test_cdc_scaffold_db_per_tenant_generates_source_pipeline_template(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """pipelines/templates/source-pipeline.yaml is created with content."""
        _scaffold_db_per_tenant_mssql(run_cdc, "tpltest")
        template = isolated_project / "pipelines" / "templates" / "source-pipeline.yaml"
        assert template.is_file()
        content = template.read_text(encoding="utf-8")
        assert len(content) > 0

    def test_cdc_scaffold_db_shared_generates_sink_pipeline_template(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """pipelines/templates/sink-pipeline.yaml is created with content."""
        _scaffold_db_shared_postgres(run_cdc, "tpltest2")
        template = isolated_project / "pipelines" / "templates" / "sink-pipeline.yaml"
        assert template.is_file()
        content = template.read_text(encoding="utf-8")
        assert len(content) > 0

    def test_cdc_scaffold_generates_both_pipeline_templates(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Both source and sink pipeline templates are created."""
        _scaffold_db_per_tenant_mssql(run_cdc, "bothpl")
        source = isolated_project / "pipelines" / "templates" / "source-pipeline.yaml"
        sink = isolated_project / "pipelines" / "templates" / "sink-pipeline.yaml"
        assert source.is_file(), "source-pipeline.yaml missing"
        assert sink.is_file(), "sink-pipeline.yaml missing"


# ═══════════════════════════════════════════════════════════════════════════
# 6. Error handling
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldErrors:
    """Test scaffold error handling for missing/invalid arguments."""

    def test_cdc_scaffold_without_args_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold (no arguments) exits with non-zero code."""
        result = run_cdc("scaffold")
        assert result.returncode != 0

    def test_cdc_scaffold_name_without_pattern_flag_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold <name> --source-type postgres (missing --pattern) exits with error."""
        result = run_cdc(
            "scaffold", "testproj",
            "--source-type", "postgres",
            "--extraction-pattern", "",
        )
        assert result.returncode != 0

    def test_cdc_scaffold_name_without_source_type_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold <name> --pattern db-shared (missing --source-type) exits with error."""
        result = run_cdc(
            "scaffold", "testproj",
            "--pattern", "db-shared",
            "--extraction-pattern", "",
            "--environment-aware",
        )
        assert result.returncode != 0

    def test_cdc_scaffold_name_without_extraction_pattern_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """Missing --extraction-pattern exits with error."""
        result = run_cdc(
            "scaffold", "testproj",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
        )
        assert result.returncode != 0

    def test_cdc_scaffold_db_shared_without_environment_aware_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold --pattern db-shared without --environment-aware exits with error."""
        result = run_cdc(
            "scaffold", "noenvaware",
            "--pattern", "db-shared",
            "--source-type", "postgres",
            "--extraction-pattern", "",
        )
        assert result.returncode != 0
        combined = result.stdout + result.stderr
        assert "environment-aware" in combined.lower() or result.returncode == 1

    def test_cdc_scaffold_same_name_twice_fails_on_second_run(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold <name> run twice rejects the second call."""
        first = run_cdc(
            "scaffold", "duptest",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^dup_(?P<customer>[^_]+)$",
        )
        assert first.returncode == 0

        second = run_cdc(
            "scaffold", "duptest",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "^dup_(?P<customer>[^_]+)$",
        )
        assert second.returncode != 0, "Duplicate scaffold should fail"

    def test_cdc_scaffold_invalid_pattern_value_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold --pattern db-unknown (invalid choice) exits with error."""
        result = run_cdc(
            "scaffold", "badpat",
            "--pattern", "db-unknown",
            "--source-type", "mssql",
            "--extraction-pattern", "",
        )
        assert result.returncode != 0

    def test_cdc_scaffold_invalid_source_type_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold --source-type oracle (invalid choice) exits with error."""
        result = run_cdc(
            "scaffold", "badsrc",
            "--pattern", "db-per-tenant",
            "--source-type", "oracle",
            "--extraction-pattern", "",
        )
        assert result.returncode != 0

    def test_cdc_scaffold_invalid_broker_topology_exits_with_error(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold --broker-topology invalid (invalid choice) exits with error."""
        result = run_cdc(
            "scaffold", "badbroker",
            "--pattern", "db-per-tenant",
            "--source-type", "mssql",
            "--extraction-pattern", "",
            "--broker-topology", "invalid",
        )
        assert result.returncode != 0


# ═══════════════════════════════════════════════════════════════════════════
# 7. --update mode
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldUpdate:
    """Test cdc scaffold --update mode."""

    def test_cdc_scaffold_update_on_existing_project_succeeds(
        self,
        run_cdc: RunCdc,
    ) -> None:
        """cdc scaffold --update refreshes an already-scaffolded project."""
        _scaffold_db_per_tenant_mssql(run_cdc, "updatetest")

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0, (
            f"Update failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

    def test_cdc_scaffold_update_creates_missing_directories(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update re-creates directories that were deleted after initial scaffold."""
        _scaffold_db_per_tenant_mssql(run_cdc, "upddir")

        # Delete a generated directory
        import shutil
        docs_dir = isolated_project / "_docs"
        if docs_dir.exists():
            shutil.rmtree(docs_dir)
        assert not docs_dir.exists()

        # Update should recreate it
        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert docs_dir.is_dir(), "_docs directory should be recreated by --update"

    def test_cdc_scaffold_update_merges_vscode_settings_preserving_existing(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update merges new keys into .vscode/settings.json without losing existing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "mergetest")

        # Add a custom key to settings.json
        custom_font_size = 14
        settings_path = isolated_project / ".vscode" / "settings.json"
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
        settings["editor.fontSize"] = custom_font_size
        settings_path.write_text(json.dumps(settings, indent=2), encoding="utf-8")

        # Run update
        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0

        # Custom key should still be there
        updated_settings = json.loads(settings_path.read_text(encoding="utf-8"))
        assert updated_settings.get("editor.fontSize") == custom_font_size
        assert "files.associations" in updated_settings

    def test_cdc_scaffold_update_appends_missing_gitignore_patterns(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update appends new patterns to .gitignore without removing existing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "giupd")

        gitignore = isolated_project / ".gitignore"
        original_content = gitignore.read_text(encoding="utf-8")

        # Remove a known pattern and add custom one
        lines = [ln for ln in original_content.splitlines() if ".lsn_cache" not in ln]
        lines.append("my-custom-pattern/")
        gitignore.write_text("\n".join(lines) + "\n", encoding="utf-8")

        # Run update
        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0

        updated = gitignore.read_text(encoding="utf-8")
        assert "my-custom-pattern/" in updated, "Custom pattern should be preserved"
        assert ".lsn_cache/" in updated, "Missing pattern should be appended"

    def test_cdc_scaffold_update_recreates_missing_docs_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update creates newly-required scaffold docs if they are missing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "upddocs")

        docs_file = isolated_project / "_docs" / "ENV_VARIABLES.md"
        docs_file.unlink(missing_ok=True)
        assert not docs_file.exists()

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert docs_file.is_file(), "Missing _docs file should be recreated by --update"

    def test_cdc_scaffold_update_recreates_missing_migrations_architecture_doc(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update recreates _docs/architecture/MIGRATIONS.md when missing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "updmigdocs")

        docs_file = isolated_project / "_docs" / "architecture" / "MIGRATIONS.md"
        docs_file.unlink(missing_ok=True)
        assert not docs_file.exists()

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert docs_file.is_file(), "Missing migrations architecture doc should be recreated by --update"

    def test_cdc_scaffold_update_recreates_missing_destructive_changes_doc(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update recreates _docs/architecture/DESTRUCTIVE_CHANGES.md when missing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "upddestructivedocs")

        docs_file = (
            isolated_project
            / "_docs"
            / "architecture"
            / "DESTRUCTIVE_CHANGES.md"
        )
        docs_file.unlink(missing_ok=True)
        assert not docs_file.exists()

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert docs_file.is_file(), "Missing destructive changes doc should be recreated by --update"

    def test_cdc_scaffold_update_recreates_missing_docker_compose(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update creates docker-compose.yml when it is missing."""
        _scaffold_db_per_tenant_mssql(run_cdc, "upddc")

        compose_file = isolated_project / "docker-compose.yml"
        compose_file.unlink(missing_ok=True)
        assert not compose_file.exists()

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert compose_file.is_file(), "Missing docker-compose.yml should be recreated by --update"
        assert "redpanda" in compose_file.read_text(encoding="utf-8").lower()

    def test_cdc_scaffold_update_recreates_missing_pgadmin_support_files(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """--update creates missing pgAdmin support files introduced by newer scaffolds."""
        _scaffold_db_per_tenant_mssql(run_cdc, "updpgadmin")

        dockerfile = isolated_project / "Dockerfile.pgadmin"
        entrypoint = isolated_project / "pgadmin" / "entrypoint.sh"
        dockerfile.unlink(missing_ok=True)
        entrypoint.unlink(missing_ok=True)

        result = run_cdc("scaffold", "--update")
        assert result.returncode == 0
        assert dockerfile.is_file(), "Missing Dockerfile.pgadmin should be recreated by --update"
        assert entrypoint.is_file(), "Missing pgadmin/entrypoint.sh should be recreated by --update"


# ═══════════════════════════════════════════════════════════════════════════
# 8. File-skip and backup behaviour
# ═══════════════════════════════════════════════════════════════════════════


class TestScaffoldFileHandling:
    """Test that scaffold respects existing files (skip / backup)."""

    def test_cdc_scaffold_does_not_overwrite_existing_env_example(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Existing .env.example is NOT overwritten by scaffold."""
        # Pre-create .env.example with custom content
        env_file = isolated_project / ".env.example"
        env_file.write_text("MY_CUSTOM_VAR=hello\n", encoding="utf-8")

        _scaffold_db_per_tenant_mssql(run_cdc, "skipenv")

        content = env_file.read_text(encoding="utf-8")
        assert "MY_CUSTOM_VAR=hello" in content, ".env.example should not be overwritten"

    def test_cdc_scaffold_does_not_overwrite_existing_readme(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Existing README.md is NOT overwritten by scaffold."""
        readme = isolated_project / "README.md"
        readme.write_text("# My Custom README\n", encoding="utf-8")

        _scaffold_db_per_tenant_mssql(run_cdc, "skipreadme")

        content = readme.read_text(encoding="utf-8")
        assert "My Custom README" in content

    def test_cdc_scaffold_docker_compose_backed_up_when_exists(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Existing docker-compose.yml is backed up to .bak before overwriting."""
        # Pre-create docker-compose.yml
        dc = isolated_project / "docker-compose.yml"
        dc.write_text("services:\n  old: {}\n", encoding="utf-8")

        _scaffold_db_per_tenant_mssql(run_cdc, "dcbak")

        backup = isolated_project / "docker-compose.yml.bak"
        assert backup.is_file(), "docker-compose.yml.bak should be created"
        backup_content = backup.read_text(encoding="utf-8")
        assert "old:" in backup_content, "Backup should contain original content"

        # New docker-compose.yml should have the scaffold content
        new_content = dc.read_text(encoding="utf-8")
        assert "dcbak" in new_content, "New docker-compose should reference the project"

    def test_cdc_scaffold_does_not_overwrite_existing_pipeline_templates(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        """Existing pipeline templates are NOT overwritten."""
        tpl_dir = isolated_project / "pipelines" / "templates"
        tpl_dir.mkdir(parents=True, exist_ok=True)
        source_tpl = tpl_dir / "source-pipeline.yaml"
        source_tpl.write_text("# My custom source template\n", encoding="utf-8")

        _scaffold_db_per_tenant_mssql(run_cdc, "skiptpl")

        content = source_tpl.read_text(encoding="utf-8")
        assert "My custom source template" in content
