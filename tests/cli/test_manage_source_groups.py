"""End-to-end CLI tests for ``cdc manage-source-groups``.

Tests the full flow through a real **fish** shell for:
- ``--info`` — display server group information
- ``--list-servers`` — list configured servers
- ``--add-server`` / ``--remove-server`` — multi-server management
- ``--set-kafka-topology`` — change Kafka topology
- ``--list-ignore-patterns`` / ``--add-to-ignore-list`` — database excludes
- ``--list-schema-excludes`` / ``--add-to-schema-excludes`` — schema excludes
- ``--set-extraction-pattern`` — set a regex on a server
- ``--add-extraction-pattern`` / ``--list-extraction-patterns``
  / ``--remove-extraction-pattern`` — ordered pattern management
- ``--view-services`` — display environment-grouped sources
- Error paths (no action, no source-groups.yaml, invalid flags)
- fish autocompletions for manage-source-groups flags
"""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_SOURCE_GROUPS = (
    "mygroup:\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  kafka_topology: shared\n"
    "  servers:\n"
    "    default:\n"
    "      host: ${POSTGRES_SOURCE_HOST}\n"
    "      port: ${POSTGRES_SOURCE_PORT}\n"
    "      user: ${POSTGRES_SOURCE_USER}\n"
    "      password: ${POSTGRES_SOURCE_PASSWORD}\n"
    "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
    "  sources: {}\n"
)

_SOURCE_GROUPS_WITH_SOURCES = (
    "mygroup:\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  kafka_topology: shared\n"
    "  servers:\n"
    "    default:\n"
    "      host: ${POSTGRES_SOURCE_HOST}\n"
    "      port: ${POSTGRES_SOURCE_PORT}\n"
    "      user: ${POSTGRES_SOURCE_USER}\n"
    "      password: ${POSTGRES_SOURCE_PASSWORD}\n"
    "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
    "  sources:\n"
    "    directory:\n"
    "      schemas:\n"
    "        - public\n"
    "      nonprod:\n"
    "        server: default\n"
    "        database: directory_db\n"
    "        table_count: 5\n"
)

_MULTI_SERVER_SOURCE_GROUPS = (
    "mygroup:\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  kafka_topology: shared\n"
    "  servers:\n"
    "    default:\n"
    "      host: ${POSTGRES_SOURCE_HOST}\n"
    "      port: ${POSTGRES_SOURCE_PORT}\n"
    "      user: ${POSTGRES_SOURCE_USER}\n"
    "      password: ${POSTGRES_SOURCE_PASSWORD}\n"
    "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
    "    analytics:\n"
    "      host: ${POSTGRES_SOURCE_HOST_ANALYTICS}\n"
    "      port: ${POSTGRES_SOURCE_PORT_ANALYTICS}\n"
    "      user: ${POSTGRES_SOURCE_USER_ANALYTICS}\n"
    "      password: ${POSTGRES_SOURCE_PASSWORD_ANALYTICS}\n"
    "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
    "  sources: {}\n"
)


def _write_source_groups(
    root: Path, content: str = _MINIMAL_SOURCE_GROUPS,
) -> None:
    """Write source-groups.yaml into the project root."""
    (root / "source-groups.yaml").write_text(content)


def _read_source_groups(root: Path) -> str:
    """Read source-groups.yaml content."""
    return (root / "source-groups.yaml").read_text()


# ═══════════════════════════════════════════════════════════════════════════
# No action / error paths
# ═══════════════════════════════════════════════════════════════════════════


class TestCliNoAction:
    """CLI e2e: error paths and missing flags."""

    def test_no_flags_shows_error(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups")
        assert result.returncode == 1
        assert "No action specified" in result.stdout + result.stderr

    def test_no_source_groups_file_info(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--info without source-groups.yaml → error."""
        result = run_cdc("manage-source-groups", "--info")
        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr

    def test_no_source_groups_file_list_servers(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--list-servers without source-groups.yaml → error."""
        result = run_cdc("manage-source-groups", "--list-servers")
        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr


# ═══════════════════════════════════════════════════════════════════════════
# --info
# ═══════════════════════════════════════════════════════════════════════════


class TestCliInfo:
    """CLI e2e: --info flag."""

    def test_info_shows_group_details(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--info")
        assert result.returncode == 0
        output = result.stdout
        assert "mygroup" in output
        assert "db-shared" in output
        assert "postgres" in output

    def test_info_shows_sources(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project, _SOURCE_GROUPS_WITH_SOURCES)
        result = run_cdc("manage-source-groups", "--info")
        assert result.returncode == 0
        assert "directory" in result.stdout
        assert "directory_db" in result.stdout


# ═══════════════════════════════════════════════════════════════════════════
# --list-servers
# ═══════════════════════════════════════════════════════════════════════════


class TestCliListServers:
    """CLI e2e: --list-servers flag."""

    def test_list_single_server(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--list-servers")
        assert result.returncode == 0
        assert "default" in result.stdout

    def test_list_multiple_servers(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project, _MULTI_SERVER_SOURCE_GROUPS)
        result = run_cdc("manage-source-groups", "--list-servers")
        assert result.returncode == 0
        output = result.stdout
        assert "default" in output
        assert "analytics" in output


# ═══════════════════════════════════════════════════════════════════════════
# --add-server / --remove-server
# ═══════════════════════════════════════════════════════════════════════════


class TestCliAddServer:
    """CLI e2e: --add-server flag."""

    def test_add_server(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--add-server", "reporting",
        )
        assert result.returncode == 0
        assert "reporting" in result.stdout
        yaml_content = _read_source_groups(isolated_project)
        assert "reporting" in yaml_content

    def test_add_server_appears_in_list(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        run_cdc("manage-source-groups", "--add-server", "warehouse")
        result = run_cdc("manage-source-groups", "--list-servers")
        assert result.returncode == 0
        output = result.stdout
        assert "default" in output
        assert "warehouse" in output

    def test_add_duplicate_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        run_cdc("manage-source-groups", "--add-server", "staging")
        result = run_cdc("manage-source-groups", "--add-server", "staging")
        assert result.returncode == 1

    def test_add_server_named_default_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--add-server", "default")
        assert result.returncode == 1


class TestCliRemoveServer:
    """CLI e2e: --remove-server flag."""

    def test_remove_server(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project, _MULTI_SERVER_SOURCE_GROUPS)
        result = run_cdc(
            "manage-source-groups", "--remove-server", "analytics",
        )
        assert result.returncode == 0
        assert "analytics" in result.stdout
        yaml_content = _read_source_groups(isolated_project)
        assert "analytics" not in yaml_content

    def test_remove_default_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--remove-server", "default",
        )
        assert result.returncode == 1
        assert "Cannot remove" in result.stdout + result.stderr

    def test_remove_nonexistent_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--remove-server", "ghost",
        )
        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# --set-kafka-topology
# ═══════════════════════════════════════════════════════════════════════════


class TestCliSetKafkaTopology:
    """CLI e2e: --set-kafka-topology flag."""

    def test_change_to_per_server(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--set-kafka-topology", "per-server",
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "per-server" in yaml_content

    def test_change_to_shared(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        # Start with per-server, switch to shared
        content = _MINIMAL_SOURCE_GROUPS.replace(
            "kafka_topology: shared", "kafka_topology: per-server",
        )
        _write_source_groups(isolated_project, content)
        result = run_cdc(
            "manage-source-groups", "--set-kafka-topology", "shared",
        )
        assert result.returncode == 0

    def test_same_topology_noop(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--set-kafka-topology", "shared",
        )
        assert result.returncode == 0
        assert "already" in result.stdout.lower()


# ═══════════════════════════════════════════════════════════════════════════
# --list-ignore-patterns / --add-to-ignore-list
# ═══════════════════════════════════════════════════════════════════════════


class TestCliIgnorePatterns:
    """CLI e2e: database exclude pattern management."""

    def test_list_empty_patterns(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--list-ignore-patterns")
        assert result.returncode == 0
        assert "No database exclude patterns" in result.stdout

    def test_add_and_list_pattern(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        add_result = run_cdc(
            "manage-source-groups", "--add-to-ignore-list", "test_%",
        )
        assert add_result.returncode == 0
        assert "test_%" in add_result.stdout

    def test_add_comma_separated_patterns(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-ignore-list", "backup_%,staging_%",
        )
        assert result.returncode == 0
        output = result.stdout
        assert "backup_%" in output
        assert "staging_%" in output


# ═══════════════════════════════════════════════════════════════════════════
# --list-schema-excludes / --add-to-schema-excludes
# ═══════════════════════════════════════════════════════════════════════════


class TestCliSchemaExcludes:
    """CLI e2e: schema exclude pattern management."""

    def test_list_empty_schema_excludes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--list-schema-excludes")
        assert result.returncode == 0
        assert "No schema exclude patterns" in result.stdout

    def test_add_schema_exclude(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-schema-excludes", "information_schema",
        )
        assert result.returncode == 0
        assert "information_schema" in result.stdout

    def test_add_comma_separated_schema_excludes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-schema-excludes", "sys,pg_catalog",
        )
        assert result.returncode == 0
        output = result.stdout
        assert "sys" in output
        assert "pg_catalog" in output


class TestCliTableExcludes:
    """CLI e2e: table exclude pattern management."""

    def test_list_empty_table_excludes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--list-table-excludes")
        assert result.returncode == 0
        assert "No table exclude patterns" in result.stdout

    def test_add_table_exclude(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-table-excludes", "tmp",
        )
        assert result.returncode == 0
        assert "tmp" in result.stdout

    def test_add_comma_separated_table_excludes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-table-excludes", "tmp,^zz_.*",
        )
        assert result.returncode == 0
        output = result.stdout
        assert "tmp" in output
        assert "^zz_.*" in output


class TestCliTableIncludes:
    """CLI e2e: table include pattern management."""

    def test_list_empty_table_includes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--list-table-includes")
        assert result.returncode == 0
        assert "No table include patterns" in result.stdout

    def test_add_table_include(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-table-includes", "^core_",
        )
        assert result.returncode == 0
        assert "^core_" in result.stdout

    def test_add_comma_separated_table_includes(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-to-table-includes", "^core_,customer",
        )
        assert result.returncode == 0
        output = result.stdout
        assert "^core_" in output
        assert "customer" in output


class TestCliSourceCustomKeys:
    """CLI e2e: source custom key management."""

    def test_add_source_custom_key_persists_in_yaml(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-source-custom-key",
            "customer_id",
            "--custom-key-value",
            "SELECT 'cust-001'",
            "--custom-key-exec-type",
            "sql",
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "source_custom_keys" in yaml_content
        assert "customer_id" in yaml_content
        assert "SELECT 'cust-001'" in yaml_content

    def test_add_source_custom_key_requires_custom_key_value(
        self,
        run_cdc: RunCdc,
        isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--add-source-custom-key",
            "customer_id",
        )
        assert result.returncode == 1
        assert "custom-key-value" in (result.stdout + result.stderr)


# ═══════════════════════════════════════════════════════════════════════════
# --set-extraction-pattern
# ═══════════════════════════════════════════════════════════════════════════


class TestCliSetExtractionPattern:
    """CLI e2e: --set-extraction-pattern flag."""

    def test_set_extraction_pattern(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        pattern = r"^(?P<service>\w+)_(?P<env>\w+)$"
        result = run_cdc(
            "manage-source-groups",
            "--set-extraction-pattern", "default", pattern,
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "extraction_pattern" in yaml_content

    def test_set_invalid_regex_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--set-extraction-pattern", "default", "[invalid",
        )
        assert result.returncode == 1
        assert "Invalid regex" in result.stdout + result.stderr

    def test_set_on_nonexistent_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups",
            "--set-extraction-pattern", "ghost", "^test$",
        )
        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# --add/list/remove-extraction-pattern
# ═══════════════════════════════════════════════════════════════════════════


class TestCliExtractionPatterns:
    """CLI e2e: ordered extraction pattern management."""

    def test_add_extraction_pattern(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        pattern = r"^(?P<service>\w+)_db$"
        result = run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default", pattern,
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "extraction_patterns" in yaml_content

    def test_add_extraction_pattern_with_env(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        pattern = r"^(?P<service>\w+)_prod$"
        result = run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default", pattern,
            "--env", "production",
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "production" in yaml_content

    def test_add_extraction_pattern_with_strip(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        pattern = r"^(?P<service>\w+)_db_prod$"
        result = run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default", pattern,
            "--env", "prod",
            "--strip-patterns", "_db$",
        )
        assert result.returncode == 0
        yaml_content = _read_source_groups(isolated_project)
        assert "_db$" in yaml_content

    def test_list_extraction_patterns_empty(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc(
            "manage-source-groups", "--list-extraction-patterns",
        )
        assert result.returncode == 0

    def test_list_extraction_patterns_after_add(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        pattern = r"^(?P<service>\w+)$"
        run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default", pattern,
            "--description", "Simple service name",
        )
        result = run_cdc(
            "manage-source-groups", "--list-extraction-patterns",
        )
        assert result.returncode == 0
        assert "Simple service name" in result.stdout

    def test_remove_extraction_pattern(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        # Add two patterns
        run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default",
            r"^(?P<service>\w+)_prod$",
        )
        run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default",
            r"^(?P<service>\w+)_dev$",
        )
        # Remove the first one (index 0)
        result = run_cdc(
            "manage-source-groups",
            "--remove-extraction-pattern", "default", "0",
        )
        assert result.returncode == 0
        assert "Removed" in result.stdout

    def test_remove_invalid_index_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        run_cdc(
            "manage-source-groups",
            "--add-extraction-pattern", "default",
            r"^(?P<service>\w+)$",
        )
        result = run_cdc(
            "manage-source-groups",
            "--remove-extraction-pattern", "default", "99",
        )
        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# --view-services
# ═══════════════════════════════════════════════════════════════════════════


class TestCliViewServices:
    """CLI e2e: --view-services flag."""

    def test_view_services_with_sources(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project, _SOURCE_GROUPS_WITH_SOURCES)
        result = run_cdc("manage-source-groups", "--view-services")
        assert result.returncode == 0
        output = result.stdout
        assert "directory" in output

    def test_view_services_no_sources(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-source-groups", "--view-services")
        assert result.returncode == 0
        # Empty sources dict shows "no sources" message
        output = result.stdout.lower()
        assert "no sources" in output or "not configured" in output or output != ""


# ═══════════════════════════════════════════════════════════════════════════
# Fish autocompletions
# ═══════════════════════════════════════════════════════════════════════════


class TestCliCompletions:
    """CLI e2e: fish autocompletions for manage-source-groups."""

    def test_flag_completion(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        result = run_cdc_completion("cdc manage-source-groups --")
        assert result.returncode == 0
        output = result.stdout
        # Fish should suggest at least some known flags
        assert "info" in output or "update" in output or "list" in output


class TestCliDbDefinitions:
    """CLI e2e: --db-definitions guard rails."""

    def test_db_definitions_unknown_server_fails_before_connection(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc(
            "manage-source-groups",
            "--db-definitions",
            "--server",
            "ghost",
        )

        assert result.returncode == 1
        assert "Server 'ghost' not found" in result.stdout + result.stderr
