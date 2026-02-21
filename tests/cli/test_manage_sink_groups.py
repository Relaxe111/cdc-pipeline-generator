"""End-to-end CLI tests for ``cdc manage-sink-groups``."""

from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


_MINIMAL_SOURCE_GROUPS = (
    "asma:\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  server_group_type: db-shared\n"
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
    "legacy:\n"
    "  pattern: db-per-tenant\n"
    "  type: mssql\n"
    "  server_group_type: db-per-tenant\n"
    "  servers:\n"
    "    default:\n"
    "      host: ${MSSQL_SOURCE_HOST}\n"
    "      port: ${MSSQL_SOURCE_PORT}\n"
    "      user: ${MSSQL_SOURCE_USER}\n"
    "      password: ${MSSQL_SOURCE_PASSWORD}\n"
    "      kafka_bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}\n"
    "  sources: {}\n"
)

_STANDALONE_SINK_GROUPS = (
    "sink_analytics:\n"
    "  source_group: asma\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  environment_aware: true\n"
    "  servers:\n"
    "    default:\n"
    "      host: localhost\n"
    "      port: '5432'\n"
    "      user: postgres\n"
    "      password: secret\n"
    "  sources: {}\n"
)

_MULTI_STANDALONE_SINK_GROUPS = (
    "sink_analytics:\n"
    "  source_group: asma\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  environment_aware: true\n"
    "  servers:\n"
    "    default:\n"
    "      host: localhost\n"
    "      port: '5432'\n"
    "      user: postgres\n"
    "      password: secret\n"
    "  sources: {}\n"
    "sink_reporting:\n"
    "  source_group: asma\n"
    "  pattern: db-shared\n"
    "  type: postgres\n"
    "  environment_aware: true\n"
    "  servers:\n"
    "    default:\n"
    "      host: localhost\n"
    "      port: '5432'\n"
    "      user: postgres\n"
    "      password: secret\n"
    "  sources: {}\n"
)

_INHERITED_SINK_GROUPS = (
    "sink_asma:\n"
    "  inherits: true\n"
    "  servers:\n"
    "    default:\n"
    "      source_ref: default\n"
    "  inherited_sources:\n"
    "    - directory\n"
)


def _write_source_groups(root: Path, content: str = _MINIMAL_SOURCE_GROUPS) -> None:
    (root / "source-groups.yaml").write_text(content)


def _write_sink_groups(root: Path, content: str) -> None:
    (root / "sink-groups.yaml").write_text(content)


def _read_sink_groups(root: Path) -> str:
    return (root / "sink-groups.yaml").read_text()


class TestCliNoAction:
    """CLI e2e: no-flag and missing-file behavior."""

    def test_no_flags_shows_help(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-sink-groups")
        assert result.returncode == 0
        assert "manage-sink-groups" in result.stdout + result.stderr

    def test_list_without_sink_file_is_ok(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        result = run_cdc("manage-sink-groups", "--list")
        assert result.returncode == 0
        assert "No sink groups file found" in result.stdout + result.stderr


class TestCliCreate:
    """CLI e2e: create flows."""

    def test_create_auto_scaffold_db_shared_only(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc("manage-sink-groups", "--create")

        assert result.returncode == 0
        content = _read_sink_groups(isolated_project)
        assert "sink_asma" in content
        assert "sink_legacy" not in content

    def test_create_from_specific_source_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc(
            "manage-sink-groups", "--create", "--source-group", "asma",
        )

        assert result.returncode == 0
        content = _read_sink_groups(isolated_project)
        assert "sink_asma" in content
        assert "source_ref" in content

    def test_create_from_missing_source_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc(
            "manage-sink-groups", "--create", "--source-group", "ghost",
        )

        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr


class TestCliAddNewSinkGroup:
    """CLI e2e: standalone sink group create."""

    def test_add_new_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc(
            "manage-sink-groups",
            "--add-new-sink-group",
            "analytics",
            "--for-source-group",
            "asma",
            "--type",
            "postgres",
            "--pattern",
            "db-shared",
        )

        assert result.returncode == 0
        content = _read_sink_groups(isolated_project)
        assert "sink_analytics" in content
        assert "source_group: asma" in content

    def test_add_existing_sink_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--add-new-sink-group",
            "analytics",
            "--for-source-group",
            "asma",
        )

        assert result.returncode == 1
        assert "already exists" in result.stdout + result.stderr


class TestCliListInfoValidate:
    """CLI e2e: list, info and validate flows."""

    def test_list_and_info(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        list_result = run_cdc("manage-sink-groups", "--list")
        info_result = run_cdc("manage-sink-groups", "--info", "sink_analytics")

        assert list_result.returncode == 0
        assert "sink_analytics" in list_result.stdout
        assert info_result.returncode == 0
        assert "Source Group:" in info_result.stdout

    def test_validate_valid_config(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--validate")

        assert result.returncode == 0

    def test_info_missing_sink_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--info", "sink_missing")

        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr

    def test_validate_without_sink_file_returns_zero(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)

        result = run_cdc("manage-sink-groups", "--validate")

        assert result.returncode == 0
        assert "No sink groups file found" in result.stdout + result.stderr


class TestCliInspectAndIntrospect:
    """CLI e2e: inspect/introspect argument and guard rails."""

    def test_inspect_requires_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--inspect")

        assert result.returncode == 1
        assert "--inspect requires --sink-group" in result.stdout + result.stderr

    def test_update_auto_resolves_single_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--update",
            "--server",
            "ghost",
        )

        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "using only available sink group" in output
        assert "Server 'ghost' not found" in output

    def test_update_without_sink_group_fails_for_multiple_groups(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _MULTI_STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--update")

        assert result.returncode == 1
        assert "More than one sink group found" in result.stdout + result.stderr

    def test_inspect_rejects_inherited_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _INHERITED_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--inspect",
            "--sink-group",
            "sink_asma",
        )

        assert result.returncode == 1
        assert "Cannot inspect inherited sink group" in result.stdout + result.stderr

    def test_inspect_unknown_server_fails_without_db_access(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--inspect",
            "--sink-group",
            "sink_analytics",
            "--server",
            "ghost",
        )

        assert result.returncode == 1
        assert "Server 'ghost' not found" in result.stdout + result.stderr

    def test_introspect_types_missing_sink_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--introspect-types")

        assert result.returncode == 1
        assert "requires --sink-group" in result.stdout + result.stderr

    def test_introspect_types_unknown_server_fails_before_connection(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--introspect-types",
            "--sink-group",
            "sink_analytics",
            "--server",
            "ghost",
        )

        assert result.returncode == 1
        assert "Server 'ghost' not found" in result.stdout + result.stderr

    def test_db_definitions_missing_sink_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--db-definitions")

        assert result.returncode == 1
        assert "requires --sink-group" in result.stdout + result.stderr

    def test_db_definitions_unknown_server_fails_before_connection(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--db-definitions",
            "--sink-group",
            "sink_analytics",
            "--server",
            "ghost",
        )

        assert result.returncode == 1
        assert "Server 'ghost' not found" in result.stdout + result.stderr


class TestCliServerManagement:
    """CLI e2e: add/remove server in standalone sink group."""

    def test_add_server_and_remove_server(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        add_result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--add-server",
            "reporting",
            "--host",
            "reporting.local",
            "--port",
            "5432",
            "--user",
            "postgres",
            "--password",
            "secret",
        )
        assert add_result.returncode == 0

        content_after_add = _read_sink_groups(isolated_project)
        assert "reporting:" in content_after_add

        remove_result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--remove-server",
            "reporting",
        )
        assert remove_result.returncode == 0

        content_after_remove = _read_sink_groups(isolated_project)
        assert "reporting:" not in content_after_remove

    def test_add_duplicate_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--add-server",
            "default",
        )

        assert result.returncode == 1
        assert "already exists" in result.stdout + result.stderr

    def test_remove_missing_server_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--remove-server",
            "ghost",
        )

        assert result.returncode == 1
        assert "not found" in result.stdout + result.stderr

    def test_update_server_extraction_patterns_with_metadata(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>\\w+)_db_(?P<env>\\w+)$",
            "--env",
            "prod_adcuris",
            "--strip-patterns",
            "_db",
            "--env-mapping",
            "prod_adcuris:prod-adcuris",
            "--description",
            "Matching pattern: {service}_db_{env}",
        )

        assert result.returncode == 0
        content = _read_sink_groups(isolated_project)
        assert "extraction_patterns:" in content
        assert "pattern: ^(?P<service>\\w+)_db_(?P<env>\\w+)$" in content
        assert "env: prod_adcuris" in content
        assert "strip_patterns:" in content
        assert "- _db" in content
        assert "env_mapping:" in content
        assert "prod_adcuris: prod-adcuris" in content

    def test_list_server_extraction_patterns(self, run_cdc: RunCdc, isolated_project: Path) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        update_result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>\\w+)_db_(?P<env>\\w+)$",
            "--strip-patterns",
            "_db",
        )
        assert update_result.returncode == 0

        list_result = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--list-server-extraction-patterns",
        )

        assert list_result.returncode == 0
        output = list_result.stdout + list_result.stderr
        assert "Sink Server Extraction Patterns: sink_analytics" in output
        assert "Server: default" in output
        assert "Pattern:" in output

    def test_list_server_extraction_patterns_requires_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc(
            "manage-sink-groups",
            "--list-server-extraction-patterns",
        )

        assert result.returncode == 1
        assert "requires --sink-group" in result.stdout + result.stderr

    def test_update_server_extraction_patterns_appends_entries(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        first = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>legacy)_(?P<env>prod)$",
        )
        assert first.returncode == 0

        second = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>\\w+)_db_(?P<env>\\w+)$",
            "--strip-patterns",
            "_db",
        )
        assert second.returncode == 0

        content = _read_sink_groups(isolated_project)
        assert content.count("pattern:") >= 2
        assert "pattern: ^(?P<service>legacy)_(?P<env>prod)$" in content
        assert "pattern: ^(?P<service>\\w+)_db_(?P<env>\\w+)$" in content

    def test_update_server_extraction_patterns_upserts_same_pattern(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        first = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>legacy)_(?P<env>prod)$",
            "--description",
            "old",
        )
        assert first.returncode == 0

        second = run_cdc(
            "manage-sink-groups",
            "--sink-group",
            "sink_analytics",
            "--server",
            "default",
            "--extraction-patterns",
            "^(?P<service>legacy)_(?P<env>prod)$",
            "--description",
            "new",
        )
        assert second.returncode == 0

        content = _read_sink_groups(isolated_project)
        assert content.count("pattern: ^(?P<service>legacy)_(?P<env>prod)$") == 1
        assert "description: new" in content


class TestCliRemoveSinkGroup:
    """CLI e2e: remove sink group behavior."""

    def test_remove_standalone_sink_group(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _STANDALONE_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--remove", "sink_analytics")

        assert result.returncode == 0
        content = _read_sink_groups(isolated_project)
        assert "sink_analytics" not in content

    def test_remove_inherited_sink_group_fails(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _write_source_groups(isolated_project)
        _write_sink_groups(isolated_project, _INHERITED_SINK_GROUPS)

        result = run_cdc("manage-sink-groups", "--remove", "sink_asma")

        assert result.returncode == 1
        assert "inherits" in result.stdout + result.stderr


class TestCliCompletions:
    """CLI e2e: fish autocompletion entries for command flags."""

    def test_manage_sink_groups_flag_completion(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        result = run_cdc_completion("cdc manage-sink-groups --")
        assert result.returncode == 0
        output = result.stdout
        assert "--create" in output
        assert "--add-new-sink-group" in output
        assert "--update" in output
        assert "--validate" in output
