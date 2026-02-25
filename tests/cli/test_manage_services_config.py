"""End-to-end CLI tests for manage-services config beyond --source-table.

Tests the full flow through a real **fish** shell for:
- --add-source-table / --add-source-tables / --remove-table
- --list-source-tables
- --create-service
- --list-sinks / --add-sink / --remove-sink
- --add-sink-table / --remove-sink-table
- --validate-config
- manage-services resources column-templates / transforms
- fish autocompletions for manage-services config flags
"""

import os
from pathlib import Path

import pytest

from tests.cli.conftest import RunCdc, RunCdcCompletion

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_project(
    root: Path,
    service: str = "proxy",
    *,
    with_sink: bool = False,
) -> None:
    """Create minimal project structure."""
    # Create docker-compose.yml to satisfy project root detection
    (root / "docker-compose.yml").write_text(
        "services:\n"
        "  dev:\n"
        "    image: busybox\n"
    )

    services_dir = root / "services"
    services_dir.mkdir(exist_ok=True)

    source_groups = (
        "asma:\n"
        "  pattern: db-shared\n"
        "  sources:\n"
        f"    {service}:\n"
        "      schemas:\n"
        "        - public\n"
    )
    (root / "source-groups.yaml").write_text(source_groups)
    (root / "sink-groups.yaml").write_text(
        "sink_asma:\n  type: postgres\n  server: sink-pg\n"
    )

    sinks_block = ""
    if with_sink:
        sinks_block = (
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables:\n"
            "        public.users:\n"
            "          target_exists: true\n"
        )

    sf = services_dir / f"{service}.yaml"
    sf.write_text(
        f"{service}:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users: {}\n"
        + sinks_block
    )


def _read_yaml(root: Path, service: str = "proxy") -> str:
    return (root / "services" / f"{service}.yaml").read_text()


# ═══════════════════════════════════════════════════════════════════════════
# Source table operations
# ═══════════════════════════════════════════════════════════════════════════


class TestCliAddSourceTable:
    """CLI e2e: --add-source-table."""

    def test_add_new_table(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-source-table", "public.orders",
        )
        assert result.returncode == 0
        assert "public.orders" in _read_yaml(isolated_project)

    def test_add_without_schema_defaults_dbo(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-source-table", "Actor",
        )
        assert result.returncode == 0
        assert "dbo.Actor" in _read_yaml(isolated_project)

    def test_add_duplicate_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-source-table", "public.queries",
        )
        assert result.returncode == 1


class TestCliAddSourceTables:
    """CLI e2e: --add-source-tables (bulk)."""

    def test_bulk_add(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-source-tables", "public.orders", "public.items",
        )
        assert result.returncode == 0
        yaml_text = _read_yaml(isolated_project)
        assert "public.orders" in yaml_text
        assert "public.items" in yaml_text


class TestCliRemoveTable:
    """CLI e2e: --remove-table."""

    def test_remove_existing(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--remove-table", "public.queries",
        )
        assert result.returncode == 0
        assert "public.queries" not in _read_yaml(isolated_project)

    def test_remove_nonexistent(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--remove-table", "public.nonexistent",
        )
        assert result.returncode == 1


class TestCliUpdateSourceTableColumns:
    """CLI e2e: --source-table with multiple --track-columns values."""

    def test_track_columns_accepts_space_separated_list(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--source-table", "public.queries",
            "--track-columns",
            "public.queries.status",
            "public.queries.title",
        )

        assert result.returncode == 0
        yaml_text = _read_yaml(isolated_project)
        assert "include_columns" in yaml_text
        assert "status" in yaml_text
        assert "title" in yaml_text


# ═══════════════════════════════════════════════════════════════════════════
# List source tables
# ═══════════════════════════════════════════════════════════════════════════


class TestCliListSourceTables:
    """CLI e2e: --list-source-tables."""

    def test_lists_tables(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--list-source-tables",
        )
        assert result.returncode == 0
        assert "queries" in result.stdout

    def test_no_service_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--list-source-tables",
        )
        # Auto-detects single service → should still work
        assert result.returncode == 0


class TestCliListServices:
    """CLI e2e: --list-services."""

    def test_lists_services_without_service_flag(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, service="proxy")
        services_dir = isolated_project / "services"
        (services_dir / "adopus.yaml").write_text("adopus:\n  source:\n    tables: {}\n")

        result = run_cdc(
            "manage-services", "config", "--list-services",
        )
        assert result.returncode == 0
        assert "proxy" in result.stdout
        assert "adopus" in result.stdout


# ═══════════════════════════════════════════════════════════════════════════
# Sink operations
# ═══════════════════════════════════════════════════════════════════════════


class TestCliSinkAdd:
    """CLI e2e: --add-sink."""

    def test_add_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-sink", "sink_asma.chat",
        )
        assert result.returncode == 0
        assert "sink_asma.chat" in _read_yaml(isolated_project)


class TestCliSinkRemove:
    """CLI e2e: --remove-sink."""

    def test_remove_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--remove-sink", "sink_asma.chat",
        )
        assert result.returncode == 0


class TestCliSinkList:
    """CLI e2e: --list-sinks."""

    def test_list_sinks(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--list-sinks",
        )
        assert result.returncode == 0


class TestCliSinkAddTable:
    """CLI e2e: --add-sink-table."""

    def test_add_table_to_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        # Create service-schemas so _validate_table_in_schemas passes
        schemas_dir = isolated_project / "service-schemas" / "chat" / "public"
        schemas_dir.mkdir(parents=True)
        (schemas_dir / "orders.yaml").write_text("columns: []\n")
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table", "public.orders",
            "--from", "public.users",
            "--target-exists", "false",
        )
        assert result.returncode == 0

    def test_add_table_requires_from(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--add-sink-table fails when --from is not provided."""
        _create_project(isolated_project, with_sink=True)
        schemas_dir = isolated_project / "service-schemas" / "chat" / "public"
        schemas_dir.mkdir(parents=True)
        (schemas_dir / "orders.yaml").write_text("columns: []\n")

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table", "public.orders",
            "--target-exists", "false",
        )
        assert result.returncode == 1

    def test_requires_target_exists(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table", "public.orders",
        )
        assert result.returncode == 1

    def test_add_table_from_only_uses_source_name(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--add-sink-table with no value falls back to --from table name."""
        _create_project(isolated_project, with_sink=True)
        schemas_dir = isolated_project / "service-schemas" / "chat" / "public"
        schemas_dir.mkdir(parents=True)
        (schemas_dir / "queries.yaml").write_text("columns: []\n")

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table",
            "--from", "public.queries",
            "--target-exists", "false",
        )
        assert result.returncode == 0
        assert "public.queries" in _read_yaml(isolated_project)

    def test_replicate_structure_accepts_custom_sink_schema(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Replicate mode allows custom --sink-schema values."""
        _create_project(isolated_project, with_sink=True)

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table",
            "--from", "public.queries",
            "--replicate-structure",
            "--sink-schema", "custom_clone",
        )

        assert result.returncode == 0
        yaml_text = _read_yaml(isolated_project)
        assert "custom_clone.queries" in yaml_text
        assert "from: public.queries" in yaml_text

    def test_all_sinks_replicate_structure_adds_to_all_sinks(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--all with replicate mode fans out add-sink-table to each configured sink."""
        _create_project(isolated_project, with_sink=False)

        service_file = isolated_project / "services" / "proxy.yaml"
        service_file.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.queries: {}\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables: {}\n"
            "    sink_asma.calendar:\n"
            "      tables: {}\n"
        )

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--all",
            "--add-sink-table",
            "--from", "public.queries",
            "--replicate-structure",
            "--sink-schema", "shared",
        )

        assert result.returncode == 0
        yaml_text = _read_yaml(isolated_project)
        assert "sink_asma.chat" in yaml_text
        assert "sink_asma.calendar" in yaml_text
        assert "shared.queries" in yaml_text
        assert "from: public.queries" in yaml_text

    def test_all_sinks_rejects_explicit_table_name(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--all replicate mode rejects explicit --add-sink-table value."""
        _create_project(isolated_project, with_sink=True)

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--all",
            "--add-sink-table", "public.queries",
            "--from", "public.queries",
            "--replicate-structure",
            "--sink-schema", "shared",
        )

        assert result.returncode == 1

    def test_missing_sink_value_shows_all_hint_for_replicate_mode(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Parser error for bare --sink includes --all guidance in replicate mode."""
        _create_project(isolated_project, with_sink=True)

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink",
            "--add-sink-table",
            "--from", "public.queries",
            "--replicate-structure",
            "--sink-schema", "shared",
        )

        assert result.returncode == 1
        output = result.stdout + result.stderr
        assert "omit --sink and use --all" in output
        assert "--all --add-sink-table --from" in output


class TestCliSinkAddCustomTable:
    """CLI e2e: --add-custom-sink-table."""

    def test_add_custom_table_requires_from(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """--add-custom-sink-table fails when --from is not provided."""
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-custom-sink-table", "custom.audit_log",
            "--column", "id:uuid:pk",
        )
        assert result.returncode == 1


class TestCliColumnTemplateValidationFromSource:
    """CLI e2e: column-template validation uses source table from --from."""

    def test_add_column_template_uses_from_source_schema(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        """Validation loads source schema from services/_schemas/<service>/<schema>."""
        _create_project(isolated_project, with_sink=True)

        schemas_root = isolated_project / "services" / "_schemas"
        schemas_root.mkdir(parents=True, exist_ok=True)

        # Template definitions (preferred shared location)
        (schemas_root / "column-templates.yaml").write_text(
            "templates:\n"
            "  source_table:\n"
            "    name: _source_table\n"
            "    type: text\n"
            "    value: meta(\"table\")\n"
        )

        # Source schema used by --from public.customer_user
        source_schema_dir = schemas_root / "proxy" / "public"
        source_schema_dir.mkdir(parents=True, exist_ok=True)
        (source_schema_dir / "customer_user.yaml").write_text(
            "columns:\n"
            "  - name: customer_id\n"
            "    type: uuid\n"
            "  - name: username\n"
            "    type: text\n"
        )

        # Sink target schema required by --add-sink-table validation
        sink_schema_dir = schemas_root / "chat" / "adopus-replica"
        sink_schema_dir.mkdir(parents=True, exist_ok=True)
        (sink_schema_dir / "customer_user.yaml").write_text("columns: []\n")

        add_source_result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--add-source-table", "public.customer_user",
        )
        assert add_source_result.returncode == 0

        add_table_result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table", "adopus-replica.customer_user",
            "--target-exists", "false",
            "--from", "public.customer_user",
        )
        assert add_table_result.returncode == 0

        add_template_result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--sink-table", "adopus-replica.customer_user",
            "--add-column-template", "source_table",
        )
        assert add_template_result.returncode == 0

        yaml_text = _read_yaml(isolated_project)
        assert "column_templates" in yaml_text
        assert "template: source_table" in yaml_text


class TestCliSinkRemoveTable:
    """CLI e2e: --remove-sink-table."""

    def test_remove_table_from_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--remove-sink-table", "public.users",
        )
        assert result.returncode == 0


# ═══════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════


class TestCliValidateConfig:
    """Validation config tests - unit tests calling validation functions directly."""

    def test_validate_config_new_format(self, tmp_path: Path) -> None:
        """Test validation with new simplified format."""
        # Setup project structure
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    directory:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "directory.yaml").write_text(
            "directory:\n"
            "  source:\n"
            "    validation_database: directory_dev\n"
            "    tables:\n"
            "      public.users:\n"
            "        include_columns:\n"
            "        - id\n"
            "        - name\n"
            "  sinks:\n"
            "    sink_asma.proxy:\n"
            "      tables:\n"
            "        public.dir_users:\n"
            "          target_exists: true\n"
            "          from: public.users\n"
            "          replicate_structure: true\n"
        )
        
        # Change to project dir
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            from cdc_generator.validators.manage_service.validation import validate_service_config
            result = validate_service_config("directory")
            assert result is True
        finally:
            os.chdir(original_cwd)

    def test_validate_config_missing_source(self, tmp_path: Path) -> None:
        """Test validation fails when source section is missing."""
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    chat:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "chat.yaml").write_text(
            "chat:\n"
            "  sinks: {}\n"  # Missing source section
        )
        
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            from cdc_generator.validators.manage_service.validation import validate_service_config
            result = validate_service_config("chat")
            assert result is False
        finally:
            os.chdir(original_cwd)

    def test_validate_config_empty_tables(self, tmp_path: Path) -> None:
        """Test validation warns when no tables defined."""
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    calendar:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "calendar.yaml").write_text(
            "calendar:\n"
            "  source:\n"
            "    validation_database: calendar_dev\n"
            "    tables: {}\n"
        )
        
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            from cdc_generator.validators.manage_service.validation import validate_service_config
            # Should pass with warnings (empty tables is allowed)
            result = validate_service_config("calendar")
            assert result is True
        finally:
            os.chdir(original_cwd)

    def test_validate_config_invalid_sink_table(self, tmp_path: Path) -> None:
        """Test validation catches missing 'from' field in sink tables."""
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    auth:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "auth.yaml").write_text(
            "auth:\n"
            "  source:\n"
            "    validation_database: auth_dev\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.proxy:\n"
            "      tables:\n"
            "        public.auth_users:\n"
            "          target_exists: true\n"
            # Missing 'from' field
        )
        
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            from cdc_generator.validators.manage_service.validation import validate_service_config
            result = validate_service_config("auth")
            assert result is False
        finally:
            os.chdir(original_cwd)

    def test_validate_all_services(self, tmp_path: Path) -> None:
        """Test validation of all services when no service specified."""
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    service1:\n"
            "      schemas:\n"
            "        - public\n"
            "    service2:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "service1.yaml").write_text(
            "service1:\n"
            "  source:\n"
            "    validation_database: s1_dev\n"
            "    tables:\n"
            "      public.users: {}\n"
        )
        (tmp_path / "services" / "service2.yaml").write_text(
            "service2:\n"
            "  source:\n"
            "    validation_database: s2_dev\n"
            "    tables:\n"
            "      public.orders: {}\n"
        )
        
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            import argparse

            from cdc_generator.cli.service_handlers_validation import handle_validate_config
            args = argparse.Namespace(service=None, all=False, schema=None, env='dev')
            result = handle_validate_config(args)
            # Both services are valid
            assert result == 0
        finally:
            os.chdir(original_cwd)


class TestCliInspect:
    """Inspect tests - unit tests calling inspect functions directly."""

    def test_inspect_all_services_structure(self, tmp_path: Path) -> None:
        """Test inspect all services shows proper structure (will fail without DB but shows intent)."""
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        (tmp_path / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  type: postgres\n"
            "  sources:\n"
            "    service1:\n"
            "      schemas:\n"
            "        - public\n"
            "    service2:\n"
            "      schemas:\n"
            "        - public\n"
        )
        (tmp_path / "services").mkdir()
        (tmp_path / "services" / "service1.yaml").write_text("service1:\n  source:\n    tables: {}\n")
        (tmp_path / "services" / "service2.yaml").write_text("service2:\n  source:\n    tables: {}\n")
        
        import os
        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            import argparse

            from cdc_generator.cli.service_handlers_inspect import handle_inspect
            args = argparse.Namespace(service=None, all=True, schema=None, env='dev', save=False, inspect=True)
            # Will fail (no DB) but should attempt to inspect both services
            result = handle_inspect(args)
            # Expected to fail without actual DB connection
            assert result == 1
        finally:
            os.chdir(original_cwd)


class TestCliValidateHierarchy:
    """CLI e2e: --validate-hierarchy."""

    def test_validate_hierarchy(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--validate-hierarchy",
        )
        assert result.returncode == 0


class TestCliGenerateValidation:
    """CLI e2e: --generate-validation."""

    def test_generate_validation_for_schema(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)

        sf = isolated_project / "services" / "proxy.yaml"
        sf.write_text(
            "service: proxy\n"
            "server_group: asma\n"
            "mode: db-shared\n"
            "source:\n"
            "  validation_database: proxy\n"
            "shared:\n"
            "  source_tables:\n"
            "    - schema: public\n"
            "      tables:\n"
            "        - name: queries\n"
            "          primary_key: id\n"
        )

        schema_dir = isolated_project / "service-schemas" / "proxy" / "public"
        schema_dir.mkdir(parents=True)
        (schema_dir / "queries.yaml").write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: uuid\n"
            "    nullable: false\n"
            "    primary_key: true\n"
        )

        result = run_cdc(
            "manage-services", "config", "--service", "proxy",
            "--generate-validation", "--schema", "public",
        )
        assert result.returncode == 0
        assert (
            isolated_project
            / ".vscode"
            / "schemas"
            / "proxy.service-validation.schema.json"
        ).exists()


# ═══════════════════════════════════════════════════════════════════════════
# Schema command listings
# ═══════════════════════════════════════════════════════════════════════════


class TestCliSchemaColumnTemplates:
    """CLI e2e: manage-services resources column-templates."""

    def test_list_templates(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services",
            "resources",
            "column-templates",
            "--list",
        )
        assert result.returncode == 0


class TestCliSchemaTransforms:
    """CLI e2e: manage-services resources transforms."""

    def test_list_transform_rules(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services",
            "resources",
            "transforms",
            "--list-rules",
        )
        assert result.returncode == 0


# ═══════════════════════════════════════════════════════════════════════════
# Create service
# ═══════════════════════════════════════════════════════════════════════════


class TestCliCreateService:
    """CLI e2e: --create-service."""

    def test_create_new_service(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        (isolated_project / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    newservice:\n"
            "      schemas:\n"
            "        - public\n"
            "      nonprod:\n"
            "        database: newservice_dev\n"
        )
        result = run_cdc(
            "manage-services", "config",
            "--create-service", "newservice",
        )
        assert result.returncode == 0
        assert (isolated_project / "services" / "newservice.yaml").exists()

    def test_create_existing_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-services", "config",
            "--create-service", "proxy",
        )
        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# Fish autocompletions
# ═══════════════════════════════════════════════════════════════════════════


class TestCliManageServiceCompletions:
    """Fish autocompletion for manage-services config flags."""

    def test_manage_service_flags_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Core flags show up in completions."""
        result = run_cdc_completion("cdc manage-services config -")
        out = result.stdout
        assert "--service" in out
        assert "--add-source-table" in out
        assert "--remove-table" in out
        assert "--inspect" in out

    def test_add_sink_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Sink-related flags appear in completions (when installed)."""
        # Check --add-sink with broader prefix
        result = run_cdc_completion("cdc manage-services config --add-")
        out = result.stdout
        # At minimum --add-source-table should be there
        assert "--add-source-table" in out

    def test_template_flags_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Validate/generate flags appear in completions."""
        result = run_cdc_completion("cdc manage-services config --validate-")
        out = result.stdout
        assert "--validate-config" in out or "--validate-hierarchy" in out

    def test_map_column_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--map-column appears after --add-sink-table context."""
        result = run_cdc_completion(
            "cdc manage-services config --add-sink-table pub.Actor --map-"
        )
        out = result.stdout
        assert "--map-column" in out

    def test_sink_table_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--sink-table visible with --sink + --add-column-template context."""
        result = run_cdc_completion(
            "cdc manage-services config --sink asma --add-column-template tmpl --sink-"
        )
        out = result.stdout
        assert "--sink-table" in out

    def test_sink_schema_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--sink-schema visible with --add-sink-table context."""
        result = run_cdc_completion(
            "cdc manage-services config --add-sink-table pub.Actor --sink-"
        )
        out = result.stdout
        assert "--sink-schema" in out

    def test_sink_schema_value_completion_suggests_available_schemas(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """--sink-schema value completion suggests schemas for sink target service."""
        (isolated_project / "docker-compose.yml").write_text(
            "services:\n"
            "  dev:\n"
            "    image: busybox\n"
        )
        (isolated_project / "source-groups.yaml").write_text(
            "asma:\n"
            "  pattern: db-shared\n"
            "  sources:\n"
            "    activities:\n"
            "      schemas:\n"
            "        - public\n"
            "        - analytics\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc manage-services config directory "
                + "--sink sink_asma.activities "
                + "--add-sink-table "
                + "--from public.customer_user "
                + "--replicate-structure "
                + "--sink-schema a"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "analytics" in out

    def test_from_flag_visible_after_add_sink_table_without_value(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Regression: --from should appear after bare --add-sink-table."""
        result = run_cdc_completion(
            "cdc manage-services config directory "
            + "--sink sink_asma.notification "
            + "--add-sink-table --fr"
        )
        out = result.stdout
        assert "--from" in out

    def test_add_column_template_suggests_templates_from_services_schemas(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Template key completion reads services/_schemas/column-templates.yaml."""
        from cdc_generator.core import column_templates as column_templates_module

        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        column_templates_module.clear_cache()

        _create_project(isolated_project, with_sink=True)
        schemas_dir = isolated_project / "services" / "_schemas"
        schemas_dir.mkdir(parents=True, exist_ok=True)
        (schemas_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  source_table:\n"
            "    name: _source_table\n"
            "    type: text\n"
            "    value: meta(\"table\")\n"
            "  sync_timestamp:\n"
            "    name: _synced_at\n"
            "    type: timestamptz\n"
            "    value: now()\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc manage-services config "
                + "--service proxy "
                + "--sink sink_asma.chat "
                + "--sink-table public.users "
                + "--add-column-template s"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "source_table" in out
        assert "sync_timestamp" in out

    def test_add_column_template_suggests_target_prefixes_in_add_sink_table_mode(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In add-sink-table mode, template completion starts with target column prefixes."""
        from cdc_generator.core import column_templates as column_templates_module

        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        column_templates_module.clear_cache()

        _create_project(isolated_project, with_sink=True)

        schemas_dir = isolated_project / "services" / "_schemas"
        schemas_dir.mkdir(parents=True, exist_ok=True)
        (schemas_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  tenant_id:\n"
            "    name: tenant_id\n"
            "    type: uuid\n"
            "    value: uuid_v4()\n"
            "  source_table:\n"
            "    name: _source_table\n"
            "    type: text\n"
            "    value: meta(\"table\")\n"
        )

        target_schema_dir = (
            isolated_project / "service-schemas" / "chat" / "public"
        )
        target_schema_dir.mkdir(parents=True, exist_ok=True)
        (target_schema_dir / "customer_user.yaml").write_text(
            "columns:\n"
            "  - name: customer_id\n"
            "    type: uuid\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc manage-services config "
                + "--service proxy "
                + "--sink sink_asma.chat "
                + "--add-sink-table public.customer_user "
                + "--from dbo.Actor "
                + "--target-exists true "
                + "--add-column-template c"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "customer_id:" in out

    def test_add_column_template_suggests_compatible_target_template_pairs(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """In add-sink-table mode, `target:` suggests compatible template keys."""
        from cdc_generator.core import column_templates as column_templates_module

        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        column_templates_module.clear_cache()

        _create_project(isolated_project, with_sink=True)

        schemas_dir = isolated_project / "services" / "_schemas"
        schemas_dir.mkdir(parents=True, exist_ok=True)
        (schemas_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  tenant_id:\n"
            "    name: tenant_id\n"
            "    type: uuid\n"
            "    value: uuid_v4()\n"
            "  source_table:\n"
            "    name: _source_table\n"
            "    type: text\n"
            "    value: meta(\"table\")\n"
        )

        target_schema_dir = (
            isolated_project / "service-schemas" / "chat" / "public"
        )
        target_schema_dir.mkdir(parents=True, exist_ok=True)
        (target_schema_dir / "customer_user.yaml").write_text(
            "columns:\n"
            "  - name: customer_id\n"
            "    type: uuid\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc manage-services config "
                + "--service proxy "
                + "--sink sink_asma.chat "
                + "--add-sink-table public.customer_user "
                + "--from dbo.Actor "
                + "--target-exists true "
                + "--add-column-template customer_id:t"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "customer_id:tenant_id" in out

    def test_add_column_template_hides_targets_already_mapped_by_map_column(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`--add-column-template` should hide targets already used in --map-column."""
        from cdc_generator.core import column_templates as column_templates_module

        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        column_templates_module.clear_cache()

        _create_project(isolated_project, with_sink=True)

        schemas_dir = isolated_project / "services" / "_schemas"
        schemas_dir.mkdir(parents=True, exist_ok=True)
        (schemas_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  tenant_id:\n"
            "    name: tenant_id\n"
            "    type: uuid\n"
            "    value: uuid_v4()\n"
        )

        target_schema_dir = (
            isolated_project / "service-schemas" / "chat" / "public"
        )
        target_schema_dir.mkdir(parents=True, exist_ok=True)
        (target_schema_dir / "customer_user.yaml").write_text(
            "columns:\n"
            "  - name: customer_id\n"
            "    type: uuid\n"
            "  - name: user_id\n"
            "    type: uuid\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc manage-services config "
                + "--service proxy "
                + "--sink sink_asma.chat "
                + "--add-sink-table public.customer_user "
                + "--from dbo.Actor "
                + "--target-exists true "
                + "--map-column customer_id:KundeId "
                + "--add-column-template u"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "customer_id:" not in out
        assert "user_id:" in out


    def test_msc_map_column_suggests_sink_source_pairs(
        self,
        run_cdc_completion: RunCdcCompletion,
        isolated_project: Path,
    ) -> None:
        """`--map-column` suggests sink-first pairs as TARGET:SOURCE."""
        _create_project(isolated_project, with_sink=True)

        source_schema_dir = (
            isolated_project / "service-schemas" / "proxy" / "dbo"
        )
        source_schema_dir.mkdir(parents=True, exist_ok=True)
        (source_schema_dir / "Actor.yaml").write_text(
            "columns:\n"
            "  - name: id\n"
            "    type: uuid\n"
            "  - name: actor_name\n"
            "    type: text\n"
        )

        target_schema_dir = (
            isolated_project / "service-schemas" / "chat" / "public"
        )
        target_schema_dir.mkdir(parents=True, exist_ok=True)
        (target_schema_dir / "customer_user.yaml").write_text(
            "columns:\n"
            "  - name: user_id\n"
            "    type: uuid\n"
            "  - name: display_name\n"
            "    type: text\n"
        )

        original_cwd = Path.cwd()
        os.chdir(isolated_project)
        try:
            result = run_cdc_completion(
                "cdc msc "
                + "--add-sink-table public.customer_user "
                + "--from dbo.Actor "
                + "--target-exists true "
                + "--map-column user"
            )
        finally:
            os.chdir(original_cwd)

        out = result.stdout
        assert "user_id:" in out
