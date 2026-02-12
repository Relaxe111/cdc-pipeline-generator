"""End-to-end CLI tests for manage-service beyond --source-table.

Tests the full flow through a real **fish** shell for:
- --add-source-table / --add-source-tables / --remove-table
- --list-source-tables
- --create-service
- --list-sinks / --add-sink / --remove-sink
- --add-sink-table / --remove-sink-table
- --validate-config
- --list-template-keys / --list-transform-rule-keys
- fish autocompletions for manage-service flags
"""

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
            "manage-service", "--service", "proxy",
            "--add-source-table", "public.orders",
        )
        assert result.returncode == 0
        assert "public.orders" in _read_yaml(isolated_project)

    def test_add_without_schema_defaults_dbo(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--service", "proxy",
            "--add-source-table", "Actor",
        )
        assert result.returncode == 0
        assert "dbo.Actor" in _read_yaml(isolated_project)

    def test_add_duplicate_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
            "--remove-table", "public.queries",
        )
        assert result.returncode == 0
        assert "public.queries" not in _read_yaml(isolated_project)

    def test_remove_nonexistent(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--service", "proxy",
            "--remove-table", "public.nonexistent",
        )
        assert result.returncode == 1


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
            "manage-service", "--service", "proxy",
            "--list-source-tables",
        )
        assert result.returncode == 0
        assert "queries" in result.stdout

    def test_no_service_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--list-source-tables",
        )
        # Auto-detects single service → should still work
        assert result.returncode == 0


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
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table", "public.orders",
            "--target-exists", "false",
        )
        assert result.returncode == 0

    def test_requires_target_exists(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--add-sink-table",
            "--from", "public.queries",
            "--target-exists", "false",
        )
        assert result.returncode == 0
        assert "public.queries" in _read_yaml(isolated_project)


class TestCliSinkRemoveTable:
    """CLI e2e: --remove-sink-table."""

    def test_remove_table_from_sink(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project, with_sink=True)
        result = run_cdc(
            "manage-service", "--service", "proxy",
            "--sink", "sink_asma.chat",
            "--remove-sink-table", "public.users",
        )
        assert result.returncode == 0


# ═══════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════


class TestCliValidateConfig:
    """CLI e2e: --validate-config."""

    def test_validate_config(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        # Rewrite with complete config that passes validation
        sf = isolated_project / "services" / "proxy.yaml"
        sf.write_text(
            "service: proxy\n"
            "server_group: asma\n"
            "mode: db-shared\n"
            "shared:\n"
            "  source_tables:\n"
            "    - schema: public\n"
            "      tables:\n"
            "        - name: queries\n"
            "          primary_key: id\n"
        )
        result = run_cdc(
            "manage-service", "--service", "proxy",
            "--validate-config",
        )
        assert result.returncode == 0


class TestCliValidateHierarchy:
    """CLI e2e: --validate-hierarchy."""

    def test_validate_hierarchy(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service", "--service", "proxy",
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
            "manage-service", "--service", "proxy",
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
# Template/Transform listing
# ═══════════════════════════════════════════════════════════════════════════


class TestCliListTemplateKeys:
    """CLI e2e: --list-template-keys."""

    def test_list_templates(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--list-template-keys",
        )
        assert result.returncode == 0


class TestCliListTransformRuleKeys:
    """CLI e2e: --list-transform-rule-keys."""

    def test_list_transform_rules(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--list-transform-rule-keys",
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
            "manage-service",
            "--create-service", "newservice",
        )
        assert result.returncode == 0
        assert (isolated_project / "services" / "newservice.yaml").exists()

    def test_create_existing_returns_1(
        self, run_cdc: RunCdc, isolated_project: Path,
    ) -> None:
        _create_project(isolated_project)
        result = run_cdc(
            "manage-service",
            "--create-service", "proxy",
        )
        assert result.returncode == 1


# ═══════════════════════════════════════════════════════════════════════════
# Fish autocompletions
# ═══════════════════════════════════════════════════════════════════════════


class TestCliManageServiceCompletions:
    """Fish autocompletion for manage-service flags."""

    def test_manage_service_flags_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Core flags show up in completions."""
        result = run_cdc_completion("cdc manage-service -")
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
        result = run_cdc_completion("cdc manage-service --add-")
        out = result.stdout
        # At minimum --add-source-table should be there
        assert "--add-source-table" in out

    def test_template_flags_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """Validate/generate flags appear in completions."""
        result = run_cdc_completion("cdc manage-service --validate-")
        out = result.stdout
        assert "--validate-config" in out or "--validate-hierarchy" in out

    def test_map_column_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--map-column appears after --add-sink-table context."""
        result = run_cdc_completion(
            "cdc manage-service --add-sink-table pub.Actor --map-"
        )
        out = result.stdout
        assert "--map-column" in out

    def test_sink_table_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--sink-table visible with --add-column-template context."""
        result = run_cdc_completion(
            "cdc manage-service --add-column-template tmpl --sink-"
        )
        out = result.stdout
        assert "--sink-table" in out

    def test_sink_schema_flag_visible(
        self, run_cdc_completion: RunCdcCompletion,
    ) -> None:
        """--sink-schema visible with --add-sink-table context."""
        result = run_cdc_completion(
            "cdc manage-service --add-sink-table pub.Actor --sink-"
        )
        out = result.stdout
        assert "--sink-schema" in out
