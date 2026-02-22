"""Unit tests for list-source-tables, create-service, and misc handlers.

Covers handle_list_source_tables, handle_create_service,
handle_no_service, and _auto_detect_service.
"""

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service import (
    _auto_detect_service,
)
from cdc_generator.cli.service_handlers_create import (
    handle_create_service,
)
from cdc_generator.cli.service_handlers_list_source import (
    handle_list_source_tables,
)
from cdc_generator.cli.service_handlers_misc import (
    handle_list_services,
    handle_no_service,
)

# project_dir fixture is provided by tests/conftest.py


@pytest.fixture()
def service_old_format(project_dir: Path) -> Path:
    """Service with old flat format tables (source.tables dict)."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "      public.users:\n"
        "        primary_key: id\n"
    )
    return sf


@pytest.fixture()
def service_hierarchical(project_dir: Path) -> Path:
    """Service with new hierarchical format (shared.source_tables)."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  shared:\n"
        "    source_tables:\n"
        "      - schema: public\n"
        "        tables:\n"
        "          - name: queries\n"
        "            primary_key: id\n"
        "          - name: users\n"
    )
    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "proxy",
        "schema": None,
        "all": False,
        "env": "nonprod",
        "save": False,
        "server": None,
        "add_validation_database": None,
        "create_service": None,
        "validate_config": False,
        "inspect": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# handle_list_source_tables
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleListSourceTables:
    """Tests for handle_list_source_tables handler."""

    def test_lists_old_format_tables(
        self, project_dir: Path, service_old_format: Path,
    ) -> None:
        """Returns 0 and prints tables in old flat format."""
        args = _ns()
        result = handle_list_source_tables(args)
        assert result == 0

    def test_lists_hierarchical_format_tables(
        self, project_dir: Path, service_hierarchical: Path,
    ) -> None:
        """Returns 0 for hierarchical format."""
        args = _ns()
        result = handle_list_source_tables(args)
        assert result == 0

    def test_empty_tables_returns_0(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 with helpful message when no tables."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text("proxy:\n  source: {}\n")
        args = _ns()
        result = handle_list_source_tables(args)
        assert result == 0

    def test_no_service_flag_returns_1(self) -> None:
        """Returns 1 when --service not provided."""
        args = _ns(service=None)
        result = handle_list_source_tables(args)
        assert result == 1

    def test_nonexistent_service_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when service YAML doesn't exist."""
        args = _ns(service="nonexistent")
        result = handle_list_source_tables(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# handle_create_service
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleCreateService:
    """Tests for handle_create_service handler."""

    def test_create_new_service(
        self, project_dir: Path,
    ) -> None:
        """Creates new service YAML when service is in source-groups."""
        args = _ns(service="proxy", create_service="proxy")
        result = handle_create_service(args)
        assert result == 0
        sf = project_dir / "services" / "proxy.yaml"
        assert sf.exists()


    # ═══════════════════════════════════════════════════════════════════════════
    # handle_list_services
    # ═══════════════════════════════════════════════════════════════════════════


    class TestHandleListServices:
        """Tests for handle_list_services handler."""

        def test_lists_services_from_yaml_files(self, project_dir: Path) -> None:
            """Returns 0 and lists service names from services/*.yaml."""
            services_dir = project_dir / "services"
            (services_dir / "adopus.yaml").write_text("adopus: {}\n")
            (services_dir / "chat.yaml").write_text("chat: {}\n")

            result = handle_list_services()
            assert result == 0

        def test_returns_0_when_services_dir_empty(self, project_dir: Path) -> None:
            """Returns 0 when no service files exist."""
            result = handle_list_services()
            assert result == 0

    def test_create_already_exists_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 if service file already exists."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text("proxy: {}\n")
        args = _ns(service="proxy", create_service="proxy")
        result = handle_create_service(args)
        assert result == 1

    def test_create_no_service_flag_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 if --service is not set."""
        args = _ns(service=None, create_service="proxy")
        result = handle_create_service(args)
        assert result == 1

    def test_create_not_in_source_groups_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when service not in source-groups.yaml."""
        args = _ns(service="unknown", create_service="unknown")
        result = handle_create_service(args)
        assert result == 1

    def test_no_server_group_found_returns_1(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when service is in source-groups but resolves to no group.

        This covers the branch: ``if not server_group: return 1``
        (after _detect_server_group returns None for the group name).
        """
        # Write source-groups with multiple groups, neither containing 'newservice'
        sg = project_dir / "source-groups.yaml"
        sg.write_text(
            "group_a:\n"
            "  sources:\n"
            "    proxy:\n"
            "      server: srv\n"
            "group_b:\n"
            "  sources:\n"
            "    other:\n"
            "      server: srv\n"
        )
        args = _ns(service="newservice", create_service="newservice")
        result = handle_create_service(args)
        assert result == 1

    def test_db_per_tenant_allows_only_server_group_name(
        self, project_dir: Path,
    ) -> None:
        """db-per-tenant service creation allows only server-group level service."""
        sg = project_dir / "source-groups.yaml"
        sg.write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  sources:\n"
            "    AVProd:\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusAVProd\n"
        )

        args = _ns(service="adopus", create_service="adopus")
        with patch(
            "cdc_generator.cli.service_handlers_create.create_service"
        ) as mock_create:
            result = handle_create_service(args)

        assert result == 0
        mock_create.assert_called_once_with("adopus", "adopus", "default", None)

    def test_db_per_tenant_rejects_source_name_as_service(
        self, project_dir: Path,
    ) -> None:
        """db-per-tenant service creation rejects per-source names."""
        sg = project_dir / "source-groups.yaml"
        sg.write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  sources:\n"
            "    AVProd:\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusAVProd\n"
        )

        args = _ns(service="AVProd", create_service="AVProd")
        with patch(
            "cdc_generator.cli.service_handlers_create.create_service"
        ) as mock_create:
            result = handle_create_service(args)

        assert result == 1
        mock_create.assert_not_called()

    def test_create_passes_validation_database_override(
        self, project_dir: Path,
    ) -> None:
        """Forwards --add-validation-database to create_service."""
        sg = project_dir / "source-groups.yaml"
        sg.write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  sources:\n"
            "    AVProd:\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusAVProd\n"
        )

        args = _ns(
            service="adopus",
            create_service="adopus",
            add_validation_database="AdOpusAVProd",
        )
        with patch(
            "cdc_generator.cli.service_handlers_create.create_service"
        ) as mock_create:
            result = handle_create_service(args)

        assert result == 0
        mock_create.assert_called_once_with(
            "adopus",
            "adopus",
            "default",
            "AdOpusAVProd",
        )

    def test_db_per_tenant_create_does_not_scaffold_customers_block(
        self, project_dir: Path,
    ) -> None:
        """Created db-per-tenant service relies on source-groups for customers."""
        sg = project_dir / "source-groups.yaml"
        sg.write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  database_ref: Test\n"
            "  sources:\n"
            "    Test:\n"
            "      schemas:\n"
            "      - dbo\n"
            "      default:\n"
            "        server: default\n"
            "        database: AdOpusTest\n"
        )

        args = _ns(service="adopus", create_service="adopus")
        result = handle_create_service(args)
        assert result == 0

        service_path = project_dir / "services" / "adopus.yaml"
        content = service_path.read_text(encoding="utf-8")
        assert "customers:" not in content


# ═══════════════════════════════════════════════════════════════════════════
# handle_no_service
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleNoService:
    """Tests for handle_no_service."""

    def test_returns_1(self, project_dir: Path) -> None:
        """Always returns 1."""
        result = handle_no_service()
        assert result == 1

    def test_lists_available_services(
        self, project_dir: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Shows available service files when they exist."""
        (project_dir / "services" / "proxy.yaml").write_text("proxy: {}\n")
        handle_no_service()
        captured = capsys.readouterr()
        assert "proxy" in captured.out


# ═══════════════════════════════════════════════════════════════════════════
# _auto_detect_service
# ═══════════════════════════════════════════════════════════════════════════


class TestAutoDetectService:
    """Tests for _auto_detect_service."""

    def test_returns_args_if_service_set(
        self, project_dir: Path,
    ) -> None:
        """Passes through when --service already set."""
        args = _ns(service="proxy")
        result = _auto_detect_service(args)
        assert result is not None
        assert result.service == "proxy"

    def test_auto_detects_single_service(
        self, project_dir: Path,
    ) -> None:
        """Sets service when exactly one YAML exists."""
        (project_dir / "services" / "proxy.yaml").write_text("proxy: {}\n")
        args = _ns(service=None, create_service=None)
        result = _auto_detect_service(args)
        assert result is not None
        assert result.service == "proxy"

    def test_returns_none_multiple_services(
        self, project_dir: Path,
    ) -> None:
        """Returns None when multiple services exist and none specified."""
        (project_dir / "services" / "proxy.yaml").write_text("a: {}\n")
        (project_dir / "services" / "chat.yaml").write_text("b: {}\n")
        args = _ns(service=None, create_service=None)
        result = _auto_detect_service(args)
        assert result is None

    def test_no_services_dir_passes_through(
        self, project_dir: Path,
    ) -> None:
        """No services/ dir → returns args unchanged (no service set)."""
        import shutil
        shutil.rmtree(project_dir / "services")
        args = _ns(service=None)
        result = _auto_detect_service(args)
        assert result is not None
        assert result.service is None

    def test_create_service_skips_multiple_services_error(
        self, project_dir: Path,
    ) -> None:
        """create_service set bypasses multiple-services error."""
        (project_dir / "services" / "proxy.yaml").write_text("a: {}\n")
        (project_dir / "services" / "chat.yaml").write_text("b: {}\n")
        args = _ns(service=None, create_service="newservice")
        result = _auto_detect_service(args)
        assert result is not None
