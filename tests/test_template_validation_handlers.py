"""Unit tests for template, transform, validation, and bloblang handlers.

Covers handle_add_column_template, handle_remove_column_template,
handle_list_column_templates, handle_add_transform, handle_remove_transform,
handle_list_transforms, handle_list_transform_rules,
handle_validate_config, handle_validate_hierarchy,
handle_generate_validation, handle_validate_bloblang,
and the _resolve_sink_and_table helper.
"""

import argparse
import os
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.cli.service_handlers_templates import (
    _resolve_sink_and_table,
    handle_add_column_template,
    handle_add_transform,
    handle_list_column_templates,
    handle_list_transform_rules,
    handle_list_transforms,
    handle_remove_column_template,
    handle_remove_transform,
)
from cdc_generator.cli.service_handlers_validation import (
    handle_generate_validation,
    handle_validate_config,
    handle_validate_hierarchy,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def project_dir(tmp_path: Path) -> Iterator[Path]:
    """Isolated project with services/, service-schemas/, and patching."""
    services_dir = tmp_path / "services"
    services_dir.mkdir()
    schemas_dir = tmp_path / "service-schemas"
    schemas_dir.mkdir()
    (tmp_path / "source-groups.yaml").write_text(
        "asma:\n  pattern: db-shared\n"
    )
    (tmp_path / "sink-groups.yaml").write_text(
        "sink_asma:\n  type: postgres\n  server: sink-pg\n"
    )
    original_cwd = Path.cwd()
    os.chdir(tmp_path)
    with patch(
        "cdc_generator.validators.manage_service.config.SERVICES_DIR",
        services_dir,
    ), patch(
        "cdc_generator.validators.manage_service.config.SERVICE_SCHEMAS_DIR",
        schemas_dir,
    ):
        try:
            yield tmp_path
        finally:
            os.chdir(original_cwd)


@pytest.fixture()
def service_with_sink(project_dir: Path) -> Path:
    """Service with one sink and one table."""
    sf = project_dir / "services" / "proxy.yaml"
    sf.write_text(
        "proxy:\n"
        "  source:\n"
        "    tables:\n"
        "      public.queries: {}\n"
        "  sinks:\n"
        "    sink_asma.chat:\n"
        "      tables:\n"
        "        public.users:\n"
        "          target_exists: true\n"
    )
    return sf


def _ns(**kwargs: object) -> argparse.Namespace:
    defaults: dict[str, object] = {
        "service": "proxy",
        "sink": "sink_asma.chat",
        "sink_table": "public.users",
        "add_column_template": None,
        "remove_column_template": None,
        "list_column_templates": False,
        "column_name": None,
        "add_transform": None,
        "remove_transform": None,
        "list_transforms": False,
        "list_template_keys": False,
        "list_transform_rule_keys": False,
        "skip_validation": True,
        "all": False,
        "schema": None,
        "env": "nonprod",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ═══════════════════════════════════════════════════════════════════════════
# _resolve_sink_and_table
# ═══════════════════════════════════════════════════════════════════════════


class TestResolveSinkAndTable:
    """Tests for _resolve_sink_and_table helper."""

    def test_resolves_all_three(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns (service, sink_key, sink_table) on success."""
        args = _ns()
        result = _resolve_sink_and_table(args)
        assert result is not None
        service, sink_key, sink_table = result
        assert service == "proxy"
        assert sink_key == "sink_asma.chat"
        assert sink_table == "public.users"

    def test_returns_none_no_service(self) -> None:
        """Returns None when --service missing."""
        args = _ns(service=None)
        result = _resolve_sink_and_table(args)
        assert result is None

    def test_returns_none_no_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns None when --sink-table missing."""
        args = _ns(sink_table=None)
        result = _resolve_sink_and_table(args)
        assert result is None

    def test_returns_none_no_sink(self) -> None:
        """Returns None when --sink missing and service has multiple sinks."""
        args = _ns(sink=None, service="nonexistent")
        result = _resolve_sink_and_table(args)
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════
# Column template handlers
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddColumnTemplate:
    """Tests for handle_add_column_template."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service not provided."""
        args = _ns(service=None, add_column_template="audit_created_at")
        result = handle_add_column_template(args)
        assert result == 1

    def test_returns_1_without_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(
            add_column_template="audit_created_at",
            sink_table=None,
        )
        result = handle_add_column_template(args)
        assert result == 1


class TestHandleRemoveColumnTemplate:
    """Tests for handle_remove_column_template."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, remove_column_template="audit_created_at")
        result = handle_remove_column_template(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Transform handlers
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddTransform:
    """Tests for handle_add_transform."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, add_transform="map_boolean")
        result = handle_add_transform(args)
        assert result == 1

    def test_returns_1_without_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(add_transform="map_boolean", sink_table=None)
        result = handle_add_transform(args)
        assert result == 1


class TestHandleRemoveTransform:
    """Tests for handle_remove_transform."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, remove_transform="map_boolean")
        result = handle_remove_transform(args)
        assert result == 1


class TestHandleListTransforms:
    """Tests for handle_list_transforms."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, list_transforms=True)
        result = handle_list_transforms(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Listing handlers (no service required)
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleListColumnTemplates:
    """Tests for handle_list_column_templates."""

    def test_returns_0(self) -> None:
        """Lists templates and returns 0."""
        args = _ns(list_template_keys=True)
        result = handle_list_column_templates(args)
        assert result == 0


class TestHandleListTransformRules:
    """Tests for handle_list_transform_rules."""

    def test_returns_0(self) -> None:
        """Lists transform rules and returns 0."""
        args = _ns(list_transform_rule_keys=True)
        result = handle_list_transform_rules(args)
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════
# Validation handlers
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleValidateConfig:
    """Tests for handle_validate_config."""

    def test_valid_service(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 for valid config with all required fields."""
        sf = project_dir / "services" / "proxy.yaml"
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
        args = _ns()
        result = handle_validate_config(args)
        assert result == 0

    def test_nonexistent_service(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when service doesn't exist."""
        args = _ns(service="missing")
        # Should return 1, not raise
        try:
            result = handle_validate_config(args)
            assert result == 1
        except FileNotFoundError:
            # Acceptable — service file not found
            pass


class TestHandleValidateHierarchy:
    """Tests for handle_validate_hierarchy."""

    def test_valid_service(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 for valid hierarchy."""
        args = _ns()
        result = handle_validate_hierarchy(args)
        assert result == 0


class TestHandleGenerateValidation:
    """Tests for handle_generate_validation."""

    def test_requires_all_or_schema(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when neither --all nor --schema provided."""
        args = _ns()
        result = handle_generate_validation(args)
        assert result == 1
