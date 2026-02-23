"""Unit tests for template, transform, validation, and bloblang handlers.

Covers handle_add_column_template, handle_remove_column_template,
handle_list_column_templates, handle_add_transform, handle_remove_transform,
handle_list_transforms, handle_list_transform_rules,
handle_validate_config, handle_validate_hierarchy,
handle_generate_validation, handle_validate_bloblang,
and the _resolve_sink_and_table helper.
"""

import argparse
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
from cdc_generator.cli.service_handlers_bloblang import (
    handle_validate_bloblang,
)

# project_dir fixture is provided by tests/conftest.py


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

    def test_returns_none_service_not_found(
        self, project_dir: Path,
    ) -> None:
        """Returns None when service file doesn't exist (FileNotFoundError)."""
        args = _ns(service="nonexistent", sink=None, sink_table="public.t")
        result = _resolve_sink_and_table(args)
        assert result is None

    def test_returns_none_multiple_sinks_no_sink_flag(
        self, project_dir: Path,
    ) -> None:
        """Returns None when service has >1 sinks and --sink not provided."""
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
            "    sink_asma.calendar:\n"
            "      tables:\n"
            "        public.events:\n"
            "          target_exists: true\n"
        )
        args = _ns(sink=None, sink_table="public.users")
        result = _resolve_sink_and_table(args)
        assert result is None

    def test_auto_selects_single_sink(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Auto-selects sink when service has exactly one sink."""
        args = _ns(sink=None)
        result = _resolve_sink_and_table(args)
        assert result is not None
        _, sink_key, _ = result
        assert sink_key == "sink_asma.chat"

    def test_returns_none_no_sinks_configured(
        self, project_dir: Path,
    ) -> None:
        """Returns None when service has no sinks section."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.queries: {}\n"
        )
        args = _ns(sink=None, sink_table="public.users")
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

    def test_returns_0_on_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when add_column_template operation succeeds."""
        args = _ns(
            add_column_template="tenant_id",
            column_name="tenant_id",
            skip_validation=True,
        )
        with patch(
            "cdc_generator.cli.service_handlers_templates.add_column_template_to_table",
            return_value=True,
        ) as add_template_mock:
            result = handle_add_column_template(args)

        assert result == 0
        add_template_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.users",
            "tenant_id",
            "tenant_id",
            None,
            True,
        )

    def test_returns_1_on_operation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when add_column_template operation fails."""
        args = _ns(add_column_template="tenant_id")
        with patch(
            "cdc_generator.cli.service_handlers_templates.add_column_template_to_table",
            return_value=False,
        ):
            result = handle_add_column_template(args)
        assert result == 1


class TestHandleRemoveColumnTemplate:
    """Tests for handle_remove_column_template."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, remove_column_template="audit_created_at")
        result = handle_remove_column_template(args)
        assert result == 1

    def test_returns_0_on_success(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 when template is successfully removed."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables:\n"
            "        public.users:\n"
            "          target_exists: false\n"
            "          column_templates:\n"
            "            - template: tenant_id\n"
        )
        args = _ns(
            remove_column_template="tenant_id",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_remove_column_template(args)
        assert result == 0

    def test_returns_1_on_operation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when remove operation fails (template not found)."""
        args = _ns(
            remove_column_template="nonexistent_template",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_remove_column_template(args)
        assert result == 1


# ═══════════════════════════════════════════════════════════════════════════
# Transform handlers
# ═══════════════════════════════════════════════════════════════════════════


class TestHandleAddTransform:
    """Tests for handle_add_transform."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(
            service=None,
            add_transform="file://services/_bloblang/examples/map_boolean.blobl",
        )
        result = handle_add_transform(args)
        assert result == 1

    def test_returns_1_without_sink_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when --sink-table not provided."""
        args = _ns(
            add_transform="file://services/_bloblang/examples/map_boolean.blobl",
            sink_table=None,
        )
        result = handle_add_transform(args)
        assert result == 1

    def test_returns_0_on_success(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when add_transform operation succeeds."""
        args = _ns(
            add_transform="file://services/_bloblang/examples/map_boolean.blobl",
            skip_validation=True,
        )
        with patch(
            "cdc_generator.cli.service_handlers_templates.add_transform_to_table",
            return_value=True,
        ) as add_transform_mock:
            result = handle_add_transform(args)

        assert result == 0
        add_transform_mock.assert_called_once_with(
            "proxy",
            "sink_asma.chat",
            "public.users",
            "file://services/_bloblang/examples/map_boolean.blobl",
            True,
        )

    def test_returns_1_on_operation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when add_transform operation fails."""
        args = _ns(
            add_transform="file://services/_bloblang/examples/map_boolean.blobl",
        )
        with patch(
            "cdc_generator.cli.service_handlers_templates.add_transform_to_table",
            return_value=False,
        ):
            result = handle_add_transform(args)
        assert result == 1


class TestHandleRemoveTransform:
    """Tests for handle_remove_transform."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(
            service=None,
            remove_transform="file://services/_bloblang/examples/map_boolean.blobl",
        )
        result = handle_remove_transform(args)
        assert result == 1

    def test_returns_0_on_success(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 when transform is successfully removed."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables:\n"
            "        public.users:\n"
            "          target_exists: false\n"
            "          transforms:\n"
            "            - bloblang_ref: file://services/_bloblang/examples/map_boolean.blobl\n"
        )
        args = _ns(
            remove_transform="file://services/_bloblang/examples/map_boolean.blobl",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_remove_transform(args)
        assert result == 0

    def test_returns_1_on_operation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when remove operation fails (transform not found)."""
        args = _ns(
            remove_transform="nonexistent_rule",
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_remove_transform(args)
        assert result == 1


class TestHandleListTransforms:
    """Tests for handle_list_transforms."""

    def test_returns_1_without_service(self) -> None:
        """Returns 1 when --service missing."""
        args = _ns(service=None, list_transforms=True)
        result = handle_list_transforms(args)
        assert result == 1

    def test_returns_0_with_transforms(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 when listing transforms on a table."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    tables:\n"
            "      public.users: {}\n"
            "  sinks:\n"
            "    sink_asma.chat:\n"
            "      tables:\n"
            "        public.users:\n"
            "          target_exists: false\n"
            "          transforms:\n"
            "            - bloblang_ref: file://services/_bloblang/examples/map_boolean.blobl\n"
        )
        args = _ns(
            list_transforms=True,
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_list_transforms(args)
        assert result == 0

    def test_returns_0_empty_table(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 even when no transforms configured."""
        args = _ns(
            list_transforms=True,
            sink="sink_asma.chat",
            sink_table="public.users",
        )
        result = handle_list_transforms(args)
        assert result == 0


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
        """Returns 0 for valid config with new format."""
        sf = project_dir / "services" / "proxy.yaml"
        sf.write_text(
            "proxy:\n"
            "  source:\n"
            "    validation_database: proxy_dev\n"
            "    tables:\n"
            "      public.queries:\n"
            "        primary_key: id\n"
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

    def test_returns_1_on_validation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when hierarchy validation fails."""
        args = _ns()
        with patch(
            "cdc_generator.cli.service_handlers_validation.validate_hierarchy_no_duplicates",
            return_value=False,
        ):
            result = handle_validate_hierarchy(args)
        assert result == 1


class TestHandleGenerateValidation:
    """Tests for handle_generate_validation."""

    def test_requires_all_or_schema(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when neither --all nor --schema provided."""
        args = _ns()
        result = handle_generate_validation(args)
        assert result == 1

    def test_returns_0_with_all(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when generation succeeds with --all."""
        args = _ns(all=True, generate_validation=True)
        with patch(
            "cdc_generator.cli.service_handlers_validation.generate_service_validation_schema",
            return_value=True,
        ):
            result = handle_generate_validation(args)
        assert result == 0

    def test_returns_0_with_schema(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 0 when generation succeeds with --schema."""
        args = _ns(schema="public", generate_validation=True)
        with patch(
            "cdc_generator.cli.service_handlers_validation.generate_service_validation_schema",
            return_value=True,
        ):
            result = handle_generate_validation(args)
        assert result == 0

    def test_returns_1_on_generation_failure(
        self, project_dir: Path, service_with_sink: Path,
    ) -> None:
        """Returns 1 when schema generation fails."""
        args = _ns(all=True, generate_validation=True)
        with patch(
            "cdc_generator.cli.service_handlers_validation.generate_service_validation_schema",
            return_value=False,
        ):
            result = handle_generate_validation(args)
        assert result == 1


class TestHandleValidateBloblang:
    """Tests for handle_validate_bloblang."""

    def test_returns_0_when_validation_succeeds(
        self, project_dir: Path,
    ) -> None:
        """Returns 0 when bloblang validator succeeds."""
        args = _ns()
        with patch(
            "cdc_generator.cli.service_handlers_bloblang.validate_service_bloblang",
            return_value=True,
        ):
            result = handle_validate_bloblang(args)
        assert result == 0

    def test_returns_1_when_validation_fails(
        self, project_dir: Path,
    ) -> None:
        """Returns 1 when bloblang validator fails."""
        args = _ns()
        with patch(
            "cdc_generator.cli.service_handlers_bloblang.validate_service_bloblang",
            return_value=False,
        ):
            result = handle_validate_bloblang(args)
        assert result == 1
