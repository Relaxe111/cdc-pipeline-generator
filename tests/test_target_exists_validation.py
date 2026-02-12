"""Tests for target_exists validation on column template operations.

When a sink table has target_exists=true, adding a column template
MUST require --column-name (name override) because the table already
exists and the pipeline cannot create new columns on it.
"""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest  # type: ignore[import-not-found]

from cdc_generator.core.column_templates import (
    clear_cache as clear_template_cache,
)
from cdc_generator.core.column_templates import (
    set_templates_path,
)
from cdc_generator.validators.manage_service.sink_template_ops import (
    add_column_template_to_table,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_caches() -> None:  # type: ignore[misc]
    """Clear template cache before each test."""
    clear_template_cache()


@pytest.fixture()
def templates_file(tmp_path: Path) -> Path:
    """Create a temporary column-templates.yaml."""
    content = """\
templates:
  tenant_id:
    name: _tenant_id
    type: uuid
    not_null: true
    description: Tenant identifier
    value: "${TENANT_ID}"
  source_table:
    name: _source_table
    type: text
    not_null: true
    description: Source table name
    value: meta("table")
"""
    file_path = tmp_path / "column-templates.yaml"
    file_path.write_text(content)
    set_templates_path(file_path)
    return file_path


def _make_service_config(
    target_exists: bool,
    table_key: str = "public.directory_user_name",
    sink_key: str = "sink_asma.proxy",
) -> dict[str, Any]:
    """Build a minimal service config dict for testing."""
    table_cfg: dict[str, Any] = {
        "target_exists": target_exists,
        "from": "public.customer_user",
    }
    if target_exists:
        table_cfg["columns"] = {"user_id": "user_id"}
    else:
        table_cfg["replicate_structure"] = True

    return {
        "service": "test_svc",
        "sinks": {
            sink_key: {
                "tables": {
                    table_key: table_cfg,
                },
            },
        },
    }


def _patch_service_io(config: dict[str, Any]):
    """Patch load_service_config and save_service_config for testing.

    Patches at the import location inside sink_template_ops so the
    mocks intercept the actual calls.
    """
    _target = "cdc_generator.validators.manage_service.sink_template_ops"
    return (
        patch(f"{_target}.load_service_config", return_value=config),
        patch(f"{_target}.save_service_config", return_value=True),
    )


# ---------------------------------------------------------------------------
# Tests — target_exists=true rejects without name override
# ---------------------------------------------------------------------------


class TestTargetExistsRejectsWithoutNameOverride:
    """Adding a column template to target_exists=true without --column-name must fail."""

    def test_rejects_add_without_name_override(
        self, templates_file: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """target_exists=true + no --column-name → error."""
        config = _make_service_config(target_exists=True)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override=None,
                skip_validation=True,
            )

        assert result is False
        mock_save.assert_not_called()
        captured = capsys.readouterr()
        assert "target_exists=true" in captured.out
        assert "--column-name" in captured.out

    def test_error_message_shows_default_column_name(
        self, templates_file: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Error message should show the default column name that would be created."""
        config = _make_service_config(target_exists=True)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch:
            add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override=None,
                skip_validation=True,
            )

        captured = capsys.readouterr()
        assert "_tenant_id" in captured.out

    def test_rejects_different_template(
        self, templates_file: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Any template without name override is rejected on target_exists=true."""
        config = _make_service_config(target_exists=True)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "source_table",
                name_override=None,
                skip_validation=True,
            )

        assert result is False
        mock_save.assert_not_called()
        captured = capsys.readouterr()
        assert "_source_table" in captured.out


# ---------------------------------------------------------------------------
# Tests — target_exists=true allows with name override
# ---------------------------------------------------------------------------


class TestTargetExistsAllowsWithNameOverride:
    """Adding a column template to target_exists=true WITH --column-name must succeed."""

    def test_allows_add_with_name_override(
        self, templates_file: Path,
    ) -> None:
        """target_exists=true + --column-name → success."""
        config = _make_service_config(target_exists=True)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override="customer_id",
                skip_validation=True,
            )

        assert result is True
        mock_save.assert_called_once()

    def test_template_added_with_correct_name(
        self, templates_file: Path,
    ) -> None:
        """The template entry in YAML should use the override name."""
        config = _make_service_config(target_exists=True)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch:
            add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override="customer_id",
                skip_validation=True,
            )

        table_cfg = config["sinks"]["sink_asma.proxy"]["tables"]["public.directory_user_name"]
        templates = table_cfg.get("column_templates", [])
        assert len(templates) == 1
        assert templates[0]["template"] == "tenant_id"
        assert templates[0]["name"] == "customer_id"


# ---------------------------------------------------------------------------
# Tests — target_exists=false allows without name override
# ---------------------------------------------------------------------------


class TestTargetExistsFalseAllowsWithoutNameOverride:
    """target_exists=false tables should allow templates without --column-name."""

    def test_allows_add_without_name_override(
        self, templates_file: Path,
    ) -> None:
        """target_exists=false + no --column-name → success (new column created)."""
        config = _make_service_config(target_exists=False)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override=None,
                skip_validation=True,
            )

        assert result is True
        mock_save.assert_called_once()

    def test_allows_add_with_name_override(
        self, templates_file: Path,
    ) -> None:
        """target_exists=false + --column-name → also allowed."""
        config = _make_service_config(target_exists=False)
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.directory_user_name", "tenant_id",
                name_override="customer_id",
                skip_validation=True,
            )

        assert result is True
        mock_save.assert_called_once()


# ---------------------------------------------------------------------------
# Tests — missing target_exists defaults to false
# ---------------------------------------------------------------------------


class TestMissingTargetExistsDefaultsFalse:
    """When target_exists is not set, treat as false (table will be created)."""

    def test_missing_target_exists_allows_template(
        self, templates_file: Path,
    ) -> None:
        """No target_exists field → treated as false → allowed."""
        config: dict[str, Any] = {
            "service": "test_svc",
            "sinks": {
                "sink_asma.proxy": {
                    "tables": {
                        "public.some_table": {
                            "from": "public.source",
                            "replicate_structure": True,
                        },
                    },
                },
            },
        }
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc", "sink_asma.proxy",
                "public.some_table", "tenant_id",
                name_override=None,
                skip_validation=True,
            )

        assert result is True
        mock_save.assert_called_once()
