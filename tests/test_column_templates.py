"""Tests for column templates loader."""

from pathlib import Path

import pytest
from cdc_generator.core import column_templates as column_templates_module

from cdc_generator.core.column_templates import (
    _parse_single_template,
    clear_cache,
    get_template,
    get_templates,
    list_template_keys,
    set_templates_path,
    validate_template_reference,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Clear template cache before each test."""
    clear_cache()


@pytest.fixture()
def templates_file(tmp_path: Path) -> Path:
    """Create a temporary column-templates.yaml for testing."""
    content = """\
templates:
  source_table:
    name: _source_table
    type: text
    not_null: true
    description: Source table name
    value: meta("table")
  sync_timestamp:
    name: _synced_at
    type: timestamptz
    not_null: true
    description: When the row was synced
    default: now()
    value: now()
  environment:
    name: _environment
    type: text
    not_null: true
    description: Deployment environment
    value: "${ENVIRONMENT}"
"""
    file_path = tmp_path / "column-templates.yaml"
    file_path.write_text(content)
    set_templates_path(file_path)
    return file_path


# ---------------------------------------------------------------------------
# Unit tests: _parse_single_template
# ---------------------------------------------------------------------------


class TestParseSingleTemplate:
    """Tests for parsing individual template entries."""

    def test_valid_template(self) -> None:
        """Parse a valid template with all fields."""
        raw = {
            "name": "_source_table",
            "type": "text",
            "not_null": True,
            "description": "Source table name",
            "value": 'meta("table")',
            "default": None,
        }
        result = _parse_single_template("source_table", raw)
        assert result is not None
        assert result.key == "source_table"
        assert result.name == "_source_table"
        assert result.column_type == "text"
        assert result.not_null is True
        assert result.value == 'meta("table")'
        assert result.default is None

    def test_valid_template_with_default(self) -> None:
        """Parse template with SQL default."""
        raw = {
            "name": "_synced_at",
            "type": "timestamptz",
            "value": "now()",
            "default": "now()",
        }
        result = _parse_single_template("sync", raw)
        assert result is not None
        assert result.default == "now()"

    def test_missing_required_field(self) -> None:
        """Return None for template missing required field."""
        raw = {"name": "_col", "type": "text"}  # missing 'value'
        result = _parse_single_template("bad", raw)
        assert result is None

    def test_not_a_dict(self) -> None:
        """Return None for non-dict input."""
        result = _parse_single_template("bad", "string")
        assert result is None

    def test_non_string_fields(self) -> None:
        """Return None if required string fields are not strings."""
        raw = {"name": 123, "type": "text", "value": "x"}
        result = _parse_single_template("bad", raw)
        assert result is None

    def test_frozen_dataclass(self) -> None:
        """Templates should be immutable (frozen)."""
        raw = {
            "name": "_col",
            "type": "text",
            "value": "x",
        }
        result = _parse_single_template("test", raw)
        assert result is not None
        with pytest.raises(AttributeError):
            result.name = "changed"  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# Integration tests: loading from file
# ---------------------------------------------------------------------------


class TestGetTemplates:
    """Tests for loading templates from YAML file."""

    def test_load_all_templates(self, templates_file: Path) -> None:
        """Load all templates from file."""
        templates = get_templates()
        expected_template_count = 3
        assert len(templates) == expected_template_count
        assert "source_table" in templates
        assert "sync_timestamp" in templates
        assert "environment" in templates

    def test_get_template_found(self, templates_file: Path) -> None:
        """Get a single template by key."""
        tpl = get_template("source_table")
        assert tpl is not None
        assert tpl.name == "_source_table"
        assert tpl.column_type == "text"

    def test_get_template_not_found(self, templates_file: Path) -> None:
        """Return None for unknown template."""
        tpl = get_template("nonexistent")
        assert tpl is None

    def test_list_keys(self, templates_file: Path) -> None:
        """List template keys sorted."""
        keys = list_template_keys()
        assert keys == ["environment", "source_table", "sync_timestamp"]

    def test_validate_valid(self, templates_file: Path) -> None:
        """Valid template reference returns None."""
        assert validate_template_reference("source_table") is None

    def test_validate_invalid(self, templates_file: Path) -> None:
        """Invalid template reference returns error message."""
        error = validate_template_reference("nonexistent")
        assert error is not None
        assert "nonexistent" in error
        assert "Available templates" in error

    def test_caching(self, templates_file: Path) -> None:
        """Templates are cached after first load."""
        t1 = get_templates()
        t2 = get_templates()
        assert t1 is t2  # Same object (cached)

    def test_file_not_found(self, tmp_path: Path) -> None:
        """Handle missing templates file gracefully."""
        set_templates_path(tmp_path / "nonexistent.yaml")
        templates = get_templates()
        assert templates == {}

    def test_missing_root_key(self, tmp_path: Path) -> None:
        """Handle YAML without 'templates' key."""
        file_path = tmp_path / "bad.yaml"
        file_path.write_text("other_key: value\n")
        set_templates_path(file_path)
        templates = get_templates()
        assert templates == {}

    def test_loads_from_services_schemas_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Default loader reads services/_schemas/column-templates.yaml."""
        set_templates_path(tmp_path / "nonexistent.yaml")
        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        clear_cache()

        schemas_dir = tmp_path / "services" / "_schemas"
        schemas_dir.mkdir(parents=True)
        (schemas_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  source_table:\n"
            "    name: _source_table\n"
            "    type: text\n"
            "    value: meta(\"table\")\n"
        )
        monkeypatch.chdir(tmp_path)

        keys = list_template_keys()
        assert "source_table" in keys

    def test_falls_back_to_legacy_service_schemas(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Loader falls back to service-schemas/column-templates.yaml."""
        set_templates_path(tmp_path / "nonexistent.yaml")
        monkeypatch.setattr(column_templates_module, "_templates_file", None)
        clear_cache()

        legacy_dir = tmp_path / "service-schemas"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "column-templates.yaml").write_text(
            "templates:\n"
            "  environment:\n"
            "    name: _environment\n"
            "    type: text\n"
            "    value: \"${ENVIRONMENT}\"\n"
        )
        monkeypatch.chdir(tmp_path)

        keys = list_template_keys()
        assert "environment" in keys
