"""Tests for column_template_definitions module — template CRUD operations."""

from pathlib import Path

import pytest  # type: ignore[import-not-found]

from cdc_generator.core.column_template_definitions import (
    add_template_definition,
    edit_template_definition,
    list_template_definitions,
    remove_template_definition,
    show_template_definition,
    validate_column_type,
    validate_template_key,
)
from cdc_generator.core.column_templates import (
    clear_cache,
    get_template,
    list_template_keys,
    set_templates_path,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_caches() -> None:  # type: ignore[misc]
    """Clear caches before each test."""
    clear_cache()


@pytest.fixture()
def templates_file(tmp_path: Path) -> Path:
    """Create a minimal column-templates.yaml for testing."""
    content = """\
templates:
  source_table:
    name: _source_table
    type: text
    not_null: true
    description: Source table name
    value: meta("table")
    value_source: bloblang
  environment:
    name: _environment
    type: text
    not_null: true
    description: Deployment environment
    value: "${ENVIRONMENT}"
    value_source: bloblang
"""
    path = tmp_path / "service-schemas" / "column-templates.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    set_templates_path(path)
    return path


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestValidateTemplateKey:
    """Tests for validate_template_key."""

    def test_valid_simple(self) -> None:
        assert validate_template_key("tenant_id") is None

    def test_valid_with_hyphens(self) -> None:
        assert validate_template_key("my-template") is None

    def test_empty_key(self) -> None:
        result = validate_template_key("")
        assert result is not None
        assert "empty" in result.lower()

    def test_starts_with_digit(self) -> None:
        result = validate_template_key("1bad")
        assert result is not None
        assert "digit" in result.lower()

    def test_special_characters(self) -> None:
        result = validate_template_key("bad!key")
        assert result is not None
        assert "invalid" in result.lower()


class TestValidateColumnType:
    """Tests for validate_column_type."""

    def test_valid_text(self) -> None:
        assert validate_column_type("text") is None

    def test_valid_timestamptz(self) -> None:
        assert validate_column_type("timestamptz") is None

    def test_valid_uuid(self) -> None:
        assert validate_column_type("uuid") is None

    def test_valid_parameterized(self) -> None:
        assert validate_column_type("varchar(255)") is None

    def test_invalid_type(self) -> None:
        result = validate_column_type("invalid_type")
        assert result is not None
        assert "Unknown" in result


# ---------------------------------------------------------------------------
# Add template tests
# ---------------------------------------------------------------------------


class TestAddTemplateDefinition:
    """Tests for add_template_definition."""

    def test_add_new_template(self, templates_file: Path) -> None:
        result = add_template_definition(
            key="tenant_id",
            name="_tenant_id",
            col_type="text",
            value="${TENANT_ID}",
            description="Tenant identifier",
            not_null=True,
        )
        assert result is True

        # Verify it was persisted
        clear_cache()
        template = get_template("tenant_id")
        assert template is not None
        assert template.name == "_tenant_id"
        assert template.column_type == "text"
        assert template.not_null is True
        assert template.value == "${TENANT_ID}"
        assert template.value_source == "env"

    def test_add_with_default(self, templates_file: Path) -> None:
        result = add_template_definition(
            key="sync_ts",
            name="_synced_at",
            col_type="timestamptz",
            value="now()",
            default="now()",
            not_null=True,
            value_source="sql",
        )
        assert result is True

        clear_cache()
        template = get_template("sync_ts")
        assert template is not None
        assert template.default == "now()"
        assert template.value_source == "sql"

    def test_add_duplicate_fails(self, templates_file: Path) -> None:
        """Cannot add a template with an existing key."""
        result = add_template_definition(
            key="source_table",  # already exists
            name="_src",
            col_type="text",
            value="meta('table')",
        )
        assert result is False

    def test_add_invalid_key_fails(self, templates_file: Path) -> None:
        result = add_template_definition(
            key="1bad",
            name="_bad",
            col_type="text",
            value="x",
        )
        assert result is False

    def test_add_invalid_type_fails(self, templates_file: Path) -> None:
        result = add_template_definition(
            key="new_one",
            name="_new",
            col_type="invalid_type",
            value="x",
        )
        assert result is False

    def test_add_shows_up_in_list(self, templates_file: Path) -> None:
        add_template_definition(
            key="zzz_last",
            name="_zzz",
            col_type="text",
            value="x",
        )
        clear_cache()
        keys = list_template_keys()
        assert "zzz_last" in keys


# ---------------------------------------------------------------------------
# Remove template tests
# ---------------------------------------------------------------------------


class TestRemoveTemplateDefinition:
    """Tests for remove_template_definition."""

    def test_remove_existing(self, templates_file: Path) -> None:
        result = remove_template_definition("source_table")
        assert result is True

        clear_cache()
        assert get_template("source_table") is None

    def test_remove_nonexistent_fails(self, templates_file: Path) -> None:
        result = remove_template_definition("nonexistent")
        assert result is False

    def test_remove_preserves_others(self, templates_file: Path) -> None:
        remove_template_definition("source_table")
        clear_cache()
        assert get_template("environment") is not None


# ---------------------------------------------------------------------------
# Edit template tests
# ---------------------------------------------------------------------------


class TestEditTemplateDefinition:
    """Tests for edit_template_definition."""

    def test_edit_value(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment",
            value="{asma.sources.*.customer_id}",
            value_source="source_ref",
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.value == "{asma.sources.*.customer_id}"
        assert template.value_source == "source_ref"

    def test_edit_value_sql(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment",
            value="current_timestamp",
            value_source="sql",
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.value == "current_timestamp"
        assert template.value_source == "sql"

    def test_edit_value_env(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment",
            value="${MY_ENV_VAR}",
            value_source="env",
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.value == "${MY_ENV_VAR}"
        assert template.value_source == "env"

    def test_edit_value_bloblang(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment",
            value="this.customer_id",
            value_source="bloblang",
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.value == "this.customer_id"
        assert template.value_source == "bloblang"

    def test_edit_name(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment", name="_deploy_env",
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.name == "_deploy_env"

    def test_edit_multiple_fields(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment",
            name="_env",
            col_type="varchar",
            not_null=False,
        )
        assert result is True

        clear_cache()
        template = get_template("environment")
        assert template is not None
        assert template.name == "_env"
        assert template.column_type == "varchar"
        assert template.not_null is False

    def test_edit_nonexistent_fails(self, templates_file: Path) -> None:
        result = edit_template_definition("nonexistent", value="x")
        assert result is False

    def test_edit_no_changes_fails(self, templates_file: Path) -> None:
        result = edit_template_definition("environment")
        assert result is False

    def test_edit_invalid_type_fails(self, templates_file: Path) -> None:
        result = edit_template_definition(
            "environment", col_type="invalid",
        )
        assert result is False


# ---------------------------------------------------------------------------
# Show / List tests
# ---------------------------------------------------------------------------


class TestShowTemplateDefinition:
    """Tests for show_template_definition."""

    def test_show_existing(self, templates_file: Path) -> None:
        template = show_template_definition("source_table")
        assert template is not None
        assert template.key == "source_table"
        assert template.name == "_source_table"

    def test_show_nonexistent(self, templates_file: Path) -> None:
        template = show_template_definition("nonexistent")
        assert template is None


class TestListTemplateDefinitions:
    """Tests for list_template_definitions."""

    def test_list_all(self, templates_file: Path) -> None:
        templates = list_template_definitions()
        assert len(templates) == 2
        keys = [t.key for t in templates]
        assert "environment" in keys
        assert "source_table" in keys

    def test_list_sorted(self, templates_file: Path) -> None:
        templates = list_template_definitions()
        keys = [t.key for t in templates]
        assert keys == sorted(keys)

    def test_list_after_add(self, templates_file: Path) -> None:
        add_template_definition(
            key="new_one", name="_new", col_type="text", value="x",
        )
        clear_cache()
        templates = list_template_definitions()
        assert len(templates) == 3
