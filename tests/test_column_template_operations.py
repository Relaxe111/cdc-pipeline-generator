"""Tests for column_template_operations module - column templates and transforms."""

from pathlib import Path

import pytest  # type: ignore[import-not-found]

from cdc_generator.core.column_template_operations import (
    add_column_template,
    add_transform,
    list_column_templates,
    list_transforms,
    remove_column_template,
    remove_transform,
    resolve_column_templates,
    resolve_transforms,
)
from cdc_generator.core.column_templates import (
    clear_cache as clear_template_cache,
)
from cdc_generator.core.column_templates import (
    set_templates_path,
)
from cdc_generator.validators.manage_service.validation import (
    validate_service_sink_preflight,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_caches() -> None:  # type: ignore[misc]
    """Clear all caches before each test."""
    clear_template_cache()


@pytest.fixture()
def setup_templates(tmp_path: Path) -> Path:
    """Create a temporary column-templates.yaml."""
    content = """\
templates:
  source_table:
    name: _source_table
    type: text
    not_null: true
    description: Source table name
    value: meta("table")
  environment:
    name: _environment
    type: text
    not_null: true
    description: Deployment environment
    value: "${ENVIRONMENT}"
  tenant_id:
    name: _tenant_id
    type: text
    not_null: true
    description: Tenant identifier
    value: "${TENANT_ID}"
"""
    file_path = tmp_path / "column-templates.yaml"
    file_path.write_text(content)
    set_templates_path(file_path)
    return file_path


@pytest.fixture()
def setup_bloblang(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        """Create a temporary services/_bloblang tree with transform files."""
        monkeypatch.chdir(tmp_path)
        bloblang_dir = tmp_path / "services" / "_bloblang" / "examples"
        bloblang_dir.mkdir(parents=True)

        splitter = bloblang_dir / "user_class_splitter.blobl"
        splitter.write_text('root = if this.Patient == true { "Patient" } else { deleted() }\n')

        active = bloblang_dir / "active_filter.blobl"
        active.write_text("root = if this.is_active == true { this } else { deleted() }\n")

        return bloblang_dir


def _make_table_cfg() -> dict[str, object]:
    """Create a minimal sink table config for testing."""
    return {
        "target_exists": False,
        "from": "public.customer_user",
        "replicate_structure": True,
    }


# ---------------------------------------------------------------------------
# Extra column tests
# ---------------------------------------------------------------------------


class TestAddExtraColumn:
    """Tests for add_column_template."""

    def test_add_valid_template(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(table_cfg, "source_table")
        assert result is True
        assert "column_templates" in table_cfg
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert len(cols) == 1
        assert cols[0] == {"template": "source_table"}

    def test_add_with_name_override(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(table_cfg, "environment", "deploy_env")
        assert result is True
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert cols[0] == {"template": "environment", "name": "deploy_env"}

    def test_add_multiple(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        add_column_template(table_cfg, "environment")
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert len(cols) == 2

    def test_add_duplicate_fails(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        result = add_column_template(table_cfg, "source_table")
        assert result is False
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert len(cols) == 1  # No duplicate added

    def test_add_unknown_template_fails(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(table_cfg, "nonexistent")
        assert result is False
        assert "column_templates" not in table_cfg

    def test_add_with_value_override(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(
            table_cfg, "tenant_id", value_override='"hardcoded_tenant"',
        )
        assert result is True
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert cols[0] == {
            "template": "tenant_id",
            "value": '"hardcoded_tenant"',
        }

    def test_add_with_source_ref_value(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(
            table_cfg, "tenant_id",
            value_override="{asma.sources.*.customer_id}",
        )
        assert result is True
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert cols[0] == {
            "template": "tenant_id",
            "value": "{asma.sources.*.customer_id}",
        }

    def test_add_with_name_and_value_override(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_column_template(
            table_cfg, "tenant_id", "my_tenant",
            value_override="{asma.sources.*.customer_id}",
        )
        assert result is True
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert cols[0] == {
            "template": "tenant_id",
            "name": "my_tenant",
            "value": "{asma.sources.*.customer_id}",
        }


class TestRemoveExtraColumn:
    """Tests for remove_column_template."""

    def test_remove_existing(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        add_column_template(table_cfg, "environment")

        result = remove_column_template(table_cfg, "source_table")
        assert result is True
        cols = table_cfg["column_templates"]
        assert isinstance(cols, list)
        assert len(cols) == 1
        assert cols[0]["template"] == "environment"

    def test_remove_last_cleans_up(self, setup_templates: Path) -> None:
        """Removing last extra column removes the key entirely."""
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        remove_column_template(table_cfg, "source_table")
        assert "column_templates" not in table_cfg

    def test_remove_nonexistent_fails(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        result = remove_column_template(table_cfg, "nonexistent")
        assert result is False


class TestListExtraColumns:
    """Tests for list_column_templates."""

    def test_list_empty(self) -> None:
        table_cfg = _make_table_cfg()
        result = list_column_templates(table_cfg)
        assert result == []

    def test_list_populated(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        add_column_template(table_cfg, "environment")
        result = list_column_templates(table_cfg)
        assert result == ["source_table", "environment"]


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------


class TestAddTransform:
    """Tests for add_transform."""

    def test_add_valid_rule(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        ref = "file://services/_bloblang/examples/user_class_splitter.blobl"
        result = add_transform(table_cfg, ref)
        assert result is True
        assert "transforms" in table_cfg
        transforms = table_cfg["transforms"]
        assert isinstance(transforms, list)
        assert len(transforms) == 1
        assert transforms[0] == {"bloblang_ref": ref}

    def test_add_multiple(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        add_transform(table_cfg, "file://services/_bloblang/examples/user_class_splitter.blobl")
        add_transform(table_cfg, "file://services/_bloblang/examples/active_filter.blobl")
        transforms = table_cfg["transforms"]
        assert isinstance(transforms, list)
        assert len(transforms) == 2

    def test_add_duplicate_fails(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        ref = "file://services/_bloblang/examples/user_class_splitter.blobl"
        add_transform(table_cfg, ref)
        result = add_transform(table_cfg, ref)
        assert result is False

    def test_add_unknown_rule_fails(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        result = add_transform(table_cfg, "file://services/_bloblang/examples/nonexistent.blobl")
        assert result is False
        assert "transforms" not in table_cfg


class TestRemoveTransform:
    """Tests for remove_transform."""

    def test_remove_existing(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        splitter = "file://services/_bloblang/examples/user_class_splitter.blobl"
        active = "file://services/_bloblang/examples/active_filter.blobl"
        add_transform(table_cfg, splitter)
        add_transform(table_cfg, active)

        result = remove_transform(table_cfg, splitter)
        assert result is True
        transforms = table_cfg["transforms"]
        assert isinstance(transforms, list)
        assert len(transforms) == 1

    def test_remove_last_cleans_up(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        active = "file://services/_bloblang/examples/active_filter.blobl"
        add_transform(table_cfg, active)
        remove_transform(table_cfg, active)
        assert "transforms" not in table_cfg

    def test_remove_nonexistent_fails(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        result = remove_transform(table_cfg, "file://services/_bloblang/examples/nonexistent.blobl")
        assert result is False


class TestListTransforms:
    """Tests for list_transforms."""

    def test_list_empty(self) -> None:
        table_cfg = _make_table_cfg()
        result = list_transforms(table_cfg)
        assert result == []

    def test_list_populated(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        splitter = "file://services/_bloblang/examples/user_class_splitter.blobl"
        active = "file://services/_bloblang/examples/active_filter.blobl"
        add_transform(table_cfg, splitter)
        add_transform(table_cfg, active)
        result = list_transforms(table_cfg)
        assert result == [splitter, active]


# ---------------------------------------------------------------------------
# Resolution tests
# ---------------------------------------------------------------------------


class TestResolveExtraColumns:
    """Tests for resolve_column_templates."""

    def test_resolve_valid(self, setup_templates: Path) -> None:
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        add_column_template(table_cfg, "environment", "deploy_env")

        resolved = resolve_column_templates(table_cfg)
        assert len(resolved) == 2
        assert resolved[0].template_key == "source_table"
        assert resolved[0].name == "_source_table"
        assert resolved[0].value == 'meta("table")'
        assert resolved[0].template.column_type == "text"
        assert resolved[1].template_key == "environment"
        assert resolved[1].name == "deploy_env"  # name override
        assert resolved[1].value == "${ENVIRONMENT}"

    def test_resolve_empty(self) -> None:
        table_cfg = _make_table_cfg()
        resolved = resolve_column_templates(table_cfg)
        assert resolved == []

    def test_resolve_skips_invalid(self, setup_templates: Path) -> None:
        """Invalid template references are skipped."""
        table_cfg = _make_table_cfg()
        table_cfg["column_templates"] = [
            {"template": "source_table"},
            {"template": "nonexistent_template"},
        ]
        resolved = resolve_column_templates(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].template_key == "source_table"

    def test_resolve_with_value_override(self, setup_templates: Path) -> None:
        """Value override replaces the template default value."""
        table_cfg = _make_table_cfg()
        add_column_template(
            table_cfg, "tenant_id",
            value_override="{asma.sources.*.customer_id}",
        )
        resolved = resolve_column_templates(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].template_key == "tenant_id"
        assert resolved[0].name == "_tenant_id"
        assert resolved[0].value == "{asma.sources.*.customer_id}"

    def test_resolve_uses_template_default_value(self, setup_templates: Path) -> None:
        """Without value override, template default is used."""
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "tenant_id")
        resolved = resolve_column_templates(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].value == "${TENANT_ID}"

    def test_resolve_mixed_overrides(self, setup_templates: Path) -> None:
        """Mix of name and value overrides."""
        table_cfg = _make_table_cfg()
        add_column_template(table_cfg, "source_table")
        add_column_template(
            table_cfg, "tenant_id", "my_tenant",
            value_override='"literal_value"',
        )
        resolved = resolve_column_templates(table_cfg)
        assert len(resolved) == 2
        # First: no overrides
        assert resolved[0].name == "_source_table"
        assert resolved[0].value == 'meta("table")'
        # Second: both overrides
        assert resolved[1].name == "my_tenant"
        assert resolved[1].value == '"literal_value"'


class TestResolveTransforms:
    """Tests for resolve_transforms."""

    def test_resolve_valid(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        splitter = "file://services/_bloblang/examples/user_class_splitter.blobl"
        active = "file://services/_bloblang/examples/active_filter.blobl"
        add_transform(table_cfg, splitter)
        add_transform(table_cfg, active)

        resolved = resolve_transforms(table_cfg)
        assert len(resolved) == 2
        assert resolved[0].bloblang_ref == splitter
        assert "Patient" in resolved[0].bloblang
        assert resolved[1].bloblang_ref == active
        assert "is_active" in resolved[1].bloblang

    def test_resolve_empty(self) -> None:
        table_cfg = _make_table_cfg()
        resolved = resolve_transforms(table_cfg)
        assert resolved == []

    def test_resolve_skips_invalid(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        table_cfg["transforms"] = [
            {"bloblang_ref": "file://services/_bloblang/examples/active_filter.blobl"},
            {"bloblang_ref": "file://services/_bloblang/examples/nonexistent.blobl"},
        ]
        resolved = resolve_transforms(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].bloblang_ref == "file://services/_bloblang/examples/active_filter.blobl"

    def test_resolve_defaults_execution_stage_to_source(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        table_cfg["transforms"] = [
            {"bloblang_ref": "file://services/_bloblang/examples/active_filter.blobl"},
        ]
        resolved = resolve_transforms(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].execution_stage == "source"

    def test_resolve_preserves_sink_execution_stage(self, setup_bloblang: Path) -> None:
        table_cfg = _make_table_cfg()
        table_cfg["transforms"] = [
            {
                "bloblang_ref": "file://services/_bloblang/examples/active_filter.blobl",
                "execution_stage": "sink",
            },
        ]
        resolved = resolve_transforms(table_cfg)
        assert len(resolved) == 1
        assert resolved[0].execution_stage == "sink"


class TestUniqueTemplateValidation:
    """Tests for unique template scope validation in manage-service flow."""

    def _write_shared_files(self, tmp_path: Path) -> None:
        (tmp_path / "docker-compose.yml").write_text(
            "services:\n  dev:\n    image: busybox\n"
        )
        schemas_dir = tmp_path / "services" / "_schemas"
        schemas_dir.mkdir(parents=True)
        templates_path = schemas_dir / "column-templates.yaml"
        templates_path.write_text(
            "templates:\n"
            "  customer_id:\n"
            "    type: text\n"
            "    value_source: source_ref\n"
            "    value: '{adopus.sources.*.customer_id}'\n"
            "    unique: true\n"
        )
        set_templates_path(templates_path)
        (tmp_path / "services").mkdir(exist_ok=True)
        (tmp_path / "services" / "adopus.yaml").write_text(
            "adopus:\n"
            "  source:\n"
            "    tables:\n"
            "      dbo.Actor: {}\n"
            "  sinks:\n"
            "    sink_asma.directory:\n"
            "      tables:\n"
            "        public.actor:\n"
            "          target_exists: false\n"
            "          from: dbo.Actor\n"
            "          column_templates:\n"
            "            - template: customer_id\n"
        )

    def test_unique_collision_scoped_per_sink_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._write_shared_files(tmp_path)
        (tmp_path / "source-groups.yaml").write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  environment_aware: false\n"
            "  sources:\n"
            "    CustomerA:\n"
            "      schemas: [dbo]\n"
            "      default:\n"
            "        server: default\n"
            "        database: A\n"
            "        customer_id: dup\n"
            "        target_sink_env: dev\n"
            "    CustomerB:\n"
            "      schemas: [dbo]\n"
            "      default:\n"
            "        server: default\n"
            "        database: B\n"
            "        customer_id: dup\n"
            "        target_sink_env: dev\n"
        )
        (tmp_path / "sink-groups.yaml").write_text(
            "sink_asma:\n"
            "  environment_aware: true\n"
            "  sources:\n"
            "    directory:\n"
            "      schemas: [public]\n"
            "      dev:\n"
            "        server: default\n"
            "        database: directory_dev\n"
        )

        monkeypatch.chdir(tmp_path)
        errors, warnings = validate_service_sink_preflight("adopus")
        assert warnings == []
        assert any("unique template collision" in err for err in errors)

    def test_unique_allows_same_value_in_different_sink_envs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._write_shared_files(tmp_path)
        (tmp_path / "source-groups.yaml").write_text(
            "adopus:\n"
            "  pattern: db-per-tenant\n"
            "  environment_aware: false\n"
            "  sources:\n"
            "    CustomerA:\n"
            "      schemas: [dbo]\n"
            "      default:\n"
            "        server: default\n"
            "        database: A\n"
            "        customer_id: dup\n"
            "        target_sink_env: dev\n"
            "    CustomerB:\n"
            "      schemas: [dbo]\n"
            "      default:\n"
            "        server: default\n"
            "        database: B\n"
            "        customer_id: dup\n"
            "        target_sink_env: stage\n"
        )
        (tmp_path / "sink-groups.yaml").write_text(
            "sink_asma:\n"
            "  environment_aware: true\n"
            "  sources:\n"
            "    directory:\n"
            "      schemas: [public]\n"
            "      dev:\n"
            "        server: default\n"
            "        database: directory_dev\n"
            "      stage:\n"
            "        server: default\n"
            "        database: directory_stage\n"
        )

        monkeypatch.chdir(tmp_path)
        errors, _warnings = validate_service_sink_preflight("adopus")
        assert not any("unique template collision" in err for err in errors)
