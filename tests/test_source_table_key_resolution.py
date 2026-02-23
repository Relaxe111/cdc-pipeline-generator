"""Tests for source table key resolution in template validation.

When a sink table references a different source schema via its ``from``
field (e.g. sink key ``directory_replica.customers`` with
``from: public.customers``), validation must resolve the schema lookup
to the *source* table rather than the non-existent sink schema.
"""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from cdc_generator.core.column_templates import (
    clear_cache as clear_template_cache,
)
from cdc_generator.core.column_templates import (
    set_templates_path,
)
from cdc_generator.validators.manage_service.sink_template_ops import (
    _resolve_source_table_key,
    add_column_template_to_table,
    add_transform_to_table,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_caches() -> None: # type: ignore
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


@pytest.fixture()
def source_schema_dir(tmp_path: Path) -> Path:
    """Create service-schemas/ with a source table schema.

    Creates ``service-schemas/test_svc/public/customers.yaml``
    representing the source table that a sink table references
    via its ``from`` field.
    """
    schema_dir = tmp_path / "service-schemas" / "test_svc" / "public"
    schema_dir.mkdir(parents=True)
    schema_file = schema_dir / "customers.yaml"
    schema_file.write_text(
        "database: test_db\n"
        "schema: public\n"
        "service: test_svc\n"
        "table: customers\n"
        "columns:\n"
        "- name: id\n"
        "  type: uuid\n"
        "  nullable: false\n"
        "  primary_key: true\n"
        "- name: name\n"
        "  type: text\n"
        "  nullable: false\n"
        "  primary_key: false\n"
        "- name: subdomain\n"
        "  type: text\n"
        "  nullable: true\n"
        "  primary_key: false\n"
    )
    return tmp_path / "service-schemas"


def _make_service_config(
    *,
    target_exists: bool,
    table_key: str = "directory_replica.customers",
    from_table: str = "public.customers",
    replicate_structure: bool = True,
    sink_key: str = "sink_asma.proxy",
) -> dict[str, Any]:
    """Build a minimal service config dict for testing."""
    table_cfg: dict[str, Any] = {
        "target_exists": target_exists,
        "from": from_table,
    }
    if not target_exists:
        table_cfg["replicate_structure"] = replicate_structure
    else:
        table_cfg["columns"] = {"user_id": "user_id"}

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


def _patch_service_io(
    config: dict[str, Any],
) -> tuple[Any, Any]:
    """Patch load_service_config and save_service_config for testing."""
    _target = "cdc_generator.validators.manage_service.sink_template_ops"
    return (
        patch(f"{_target}.load_service_config", return_value=config),
        patch(f"{_target}.save_service_config", return_value=True),
    )


# ---------------------------------------------------------------------------
# Tests — _resolve_source_table_key helper
# ---------------------------------------------------------------------------


class TestResolveSourceTableKey:
    """Unit tests for _resolve_source_table_key()."""

    def test_returns_from_when_schemas_differ(self) -> None:
        """Sink schema != source schema → returns from value."""
        table_cfg: dict[str, object] = {
            "from": "public.customers",
            "target_exists": False,
        }
        result = _resolve_source_table_key(table_cfg, "directory_replica.customers")
        assert result == "public.customers"

    def test_returns_from_when_table_names_differ(self) -> None:
        """Same schema but different table names → returns from value."""
        table_cfg: dict[str, object] = {
            "from": "public.customer_user",
            "target_exists": True,
        }
        result = _resolve_source_table_key(table_cfg, "public.directory_user_name")
        assert result == "public.customer_user"

    def test_returns_none_when_from_equals_table_key(self) -> None:
        """from == table_key → returns None (no override needed)."""
        table_cfg: dict[str, object] = {
            "from": "public.customers",
            "target_exists": True,
        }
        result = _resolve_source_table_key(table_cfg, "public.customers")
        assert result is None

    def test_returns_none_when_no_from(self) -> None:
        """No 'from' field → returns None."""
        table_cfg: dict[str, object] = {
            "target_exists": False,
        }
        result = _resolve_source_table_key(table_cfg, "public.some_table")
        assert result is None

    def test_returns_none_when_from_is_empty(self) -> None:
        """Empty 'from' field → returns None."""
        table_cfg: dict[str, object] = {
            "from": "",
            "target_exists": False,
        }
        result = _resolve_source_table_key(table_cfg, "public.some_table")
        assert result is None

    def test_different_schema_prefix_returns_from(self) -> None:
        """dbo → public cross-schema → returns from value."""
        table_cfg: dict[str, object] = {
            "from": "dbo.users",
            "target_exists": False,
        }
        result = _resolve_source_table_key(table_cfg, "replica.users")
        assert result == "dbo.users"

    def test_same_table_different_schema(self) -> None:
        """Same table name but different schema → returns from value."""
        table_cfg: dict[str, object] = {
            "from": "logs.events",
            "target_exists": False,
        }
        result = _resolve_source_table_key(table_cfg, "archive.events")
        assert result == "logs.events"


# ---------------------------------------------------------------------------
# Tests — validation uses source_table_key for schema lookup
# ---------------------------------------------------------------------------


class TestValidationUsesSourceTableKey:
    """End-to-end: validation resolves the correct source schema."""

    def test_validates_against_source_schema(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """target_exists=false with from → validates via source table schema.

        The sink table is ``directory_replica.customers`` but the source
        schema lives under ``public/customers.yaml``.  Validation must
        resolve the ``from`` field and find the source schema.
        """
        config = _make_service_config(
            target_exists=False,
            table_key="directory_replica.customers",
            from_table="public.customers",
        )
        load_patch, save_patch = _patch_service_io(config)

        # Patch _find_schema_dir to point to our temp schemas
        with (
            load_patch,
            save_patch as mock_save,
            patch(
                "cdc_generator.core.structure_replicator._find_schema_dir",
                return_value=source_schema_dir,
            ),
        ):
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "directory_replica.customers",
                "tenant_id",
                name_override="customer_id",
            )

        assert result is True
        mock_save.assert_called_once()

    def test_fails_without_source_schema_file(
        self,
        templates_file: Path,
        tmp_path: Path,
    ) -> None:
        """When source schema file doesn't exist, validation fails cleanly."""
        config = _make_service_config(
            target_exists=False,
            table_key="directory_replica.customers",
            from_table="public.nonexistent",
        )
        # Empty service-schemas/ directory
        empty_schemas = tmp_path / "empty_schemas"
        empty_schemas.mkdir()

        load_patch, save_patch = _patch_service_io(config)

        with (
            load_patch,
            save_patch as mock_save,
            patch(
                "cdc_generator.core.structure_replicator._find_schema_dir",
                return_value=empty_schemas,
            ),
        ):
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "directory_replica.customers",
                "tenant_id",
            )

        assert result is False
        mock_save.assert_not_called()

    def test_same_schema_no_override(
        self,
        templates_file: Path,
        source_schema_dir: Path,
    ) -> None:
        """When from == table_key, no override needed."""
        config = _make_service_config(
            target_exists=False,
            table_key="public.customers",
            from_table="public.customers",
        )
        load_patch, save_patch = _patch_service_io(config)

        with (
            load_patch,
            save_patch as mock_save,
            patch(
                "cdc_generator.core.structure_replicator._find_schema_dir",
                return_value=source_schema_dir,
            ),
        ):
            # from == table_key → no source_table_key override
            # Schema lookup uses "public.customers" → loads public/customers.yaml
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "public.customers",
                "tenant_id",
            )

        assert result is True
        mock_save.assert_called_once()

    def test_skip_validation_bypasses_schema_lookup(
        self,
        templates_file: Path,
    ) -> None:
        """skip_validation=True bypasses schema lookup entirely."""
        config = _make_service_config(
            target_exists=False,
            table_key="directory_replica.customers",
            from_table="public.customers",
        )
        load_patch, save_patch = _patch_service_io(config)

        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "directory_replica.customers",
                "tenant_id",
                skip_validation=True,
            )

        assert result is True
        mock_save.assert_called_once()

    def test_validates_using_services_schemas_preferred_layout(
        self,
        templates_file: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Validation loads source schema from services/_schemas in from flow."""
        preferred_schema_dir = tmp_path / "services" / "_schemas" / "test_svc" / "public"
        preferred_schema_dir.mkdir(parents=True)
        (preferred_schema_dir / "customer_user.yaml").write_text(
            "database: test_db\n"
            "schema: public\n"
            "service: test_svc\n"
            "table: customer_user\n"
            "columns:\n"
            "- name: customer_id\n"
            "  type: uuid\n"
            "  nullable: false\n"
            "  primary_key: true\n"
            "- name: username\n"
            "  type: text\n"
            "  nullable: false\n"
            "  primary_key: false\n"
        )

        config = _make_service_config(
            target_exists=False,
            table_key="adopus-replica.customer_user",
            from_table="public.customer_user",
        )
        load_patch, save_patch = _patch_service_io(config)

        monkeypatch.chdir(tmp_path)
        with load_patch, save_patch as mock_save:
            result = add_column_template_to_table(
                "test_svc",
                "sink_asma.proxy",
                "adopus-replica.customer_user",
                "tenant_id",
                name_override="customer_id",
            )

        assert result is True
        mock_save.assert_called_once()


# ---------------------------------------------------------------------------
# Tests — transform validation also resolves source table key
# ---------------------------------------------------------------------------


class TestTransformValidationUsesSourceTableKey:
    """Transform validation should also resolve source table key from 'from' field."""

    def test_transform_validates_against_source_schema(
        self,
        source_schema_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Transform validation resolves 'from' field for schema lookup."""
        monkeypatch.chdir(tmp_path)
        bloblang_file = (
            tmp_path
            / "services"
            / "_bloblang"
            / "examples"
            / "uppercase_name.blobl"
        )
        bloblang_file.parent.mkdir(parents=True)
        bloblang_file.write_text('root._upper_name = this.name.uppercase()\n')

        config = _make_service_config(
            target_exists=False,
            table_key="directory_replica.customers",
            from_table="public.customers",
        )
        load_patch, save_patch = _patch_service_io(config)

        with (
            load_patch,
            save_patch as mock_save,
            patch(
                "cdc_generator.core.structure_replicator._find_schema_dir",
                return_value=source_schema_dir,
            ),
        ):
            result = add_transform_to_table(
                "test_svc",
                "sink_asma.proxy",
                "directory_replica.customers",
                "file://services/_bloblang/examples/uppercase_name.blobl",
            )

        assert result is True
        mock_save.assert_called_once()
