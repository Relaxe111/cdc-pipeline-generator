"""Unit tests for custom table operations in manage-service-schema."""

from pathlib import Path
from unittest.mock import patch

from cdc_generator.validators.manage_service_schema.custom_table_ops import (
    create_custom_table,
    get_custom_table_columns,
    list_custom_tables,
    list_services_with_schemas,
    parse_column_spec,
    remove_custom_table,
    show_custom_table,
)


def test_parse_column_spec_valid_pk_not_null_default() -> None:
    col = parse_column_spec("id:uuid:pk:not_null:default_now")
    assert col is not None
    assert col["name"] == "id"
    assert col["type"] == "uuid"
    assert col["primary_key"] is True
    assert col["nullable"] is False
    assert col["default"] == "now()"


def test_parse_column_spec_invalid_returns_none() -> None:
    assert parse_column_spec("id") is None


def test_parse_column_spec_unknown_modifier_is_ignored() -> None:
    col = parse_column_spec("name:text:unknown_mod")
    assert col is not None
    assert col["name"] == "name"


def test_create_show_list_get_columns_remove_custom_table(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        created = create_custom_table(
            "chat",
            "public.audit_log",
            ["id:uuid:pk", "event_type:text:not_null"],
        )
        assert created is True

        tables = list_custom_tables("chat")
        assert tables == ["public.audit_log"]

        data = show_custom_table("chat", "public.audit_log")
        assert data is not None
        assert data["service"] == "chat"
        assert data["table"] == "audit_log"

        columns = get_custom_table_columns("chat", "public.audit_log")
        assert columns == ["id", "event_type"]

        removed = remove_custom_table("chat", "public.audit_log")
        assert removed is True


def test_create_duplicate_custom_table_fails(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        assert create_custom_table(
            "chat",
            "public.audit_log",
            ["id:uuid:pk"],
        ) is True
        assert create_custom_table(
            "chat",
            "public.audit_log",
            ["id:uuid:pk"],
        ) is False


def test_create_invalid_table_ref_fails(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        assert create_custom_table(
            "chat",
            "audit_log",
            ["id:uuid:pk"],
        ) is False


def test_remove_missing_custom_table_fails(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        assert remove_custom_table("chat", "public.ghost") is False


def test_list_services_with_schemas_skips_special_dirs(tmp_path: Path) -> None:
    schemas_root = tmp_path / "service-schemas"
    (schemas_root / "chat").mkdir(parents=True)
    (schemas_root / "directory").mkdir()
    (schemas_root / "definitions").mkdir()
    (schemas_root / "bloblang").mkdir()
    (schemas_root / ".hidden").mkdir()
    (schemas_root / "_private").mkdir()

    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        services = list_services_with_schemas()

    assert services == ["chat", "directory"]


def test_show_custom_table_missing_returns_none(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        data = show_custom_table("chat", "public.audit_log")

    assert data is None
