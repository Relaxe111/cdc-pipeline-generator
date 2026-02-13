"""Additional unit tests for custom table schema operations."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cdc_generator.validators.manage_service_schema import custom_table_ops as ops


def test_parse_column_spec_empty_name_returns_none() -> None:
    assert ops.parse_column_spec(":uuid") is None


def test_parse_column_spec_empty_type_returns_none() -> None:
    assert ops.parse_column_spec("id:") is None


def test_parse_column_spec_default_alias_uuid() -> None:
    col = ops.parse_column_spec("id:uuid:default_uuid")

    assert col is not None
    assert col["default"] == "gen_random_uuid()"


def test_parse_column_spec_pk_nullable_conflict_returns_none() -> None:
    assert ops.parse_column_spec("id:uuid:pk:nullable") is None


def test_parse_column_spec_not_null_nullable_conflict_returns_none() -> None:
    assert ops.parse_column_spec("id:text:not_null:nullable") is None


@patch("cdc_generator.validators.manage_service_schema.type_definitions.get_all_type_names")
def test_try_type_definitions_check_with_no_types_warns(mock_get_types: Mock) -> None:
    mock_get_types.return_value = []

    ops._try_type_definitions_check("mystery_type")


@patch("cdc_generator.validators.manage_service_schema.type_definitions.get_all_type_names")
def test_try_type_definitions_check_with_missing_type_warns(mock_get_types: Mock) -> None:
    mock_get_types.return_value = ["uuid", "text"]

    ops._try_type_definitions_check("mystery_type")


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.yaml", None)
def test_create_custom_table_without_yaml_returns_false(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        result = ops.create_custom_table("chat", "public.audit_log", ["id:uuid:pk"])

    assert result is False


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.parse_column_spec")
def test_create_custom_table_parse_failure_returns_false(mock_parse: Mock, tmp_path: Path) -> None:
    mock_parse.return_value = None

    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        result = ops.create_custom_table("chat", "public.audit_log", ["id:uuid:pk"])

    assert result is False


def test_create_custom_table_multiple_primary_keys_stored_as_list(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        created = ops.create_custom_table(
            "chat",
            "public.audit_log",
            ["id:uuid:pk", "tenant_id:uuid:pk", "event_type:text:not_null"],
        )
        assert created is True

        data = ops.show_custom_table("chat", "public.audit_log")

    assert data is not None
    assert data["primary_key"] == ["id", "tenant_id"]


def test_remove_custom_table_removes_empty_directory(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        created = ops.create_custom_table("chat", "public.audit_log", ["id:uuid:pk"])
        assert created is True

        removed = ops.remove_custom_table("chat", "public.audit_log")

    assert removed is True
    custom_dir = tmp_path / "service-schemas" / "chat" / "custom-tables"
    assert not custom_dir.exists()


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.yaml", None)
def test_show_custom_table_without_yaml_returns_none(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        result = ops.show_custom_table("chat", "public.audit_log")

    assert result is None


def test_show_custom_table_non_dict_yaml_returns_none(tmp_path: Path) -> None:
    custom_dir = tmp_path / "service-schemas" / "chat" / "custom-tables"
    custom_dir.mkdir(parents=True)
    (custom_dir / "public.audit_log.yaml").write_text("- not\n- a\n- dict\n")

    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        result = ops.show_custom_table("chat", "public.audit_log")

    assert result is None


def test_get_custom_table_columns_missing_columns_returns_empty(tmp_path: Path) -> None:
    custom_dir = tmp_path / "service-schemas" / "chat" / "custom-tables"
    custom_dir.mkdir(parents=True)
    (custom_dir / "public.audit_log.yaml").write_text(
        "schema: public\n"
        "service: chat\n"
        "table: audit_log\n",
    )

    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        cols = ops.get_custom_table_columns("chat", "public.audit_log")

    assert cols == []


def test_list_custom_tables_returns_empty_when_dir_missing(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        tables = ops.list_custom_tables("chat")

    assert tables == []


def test_list_services_with_schemas_returns_empty_when_root_missing(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.custom_table_ops.get_project_root",
        return_value=tmp_path,
    ):
        services = ops.list_services_with_schemas()

    assert services == []


@pytest.mark.parametrize(
    "table_ref,expected",
    [
        ("public.audit_log", ("public", "audit_log")),
        ("audit_log", None),
        ("public.", None),
        (".audit_log", None),
    ],
)
def test_parse_table_ref_variants(
    table_ref: str,
    expected: tuple[str, str] | None,
) -> None:
    assert ops._parse_table_ref(table_ref) == expected
