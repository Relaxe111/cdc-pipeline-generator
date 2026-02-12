"""Unit tests for manage-service-schema type definitions."""

from pathlib import Path
from unittest.mock import Mock, patch

from cdc_generator.validators.manage_service_schema.type_definitions import (
    _build_type_yaml,
    _db_type_to_engine,
    generate_type_definitions,
    get_all_type_names,
    load_type_definitions,
)


def test_db_type_to_engine_mapping() -> None:
    assert _db_type_to_engine("postgres") == "pgsql"
    assert _db_type_to_engine("mssql") == "mssql"
    assert _db_type_to_engine("oracle") == "oracle"


def test_build_type_yaml_preserves_manual_sections() -> None:
    categories = {"numeric": ["int4", "int8"], "text": ["text"]}
    existing = {
        "categories": {
            "numeric": {
                "types": ["int4"],
                "constraints": ["x > 0"],
            },
            "manual": {
                "types": ["custom_type"],
            },
        },
        "constraints": {"numeric": ["x > 0"]},
        "common_defaults": {"uuid": "gen_random_uuid()"},
    }

    result = _build_type_yaml(categories, existing)

    assert "categories" in result
    assert result["categories"]["numeric"]["constraints"] == ["x > 0"]
    assert "manual" in result["categories"]
    assert "constraints" in result
    assert "common_defaults" in result


def test_load_type_definitions_and_flatten(tmp_path: Path) -> None:
    defs_dir = tmp_path / "service-schemas" / "definitions"
    defs_dir.mkdir(parents=True)
    (defs_dir / "pgsql.yaml").write_text(
        "categories:\n"
        "  numeric:\n"
        "    types:\n"
        "      - int4\n"
        "      - int8\n"
        "  text:\n"
        "    - text\n",
    )

    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        categories = load_type_definitions("pgsql")
        all_types = get_all_type_names("pgsql")

    assert categories is not None
    assert categories["numeric"] == ["int4", "int8"]
    assert categories["text"] == ["text"]
    assert all_types == ["int4", "int8", "text"]


def test_generate_type_definitions_unsupported_db_type() -> None:
    result = generate_type_definitions("oracle", {})
    assert result is False


@patch("cdc_generator.validators.manage_service_schema.type_definitions._save_definitions_file")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._introspect_postgres_types")
def test_generate_type_definitions_postgres_success(
    mock_introspect: Mock,
    mock_save: Mock,
) -> None:
    mock_introspect.return_value = {"numeric": ["int4"], "text": ["text"]}
    mock_save.return_value = True

    result = generate_type_definitions(
        "postgres",
        {"host": "localhost", "port": 5432, "user": "u", "password": "p"},
        source_label="test-source",
    )

    assert result is True
    assert mock_introspect.called
    assert mock_save.called


@patch("cdc_generator.validators.manage_service_schema.type_definitions._save_definitions_file")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._introspect_mssql_types")
def test_generate_type_definitions_mssql_success(
    mock_introspect: Mock,
    mock_save: Mock,
) -> None:
    mock_introspect.return_value = {"numeric": ["int"], "text": ["nvarchar"]}
    mock_save.return_value = True

    result = generate_type_definitions(
        "mssql",
        {"host": "localhost", "port": 1433, "user": "u", "password": "p"},
        source_label="test-source",
    )

    assert result is True
    assert mock_introspect.called
    assert mock_save.called
