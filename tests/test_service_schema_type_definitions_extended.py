"""Additional unit tests for service schema type definitions."""

from pathlib import Path
from unittest.mock import Mock, patch

from cdc_generator.validators.manage_service_schema import type_definitions as defs


def test_load_type_definitions_missing_file_returns_none(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs.load_type_definitions("pgsql")

    assert result is None


def test_load_type_definitions_malformed_yaml_returns_none(tmp_path: Path) -> None:
    defs_dir = tmp_path / "service-schemas" / "definitions"
    defs_dir.mkdir(parents=True)
    (defs_dir / "pgsql.yaml").write_text("foo: bar\n")

    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs.load_type_definitions("pgsql")

    assert result is None


def test_get_all_type_names_empty_when_no_categories(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs.get_all_type_names("pgsql")

    assert result == []


@patch("cdc_generator.validators.manage_service_schema.type_definitions.yaml", None)
def test_save_definitions_file_without_yaml_returns_false(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs._save_definitions_file("pgsql", {"categories": {}})

    assert result is False


def test_save_definitions_file_writes_header_and_file(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        ok = defs._save_definitions_file(
            "pgsql",
            {"categories": {"numeric": {"types": ["int4"]}}},
            source_label="unit-test",
        )

    assert ok is True
    content = (tmp_path / "service-schemas" / "definitions" / "pgsql.yaml").read_text()
    assert "# Source: unit-test" in content
    assert "categories:" in content


@patch("cdc_generator.validators.manage_service_schema.type_definitions.yaml", None)
def test_load_existing_without_yaml_returns_none(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs._load_existing("pgsql")

    assert result is None


def test_load_existing_missing_file_returns_none(tmp_path: Path) -> None:
    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs._load_existing("pgsql")

    assert result is None


def test_load_existing_reads_file(tmp_path: Path) -> None:
    defs_dir = tmp_path / "service-schemas" / "definitions"
    defs_dir.mkdir(parents=True)
    (defs_dir / "pgsql.yaml").write_text("categories:\n  text:\n    types:\n      - text\n")

    with patch(
        "cdc_generator.validators.manage_service_schema.type_definitions.get_project_root",
        return_value=tmp_path,
    ):
        result = defs._load_existing("pgsql")

    assert isinstance(result, dict)
    assert "categories" in result


@patch("cdc_generator.validators.manage_service_schema.type_definitions._save_definitions_file")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._load_existing")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._introspect_postgres_types")
def test_generate_type_definitions_returns_false_when_introspection_fails(
    mock_introspect: Mock,
    mock_existing: Mock,
    mock_save: Mock,
) -> None:
    mock_introspect.return_value = None

    result = defs.generate_type_definitions(
        "postgres",
        {"host": "localhost", "port": 5432, "user": "u", "password": "p"},
    )

    assert result is False
    assert not mock_existing.called
    assert not mock_save.called


@patch("cdc_generator.validators.manage_service_schema.type_definitions._save_definitions_file")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._load_existing")
@patch("cdc_generator.validators.manage_service_schema.type_definitions._introspect_mssql_types")
def test_generate_type_definitions_returns_false_when_save_fails(
    mock_introspect: Mock,
    mock_existing: Mock,
    mock_save: Mock,
) -> None:
    mock_introspect.return_value = {"numeric": ["int"]}
    mock_existing.return_value = None
    mock_save.return_value = False

    result = defs.generate_type_definitions(
        "mssql",
        {"host": "localhost", "port": 1433, "user": "u", "password": "p"},
    )

    assert result is False


@patch("cdc_generator.validators.manage_service_schema.type_definitions.has_psycopg2", False)
def test_introspect_postgres_types_without_driver_returns_none() -> None:
    result = defs._introspect_postgres_types(
        {"host": "localhost", "port": 5432, "user": "u", "password": "p"},
    )

    assert result is None


@patch("cdc_generator.validators.manage_service_schema.type_definitions.create_postgres_connection")
@patch("cdc_generator.validators.manage_service_schema.type_definitions.has_psycopg2", True)
def test_introspect_postgres_types_categorizes_rows(mock_conn_factory: Mock) -> None:
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [
        ("uuid", "U"),
        ("jsonb", "U"),
        ("bytea", "U"),
        ("int4", "N"),
        ("text", "S"),
    ]
    mock_conn.cursor.return_value = mock_cursor
    mock_conn_factory.return_value = mock_conn

    result = defs._introspect_postgres_types(
        {
            "host": "localhost",
            "port": "5432",
            "user": "u",
            "password": "p",
            "database": "postgres",
        },
    )

    assert result is not None
    assert result["uuid"] == ["uuid"]
    assert result["json"] == ["jsonb"]
    assert result["binary"] == ["bytea"]
    assert "numeric" in result
    assert "text" in result


@patch("cdc_generator.validators.manage_service_schema.type_definitions.create_mssql_connection")
@patch("cdc_generator.validators.manage_service_schema.type_definitions.has_pymssql", True)
def test_introspect_mssql_types_categorizes_rows(mock_conn_factory: Mock) -> None:
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [("int",), ("nvarchar",), ("mystery_type",)]
    mock_conn.cursor.return_value = mock_cursor
    mock_conn_factory.return_value = mock_conn

    result = defs._introspect_mssql_types(
        {"host": "localhost", "port": 1433, "user": "u", "password": "p"},
    )

    assert result is not None
    assert "int" in result["numeric"]
    assert "nvarchar" in result["text"]
    assert "mystery_type" in result["other"]


@patch("cdc_generator.validators.manage_service_schema.type_definitions.has_pymssql", False)
def test_introspect_mssql_types_without_driver_returns_none() -> None:
    result = defs._introspect_mssql_types(
        {"host": "localhost", "port": 1433, "user": "u", "password": "p"},
    )

    assert result is None
