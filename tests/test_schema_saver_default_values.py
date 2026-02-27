"""Tests for schema saver: default values and output paths."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from cdc_generator.validators.manage_service.schema_saver import (
    _save_tables_to_yaml,
    save_detailed_schema_mssql,
    save_detailed_schema_postgres,
)


@patch("cdc_generator.validators.manage_service.schema_saver.has_psycopg2", True)
def test_save_detailed_schema_postgres_includes_default_value() -> None:
    """PostgreSQL schema save should include column_default as default_value."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "column_name": "id",
            "data_type": "integer",
            "character_maximum_length": None,
            "is_nullable": "NO",
            "column_default": "nextval('public.users_id_seq'::regclass)",
            "is_primary_key": True,
        },
        {
            "column_name": "created_at",
            "data_type": "timestamp without time zone",
            "character_maximum_length": None,
            "is_nullable": "YES",
            "column_default": None,
            "is_primary_key": False,
        },
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_pg = MagicMock()
    mock_pg.extras.RealDictCursor = object()
    mock_pg.connect.return_value = mock_conn

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.ensure_psycopg2",
        return_value=mock_pg,
    ):
        result = save_detailed_schema_postgres(
            service="directory",
            schema="public",
            tables=[{"TABLE_SCHEMA": "public", "TABLE_NAME": "users"}],
            conn_params={
                "host": "localhost",
                "port": 5432,
                "database": "directory_dev",
                "user": "postgres",
                "password": "postgres",
            },
        )

    users = result["users"]
    assert users["columns"][0]["default_value"] == "nextval('public.users_id_seq'::regclass)"
    assert users["columns"][1]["default_value"] is None


@patch("cdc_generator.validators.manage_service.schema_saver.has_pymssql", True)
def test_save_detailed_schema_mssql_includes_default_value() -> None:
    """MSSQL schema save should include COLUMN_DEFAULT as default_value."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("Id", "int", None, "NO", "((0))", 1),
        ("Name", "nvarchar", 255, "YES", None, 0),
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.create_mssql_connection",
        return_value=mock_conn,
    ):
        result = save_detailed_schema_mssql(
            service="directory",
            schema="dbo",
            tables=[{"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Actor"}],
            conn_params={
                "host": "localhost",
                "port": 1433,
                "database": "directory_dev",
                "user": "sa",
                "password": "secret",
            },
        )

    actor = result["Actor"]
    assert actor["columns"][0]["default_value"] == "((0))"
    assert actor["columns"][1]["default_value"] is None


@patch("cdc_generator.validators.manage_service.schema_saver.has_pymssql", True)
def test_save_detailed_schema_mssql_deduplicates_duplicate_rows() -> None:
    """MSSQL schema save should deduplicate repeated column rows from inspector query."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("Id", "int", None, "NO", None, 1),
        ("Id", "int", None, "NO", None, 1),
        ("Name", "nvarchar", 255, "YES", None, 0),
    ]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.create_mssql_connection",
        return_value=mock_conn,
    ):
        result = save_detailed_schema_mssql(
            service="directory",
            schema="dbo",
            tables=[{"TABLE_SCHEMA": "dbo", "TABLE_NAME": "Actor"}],
            conn_params={
                "host": "localhost",
                "port": 1433,
                "database": "directory_dev",
                "user": "sa",
                "password": "secret",
            },
        )

    actor = result["Actor"]
    assert [col["name"] for col in actor["columns"]] == ["Id", "Name"]
    assert actor["primary_key"] == "Id"


# ---------------------------------------------------------------------------
# Output path tests (merged from test_schema_saver_paths.py)
# ---------------------------------------------------------------------------


def test_save_tables_to_yaml_writes_to_services_schemas(tmp_path: Path) -> None:
    """Schema saver should write under services/_schemas, not service-schemas."""
    tables_data = {
        "Actor": {
            "database": "directory_dev",
            "schema": "adopus",
            "service": "directory",
            "table": "Actor",
            "columns": [{"name": "id", "type": "int", "nullable": False, "primary_key": True}],
            "primary_key": "id",
        },
    }

    write_dir = tmp_path / "services" / "_schemas" / "directory"
    legacy_dir = tmp_path / "service-schemas" / "directory"

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.get_service_schema_write_dir",
        return_value=write_dir,
    ):
        ok = _save_tables_to_yaml("directory", tables_data)

    assert ok is True

    output_file = write_dir / "adopus" / "Actor.yaml"
    assert output_file.exists()
    assert not (legacy_dir / "adopus" / "Actor.yaml").exists()

    saved = yaml.safe_load(output_file.read_text())
    assert saved["service"] == "directory"
    assert saved["schema"] == "adopus"
    assert saved["table"] == "Actor"
