"""Tests for default_value extraction in schema saver."""

from unittest.mock import MagicMock, patch

from cdc_generator.validators.manage_service.schema_saver import (
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
