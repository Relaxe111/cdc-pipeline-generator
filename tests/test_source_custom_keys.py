"""Tests for SQL source custom key helpers."""

from unittest.mock import MagicMock, patch

from cdc_generator.helpers.source_custom_keys import (
    execute_source_custom_keys,
    normalize_source_custom_keys,
)


def test_normalize_source_custom_keys_supports_mapping_and_legacy_string() -> None:
    raw = {
        "customer_id": {"exec_type": "sql", "value": "SELECT 1"},
        "tenant_key": "SELECT 2",
    }

    normalized = normalize_source_custom_keys(raw)

    assert normalized == {
        "customer_id": "SELECT 1",
        "tenant_key": "SELECT 2",
    }


@patch("cdc_generator.helpers.source_custom_keys._run_sql_postgres")
def test_execute_source_custom_keys_applies_values_to_database_entries(
    mock_run_sql_postgres: MagicMock,
) -> None:
    mock_run_sql_postgres.return_value = "cust-001"

    databases = [
        {
            "name": "db1",
            "service": "directory",
            "environment": "nonprod",
            "schemas": ["public"],
            "table_count": 1,
            "server": "default",
            "customer": "",
        }
    ]

    execute_source_custom_keys(
        databases,
        db_type="postgres",
        server_name="default",
        server_config={
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "secret",
        },
        source_custom_keys={"customer_id": "SELECT 'cust-001'"},
        context_label="test",
    )

    assert databases[0]["source_custom_values"]["customer_id"] == "cust-001"


@patch("cdc_generator.helpers.source_custom_keys._run_sql_postgres")
def test_execute_source_custom_keys_sets_null_when_value_missing(
    mock_run_sql_postgres: MagicMock,
) -> None:
    mock_run_sql_postgres.return_value = None

    databases = [
        {
            "name": "db1",
            "service": "directory",
            "environment": "nonprod",
            "schemas": ["public"],
            "table_count": 1,
            "server": "default",
            "customer": "",
        }
    ]

    warnings = execute_source_custom_keys(
        databases,
        db_type="postgres",
        server_name="default",
        server_config={
            "host": "localhost",
            "port": "5432",
            "user": "postgres",
            "password": "secret",
        },
        source_custom_keys={"customer_id": "SELECT 'cust-001'"},
        context_label="test",
    )

    assert databases[0]["source_custom_values"]["customer_id"] is None
    assert any("Missing value" in warning for warning in warnings)
