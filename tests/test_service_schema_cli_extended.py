"""Additional unit tests for ``manage-service-schema`` CLI handlers."""

from argparse import Namespace
from typing import Any
from unittest.mock import Mock, patch

from cdc_generator.cli.service_schema import (
    _dispatch_service_action,
    _handle_add_custom_table,
    _handle_list,
    _handle_list_services,
    _handle_remove,
    _handle_show,
)


def _ns(**kwargs: Any) -> Namespace:
    defaults: dict[str, Any] = {
        "service": "chat",
        "add_custom_table": None,
        "column": None,
        "show": None,
        "remove_custom_table": None,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


@patch(
    "cdc_generator.validators.manage_service_schema.custom_table_ops.list_services_with_schemas",
)
def test_handle_list_services_empty_returns_zero(mock_list: Mock) -> None:
    mock_list.return_value = []

    assert _handle_list_services() == 0


@patch(
    "cdc_generator.validators.manage_service_schema.custom_table_ops.list_services_with_schemas",
)
def test_handle_list_services_with_values_returns_zero(mock_list: Mock) -> None:
    mock_list.return_value = ["chat", "directory"]

    assert _handle_list_services() == 0


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.list_custom_tables")
def test_handle_list_no_tables_returns_zero(mock_list: Mock) -> None:
    mock_list.return_value = []

    assert _handle_list("chat") == 0


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.list_custom_tables")
def test_handle_list_with_tables_returns_zero(mock_list: Mock) -> None:
    mock_list.return_value = ["public.audit_log", "public.events"]

    assert _handle_list("chat") == 0


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.create_custom_table")
def test_handle_add_custom_table_create_failure_returns_one(mock_create: Mock) -> None:
    mock_create.return_value = False

    result = _handle_add_custom_table(
        "chat",
        "public.audit_log",
        ["id:uuid:pk"],
    )

    assert result == 1


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.create_custom_table")
def test_handle_add_custom_table_success_returns_zero(mock_create: Mock) -> None:
    mock_create.return_value = True

    result = _handle_add_custom_table(
        "chat",
        "public.audit_log",
        ["id:uuid:pk"],
    )

    assert result == 0


def test_handle_add_custom_table_requires_columns() -> None:
    result = _handle_add_custom_table("chat", "public.audit_log", None)

    assert result == 1


@patch("cdc_generator.cli.service_schema._print_table_details")
@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.show_custom_table")
def test_handle_show_success_prints_and_returns_zero(
    mock_show: Mock,
    mock_print: Mock,
) -> None:
    mock_show.return_value = {
        "schema": "public",
        "table": "audit_log",
        "service": "chat",
        "columns": [],
    }

    result = _handle_show("chat", "public.audit_log")

    assert result == 0
    assert mock_print.called


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.show_custom_table")
def test_handle_show_missing_returns_one(mock_show: Mock) -> None:
    mock_show.return_value = None

    assert _handle_show("chat", "public.missing") == 1


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.remove_custom_table")
def test_handle_remove_propagates_success(mock_remove: Mock) -> None:
    mock_remove.return_value = True

    assert _handle_remove("chat", "public.audit_log") == 0


@patch("cdc_generator.validators.manage_service_schema.custom_table_ops.remove_custom_table")
def test_handle_remove_propagates_failure(mock_remove: Mock) -> None:
    mock_remove.return_value = False

    assert _handle_remove("chat", "public.audit_log") == 1


@patch("cdc_generator.cli.service_schema._handle_add_custom_table")
def test_dispatch_service_action_prefers_add_custom_table(mock_add: Mock) -> None:
    mock_add.return_value = 0

    result = _dispatch_service_action(
        _ns(add_custom_table="public.audit_log", column=["id:uuid:pk"]),
    )

    assert result == 0
    mock_add.assert_called_once_with("chat", "public.audit_log", ["id:uuid:pk"])


@patch("cdc_generator.cli.service_schema._handle_show")
def test_dispatch_service_action_show_path(mock_show: Mock) -> None:
    mock_show.return_value = 0

    result = _dispatch_service_action(_ns(show="public.audit_log"))

    assert result == 0
    mock_show.assert_called_once_with("chat", "public.audit_log")


@patch("cdc_generator.cli.service_schema._handle_remove")
def test_dispatch_service_action_remove_path(mock_remove: Mock) -> None:
    mock_remove.return_value = 0

    result = _dispatch_service_action(_ns(remove_custom_table="public.audit_log"))

    assert result == 0
    mock_remove.assert_called_once_with("chat", "public.audit_log")
