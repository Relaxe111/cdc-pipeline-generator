"""Tests for column templates CLI behavior."""

from __future__ import annotations

from argparse import Namespace
from unittest.mock import patch

from cdc_generator.cli.column_templates import _handle_add


def test_handle_add_defaults_name_to_key() -> None:
    """--add defaults template name to key when --name is omitted."""
    args = Namespace(
        add="customer_id",
        name=None,
        col_type="text",
        value='meta("table")',
        value_source=None,
        description=None,
        not_null=None,
        sql_default=None,
        applies_to=None,
    )

    with patch(
        "cdc_generator.core.column_template_definitions.add_template_definition",
        return_value=True,
    ) as mock_add:
        result = _handle_add(args)

    assert result == 0
    kwargs = mock_add.call_args.kwargs
    assert kwargs["name"] == "customer_id"
