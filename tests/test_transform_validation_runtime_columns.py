"""Tests for runtime-aware transform validation.

Ensures transform validation allows references to columns produced by earlier
transforms in the same chain, while still rejecting true unknown references.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from cdc_generator.validators.template_validator import (
    TableSchema,
    validate_transforms_for_table,
)


def _write_bloblang(project_root: Path, path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    rel = path.relative_to(project_root).as_posix()
    return f"file://{rel}"


def test_allows_intermediate_computed_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later transform may read a column produced by an earlier transform."""
    monkeypatch.chdir(tmp_path)

    first_ref = _write_bloblang(
        tmp_path,
        tmp_path / "services" / "_bloblang" / "examples" / "first.blobl",
        'root._normalized_name = this.name.lowercase()\n',
    )
    second_ref = _write_bloblang(
        tmp_path,
        tmp_path / "services" / "_bloblang" / "examples" / "second.blobl",
        'root._name_len = this._normalized_name.length()\n',
    )

    schema = TableSchema(
        table_name="customers",
        schema_name="public",
        columns={"name": "text"},
    )

    with patch(
        "cdc_generator.validators.template_validator.get_source_table_schema",
        return_value=schema,
    ):
        is_valid = validate_transforms_for_table(
            "svc",
            "public.customers",
            [first_ref, second_ref],
        )

    assert is_valid is True


def test_rejects_unknown_runtime_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown this.* references still fail validation."""
    monkeypatch.chdir(tmp_path)

    ref = _write_bloblang(
        tmp_path,
        tmp_path / "services" / "_bloblang" / "examples" / "bad.blobl",
        'root._x = this.not_a_real_column\n',
    )

    schema = TableSchema(
        table_name="customers",
        schema_name="public",
        columns={"name": "text"},
    )

    with patch(
        "cdc_generator.validators.template_validator.get_source_table_schema",
        return_value=schema,
    ):
        is_valid = validate_transforms_for_table(
            "svc",
            "public.customers",
            [ref],
        )

    assert is_valid is False
