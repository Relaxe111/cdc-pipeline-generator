"""Tests for PostgreSQL type autocompletion fallbacks."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_generator.helpers.autocompletions import types


@pytest.fixture
def temp_definitions_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Workspace root with services/_schemas/_definitions path."""
    root = tmp_path
    (root / "services" / "_schemas" / "_definitions").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(types, "get_project_root", lambda: root)
    return root


def test_list_pg_column_types_reads_pgsql_yaml_when_present(
    temp_definitions_root: Path,
) -> None:
    (temp_definitions_root / "services" / "_schemas" / "_definitions" / "pgsql.yaml").write_text(
        "categories:\n"
        "  Numeric:\n"
        "    types:\n"
        "      - integer\n"
        "      - numeric\n"
        "  Text:\n"
        "    types:\n"
        "      - text\n",
        encoding="utf-8",
    )

    result = types.list_pg_column_types()

    assert result == ["integer", "numeric", "text"]


def test_list_pg_column_types_falls_back_to_mapping_file(
    temp_definitions_root: Path,
) -> None:
    (temp_definitions_root / "services" / "_schemas" / "_definitions" / "map-mssql-pgsql.yaml").write_text(
        "mssql_to_pgsql:\n"
        "  int: integer\n"
        "  uniqueidentifier: uuid\n"
        "pgsql_to_mssql:\n"
        "  text: nvarchar\n"
        "  timestamp with time zone: datetimeoffset\n",
        encoding="utf-8",
    )

    result = types.list_pg_column_types()

    assert "integer" in result
    assert "uuid" in result
    assert "text" in result
    assert "timestamp with time zone" in result


def test_list_pg_column_types_falls_back_to_builtins_when_files_missing(
    temp_definitions_root: Path,
) -> None:
    result = types.list_pg_column_types()

    assert "text" in result
    assert "uuid" in result
    assert "numeric" in result
