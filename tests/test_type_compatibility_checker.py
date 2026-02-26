"""Tests for mapper-driven source/sink type compatibility checks."""

from pathlib import Path

import pytest

from cdc_generator.validators.manage_service.sink_operations import (
    _available_type_map_pairs,
    _load_source_type_overrides,
    _load_type_compatibility_map,
    check_type_compatibility,
)


@pytest.fixture()
def mapped_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create isolated project with runtime type map and set cwd there."""
    definitions = tmp_path / "services" / "_schemas" / "_definitions"
    definitions.mkdir(parents=True)
    (tmp_path / "source-groups.yaml").write_text("adopus: {}\n", encoding="utf-8")

    (definitions / "map-mssql-pgsql.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "aliases:",
                "  source:",
                "    integer: int",
                "  sink:",
                "    int4: integer",
                "mappings:",
                "  int: integer",
                "  smallint: smallint",
                "  bigint: bigint",
                "  decimal: numeric",
                "  numeric: numeric",
                "  real: real",
                "  datetime2: timestamp without time zone",
                "  datetimeoffset: timestamp with time zone",
                "  date: date",
                "  uniqueidentifier: uuid",
                "compatibility:",
                "  \"*\":",
                "  - text",
                "  - character varying",
                "  int:",
                "  - integer",
                "  - bigint",
                "  - numeric",
                "  - real",
                "  - double precision",
                "  smallint:",
                "  - smallint",
                "  - integer",
                "  - bigint",
                "  - numeric",
                "  - real",
                "  - double precision",
                "  bigint:",
                "  - bigint",
                "  - numeric",
                "  - real",
                "  - double precision",
                "  decimal:",
                "  - numeric",
                "  - real",
                "  - double precision",
                "  numeric:",
                "  - numeric",
                "  - real",
                "  - double precision",
                "  real:",
                "  - real",
                "  - double precision",
                "  datetime2:",
                "  - timestamp without time zone",
                "  - timestamp with time zone",
                "  date:",
                "  - date",
                "  - timestamp without time zone",
                "  - timestamp with time zone",
                "  uniqueidentifier:",
                "  - uuid",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    _load_type_compatibility_map.cache_clear()
    _available_type_map_pairs.cache_clear()
    _load_source_type_overrides.cache_clear()
    return tmp_path


@pytest.mark.parametrize(
    ("source_type", "sink_type", "expected"),
    [
        ("int", "integer", True),
        ("int", "bigint", True),
        ("int", "numeric", True),
        ("int", "smallint", False),
        ("smallint", "integer", True),
        ("bigint", "numeric", True),
        ("real", "double precision", True),
        ("double precision", "real", False),
        ("datetime2", "timestamp without time zone", True),
        ("datetime2", "timestamp with time zone", True),
        ("date", "timestamp with time zone", True),
        ("uniqueidentifier", "uuid", True),
    ],
)
def test_checker_reflects_realistic_cast_compatibility(
    source_type: str,
    sink_type: str,
    expected: bool,
    mapped_project: Path,
) -> None:
    """Core checker should support widening-safe casts and reject narrowing."""
    assert check_type_compatibility(source_type, sink_type) is expected


def test_checker_uses_project_compatibility_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compatibility overrides in map-mssql-pgsql.yaml should be honored."""
    services_dir = tmp_path / "services" / "_schemas" / "_definitions"
    services_dir.mkdir(parents=True)
    (tmp_path / "source-groups.yaml").write_text("adopus: {}\n", encoding="utf-8")

    (services_dir / "map-mssql-pgsql.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "mappings:",
                "  int: integer",
                "compatibility:",
                "  int:",
                "  - smallint",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    _load_type_compatibility_map.cache_clear()
    _available_type_map_pairs.cache_clear()
    _load_source_type_overrides.cache_clear()

    assert check_type_compatibility("int", "smallint") is True


def test_checker_ignores_invalid_unrelated_map_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid reverse map file must not break forward compatibility checks."""
    definitions = tmp_path / "services" / "_schemas" / "_definitions"
    definitions.mkdir(parents=True)
    (tmp_path / "source-groups.yaml").write_text("adopus: {}\n", encoding="utf-8")

    (definitions / "map-mssql-pgsql.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "mappings:",
                "  int: integer",
                "compatibility:",
                "  int:",
                "  - integer",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (definitions / "map-pgsql-mssql.yaml").write_text("", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    _load_type_compatibility_map.cache_clear()
    _available_type_map_pairs.cache_clear()
    _load_source_type_overrides.cache_clear()

    assert check_type_compatibility("int", "integer") is True


def test_checker_requires_runtime_map_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing map file should raise friendly error for execution paths."""
    (tmp_path / "services" / "_schemas" / "_definitions").mkdir(parents=True)
    (tmp_path / "source-groups.yaml").write_text("adopus: {}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    _load_type_compatibility_map.cache_clear()
    _available_type_map_pairs.cache_clear()
    _load_source_type_overrides.cache_clear()

    with pytest.raises(ValueError, match="No type compatibility maps found"):
        check_type_compatibility("uniqueidentifier", "uuid")


def test_checker_applies_source_type_override_with_column_context(
    mapped_project: Path,
) -> None:
    """Source-only override should narrow effective type for one source column."""
    definitions = mapped_project / "services" / "_schemas" / "_definitions"
    (definitions / "source-adopus-type-overrides.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "  version: 1",
                "overrides:",
                "  dbo.adganglinjer:",
                "    adgangkode: smallint",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _load_source_type_overrides.cache_clear()

    assert check_type_compatibility("int", "smallint") is False
    assert check_type_compatibility(
        "int",
        "smallint",
        source_table="dbo.AdgangLinjer",
        source_column="Adgangkode",
    ) is True
    assert check_type_compatibility(
        "int",
        "smallint",
        source_table="dbo.AdgangLinjer",
        source_column="OtherColumn",
    ) is False


def test_checker_fails_on_invalid_source_override_file(
    mapped_project: Path,
) -> None:
    """Invalid source-type override structure should fail with friendly error."""
    definitions = mapped_project / "services" / "_schemas" / "_definitions"
    (definitions / "source-adopus-type-overrides.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "  version: 1",
                "bad_key: true",
                "overrides: {}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _load_source_type_overrides.cache_clear()

    with pytest.raises(ValueError, match="source type overrides"):
        check_type_compatibility(
            "int",
            "integer",
            source_table="dbo.Actor",
            source_column="ActNo",
        )
