"""Tests for source-type overrides applied in sink autocompletion helpers."""

from pathlib import Path

import pytest

from cdc_generator.helpers.autocompletions.sinks import (
    list_compatible_target_columns_for_source_column,
)
from cdc_generator.validators.manage_service.sink_operations import (
    _available_type_map_pairs,
    _load_source_type_overrides,
    _load_type_compatibility_map,
)


def test_map_column_completion_applies_source_type_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Source override should make narrowed source column compatible in completion."""
    project_root = tmp_path
    definitions = project_root / "services" / "_schemas" / "_definitions"
    definitions.mkdir(parents=True)

    (project_root / "source-groups.yaml").write_text("adopus: {}\n", encoding="utf-8")

    (definitions / "map-mssql-pgsql.yaml").write_text(
        "\n".join(
            [
                "metadata:",
                "mappings:",
                "  int: integer",
                "  smallint: smallint",
                "compatibility:",
                "  int:",
                "  - integer",
                "  smallint:",
                "  - smallint",
                "  - integer",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

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

    source_table_file = (
        project_root
        / "services"
        / "_schemas"
        / "myservice"
        / "dbo"
        / "AdgangLinjer.yaml"
    )
    source_table_file.parent.mkdir(parents=True)
    source_table_file.write_text(
        "\n".join(
            [
                "columns:",
                "  - name: Adgangkode",
                "    type: int",
                "    nullable: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    target_table_file = (
        project_root
        / "services"
        / "_schemas"
        / "proxy"
        / "public"
        / "adganglinjer.yaml"
    )
    target_table_file.parent.mkdir(parents=True)
    target_table_file.write_text(
        "\n".join(
            [
                "columns:",
                "  - name: adgangkode_small",
                "    type: smallint",
                "    nullable: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(project_root)
    _load_type_compatibility_map.cache_clear()
    _available_type_map_pairs.cache_clear()
    _load_source_type_overrides.cache_clear()

    suggestions = list_compatible_target_columns_for_source_column(
        "myservice",
        "sink_asma.proxy",
        "dbo.AdgangLinjer",
        "public.adganglinjer",
        "Adgangkode",
    )

    assert suggestions == ["adgangkode_small"]
