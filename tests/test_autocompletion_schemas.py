"""Tests for schema autocompletion helpers."""

from pathlib import Path
from unittest.mock import patch

from cdc_generator.helpers.autocompletions.schemas import list_schemas_for_service


def test_list_schemas_for_service_reads_schema_directories(tmp_path: Path) -> None:
    (tmp_path / "source-groups.yaml").write_text(
        """
adopus:
  server_group_type: db-per-tenant
  sources:
    AVProd:
      prod:
        database: AdOpusAVProd
""".strip()
        + "\n",
        encoding="utf-8",
    )

    service_dir = tmp_path / "services" / "_schemas" / "directory"
    (service_dir / "public").mkdir(parents=True)
    (service_dir / "logs").mkdir(parents=True)
    (service_dir / "custom-tables").mkdir(parents=True)

    with patch(
        "cdc_generator.helpers.autocompletions.schemas.find_file_upward",
        return_value=tmp_path / "source-groups.yaml",
    ):
        schemas = list_schemas_for_service("directory")

    assert schemas == ["logs", "public"]
