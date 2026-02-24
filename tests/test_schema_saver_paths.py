"""Tests for schema saver output paths."""

from pathlib import Path
from unittest.mock import patch

import yaml

from cdc_generator.validators.manage_service.schema_saver import _save_tables_to_yaml


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
