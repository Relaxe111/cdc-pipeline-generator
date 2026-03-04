"""Table definition loading helpers for migration generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from cdc_generator.helpers.service_schema_paths import get_service_schema_read_dirs
from cdc_generator.helpers.yaml_loader import load_yaml_file


def load_table_definitions(
    service_name: str,
    project_root: Path,
) -> dict[str, dict[str, Any]]:
    """Load table definitions from services/_schemas/{service}/{schema}/{table}.yaml."""
    schema_dirs = get_service_schema_read_dirs(service_name, project_root)

    tables: dict[str, dict[str, Any]] = {}
    for schema_dir in schema_dirs:
        if not schema_dir.exists():
            continue
        for sub_dir in sorted(schema_dir.iterdir()):
            if not sub_dir.is_dir():
                continue
            schema_name = sub_dir.name
            for yaml_file in sorted(sub_dir.glob("*.yaml")):
                raw_dict = cast(dict[str, Any], load_yaml_file(yaml_file))
                table_name = raw_dict.get("table")
                if not isinstance(table_name, str):
                    table_name = yaml_file.stem
                key = f"{schema_name}.{table_name}"
                if key not in tables:
                    tables[key] = raw_dict
    return tables
