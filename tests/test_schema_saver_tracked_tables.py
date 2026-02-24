"""Tests for tracked table whitelist support in schema saver."""

from pathlib import Path
from unittest.mock import patch

import yaml

from cdc_generator.validators.manage_service.schema_saver import (
    add_tracked_tables,
    filter_tables_by_tracked,
    load_tracked_tables,
)


def test_add_tracked_tables_writes_schema_grouped_yaml(tmp_path: Path) -> None:
    """Tracked tables are stored as schema -> [tables] in tracked-tables.yaml."""
    service_write_dir = tmp_path / "services" / "_schemas" / "directory"

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.get_service_schema_write_dir",
        return_value=service_write_dir,
    ):
        ok = add_tracked_tables(
            "directory",
            ["public.users", "public.rooms", "logs.activity"],
        )

    assert ok is True
    tracked_file = service_write_dir / "tracked-tables.yaml"
    assert tracked_file.exists()

    payload = yaml.safe_load(tracked_file.read_text())
    assert payload == {
        "logs": ["activity"],
        "public": ["rooms", "users"],
    }


def test_add_tracked_tables_merges_with_existing_file(tmp_path: Path) -> None:
    """Adding tracked tables keeps existing entries and appends new ones."""
    service_write_dir = tmp_path / "services" / "_schemas" / "directory"
    service_write_dir.mkdir(parents=True)
    tracked_file = service_write_dir / "tracked-tables.yaml"
    tracked_file.write_text(
        "public:\n"
        "  - users\n"
    )

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.get_service_schema_write_dir",
        return_value=service_write_dir,
    ):
        ok = add_tracked_tables(
            "directory",
            ["public.rooms", "logs.activity"],
        )
        loaded = load_tracked_tables("directory")

    assert ok is True
    assert loaded == {
        "logs": ["activity"],
        "public": ["rooms", "users"],
    }


def test_filter_tables_by_tracked_no_file_returns_all(tmp_path: Path) -> None:
    """Without tracked file, save flow keeps all discovered tables."""
    service_write_dir = tmp_path / "services" / "_schemas" / "directory"
    tables = [
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "users"},
        {"TABLE_SCHEMA": "logs", "TABLE_NAME": "activity"},
    ]

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.get_service_schema_write_dir",
        return_value=service_write_dir,
    ):
        filtered = filter_tables_by_tracked("directory", tables)

    assert filtered == tables


def test_filter_tables_by_tracked_applies_whitelist(tmp_path: Path) -> None:
    """When tracked tables exist, only tracked schema.table refs are saved."""
    service_write_dir = tmp_path / "services" / "_schemas" / "directory"
    service_write_dir.mkdir(parents=True)
    (service_write_dir / "tracked-tables.yaml").write_text(
        "public:\n"
        "  - users\n"
        "logs:\n"
        "  - activity\n"
    )
    tables = [
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "users"},
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "rooms"},
        {"TABLE_SCHEMA": "logs", "TABLE_NAME": "activity"},
    ]

    with patch(
        "cdc_generator.validators.manage_service.schema_saver.get_service_schema_write_dir",
        return_value=service_write_dir,
    ):
        filtered = filter_tables_by_tracked("directory", tables)

    assert filtered == [
        {"TABLE_SCHEMA": "public", "TABLE_NAME": "users"},
        {"TABLE_SCHEMA": "logs", "TABLE_NAME": "activity"},
    ]
