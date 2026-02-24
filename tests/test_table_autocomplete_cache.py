"""Tests for table autocomplete cache reader."""

from pathlib import Path
from unittest.mock import patch

from cdc_generator.helpers.autocompletions.tables import (
    list_tables_for_service_autocomplete,
)
from cdc_generator.helpers.yaml_loader import load_yaml_file
from cdc_generator.validators.manage_server_group.autocomplete_definitions import (
    generate_service_autocomplete_definitions,
)


def test_list_tables_for_service_autocomplete_reads_cache_file(
    tmp_path: Path,
) -> None:
    """Reads schema.table suggestions from _definitions/{service}-autocompletes.yaml."""
    defs_dir = tmp_path / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True)
    (defs_dir / "directory-autocompletes.yaml").write_text(
        "public:\n"
        "  - users\n"
        "  - rooms\n"
        "logs:\n"
        "  - activity\n"
    )

    with patch(
        "cdc_generator.helpers.autocompletions.tables.get_project_root",
        return_value=tmp_path,
    ):
        result = list_tables_for_service_autocomplete("directory")

    assert result == ["logs.activity", "public.rooms", "public.users"]


def test_list_tables_for_service_autocomplete_falls_back_to_schema_files(
    tmp_path: Path,
) -> None:
    """Falls back to scanning service schema files when cache file is absent."""
    schemas_dir = tmp_path / "services" / "_schemas" / "directory" / "public"
    schemas_dir.mkdir(parents=True)
    (schemas_dir / "users.yaml").write_text("table: users\n")

    with patch(
        "cdc_generator.helpers.autocompletions.tables.get_project_root",
        return_value=tmp_path,
    ), patch(
        "cdc_generator.helpers.autocompletions.tables.find_service_schemas_dir_upward",
        return_value=tmp_path / "services" / "_schemas",
    ):
        result = list_tables_for_service_autocomplete("directory")

    assert result == ["public.users"]


def test_generate_service_autocomplete_definitions_replaces_regenerated_schemas(
    tmp_path: Path,
) -> None:
    """Replaces tables for regenerated schemas while preserving unrelated schemas."""
    defs_dir = tmp_path / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True)
    target = defs_dir / "directory-autocompletes.yaml"
    target.write_text(
        "dbo:\n"
        "  - old_table\n"
        "keep:\n"
        "  - keep_table\n"
    )

    server_group = {
        "type": "mssql",
        "servers": {"default": {"host": "x", "port": 1433, "user": "u", "password": "p"}},
    }
    scanned_databases = [
        {
            "name": "directory_db",
            "server": "default",
            "service": "directory",
            "environment": "default",
            "customer": "",
            "schemas": ["dbo"],
            "table_count": 1,
        },
    ]

    with patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._definitions_dir",
        return_value=defs_dir,
    ), patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._fetch_tables_by_schema",
        return_value={"dbo": ["new_table"]},
    ):
        result = generate_service_autocomplete_definitions(server_group, scanned_databases)

    assert result is True
    saved = load_yaml_file(target)
    assert saved == {
        "dbo": ["new_table"],
        "keep": ["keep_table"],
    }


def test_generate_service_autocomplete_definitions_removes_regenerated_schema_when_empty(
    tmp_path: Path,
) -> None:
    """Removes stale schema entry when regenerated schema ends up with no tables."""
    defs_dir = tmp_path / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True)
    target = defs_dir / "directory-autocompletes.yaml"
    target.write_text(
        "dbo:\n"
        "  - old_table\n"
        "keep:\n"
        "  - keep_table\n"
    )

    server_group = {
        "type": "mssql",
        "servers": {"default": {"host": "x", "port": 1433, "user": "u", "password": "p"}},
    }
    scanned_databases = [
        {
            "name": "directory_db",
            "server": "default",
            "service": "directory",
            "environment": "default",
            "customer": "",
            "schemas": ["dbo"],
            "table_count": 1,
        },
    ]

    with patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._definitions_dir",
        return_value=defs_dir,
    ), patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._fetch_tables_by_schema",
        return_value={"dbo": ["LogTable"]},
    ):
        result = generate_service_autocomplete_definitions(
            server_group,
            scanned_databases,
            table_exclude_patterns=["^Log"],
        )

    assert result is True
    saved = load_yaml_file(target)
    assert saved == {"keep": ["keep_table"]}


def test_generate_service_autocomplete_definitions_prunes_excluded_schema_from_existing(
    tmp_path: Path,
) -> None:
    """Removes stale schemas from existing cache when schema is now excluded."""
    defs_dir = tmp_path / "services" / "_schemas" / "_definitions"
    defs_dir.mkdir(parents=True)
    target = defs_dir / "directory-autocompletes.yaml"
    target.write_text(
        "logs:\n"
        "  - users_log\n"
        "public:\n"
        "  - users\n"
    )

    server_group = {
        "type": "mssql",
        "servers": {
            "default": {
                "host": "x",
                "port": 1433,
                "user": "u",
                "password": "p",
            },
        },
    }
    scanned_databases = [
        {
            "name": "directory_db",
            "server": "default",
            "service": "directory",
            "environment": "default",
            "customer": "",
            "schemas": ["public"],
            "table_count": 1,
        },
    ]

    with patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._definitions_dir",
        return_value=defs_dir,
    ), patch(
        "cdc_generator.validators.manage_server_group.autocomplete_definitions._fetch_tables_by_schema",
        return_value={"public": ["users"]},
    ):
        result = generate_service_autocomplete_definitions(
            server_group,
            scanned_databases,
            schema_exclude_patterns=["logs"],
        )

    assert result is True
    saved = load_yaml_file(target)
    assert saved == {"public": ["users"]}
